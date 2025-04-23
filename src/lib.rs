use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::SocketAddr;
use std::os::unix::prelude::FileExt;
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

struct PageCache {
    file: std::fs::File,
    page_cache: HashMap<u64, [u8; PAGESIZE as usize]>,
    page_cache_size: usize,
    buffer: Vec<u8>,
    buffer_write_at: Option<u64>,
    buffer_write_at_offset: u64,
}

impl PageCache {
    fn new(file: std::fs::File, page_cache_size: usize) -> PageCache {
        let mut page_cache = std::collections::HashMap::new();
        // Allocate the space up front! The page cache should never
        // allocate after this. This is a big deal.
        page_cache.reserve(page_cache_size + 1);

        PageCache {
            file,
            page_cache_size,
            page_cache,

            buffer: vec![],
            buffer_write_at: None,
            buffer_write_at_offset: 0,
        }
    }

    fn insert_or_replace_in_cache(&mut self, offset: u64, page: [u8; PAGESIZE as usize]) {
        if self.page_cache_size == 0 {
            return;
        }

        // If it's already in the cache, just overwrite it.
        if let Some(existing) = self.page_cache.get(&offset) {
            if page != *existing {
                self.page_cache.insert(offset, page);
            }
            return;
        }

        // TODO: Come up with a better cache policy.
        if self.page_cache.len() == self.page_cache_size {
            self.page_cache.clear();
        }

        // Otherwise insert and evict something if we're out of space.
        self.page_cache.insert(offset, page);
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.page_cache.len()
    }

    fn read(&mut self, offset: u64, buf_into: &mut [u8; PAGESIZE as usize]) {
        // For now, must to read() while a `write()` is ongoing. See
        // the comment in `self.write()`.
        assert_eq!(self.buffer_write_at, None);

        assert_eq!(buf_into.len(), PAGESIZE as usize);
        if let Some(page) = self.page_cache.get(&offset) {
            buf_into.copy_from_slice(page);
            return;
        }

        self.file.read_exact_at(&mut buf_into[0..], offset).unwrap();
        self.insert_or_replace_in_cache(offset, *buf_into);
    }

    fn write(&mut self, offset: u64, page: [u8; PAGESIZE as usize]) {
        if self.buffer_write_at.is_none() {
            self.buffer_write_at = Some(offset);
            self.buffer_write_at_offset = offset;
        } else {
            // Make sure we're always doing sequential writes in
            // between self.flush() call.
            assert_eq!(self.buffer_write_at_offset, offset - PAGESIZE);
            self.buffer_write_at_offset = offset;
        }

        assert_ne!(self.buffer_write_at, None);

        // TODO: It is potentially unsafe if we are doing reads
        // inbetween writes. That isn't possible in the current
        // code. The case to worry about would be `self.write()`
        // before `self.sync()` where the pagecache gets filled up and
        // this particular page isn't in the pagecache and hasn't yet
        // been written to disk. The only correct thing to do would be
        // for `self.read()` to also check `self.buffer` before
        // reading from disk.
        self.buffer.extend(page);

        self.insert_or_replace_in_cache(offset, page);
    }

    fn sync(&mut self) {
        self.file
            .write_all_at(&self.buffer, self.buffer_write_at.unwrap())
            .unwrap();
        self.buffer.clear();
        self.buffer_write_at = None;
        self.buffer_write_at_offset = 0;
        self.file.sync_all().unwrap();
    }
}

struct PageCacheIO<'this> {
    offset: u64,
    pagecache: &'this mut PageCache,
}

impl<'this> Read for &mut PageCacheIO<'this> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert_eq!(buf.len(), PAGESIZE as usize);
        let fixed_buf = <&mut [u8; PAGESIZE as usize]>::try_from(buf).unwrap();
        self.pagecache.read(self.offset, fixed_buf);
        self.offset += PAGESIZE;
        Ok(PAGESIZE as usize)
    }
}

impl<'this> Write for PageCacheIO<'this> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        assert_eq!(buf.len(), PAGESIZE as usize);
        let fixed_buf = <&[u8; PAGESIZE as usize]>::try_from(buf).unwrap();
        self.pagecache.write(self.offset, *fixed_buf);
        self.offset += PAGESIZE;
        Ok(PAGESIZE as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.pagecache.sync();
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LogEntry {
    command: Vec<u8>,
    index: u64,
    term: u64,
    client_serial_id: u128,
    client_id: u128,
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.command == other.command && self.term == other.term
    }
}

const PAGESIZE: u64 = 512;

impl LogEntry {
    fn command_first_page(command_length: usize) -> usize {
        let page_minus_metadata = (PAGESIZE - 61) as usize;
        if command_length <= page_minus_metadata {
            command_length
        } else {
            page_minus_metadata
        }
    }

    fn store_metadata(&self, buffer: &mut [u8; PAGESIZE as usize]) -> usize {
        *buffer = [0; PAGESIZE as usize];
        let command_length = self.command.len();

        buffer[0] = 1; // Entry start marker.
        buffer[5..13].copy_from_slice(&self.term.to_le_bytes());
        buffer[13..21].copy_from_slice(&self.index.to_le_bytes());
        buffer[21..37].copy_from_slice(&self.client_serial_id.to_le_bytes());
        buffer[37..53].copy_from_slice(&self.client_id.to_le_bytes());
        buffer[53..61].copy_from_slice(&command_length.to_le_bytes());

        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&buffer[5..61]);
        checksum.update(&self.command);
        buffer[1..5].copy_from_slice(&checksum.finalize().to_le_bytes());

        let command_first_page = LogEntry::command_first_page(command_length);
        buffer[61..61 + command_first_page].copy_from_slice(&self.command[0..command_first_page]);
        command_length - command_first_page
    }

    fn store_overflow(&self, buffer: &mut [u8; PAGESIZE as usize], offset: usize) -> usize {
        let to_write = self.command.len() - offset;
        let filled = if to_write > PAGESIZE as usize - 1 {
            // -1 for the overflow marker.
            PAGESIZE as usize - 1
        } else {
            to_write
        };
        buffer[0] = 0; // Overflow marker.
        buffer[1..1 + filled].copy_from_slice(&self.command[offset..offset + filled]);
        filled
    }

    fn encode(&self, buffer: &mut [u8; PAGESIZE as usize], mut writer: impl std::io::Write) -> u64 {
        let to_write = self.store_metadata(buffer);
        writer.write_all(buffer).unwrap();
        let mut pages = 1;

        let mut written = self.command.len() - to_write;

        while written < self.command.len() {
            let filled = self.store_overflow(buffer, written);
            writer.write_all(buffer).unwrap();
            written += filled;
            pages += 1;
        }

        pages
    }

    fn recover_metadata(page: &[u8; PAGESIZE as usize]) -> (LogEntry, u32, usize) {
        assert_eq!(page[0], 1); // Start of entry marker.
        let term = u64::from_le_bytes(page[5..13].try_into().unwrap());
        let index = u64::from_le_bytes(page[13..21].try_into().unwrap());
        let client_serial_id = u128::from_le_bytes(page[21..37].try_into().unwrap());
        let client_id = u128::from_le_bytes(page[37..53].try_into().unwrap());
        let command_length = u64::from_le_bytes(page[53..61].try_into().unwrap()) as usize;
        let stored_checksum = u32::from_le_bytes(page[1..5].try_into().unwrap());

        // recover_metadata() will only decode the first page's worth of
        // the command. Call recover_overflow() to decode any
        // additional pages.
        let command_first_page = LogEntry::command_first_page(command_length);
        let mut command = vec![0; command_length];
        command[0..command_first_page].copy_from_slice(&page[61..61 + command_first_page]);

        (
            LogEntry {
                index,
                term,
                command,
                client_serial_id,
                client_id,
            },
            stored_checksum,
            command_first_page,
        )
    }

    fn recover_overflow(
        page: &[u8; PAGESIZE as usize],
        command: &mut [u8],
        command_read: usize,
    ) -> usize {
        let to_read = command.len() - command_read;

        // Entry start marker is false for overflow page.
        assert_eq!(page[0], 0);

        let fill = if to_read > PAGESIZE as usize - 1 {
            // -1 for the entry start marker.
            PAGESIZE as usize - 1
        } else {
            to_read
        };
        command[command_read..command_read + fill].copy_from_slice(&page[1..1 + fill]);
        fill
    }

    fn decode(mut reader: impl std::io::Read) -> LogEntry {
        let mut page = [0; PAGESIZE as usize];
        // Since entries are always encoded into complete PAGESIZE
        // bytes, for network or for disk, it should always be
        // reasonable to block on an entire PAGESIZE of bytes, for
        // network or for disk.
        reader.read_exact(&mut page).unwrap();

        let (mut entry, stored_checksum, command_read) = LogEntry::recover_metadata(&page);
        let mut actual_checksum = crc32fast::Hasher::new();
        actual_checksum.update(&page[5..61]);

        let mut read = command_read;
        while read < entry.command.len() {
            reader.read_exact(&mut page).unwrap();
            let filled = LogEntry::recover_overflow(&page, &mut entry.command, read);
            read += filled;
        }

        actual_checksum.update(&entry.command);
        assert_eq!(stored_checksum, actual_checksum.finalize());
        entry
    }

    fn decode_from_pagecache(pagecache: &mut PageCache, offset: u64) -> (LogEntry, u64) {
        let mut reader = PageCacheIO { offset, pagecache };
        let entry = LogEntry::decode(&mut reader);
        let offset = reader.offset;

        (entry, offset)
    }
}

struct DurableState {
    last_log_term: u64,
    next_log_index: u64,
    next_log_offset: u64,
    pagecache: PageCache,
    current_term: u64,
    voted_for: u128,
}

impl DurableState {
    fn new(data_directory: &std::path::Path, id: u128, page_cache_size: usize) -> DurableState {
        let mut filename = data_directory.to_path_buf();
        filename.push(format!("server_{}.data", id));
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)
            .expect("could not open data file");

        DurableState {
            last_log_term: 0,
            next_log_index: 0,
            next_log_offset: PAGESIZE,
            pagecache: PageCache::new(file, page_cache_size),

            current_term: 0,
            voted_for: 0,
        }
    }

    fn restore(&mut self) {
        // If there's nothing to restore, calling append with the
        // required 0th empty log entry will be sufficient to get
        // state into the right place.
        if let Ok(m) = self.pagecache.file.metadata() {
            if m.len() == 0 {
                self.append(&mut [LogEntry {
                    index: 0,
                    term: 0,
                    command: vec![],
                    client_serial_id: 0,
                    client_id: 0,
                }]);
                return;
            }
        }

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        self.pagecache.read(0, &mut metadata);

        // Magic number check.
        assert_eq!(metadata[0..4], 0xFABEF15E_u32.to_le_bytes());

        // Version number check.
        assert_eq!(metadata[4..8], 1_u32.to_le_bytes());

        self.current_term = u64::from_le_bytes(metadata[8..16].try_into().unwrap());
        self.voted_for = u128::from_le_bytes(metadata[16..32].try_into().unwrap());

        let checksum = u32::from_le_bytes(metadata[40..44].try_into().unwrap());
        if checksum != crc32fast::hash(&metadata[0..40]) {
            panic!("Bad checksum for data file.");
        }

        let log_length = u64::from_le_bytes(metadata[32..40].try_into().unwrap()) as usize;

        let mut scanned = 0;
        while scanned < log_length {
            self.next_log_index += 1;

            let (e, new_offset) =
                LogEntry::decode_from_pagecache(&mut self.pagecache, self.next_log_offset);
            self.last_log_term = e.term;
            self.next_log_offset = new_offset;
            scanned += 1;
        }
    }

    fn append_from_index(&mut self, entries: &mut [LogEntry], from_index: u64) {
        let mut buffer: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        self.next_log_offset = self.offset_from_index(from_index);
        self.next_log_index = from_index;

        let mut writer = PageCacheIO {
            offset: self.next_log_index,
            pagecache: &mut self.pagecache,
        };

        if !entries.is_empty() {
            for entry in entries.iter_mut() {
                entry.index = self.next_log_index;
                self.next_log_index += 1;

                assert!(self.next_log_offset >= PAGESIZE);
                let pages = entry.encode(&mut buffer, &mut writer);
                println!("wrote {:?} at {}", entry.command, entry.index);
                self.next_log_offset += pages * PAGESIZE;

                self.last_log_term = entry.term;
            }
        }
    }

    fn update(&mut self, term: u64, voted_for: u128) {
        self.voted_for = voted_for;
        self.current_term = term;

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        metadata[0..4].copy_from_slice(&0xFABEF15E_u32.to_le_bytes());
        metadata[4..8].copy_from_slice(&1_u32.to_le_bytes());

        metadata[8..16].copy_from_slice(&term.to_le_bytes());

        metadata[16..32].copy_from_slice(&voted_for.to_le_bytes());

        let log_length = self.next_log_index;
        metadata[32..40].copy_from_slice(&log_length.to_le_bytes());

        let checksum = crc32fast::hash(&metadata[0..40]);
        metadata[40..44].copy_from_slice(&checksum.to_le_bytes());

        self.pagecache.write(0, metadata);
        self.pagecache.sync();
    }

    fn offset_from_index(&mut self, index: u64) -> u64 {
        if index == self.next_log_index {
            return self.next_log_offset;
        }

        assert!(index < self.next_log_index);
        let mut page: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];

        // Rather than linear search backwards, we store the index in
        // the page itself and then do a binary search on disk.
        let mut l = PAGESIZE;
        let mut r = self.next_log_offset - PAGESIZE;
        while l <= r {
            let mut m = l + (r - l) / 2;
            // Round up to the nearest page.
            m += m % PAGESIZE;
            assert_eq!(m % PAGESIZE, 0);

            // Look for a start of entry page.
            self.pagecache.read(m, &mut page);
            while page[0] != 1 {
                m -= PAGESIZE;
                self.pagecache.read(m, &mut page);
            }

            // TODO: Bad idea to hardcode the offset.
            let current_index = u64::from_le_bytes(page[13..21].try_into().unwrap());
            if current_index == index {
                return m;
            }

            if current_index < index {
                // Read until the next entry, set m to the next entry.
                page[0] = 0;
                m += PAGESIZE;
                self.pagecache.read(m, &mut page);
                while page[0] != 1 {
                    m += PAGESIZE;
                    self.pagecache.read(m, &mut page);
                }

                l = m;
            } else {
                r = m - PAGESIZE;
            }
        }

        unreachable!(
            "Could not find index {index} with log length: {}.",
            self.next_log_index
        );
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum Condition {
    Leader,
    Follower,
    Candidate,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct VolatileState {
    condition: Condition,

    commit_index: u64,
    last_applied: u64,

    // Timeouts
    election_frequency: Duration,
    election_timeout: Instant,
    rand: Random,

    // Leader-only state.
    next_index: Vec<u64>,
    match_index: Vec<u64>,

    // Candidate-only state.
    votes: usize,
}

impl VolatileState {
    fn new(cluster_size: usize, election_frequency: Duration, rand: Random) -> VolatileState {
        let jitter = election_frequency.as_secs_f64() / 3.0;
        VolatileState {
            condition: Condition::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; cluster_size],
            match_index: vec![0; cluster_size],
            votes: 0,

            election_frequency,
            election_timeout: Instant::now() + Duration::from_secs_f64(jitter),
            rand,
        }
    }

    fn reset(&mut self) {
        let count = self.next_index.len();
        for i in 0..count {
            self.next_index[i] = 0;
            self.match_index[i] = 0;
        }
        self.votes = 0;
    }
}

#[derive(Clone)]
struct Logger {
    server_id: u128,
    debug: bool,
}

impl Logger {
    fn log<S: AsRef<str> + std::fmt::Display>(
        &self,
        term: u64,
        log_len: u64,
        condition: Condition,
        msg: S,
    ) {
        if !self.debug {
            return;
        }

        println!(
            "[S: {: <3} T: {: <3} L: {: <3} C: {}] {}",
            self.server_id,
            term,
            log_len,
            match condition {
                Condition::Leader => "L",
                Condition::Candidate => "C",
                Condition::Follower => "F",
            },
            msg
        );
    }
}

struct State {
    logger: Logger,
    durable: DurableState,
    volatile: VolatileState,
    stopped: bool,
}

impl State {
    fn log<S: AsRef<str> + std::fmt::Display>(&self, msg: S) {
        self.logger.log(
            self.durable.current_term,
            self.durable.next_log_index,
            self.volatile.condition,
            msg,
        );
    }

    fn next_request_id(&mut self) -> u64 {
        self.volatile.rand.generate_u64()
    }

    fn reset_election_timeout(&mut self) {
        let random_percent = self.volatile.rand.generate_percent();
        let positive = self.volatile.rand.generate_bool();
        let jitter = random_percent as f64 * (self.volatile.election_frequency.as_secs_f64() / 2.0);

        let mut new_timeout = self.volatile.election_frequency;
        // Duration apparently isn't allowed to be negative.
        if positive {
            new_timeout += Duration::from_secs_f64(jitter);
        } else {
            new_timeout -= Duration::from_secs_f64(jitter);
        }

        self.volatile.election_timeout = Instant::now() + new_timeout;

        self.log(format!(
            "Resetting election timeout: {}ms.",
            new_timeout.as_millis()
        ));
    }

    fn transition(&mut self, condition: Condition, term_increase: u64, voted_for: u128) {
        assert_ne!(self.volatile.condition, condition);
        self.log(format!("Became {}.", condition));
        self.volatile.condition = condition;
        // Reset vote.
        self.durable
            .update(self.durable.current_term + term_increase, voted_for);
    }
}

#[derive(Copy, Clone)]
struct ServerConfig {
    id: u128,
    address: SocketAddr,
}

struct Config {
    server_index: usize,
    server_id: u128,
    cluster: Vec<ServerConfig>,
    election_freq: Duration,
    page_cache_size: usize,
}

trait StateMachine {
    fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>>;
}

struct Server<S: StateMachine> {
    config: Config,
    sm: S,
    state: Mutex<State>,
    client_id: u128,
    apply_sender: mpsc::Sender<Vec<u8>>,
}

impl<S: StateMachine> Drop for Server<S> {
    fn drop(&mut self) {
        self.stop();
    }
}
