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
