use futures::future::FutureExt;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tarpc::client::{self, Config};
use tarpc::context;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel, Handler};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::time::{Instant, Interval};

// Replaces const DebugCM = 1 - use standard logging levels
macro_rules! dlog {
    ($id:expr, $($arg:tt)*) => {
        debug!(target: "raft::cm", "[{}] {}", $id, format_args!($($arg)*));
    };
}

// Type alias for peer IDs
type PeerId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub command: Vec<u8>, // Using Vec<u8> for generic command data
    pub term: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmState {
    Follower,
    Candidate,
    Leader,
    Dead,
}

impl fmt::Display for CmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CmState::Follower => write!(f, "Follower"),
            CmState::Candidate => write!(f, "Candidate"),
            CmState::Leader => write!(f, "Leader"),
            CmState::Dead => write!(f, "Dead"),
        }
    }
}

// Define the RPC service trait using tarpc
#[tarpc::service]
pub trait RaftService {
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
    async fn append_entries(args: AppendEntriesArgs) -> AppendEntriesReply;
}

struct CmInner {
    current_term: usize,
    voted_for: Option<PeerId>,
    log: Vec<LogEntry>,

    state: CmState,
    election_reset_event: Instant,
}

// ConsensusModule (CM) implements a single node of Raft consensus.
// The main struct now holds the Arc<Mutex<>> to the inner state
// and other non-shared components like ID and peer clients.
#[derive(Clone)]
pub struct ConsensusModule {
    id: PeerId,
    peer_ids: Vec<PeerId>,
    // Clients to communicate with peers. The String key is the peer's address.
    // In a real setup, you might resolve IDs to addresses differently.
    // Using Option<> because clients might not be ready immediately.
    peer_clients: Arc<Mutex<HashMap<PeerId, RaftServiceClient>>>,
    inner: Arc<Mutex<CmInner>>,
    // Used to signal the CM to fully stop background tasks
    stop_notify: Arc<Notify>,
}

impl ConsensusModule {
    /// Creates a new CM with the given ID, list of peer IDs and
    /// channel to receive peer clients.
    pub async fn new(
        id: PeerId,
        peer_ids: Vec<PeerId>,
        // Channel to receive clients once connected
        mut client_rx: mpsc::Receiver<(PeerId, RaftServiceClient)>,
        ready_signal: oneshot::Receiver<()>, // Signal that peers are ready
    ) -> Self {
        let inner = Arc::new(Mutex::new(CmInner {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            state: CmState::Follower,
            election_reset_event: Instant::now(), // Initial reset
        }));

        let peer_clients = Arc::new(Mutex::new(HashMap::new()));
        let stop_notify = Arc::new(Notify::new());

        let cm = ConsensusModule {
            id,
            peer_ids,
            peer_clients: peer_clients.clone(),
            inner,
            stop_notify,
        };

        // Task to receive peer clients as they connect
        let peer_clients_clone = peer_clients.clone();
        tokio::spawn(async move {
            while let Some((peer_id, client)) = client_rx.recv().await {
                let mut clients = peer_clients_clone.lock().await;
                clients.insert(peer_id, client);
                info!(target: "raft::cm", "[{}] Established client connection to peer {}", id, peer_id);
            }
            info!(target: "raft::cm", "[{}] Client receiver channel closed.", id);
        });

        // Background task starter
        let cm_clone = cm.clone();
        tokio::spawn(async move {
            // The CM is quiescent until ready is signaled; then, it starts a countdown
            // for leader election.
            if ready_signal.await.is_ok() {
                info!(target: "raft::cm", "[{}] Ready signal received, starting election timer.", cm_clone.id);
                let mut inner = cm_clone.inner.lock().await;
                inner.election_reset_event = Instant::now(); // Reset timer upon readiness
                drop(inner); // Release lock before spawning timer task
                cm_clone.run_election_timer();
            } else {
                warn!(target: "raft::cm", "[{}] Ready signal channel closed before signaling.", cm_clone.id);
            }
        });

        cm
    }

    /// Report reports the state of this CM.
    pub async fn report(&self) -> (PeerId, usize, bool) {
        let inner = self.inner.lock().await;
        (self.id, inner.current_term, inner.state == CmState::Leader)
    }

    /// Stop stops this CM, cleaning up its state. This method returns quickly, but
    /// it may take a bit of time (up to ~election timeout) for all goroutines to exit.
    pub async fn stop(&self) {
        let mut inner = self.inner.lock().await;
        if inner.state == CmState::Dead {
            return; // Already stopped
        }
        inner.state = CmState::Dead;
        dlog!(self.id, "becomes Dead");
        // Signal background tasks to stop
        self.stop_notify.notify_waiters();

        // Clear clients? Or let them linger? Depends on desired shutdown behavior.
        let mut clients = self.peer_clients.lock().await;
        clients.clear();
    }

    fn is_dead(&self, inner: &CmInner) -> bool {
        inner.state == CmState::Dead
    }

    /// Generates a pseudo-random election timeout duration.
    fn election_timeout(&self) -> Duration {
        // If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
        // generating a hard-coded number very often. This will create collisions
        // between different servers and force more re-elections.
        if env::var("RAFT_FORCE_MORE_REELECTION").is_ok() && rand::random_range(0..3) == 0 {
            Duration::from_millis(150)
        } else {
            Duration::from_millis(150 + rand::random_range(0..150))
        }
    }

    /// Runs the election timer. Should be launched whenever
    /// we want to start a timer towards becoming a candidate in a new election.
    ///
    /// This function spawns a Tokio task internally and is designed to work for a
    /// single (one-shot) election timer, as it exits whenever the CM state changes
    /// from follower/candidate or the term changes.
    fn run_election_timer(&self) {
        let timeout_duration = self.election_timeout();
        let cm_clone = self.clone(); // Clone Arc for the new task

        tokio::spawn(async move {
            let term_started = cm_clone.inner.lock().await.current_term;
            dlog!(
                cm_clone.id,
                "election timer started ({:?}), term={}",
                timeout_duration,
                term_started
            );

            // Use interval for periodic checks and select! to also wait for stop signal
            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            let stop_signal = cm_clone.stop_notify.notified().fuse(); // Fuse for select!
            futures::pin_mut!(stop_signal); // Pin the fused future

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut inner = cm_clone.inner.lock().await;

                        if inner.state != CmState::Candidate && inner.state != CmState::Follower {
                            dlog!(cm_clone.id, "in election timer state={}, bailing out", inner.state);
                             return; // Exit task
                        }

                        if term_started != inner.current_term {
                             dlog!(cm_clone.id, "in election timer term changed from {} to {}, bailing out", term_started, inner.current_term);
                            return; // Exit task
                        }

                        // Start an election if we haven't heard from a leader or haven't voted for
                        // someone for the duration of the timeout.
                         if inner.election_reset_event.elapsed() >= timeout_duration {
                            dlog!(cm_clone.id, "election timeout reached, starting election");
                            cm_clone.start_election(&mut inner).await; // Pass mutable ref to locked inner
                            // start_election transitions state, so the next loop iteration will exit.
                             return; // Exit timer task after starting election
                         }
                    }
                    _ = &mut stop_signal => {
                        dlog!(cm_clone.id, "election timer stopping due to stop signal.");
                        return; // Exit task
                    }
                }
            }
        });
    }

    /// Starts a new election with this CM as a candidate.
    /// Expects `inner` to be locked.
    async fn start_election(&self, inner: &mut tokio::sync::MutexGuard<'_, CmInner>) {
        inner.state = CmState::Candidate;
        inner.current_term += 1;
        let saved_current_term = inner.current_term;
        inner.election_reset_event = Instant::now();
        inner.voted_for = Some(self.id); // Vote for self
        // Get log details for RequestVote RPC (simplified here)
        let last_log_index = inner.log.len().saturating_sub(1); // Use saturating_sub for empty log
        let last_log_term = if last_log_index > 0 {
            // Check if log has entries before accessing
            inner.log.get(last_log_index).map_or(0, |entry| entry.term)
        } else {
            0
        };

        dlog!(
            self.id,
            "becomes Candidate (currentTerm={}); log len={}",
            saved_current_term,
            inner.log.len()
        );

        let mut votes_received = 1; // Count self vote

        // Must release lock before making async calls within the loop
        drop(inner);

        // --- Send RequestVote RPCs ---
        let required_votes = (self.peer_ids.len() + 1) / 2 + 1; // Need majority: (n/2) + 1

        // Use a channel to collect vote results asynchronously
        let (vote_tx, mut vote_rx) = mpsc::channel::<RequestVoteReply>(self.peer_ids.len());

        for &peer_id in &self.peer_ids {
            let cm_clone = self.clone();
            let vote_tx_clone = vote_tx.clone();
            tokio::spawn(async move {
                let args = RequestVoteArgs {
                    term: saved_current_term,
                    candidate_id: cm_clone.id,
                    last_log_index,
                    last_log_term,
                };

                dlog!(
                    cm_clone.id,
                    "sending RequestVote to {}: {:?}",
                    peer_id,
                    args
                );

                let clients = cm_clone.peer_clients.lock().await;
                if let Some(client) = clients.get(&peer_id) {
                    // Clone the client to avoid holding the lock during the RPC call
                    let client = client.clone();
                    // Release the lock *before* the await
                    drop(clients);

                    match client.request_vote(context::current(), args).await {
                        Ok(reply) => {
                            dlog!(
                                cm_clone.id,
                                "received RequestVoteReply from {}: {:?}",
                                peer_id,
                                reply
                            );
                            // Send reply back to the main election logic
                            if vote_tx_clone.send(reply).await.is_err() {
                                warn!(target: "raft::cm", "[{}] Failed to send vote reply to election processor", cm_clone.id);
                            }
                        }
                        Err(e) => {
                            warn!(target: "raft::cm", "[{}] RPC error sending RequestVote to {}: {}", cm_clone.id, peer_id, e);
                            // Optionally send a 'failed' reply or handle timeout differently
                        }
                    }
                } else {
                    warn!(target: "raft::cm", "[{}] No client found for peer {}", cm_clone.id, peer_id);
                }
            });
        }
        // Drop the original sender so the receiver knows when all tasks are done (or errored)
        drop(vote_tx);

        // --- Process votes ---
        loop {
            // Re-acquire lock briefly in each iteration if needed, or just at the end
            // But need to check state changes concurrently.
            tokio::select! {
                 Some(reply) = vote_rx.recv() => {
                     let mut inner = self.inner.lock().await;
                     if self.is_dead(&inner) { return; } // Stop processing if dead

                     if inner.state != CmState::Candidate {
                         dlog!(self.id, "while waiting for vote reply, state changed to {}, stopping election vote count", inner.state);
                         return; // No longer candidate
                     }

                     if reply.term > saved_current_term {
                         dlog!(self.id, "term out of date in RequestVoteReply ({} > {}), becoming Follower", reply.term, saved_current_term);
                         self.become_follower(&mut inner, reply.term);
                         return; // Stop election process
                     } else if reply.term == saved_current_term {
                         if reply.vote_granted {
                             votes_received += 1;
                             dlog!(self.id, "vote granted, votes received = {}", votes_received);
                             if votes_received >= required_votes {
                                 // Won the election!
                                 dlog!(self.id, "wins election with {} votes", votes_received);
                                 self.start_leader(&mut inner); // Pass mutable ref
                                 return; // Election over
                             }
                         }
                     } else {
                         // Reply term is lower, ignore the vote status but log it
                         dlog!(self.id, "received stale vote reply from term {}", reply.term);
                     }
                     // Drop lock after processing one reply
                     drop(inner);
                }
                // Need a way to timeout the election itself if it doesn't conclude
                 _ = tokio::time::sleep(self.election_timeout()) => { // Use election timeout as election duration limit?
                      let inner = self.inner.lock().await;
                     if inner.state == CmState::Candidate {
                         dlog!(self.id, "election timed out without reaching majority");
                         // The existing timer mechanism will eventually trigger another election
                         // if no leader emerges or heartbeats are received. No need to explicitly
                         // become follower here unless dictated by specific Raft variant rules.
                     }
                     // Exit this processing loop regardless, let run_election_timer handle next steps
                     return;
                 }
                 _ = self.stop_notify.notified() => {
                    dlog!(self.id, "election stopping due to stop signal.");
                    return; // Exit task
                 }
                 else => {
                     // Channel closed, all votes processed or failed
                     dlog!(self.id, "vote receiver channel closed.");
                     let inner = self.inner.lock().await;
                     if inner.state == CmState::Candidate {
                          dlog!(self.id, "election finished without reaching majority ({} votes)", votes_received);
                         // Remain candidate, the election timer will restart
                     }
                     return; // Election attempt finished
                }
            }
        }

        // Note: The Go code runs another election timer *concurrently* with waiting
        // for votes. Here, if the election fails (split vote, timeout), the *existing*
        // timer mechanism (which should have been restarted/continued by become_follower
        // or will eventually expire if still candidate) will trigger a *new* election later.
        // Explicitly restarting the timer *here* might lead to faster retries in split votes.
        // If needed:
        // let mut inner = self.inner.lock().await;
        // if !self.is_dead(&inner) && inner.state == CmState::Candidate {
        //    self.run_election_timer(); // Restart timer if still candidate
        // }
    }

    /// Makes cm a follower and resets its state.
    /// Expects `inner` to be locked.
    fn become_follower(&self, inner: &mut tokio::sync::MutexGuard<'_, CmInner>, term: usize) {
        if inner.term < term {
            // Only update term if the new term is higher
            inner.voted_for = None; // Clear vote when term changes
        }
        inner.state = CmState::Follower;
        inner.current_term = term;
        inner.election_reset_event = Instant::now();
        dlog!(
            self.id,
            "becomes Follower with term={}; log len={}",
            term,
            inner.log.len()
        );

        // Spawn a *new* election timer for the follower state
        self.run_election_timer();
    }

    /// Switches cm into a leader state and begins process of heartbeats.
    /// Expects `inner` to be locked.
    fn start_leader(&self, inner: &mut tokio::sync::MutexGuard<'_, CmInner>) {
        inner.state = CmState::Leader;
        // TODO: Initialize leader volatile state (next_index, match_index) here
        dlog!(
            self.id,
            "becomes Leader; term={}, log len={}",
            inner.current_term,
            inner.log.len()
        );

        // Drop the lock before spawning the background task
        let cm_clone = self.clone();
        drop(inner);

        tokio::spawn(async move {
            // Use interval_at for immediate first tick, then periodic
            let mut ticker = tokio::time::interval_at(
                Instant::now() + Duration::from_millis(50), // Start after a short delay
                Duration::from_millis(50),
            );
            let stop_signal = cm_clone.stop_notify.notified().fuse();
            futures::pin_mut!(stop_signal);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Check state *before* sending heartbeats
                         let inner = cm_clone.inner.lock().await;
                         if inner.state != CmState::Leader {
                             dlog!(cm_clone.id, "no longer leader, stopping heartbeats");
                             drop(inner);
                             return; // Exit heartbeat task
                         }
                         let saved_current_term = inner.current_term;
                         drop(inner); // Release lock before async calls

                         cm_clone.leader_send_heartbeats(saved_current_term).await;
                     }
                    _ = &mut stop_signal => {
                        dlog!(cm_clone.id, "heartbeat loop stopping due to stop signal.");
                        return; // Exit task
                    }
                }
            }
        });
    }

    /// Sends a round of heartbeats to all peers, collects their
    /// replies and adjusts cm's state.
    /// Takes `saved_current_term` to avoid re-locking initially.
    async fn leader_send_heartbeats(&self, saved_current_term: usize) {
        // Need to re-lock briefly to get peer clients map, but don't hold it during RPCs
        let clients_guard = self.peer_clients.lock().await;
        let peer_client_map: Vec<(PeerId, RaftServiceClient)> = clients_guard
            .iter()
            .map(|(&id, client)| (id, client.clone())) // Clone clients to use after dropping lock
            .collect();
        drop(clients_guard); // Release lock

        for (peer_id, client) in peer_client_map {
            // TODO: Real implementation needs per-peer log details (PrevLogIndex, PrevLogTerm, Entries)
            let args = AppendEntriesArgs {
                term: saved_current_term,
                leader_id: self.id,
                prev_log_index: 0, // Placeholder
                prev_log_term: 0,  // Placeholder
                entries: vec![],   // Heartbeat has empty entries
                leader_commit: 0,  // Placeholder
            };

            let cm_clone = self.clone(); // Clone Arc for the task
            tokio::spawn(async move {
                dlog!(
                    cm_clone.id,
                    "sending AppendEntries (heartbeat) to {}: ni=0, args={:?}",
                    peer_id,
                    args
                ); // ni=0 placeholder
                match client.append_entries(context::current(), args).await {
                    Ok(reply) => {
                        cm_clone
                            .process_append_entries_reply(peer_id, saved_current_term, reply)
                            .await;
                    }
                    Err(e) => {
                        warn!(target: "raft::cm", "[{}] RPC error sending AppendEntries to {}: {}", cm_clone.id, peer_id, e);
                        // Handle error - maybe mark peer as unreachable?
                    }
                }
            });
        }
    }

    /// Processes a reply from an AppendEntries RPC (used for heartbeats or log replication).
    async fn process_append_entries_reply(
        &self,
        peer_id: PeerId,
        leader_term: usize,
        reply: AppendEntriesReply,
    ) {
        let mut inner = self.inner.lock().await;
        if self.is_dead(&inner) {
            return;
        }

        dlog!(
            self.id,
            "received AppendEntriesReply from {}: {:?}",
            peer_id,
            reply
        );

        if reply.term > leader_term {
            dlog!(
                self.id,
                "term out of date in heartbeat reply from {} ({} > {}), becoming Follower",
                peer_id,
                reply.term,
                leader_term
            );
            self.become_follower(&mut inner, reply.term);
            // No return needed here, become_follower starts the necessary timer
        } else if inner.state == CmState::Leader && reply.term == leader_term {
            // If leader and term matches, process success/failure for log matching
            if reply.success {
                // TODO: Update next_index and match_index for this peer
                // TODO: Check if commit_index can be advanced
                dlog!(self.id, "AppendEntries successful for peer {}", peer_id);
            } else {
                // TODO: Decrement next_index for this peer and retry
                dlog!(
                    self.id,
                    "AppendEntries failed for peer {}, will retry",
                    peer_id
                );
            }
        }
    }

    // --- RPC Handler Methods ---

    /// Handles RequestVote RPC.
    pub async fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut inner = self.inner.lock().await;
        if self.is_dead(&inner) {
            // Maybe return error or specific reply? Raft paper isn't explicit.
            // Returning default seems least harmful.
            return RequestVoteReply {
                term: inner.current_term,
                vote_granted: false,
            };
        }

        dlog!(
            self.id,
            "RequestVote: {:?} [currentTerm={}, votedFor={:?}]",
            args,
            inner.current_term,
            inner.voted_for
        );

        let mut vote_granted = false;
        if args.term > inner.current_term {
            dlog!(self.id, "... term out of date in RequestVote");
            // Dropping the lock before calling become_follower which might re-acquire it
            let new_term = args.term;
            drop(inner); // Drop lock before calling another method that might lock
            let mut new_inner = self.inner.lock().await;
            self.become_follower(&mut new_inner, new_term); // Re-acquire lock inside or here
            // Reassign inner after potentially changing state
            inner = new_inner;
            // No vote grant logic here yet, need to re-evaluate below based on updated state
        }

        // Check vote granting conditions *after* potential state change
        if inner.current_term == args.term
            && (inner.voted_for.is_none() || inner.voted_for == Some(args.candidate_id))
        {
            // TODO: Add Raft log matching check: "if the voter’s log is at least as up-to-date as candidate’s log"
            // Simplified: grant if term matches and haven't voted or voted for same candidate
            vote_granted = true;
            inner.voted_for = Some(args.candidate_id);
            inner.election_reset_event = Instant::now(); // Reset timer if vote is granted
        }

        let reply = RequestVoteReply {
            term: inner.current_term, // Use potentially updated term
            vote_granted,
        };
        dlog!(self.id, "... RequestVote reply: {:?}", reply);
        reply
    }

    /// Handles AppendEntries RPC.
    pub async fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut inner = self.inner.lock().await;
        if self.is_dead(&inner) {
            return AppendEntriesReply {
                term: inner.current_term,
                success: false,
            };
        }
        dlog!(self.id, "AppendEntries: {:?}", args); // Simplified logging for brevity

        let mut success = false;
        if args.term > inner.current_term {
            dlog!(self.id, "... term out of date in AppendEntries");
            let new_term = args.term;
            drop(inner); // Drop lock before potentially state-changing call
            let mut new_inner = self.inner.lock().await;
            self.become_follower(&mut new_inner, new_term);
            // Reassign inner after potential state change
            inner = new_inner;
            // Do not set success = true yet, check term again below
        }

        if args.term == inner.current_term {
            // If Candidate receives AppendEntries from new leader, convert to Follower
            if inner.state == CmState::Candidate {
                dlog!(
                    self.id,
                    "... received AppendEntries from leader while Candidate, becoming Follower"
                );
                // No need to drop lock here as become_follower takes &mut
                self.become_follower(&mut inner, args.term);
            }

            // Reset election timer if Follower and term matches
            if inner.state == CmState::Follower {
                inner.election_reset_event = Instant::now();
                dlog!(
                    self.id,
                    "... reset election timer due to AppendEntries from leader {}",
                    args.leader_id
                );

                // TODO: Implement log consistency check (Rule 2)
                // Check if log contains entry at PrevLogIndex matching PrevLogTerm
                // Simplified: assume success for heartbeat
                success = true;

                // TODO: Handle Entries (Rule 3, 4)
                // Delete conflicting entries, append new entries

                // TODO: Update commitIndex (Rule 5)
            }
        }

        let reply = AppendEntriesReply {
            term: inner.current_term, // Use potentially updated term
            success,
        };
        dlog!(self.id, "AppendEntries reply: {:?}", reply);
        reply
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: usize,
    pub candidate_id: PeerId,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: usize,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: usize,
    pub leader_id: PeerId,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub entries: Vec<LogEntry>, // Can be empty for heartbeat
    pub leader_commit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: usize,
    pub success: bool, // False if log inconsistency or term mismatch
}
