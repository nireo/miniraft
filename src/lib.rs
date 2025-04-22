use anyhow::anyhow;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tarpc::{client, context, tokio_serde::formats::Json};

use tokio::time::{Duration, Instant, interval};

type NodeId = i64;

#[derive(Debug, Clone)]
struct LogEntry {
    command: Vec<u8>,
    term: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
    Dead,
}

#[derive(Debug)]
struct VolatileState {
    state: NodeState,
    election_reset_event: Instant,
}

impl VolatileState {
    fn is_dead(&self) -> bool {
        self.state == NodeState::Dead
    }
}

#[derive(Debug)]
struct PersistentState {
    current_term: usize,
    voted_for: NodeId,
    log: Vec<LogEntry>,
}

#[derive(Debug)]
struct NodeInnerState {
    vol_state: VolatileState,
    persistent_state: PersistentState,
}

#[derive(Debug)]
struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    inner: tokio::sync::Mutex<NodeInnerState>,
    peer_clients: HashMap<NodeId, RaftClient>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RequestVoteArgs {
    term: usize,
    last_log_idx: usize,
    last_log_term: usize,
    candidate_id: NodeId,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RequestVoteReply {
    term: usize,
    vote_granted: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AppendEntriesArgs {
    term: usize,
    leader_id: NodeId,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AppendEntriesReply {
    term: usize,
    success: bool,
}

#[tarpc::service]
trait Raft {
    async fn request_vote(req: RequestVoteArgs) -> RequestVoteReply;
    async fn append_entries(req: AppendEntriesArgs) -> AppendEntriesReply;
}

impl Raft for Arc<Node> {
    async fn request_vote(
        self,
        context: tarpc::context::Context,
        req: RequestVoteArgs,
    ) -> RequestVoteReply {
        let mut st = self.inner.lock().await;

        if st.vol_state.is_dead() {
            return RequestVoteReply {
                term: st.persistent_state.current_term,
                vote_granted: false,
            };
        }

        if req.term > st.persistent_state.current_term {
            drop(st);
            Arc::clone(&self).become_follower(req.term).await;
            st = self.inner.lock().await;
        }

        if st.persistent_state.current_term == req.term
            && (st.persistent_state.voted_for == -1
                || st.persistent_state.voted_for == req.candidate_id)
        {
            st.vol_state.election_reset_event = Instant::now();
            RequestVoteReply {
                term: st.persistent_state.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteReply {
                term: st.persistent_state.current_term,
                vote_granted: false,
            }
        }
    }

    async fn append_entries(
        self,
        _context: tarpc::context::Context,
        req: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        let mut inner = self.inner.lock().await;
        if inner.vol_state.is_dead() {
            return AppendEntriesReply {
                term: 0,
                success: true,
            };
        }

        if req.term > inner.persistent_state.current_term {
            drop(inner);
            Arc::clone(&self).become_follower(req.term).await;
            inner = self.inner.lock().await;
        }

        let mut success = true;
        if req.term == inner.persistent_state.current_term {
            if inner.vol_state.state != NodeState::Follower {
                drop(inner);
                Arc::clone(&self).become_follower(req.term).await;
                inner = self.inner.lock().await;
            }

            inner.vol_state.election_reset_event = Instant::now();
            success = true
        }

        AppendEntriesReply {
            term: inner.persistent_state.current_term,
            success,
        }
    }
}

impl Node {
    async fn stop(&self) {
        let mut inner = self.inner.lock().await;
        inner.vol_state.state = NodeState::Dead;
    }

    fn election_timeout(&self) -> Duration {
        Duration::from_millis(150)
    }

    async fn run_election_timer(self: Arc<Self>) {
        let timeout_duration = self.election_timeout();
        let mut interval = interval(Duration::from_millis(10));
        let start_term;
        {
            let inner = self.inner.lock().await;
            start_term = inner.persistent_state.current_term;
        }

        loop {
            interval.tick().await;

            let inner = self.inner.lock().await;
            if inner.vol_state.state != NodeState::Candidate
                && inner.vol_state.state != NodeState::Follower
            {
                return;
            }

            if start_term != inner.persistent_state.current_term {
                return;
            }

            if inner.vol_state.election_reset_event.elapsed() >= timeout_duration {
                drop(inner);
                self.start_election().await;
                return;
            }
        }
    }

    async fn become_follower(self: Arc<Self>, term: usize) {
        let mut inner = self.inner.lock().await;
        inner.vol_state.state = NodeState::Follower;
        inner.persistent_state.current_term = term;
        inner.persistent_state.voted_for = -1;
        inner.vol_state.election_reset_event = Instant::now();

        tokio::spawn(Arc::clone(&self).run_election_timer());
    }

    async fn start_election(self: Arc<Self>) {
        let mut inner = self.inner.lock().await;
        inner.persistent_state.current_term += 1;

        let saved_term = inner.persistent_state.current_term;
        inner.vol_state.election_reset_event = Instant::now();
        inner.persistent_state.voted_for = self.id;

        let votes_received = Arc::new(AtomicUsize::new(1));

        let peer_ids = self.peers.clone();
        let majority = (peer_ids.len() / 2) + 1;

        for peer in peer_ids {
            let node_clone = Arc::clone(&self);
            let votes_clone = Arc::clone(&votes_received);

            tokio::spawn(async move {
                let args = RequestVoteArgs {
                    term: saved_term,
                    candidate_id: self.id,
                    last_log_term: 0,
                    last_log_idx: 0,
                };

                if let Some(client) = node_clone.peer_clients.get(&peer) {
                    let client = client.clone();

                    let rpc_result: Result<RequestVoteReply, anyhow::Error> = async {
                        let mut context = context::current();
                        context.deadline = std::time::Instant::now() + Duration::from_millis(100); // Timeout
                        client.request_vote(context, args).await.map_err(|e| {
                            anyhow!("RPC call using client for peer {} failed: {}", peer, e)
                        })
                    }
                    .await;

                    match rpc_result {
                        Ok(reply) => {
                            let mut inner_guard = node_clone.inner.lock().await;

                            if inner_guard.vol_state.state != NodeState::Candidate {
                                return;
                            }
                            if reply.term > saved_term {
                                drop(inner_guard);
                                Arc::clone(&node_clone).become_follower(reply.term).await;
                                return;
                            }

                            if reply.term == saved_term && reply.vote_granted {
                                let current_votes = votes_clone.fetch_add(1, Ordering::SeqCst) + 1;
                                if current_votes >= majority {
                                    if inner_guard.vol_state.state == NodeState::Candidate {
                                        drop(inner_guard);
                                        Arc::clone(&node_clone).start_leader().await;
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {}
                    }
                }
            });
        }

        drop(inner);
        tokio::spawn(Arc::clone(&self).run_election_timer());
    }

    async fn leader_send_heartbeats(self: Arc<Node>) {
        let inner = self.inner.lock().await;
        if inner.vol_state.state != NodeState::Leader {
            return;
        }

        let saved_term = inner.persistent_state.current_term;
        drop(inner);

        let peer_ids = self.peers.clone();
        for peer in peer_ids {
            let node_clone = Arc::clone(&self);
            tokio::spawn(async move {
                let args = AppendEntriesArgs {
                    term: saved_term,
                    leader_id: node_clone.id,
                };

                if let Some(client) = node_clone.peer_clients.get(&peer) {
                    let client = client.clone();

                    let rpc_result: Result<AppendEntriesReply, anyhow::Error> = async {
                        let mut context = context::current();
                        context.deadline = std::time::Instant::now() + Duration::from_millis(100); // Timeout
                        client.append_entries(context, args).await.map_err(|e| {
                            anyhow!("RPC call using client for peer {} failed: {}", peer, e)
                        })
                    }
                    .await;

                    match rpc_result {
                        Ok(reply) => {
                            let inner_guard = node_clone.inner.lock().await;
                            if reply.term > saved_term {
                                drop(inner_guard);
                                Arc::clone(&node_clone).become_follower(reply.term).await;
                                return;
                            }
                        }
                        Err(_e) => {}
                    }
                }
            });
        }
    }

    async fn connect_and_create_client(
        peer_id: NodeId,
        addrs: &HashMap<NodeId, String>,
    ) -> Result<RaftClient, anyhow::Error> {
        let addr_o = addrs.get(&peer_id);
        if let Some(addr) = addr_o {
            let peer_addr: std::net::SocketAddr = addr.parse()?;
            let mut transport = tarpc::serde_transport::tcp::connect(peer_addr, Json::default);
            transport.config_mut().max_frame_length(usize::MAX);
            let client = RaftClient::new(client::Config::default(), transport.await?).spawn();
            Ok(client)
        } else {
            Err(anyhow!("didn't find address for peer"))
        }
    }

    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        peer_clients: HashMap<NodeId, RaftClient>, // Added parameter
        initial_term: usize,                       // Example: Allow setting initial state
        initial_state: NodeState,
    ) -> Self {
        let initial_inner_state = NodeInnerState {
            vol_state: VolatileState {
                state: initial_state,
                election_reset_event: Instant::now(),
            },
            persistent_state: PersistentState {
                current_term: initial_term,
                voted_for: -1, // Or Option<NodeId>::None
                log: vec![],
            },
        };

        Node {
            id,
            peers,
            inner: tokio::sync::Mutex::new(initial_inner_state),
            peer_clients, // Initialize directly
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tarpc::transport;
    use tokio::sync::Mutex;

    use super::*;

    struct TestCluster {
        nodes: Vec<Node>,
        connected: Vec<bool>,
    }

    impl TestCluster {
        async fn new(n: usize) -> Result<Self> {
            if n == 0 {
                return Err(anyhow!("Cluster size must be at least 1"));
            }

            let all_ids: Vec<NodeId> = (0..n as i64).collect();
            let mut clients = HashMap::new();
            let mut server_transports = HashMap::new(); // Store server side of transport

            println!("Harness: Creating transports and clients...");
            for &node_id in &all_ids {
                let (client_transport, server_transport) = transport::channel::unbounded();

                let client_config = client::Config::default();
                let client = RaftClient::new(client_config, client_transport).spawn();

                clients.insert(node_id, client);
                server_transports.insert(node_id, server_transport);
            }
            println!("Harness: Transports and clients created.");

            let mut nodes = HashMap::new();
            let mut server_handles = HashMap::new();
            let connectivity_map = Arc::new(Mutex::new(HashMap::new()));

            println!("Harness: Creating nodes and servers...");
            for &node_id in &all_ids {
                let peer_ids: Vec<NodeId> = all_ids
                    .iter()
                    .filter(|&&id| id != node_id)
                    .cloned()
                    .collect();

                let mut peer_clients = HashMap::new();
                for &peer_id in &peer_ids {
                    if let Some(client) = clients.get(&peer_id) {
                        peer_clients.insert(peer_id, client.clone());
                    } else {
                        // Should not happen if Phase 1 was correct
                        return Err(anyhow!(
                            "Failed to find client for peer {} when creating node {}",
                            peer_id,
                            node_id
                        ));
                    }
                }

                let server_transport = server_transports
                    .remove(&node_id)
                    .ok_or_else(|| anyhow!("Server transport not found for node {}", node_id))?;

                let node = Arc::new(Node::new(
                    node_id,
                    peer_ids.clone(),    // Pass peer IDs
                    peer_clients,        // Pass the map of clients to peers
                    0,                   // Initial term
                    NodeState::Follower, // Initial state
                ));

                // Create and spawn the server for this node
                let server = BaseChannel::with_defaults(server_transport);
                let node_clone_for_server = Arc::clone(&node);

                // Spawn the server task
                let server_handle = tokio::spawn(async move {
                    // The server::BaseChannel needs a function that returns a Context.
                    // We can use the default context.
                    server.execute(node_clone_for_server.serve()).await;
                    // Log when a server task ends (optional)
                    // println!("Server task for Node {} finished.", node_id);
                });

                nodes.insert(node_id, node);
                server_handles.insert(node_id, server_handle);

                // Initialize connectivity state (all connected initially)
                let mut conn_guard = connectivity_map.lock().await;
                for &peer_id in &peer_ids {
                    conn_guard.insert((node_id, peer_id), true);
                }
                println!("Harness: Node {} created and server spawned.", node_id);
            }
            println!("Harness: Nodes and servers created.");

            println!("Harness: Initialization complete.");
            Ok(Self {
                nodes,
                clients, // Keep clients map for potential future use/inspection
                server_handles,
                all_ids,
                n,
                connectivity: connectivity_map,
            })
        }
    }
}
