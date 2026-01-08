use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};
use serde::{Serialize, Deserialize};

pub struct RaftNode {
    id: String,
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<Log>>,
    state_machine: Arc<Box<dyn StateMachine>>,
    network: Arc<Box<dyn Network>>,
    election_timer: Arc<RwLock<ElectionTimer>>,
    heartbeat_timer: Arc<RwLock<HeartbeatTimer>>,
    metrics: Arc<RwLock<RaftMetrics>>,
}

pub struct RaftState {
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    leader_id: Option<String>,
    commit_index: u64,
    last_applied: u64,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
}

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Log {
    entries: Vec<LogEntry>,
    metadata: LogMetadata,
}

pub struct LogEntry {
    term: u64,
    index: u64,
    command: Command,
}

pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    ConfigChange { nodes: Vec<String> },
    NoOp,
}

pub struct LogMetadata {
    last_index: u64,
    last_term: u64,
    snapshot_index: u64,
    snapshot_term: u64,
}

pub trait StateMachine: Send + Sync {
    fn apply(&self, command: &Command) -> Result<ApplyResult, StateMachineError>;
    fn snapshot(&self) -> Result<Snapshot, StateMachineError>;
    fn restore(&self, snapshot: Snapshot) -> Result<(), StateMachineError>;
}

pub enum ApplyResult {
    Success(Vec<u8>),
    Failure(String),
}

pub struct Snapshot {
    data: Vec<u8>,
    last_included_index: u64,
    last_included_term: u64,
}

pub trait Network: Send + Sync {
    fn send_request_vote(&self, node_id: &str, request: RequestVoteRequest) -> Result<RequestVoteResponse, NetworkError>;
    fn send_append_entries(&self, node_id: &str, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, NetworkError>;
    fn send_install_snapshot(&self, node_id: &str, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, NetworkError>;
    fn broadcast_request_vote(&self, request: RequestVoteRequest) -> Vec<Result<RequestVoteResponse, NetworkError>>;
    fn broadcast_append_entries(&self, request: AppendEntriesRequest) -> Vec<Result<AppendEntriesResponse, NetworkError>>;
    fn get_nodes(&self) -> Vec<String>;
}

pub struct ElectionTimer {
    timeout: Duration,
    last_reset: Instant,
}

pub struct HeartbeatTimer {
    interval: Duration,
    last_sent: Instant,
}

pub struct RaftMetrics {
    election_count: u64,
    leader_count: u64,
    log_append_latency_ms: f64,
    replication_lag_ms: f64,
    uncommitted_entries: usize,
}

pub struct RequestVoteRequest {
    term: u64,
    candidate_id: String,
    last_log_index: u64,
    last_log_term: u64,
}

pub struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
    reason: Option<String>,
}

pub struct AppendEntriesRequest {
    term: u64,
    leader_id: String,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

pub struct AppendEntriesResponse {
    term: u64,
    success: bool,
    match_index: u64,
    next_index: u64,
}

pub struct InstallSnapshotRequest {
    term: u64,
    leader_id: String,
    last_included_index: u64,
    last_included_term: u64,
    data: Vec<u8>,
}

pub struct InstallSnapshotResponse {
    term: u64,
    success: bool,
}

pub enum RaftError {
    NotLeader(Option<String>),
    LogError(String),
    StateMachineError(String),
    NetworkError(String),
    InternalError(String),
}

pub enum StateMachineError {
    ApplyError(String),
    SnapshotError(String),
    RestoreError(String),
}

pub enum NetworkError {
    SendError(String),
    Timeout,
    NodeUnreachable(String),
}

pub enum ProposeResult {
    Accepted(u64),
    Rejected(String),
}

impl RaftNode {
    pub fn new(
        id: &str,
        state_machine: Box<dyn StateMachine>,
        network: Box<dyn Network>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RaftState {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }));

        let log = Arc::new(RwLock::new(Log {
            entries: Vec::new(),
            metadata: LogMetadata {
                last_index: 0,
                last_term: 0,
                snapshot_index: 0,
                snapshot_term: 0,
            },
        }));

        let election_timer = Arc::new(RwLock::new(ElectionTimer {
            timeout: Duration::from_millis(1500),
            last_reset: Instant::now(),
        }));

        let heartbeat_timer = Arc::new(RwLock::new(HeartbeatTimer {
            interval: Duration::from_millis(150),
            last_sent: Instant::now(),
        }));

        let metrics = Arc::new(RwLock::new(RaftMetrics {
            election_count: 0,
            leader_count: 0,
            log_append_latency_ms: 0.0,
            replication_lag_ms: 0.0,
            uncommitted_entries: 0,
        }));

        // Initialize next_index and match_index for all nodes
        let mut state_write = state.write().unwrap();
        for node_id in network.get_nodes() {
            if node_id != id {
                state_write.next_index.insert(node_id.clone(), 1);
                state_write.match_index.insert(node_id.clone(), 0);
            }
        }

        Self {
            id: id.to_string(),
            state,
            log,
            state_machine: Arc::new(state_machine),
            network: Arc::new(network),
            election_timer,
            heartbeat_timer,
            metrics,
        }
    }

    pub fn start(&self) {
        let node = self.clone();
        thread::spawn(move || node.run());
    }

    pub fn run(&self) {
        loop {
            match self.get_role() {
                Role::Follower => self.run_follower(),
                Role::Candidate => self.run_candidate(),
                Role::Leader => self.run_leader(),
            }
        }
    }

    pub fn get_role(&self) -> Role {
        let state = self.state.read().unwrap();
        state.role.clone()
    }

    pub fn propose(&self, command: Command) -> Result<ProposeResult, RaftError> {
        let state = self.state.read().unwrap();
        if state.role != Role::Leader {
            return Err(RaftError::NotLeader(state.leader_id.clone()));
        }

        let log_entry = LogEntry {
            term: state.current_term,
            index: state.commit_index + 1,
            command,
        };

        let mut log = self.log.write().unwrap();
        log.entries.push(log_entry);
        log.metadata.last_index += 1;
        log.metadata.last_term = state.current_term;

        Ok(ProposeResult::Accepted(log.metadata.last_index))
    }

    fn run_follower(&self) {
        let election_timeout = self.election_timer.read().unwrap().timeout;
        thread::sleep(election_timeout);

        let state = self.state.read().unwrap();
        if state.role == Role::Follower {
            self.convert_to_candidate();
        }
    }

    fn run_candidate(&self) {
        let mut state = self.state.write().unwrap();
        state.role = Role::Candidate;
        state.current_term += 1;
        state.voted_for = Some(self.id.clone());
        drop(state);

        // Increment election count
        let mut metrics = self.metrics.write().unwrap();
        metrics.election_count += 1;
        drop(metrics);

        let log = self.log.read().unwrap();
        let request = RequestVoteRequest {
            term: self.state.read().unwrap().current_term,
            candidate_id: self.id.clone(),
            last_log_index: log.metadata.last_index,
            last_log_term: log.metadata.last_term,
        };

        let responses = self.network.broadcast_request_vote(request);
        let votes_granted = responses.into_iter().filter(|r| r.as_ref().map(|resp| resp.vote_granted).unwrap_or(false)).count();

        // Calculate majority
        let total_nodes = self.network.get_nodes().len() + 1; // Include self
        let majority = (total_nodes / 2) + 1;

        if votes_granted >= majority {
            self.convert_to_leader();
        } else {
            self.convert_to_follower();
        }
    }

    fn run_leader(&self) {
        let state = self.state.read().unwrap();
        let log = self.log.read().unwrap();

        let request = AppendEntriesRequest {
            term: state.current_term,
            leader_id: self.id.clone(),
            prev_log_index: log.metadata.last_index,
            prev_log_term: log.metadata.last_term,
            entries: Vec::new(),
            leader_commit: state.commit_index,
        };

        let responses = self.network.broadcast_append_entries(request);
        for (node_id, response) in self.network.get_nodes().iter().zip(responses.into_iter()) {
            if let Ok(resp) = response {
                if resp.success {
                    let mut state_write = self.state.write().unwrap();
                    if let Some(next_index) = state_write.next_index.get_mut(node_id) {
                        *next_index = resp.next_index;
                    }
                    if let Some(match_index) = state_write.match_index.get_mut(node_id) {
                        *match_index = resp.match_index;
                    }
                }
            }
        }

        // Update commit index
        self.update_commit_index();

        thread::sleep(self.heartbeat_timer.read().unwrap().interval);
    }

    fn update_commit_index(&self) {
        let state = self.state.read().unwrap();
        let mut match_indices: Vec<u64> = state.match_index.values().cloned().collect();
        match_indices.push(state.commit_index); // Include self
        match_indices.sort();

        let majority = (match_indices.len() / 2) + 1;
        if majority <= match_indices.len() {
            let new_commit_index = match_indices[majority - 1];
            let mut state_write = self.state.write().unwrap();
            if new_commit_index > state.commit_index {
                state_write.commit_index = new_commit_index;
            }
        }
    }

    fn convert_to_candidate(&self) {
        let mut state = self.state.write().unwrap();
        state.role = Role::Candidate;
        state.current_term += 1;
        state.voted_for = Some(self.id.clone());
    }

    fn convert_to_leader(&self) {
        let mut state = self.state.write().unwrap();
        state.role = Role::Leader;
        state.leader_id = Some(self.id.clone());

        // Increment leader count
        let mut metrics = self.metrics.write().unwrap();
        metrics.leader_count += 1;
        drop(metrics);

        let log = self.log.read().unwrap();
        for node_id in self.network.get_nodes() {
            if node_id != self.id {
                state.next_index.insert(node_id.clone(), log.metadata.last_index + 1);
                state.match_index.insert(node_id.clone(), 0);
            }
        }
    }

    fn convert_to_follower(&self) {
        let mut state = self.state.write().unwrap();
        state.role = Role::Follower;
        state.leader_id = None;
    }

    pub fn handle_request_vote(&self, request: RequestVoteRequest) -> RequestVoteResponse {
        let mut state = self.state.write().unwrap();

        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = Role::Follower;
            state.voted_for = None;
            state.leader_id = None;
        }

        // Check if we can vote for this candidate
        let can_vote = (state.voted_for.is_none() || state.voted_for == Some(request.candidate_id))
            && request.term == state.current_term;

        // Check if candidate's log is at least as up-to-date as ours
        let log = self.log.read().unwrap();
        let log_up_to_date = request.last_log_term > log.metadata.last_term
            || (request.last_log_term == log.metadata.last_term && request.last_log_index >= log.metadata.last_index);

        if can_vote && log_up_to_date {
            state.voted_for = Some(request.candidate_id);
            RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
                reason: None,
            }
        } else {
            RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
                reason: Some("Either already voted or log not up-to-date".to_string()),
            }
        }
    }

    pub fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut state = self.state.write().unwrap();

        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = Role::Follower;
            state.voted_for = None;
            state.leader_id = Some(request.leader_id);
        }

        // Check if request term matches current term
        if request.term != state.current_term {
            return AppendEntriesResponse {
                term: state.current_term,
                success: false,
                match_index: state.commit_index,
                next_index: state.commit_index + 1,
            };
        }

        // Update leader ID
        state.leader_id = Some(request.leader_id);

        // Reset election timer
        let mut election_timer = self.election_timer.write().unwrap();
        election_timer.last_reset = Instant::now();
        drop(election_timer);

        // Check if previous log entry exists and matches
        let log = self.log.read().unwrap();
        let prev_log_exists = request.prev_log_index == 0 || 
            (request.prev_log_index <= log.metadata.last_index && 
             log.entries[(request.prev_log_index - 1) as usize].term == request.prev_log_term);

        if !prev_log_exists {
            return AppendEntriesResponse {
                term: state.current_term,
                success: false,
                match_index: log.metadata.last_index,
                next_index: log.metadata.last_index + 1,
            };
        }

        // Append new entries
        let mut log_write = self.log.write().unwrap();
        for entry in request.entries {
            if entry.index > log_write.metadata.last_index {
                log_write.entries.push(entry);
                log_write.metadata.last_index = entry.index;
                log_write.metadata.last_term = entry.term;
            }
        }

        // Update commit index
        if request.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(request.leader_commit, log_write.metadata.last_index);
        }

        AppendEntriesResponse {
            term: state.current_term,
            success: true,
            match_index: log_write.metadata.last_index,
            next_index: log_write.metadata.last_index + 1,
        }
    }

    pub fn handle_install_snapshot(&self, request: InstallSnapshotRequest) -> InstallSnapshotResponse {
        let mut state = self.state.write().unwrap();

        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = Role::Follower;
            state.voted_for = None;
            state.leader_id = Some(request.leader_id);
        }

        // Check if request term matches current term
        if request.term != state.current_term {
            return InstallSnapshotResponse {
                term: state.current_term,
                success: false,
            };
        }

        // Update leader ID
        state.leader_id = Some(request.leader_id);

        // Reset election timer
        let mut election_timer = self.election_timer.write().unwrap();
        election_timer.last_reset = Instant::now();
        drop(election_timer);

        // Restore snapshot
        let snapshot = Snapshot {
            data: request.data,
            last_included_index: request.last_included_index,
            last_included_term: request.last_included_term,
        };

        if let Err(e) = self.state_machine.restore(snapshot) {
            eprintln!("Error restoring snapshot: {}", e);
            return InstallSnapshotResponse {
                term: state.current_term,
                success: false,
            };
        }

        // Update log metadata
        let mut log = self.log.write().unwrap();
        log.metadata.snapshot_index = request.last_included_index;
        log.metadata.snapshot_term = request.last_included_term;
        log.entries.clear(); // Clear old entries
        log.metadata.last_index = request.last_included_index;
        log.metadata.last_term = request.last_included_term;

        // Update state
        state.commit_index = request.last_included_index;
        state.last_applied = request.last_included_index;

        InstallSnapshotResponse {
            term: state.current_term,
            success: true,
        }
    }
}

impl Default for Log {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            metadata: LogMetadata {
                last_index: 0,
                last_term: 0,
                snapshot_index: 0,
                snapshot_term: 0,
            },
        }
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }
}

impl Clone for RaftNode {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            state: self.state.clone(),
            log: self.log.clone(),
            state_machine: self.state_machine.clone(),
            network: self.network.clone(),
            election_timer: self.election_timer.clone(),
            heartbeat_timer: self.heartbeat_timer.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockStateMachine;

    impl StateMachine for MockStateMachine {
        fn apply(&self, command: &Command) -> Result<ApplyResult, StateMachineError> {
            Ok(ApplyResult::Success(Vec::new()))
        }

        fn snapshot(&self) -> Result<Snapshot, StateMachineError> {
            Ok(Snapshot {
                data: Vec::new(),
                last_included_index: 0,
                last_included_term: 0,
            })
        }

        fn restore(&self, snapshot: Snapshot) -> Result<(), StateMachineError> {
            Ok(())
        }
    }

    struct MockNetwork;

    impl Network for MockNetwork {
        fn send_request_vote(&self, node_id: &str, request: RequestVoteRequest) -> Result<RequestVoteResponse, NetworkError> {
            Ok(RequestVoteResponse {
                term: request.term,
                vote_granted: true,
                reason: None,
            })
        }

        fn send_append_entries(&self, node_id: &str, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, NetworkError> {
            Ok(AppendEntriesResponse {
                term: request.term,
                success: true,
                match_index: request.prev_log_index,
                next_index: request.prev_log_index + 1,
            })
        }

        fn send_install_snapshot(&self, node_id: &str, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, NetworkError> {
            Ok(InstallSnapshotResponse {
                term: request.term,
                success: true,
            })
        }

        fn broadcast_request_vote(&self, request: RequestVoteRequest) -> Vec<Result<RequestVoteResponse, NetworkError>> {
            vec![Ok(RequestVoteResponse {
                term: request.term,
                vote_granted: true,
                reason: None,
            })]
        }

        fn broadcast_append_entries(&self, request: AppendEntriesRequest) -> Vec<Result<AppendEntriesResponse, NetworkError>> {
            vec![Ok(AppendEntriesResponse {
                term: request.term,
                success: true,
                match_index: request.prev_log_index,
                next_index: request.prev_log_index + 1,
            })]
        }

        fn get_nodes(&self) -> Vec<String> {
            vec!["node2".to_string(), "node3".to_string()]
        }
    }

    #[test]
    fn test_raft_node_creation() {
        let state_machine = MockStateMachine;
        let network = MockNetwork;
        let node = RaftNode::new("node1", Box::new(state_machine), Box::new(network));
        assert_eq!(node.get_role(), Role::Follower);
    }

    #[test]
    fn test_propose_command() {
        let state_machine = MockStateMachine;
        let network = MockNetwork;
        let node = RaftNode::new("node1", Box::new(state_machine), Box::new(network));
        
        let command = Command::Put { key: b"test".to_vec(), value: b"value".to_vec() };
        let result = node.propose(command);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_request_vote() {
        let state_machine = MockStateMachine;
        let network = MockNetwork;
        let node = RaftNode::new("node1", Box::new(state_machine), Box::new(network));
        
        let request = RequestVoteRequest {
            term: 1,
            candidate_id: "node2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response = node.handle_request_vote(request);
        assert!(response.vote_granted);
    }

    #[test]
    fn test_handle_append_entries() {
        let state_machine = MockStateMachine;
        let network = MockNetwork;
        let node = RaftNode::new("node1", Box::new(state_machine), Box::new(network));
        
        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "node2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        
        let response = node.handle_append_entries(request);
        assert!(response.success);
    }
}
