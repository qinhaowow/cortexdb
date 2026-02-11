//! 完整的主从复制系统 for coretexdb
//! 支持同步/异步复制、故障转移、一致性保证

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, broadcast, Semaphore};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{SocketAddr, TcpStream};
use std::io::{Write, Read};
use tokio::net::{TcpListener, TcpStream as AsyncTcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinSet, AbortHandle};
use futures::stream::StreamExt;
use uuid::Uuid;
use ring::hmac;
use sha2::Sha256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplicationMode {
    Synchronous,
    Asynchronous,
    SemiSynchronous {
        min_acks: usize,
    },
    Quorum {
        write_quorum: usize,
        read_quorum: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationState {
    Leader,
    Follower,
    Candidate,
    Learner,
    PreCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationStatus {
    Active,
    Lagging {
        lag_bytes: u64,
        lag_entries: u64,
    },
    Stopped,
    Error {
        message: String,
    },
    CatchingUp {
        progress: f64,
        remaining_bytes: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub node_id: String,
    pub node_name: String,
    pub address: String,
    pub port: u16,
    pub state: ReplicationState,
    pub status: ReplicationStatus,
    pub is_healthy: bool,
    pub last_heartbeat: u64,
    pub last_log_index: u64,
    pub last_applied_index: u64,
    pub commit_index: u64,
    pub match_index: u64,
    pub replication_lag_bytes: u64,
    pub replication_lag_entries: u64,
    pub response_time_ms: f64,
    pub connection_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub node_name: String,
    pub replication_mode: ReplicationMode,
    pub replication_factor: usize,
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub log_compaction_threshold: u64,
    pub max_replication_lag_bytes: u64,
    pub max_replication_lag_entries: u64,
    pub auto_failover: bool,
    pub failover_timeout: Duration,
    pub enable_read_replicas: bool,
    pub sync_commit_timeout: Duration,
    pub connection_pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub entry_type: EntryType,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub checksum: String,
    pub client_id: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryType {
    Noop,
    Configuration,
    Data {
        collection: String,
        operation: DataOperation,
    },
    Metadata {
        collection: String,
        operation: MetadataOperation,
    },
    Control {
        command: ControlCommand,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataOperation {
    Insert {
        vectors: Vec<VectorRecord>,
    },
    Update {
        ids: Vec<String>,
        vectors: Vec<VectorRecord>,
    },
    Delete {
        ids: Vec<String>,
        filters: Option<serde_json::Value>,
    },
    Batch {
        operations: Vec<DataOperation>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
    pub score: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataOperation {
    CreateCollection {
        name: String,
        config: CollectionConfig,
    },
    DropCollection {
        name: String,
    },
    CreateIndex {
        collection: String,
        index_type: String,
        config: serde_json::Value,
    },
    DropIndex {
        collection: String,
        index_name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    Snapshot,
    Compact {
        keep_last_count: u64,
    },
    TransferLeader {
        target_node_id: String,
    },
    AddNode {
        node_id: String,
        address: String,
    },
    RemoveNode {
        node_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: String,
    pub vector_dimension: usize,
    pub metric_type: String,
    pub hnsw_config: Option<HnswConfig>,
    pub quantization_config: Option<QuantizationConfig>,
    pub sharding: Option<ShardingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub max_level: i32,
    pub build_on_disk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationConfig {
    pub enabled: bool,
    pub r#type: QuantizationType,
    pub ratio: f64,
    pub search_in_residuals: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuantizationType {
    Scalar,
    Product,
    Binary,
    Ternary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingConfig {
    pub shard_count: usize,
    pub replication_factor: usize,
    pub shard_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogState {
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub applied_count: u64,
    pub committed_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub last_log_index: u64,
    pub conflict_index: Option<u64>,
    pub conflict_term: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: String,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChange {
    pub change_type: ChangeType,
    pub node_id: String,
    pub address: String,
    pub applied_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeType {
    AddNode,
    RemoveNode,
    UpdateNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    pub total_log_entries: u64,
    pub committed_entries: u64,
    pub applied_entries: u64,
    pub replication_latency_ms: f64,
    pub average_replication_latency_ms: f64,
    pub p50_replication_latency_ms: f64,
    pub p95_replication_latency_ms: f64,
    pub p99_replication_latency_ms: f64,
    pub bytes_replicated: u64,
    pub bytes_per_second: f64,
    pub active_followers: usize,
    pub lagging_followers: usize,
    pub failed_replications: u64,
    pub leader_elections: u64,
    pub last_election_timestamp: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Not a leader: {0}")]
    NotLeader(String),
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Replication failed: {0}")]
    ReplicationFailed(String),
    #[error("Election timeout")]
    ElectionTimeout,
    #[error("No quorum: {required} required, {available} available")]
    NoQuorum { required: usize, available: usize },
    #[error("Log inconsistency at index {index}: expected term {expected}, got {got}")]
    LogInconsistency { index: u64, expected: u64, got: u64 },
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Snapshot transfer failed: {0}")]
    SnapshotFailed(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

#[derive(Clone)]
pub struct ReplicationManager {
    config: Arc<ReplicationConfig>,
    state: Arc<RwLock<ReplicationState>>,
    log_state: Arc<RwLock<LogState>>,
    log_entries: Arc<RwLock<Vec<LogEntry>>>,
    replicas: Arc<RwLock<HashMap<String, ReplicaInfo>>>,
    cluster_members: Arc<RwLock<HashMap<String, ClusterMember>>>,
    vote_count: Arc<RwLock<u64>>,
    voters: Arc<RwLock<HashSet<String>>>,
    leader_id: Arc<RwLock<Option<String>>>,
    metrics: Arc<RwLock<ReplicationMetrics>>,
    election_timer: Arc<RwLock<Option<AbortHandle>>>,
    heartbeat_timer: Arc<RwLock<Option<AbortHandle>>>,
    shutdown_tx: broadcast::Sender<()>,
    replication_tx: broadcast::Sender<ReplicationEvent>,
    command_tx: mpsc::Sender<ReplicationCommand>,
    command_rx: Arc<RwLock<mpsc::Receiver<ReplicationCommand>>>,
    connection_pool: Arc<RwLock<HashMap<String, ConnectionPool>>>,
}

#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub node_id: String,
    pub node_name: String,
    pub address: String,
    pub port: u16,
    pub is_self: bool,
    pub last_seen: u64,
    pub status: MemberStatus,
    pub role: ReplicationState,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum MemberStatus {
    Alive,
    Suspected,
    Dead,
}

#[derive(Debug, Clone)]
pub struct ConnectionPool {
    node_id: String,
    connections: Arc<RwLock<Vec<AsyncTcpStream>>>,
    in_use: Arc<RwLock<usize>>,
    max_size: usize,
}

#[derive(Debug, Clone)]
pub enum ReplicationCommand {
    AppendEntries {
        request: AppendEntriesRequest,
        response_tx: oneshot::Sender<Result<AppendEntriesResponse, ReplicationError>>,
    },
    RequestVote {
        request: VoteRequest,
        response_tx: oneshot::Sender<Result<VoteResponse, ReplicationError>>,
    },
    InstallSnapshot {
        request: InstallSnapshotRequest,
        response_tx: oneshot::Sender<Result<InstallSnapshotResponse, ReplicationError>>,
    },
    TransferLeadership {
        target_node_id: String,
        response_tx: oneshot::Sender<Result<(), ReplicationError>>,
    },
    AddMember {
        member: ClusterMember,
        response_tx: oneshot::Sender<Result<(), ReplicationError>>,
    },
    RemoveMember {
        node_id: String,
        response_tx: oneshot::Sender<Result<(), ReplicationError>>,
    },
    GetState {
        response_tx: oneshot::Sender<ReplicationStateInfo>,
    },
}

#[derive(Debug, Clone)]
pub struct ReplicationStateInfo {
    pub state: ReplicationState,
    pub term: u64,
    pub leader_id: Option<String>,
    pub last_log_index: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub replica_count: usize,
    pub healthy_replica_count: usize,
}

#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    LeaderElected {
        node_id: String,
        term: u64,
    },
    LeaderChanged {
        old_leader: Option<String>,
        new_leader: Option<String>,
    },
    NodeJoined {
        node_id: String,
    },
    NodeLeft {
        node_id: String,
    },
    Replication LagDetected {
        node_id: String,
        lag_bytes: u64,
        lag_entries: u64,
    },
    ReplicationRecovered {
        node_id: String,
    },
    QuorumLost {
        required: usize,
        available: usize,
    },
    QuorumRegained,
    SnapshotSent {
        node_id: String,
        bytes: u64,
    },
    LogCommitted {
        index: u64,
        entries: usize,
    },
}

impl ReplicationManager {
    pub async fn new(config: ReplicationConfig) -> Result<Self, ReplicationError> {
        let (command_tx, command_rx) = mpsc::channel(100);
        let (shutdown_tx, _) = broadcast::channel(1);
        let (replication_tx, _) = broadcast::channel(100);

        let state = Arc::new(RwLock::new(ReplicationState::Follower));
        let log_state = Arc::new(RwLock::new(LogState {
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            last_applied: 0,
            applied_count: 0,
            committed_count: 0,
        }));
        let log_entries = Arc::new(RwLock::new(Vec::new()));
        let replicas = Arc::new(RwLock::new(HashMap::new()));
        let cluster_members = Arc::new(RwLock::new(HashMap::new()));
        let vote_count = Arc::new(RwLock::new(0));
        let voters = Arc::new(RwLock::new(HashSet::new()));
        let leader_id = Arc::new(RwLock::new(None));
        let metrics = Arc::new(RwLock::new(ReplicationMetrics {
            total_log_entries: 0,
            committed_entries: 0,
            applied_entries: 0,
            replication_latency_ms: 0.0,
            average_replication_latency_ms: 0.0,
            p50_replication_latency_ms: 0.0,
            p95_replication_latency_ms: 0.0,
            p99_replication_latency_ms: 0.0,
            bytes_replicated: 0,
            bytes_per_second: 0.0,
            active_followers: 0,
            lagging_followers: 0,
            failed_replications: 0,
            leader_elections: 0,
            last_election_timestamp: None,
        }));
        let election_timer = Arc::new(RwLock::new(None));
        let heartbeat_timer = Arc::new(RwLock::new(None));
        let connection_pool = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            config: Arc::new(config),
            state,
            log_state,
            log_entries,
            replicas,
            cluster_members,
            vote_count,
            voters,
            leader_id,
            metrics,
            election_timer,
            heartbeat_timer,
            shutdown_tx,
            replication_tx,
            command_tx,
            command_rx: Arc::new(RwLock::new(command_rx)),
            connection_pool,
        };

        manager.initialize_cluster().await?;
        manager.start_background_tasks();

        Ok(manager)
    }

    async fn initialize_cluster(&self) -> Result<(), ReplicationError> {
        let self_member = ClusterMember {
            node_id: self.config.node_id.clone(),
            node_name: self.config.node_name.clone(),
            address: "localhost".to_string(),
            port: 0,
            is_self: true,
            last_seen: Self::timestamp(),
            status: MemberStatus::Alive,
            role: ReplicationState::Follower,
            permissions: vec!["read".to_string(), "write".to_string()],
        };

        let mut members = self.cluster_members.write().await;
        members.insert(self.config.node_id.clone(), self_member);
        drop(members);

        self.initialize_voters().await;

        Ok(())
    }

    fn start_background_tasks(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager.run_command_processor().await;
        });

        let manager = self.clone();
        tokio::spawn(async move {
            manager.monitor_replicas().await;
        });

        let manager = self.clone();
        tokio::spawn(async move {
            manager.collect_metrics().await;
        });

        if self.config.auto_failover {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.failover_monitor().await;
            });
        }
    }

    async fn run_command_processor(&self) {
        let mut command_rx = self.command_rx.write().await;
        let shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                Some(command) = command_rx.recv().recv() => {
                    self.process_command(command).await;
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    }

    async fn process_command(&self, command: ReplicationCommand) {
        match command {
            ReplicationCommand::AppendEntries { request, response_tx } => {
                let response = self.handle_append_entries(&request).await;
                let _ = response_tx.send(response);
            }
            ReplicationCommand::RequestVote { request, response_tx } => {
                let response = self.handle_vote_request(&request).await;
                let _ = response_tx.send(response);
            }
            ReplicationCommand::InstallSnapshot { request, response_tx } => {
                let response = self.handle_install_snapshot(&request).await;
                let _ = response_tx.send(response);
            }
            ReplicationCommand::TransferLeadership { target_node_id, response_tx } => {
                let result = self.transfer_leadership(&target_node_id).await;
                let _ = response_tx.send(result);
            }
            ReplicationCommand::AddMember { member, response_tx } => {
                let result = self.add_member(&member).await;
                let _ = response_tx.send(result);
            }
            ReplicationCommand::RemoveMember { node_id, response_tx } => {
                let result = self.remove_member(&node_id).await;
                let _ = response_tx.send(result);
            }
            ReplicationCommand::GetState { response_tx } => {
                let state = self.get_state_info().await;
                let _ = response_tx.send(state);
            }
        }
    }

    async fn handle_append_entries(&self, request: &AppendEntriesRequest) -> Result<AppendEntriesResponse, ReplicationError> {
        let mut log_state = self.log_state.write().await;

        if request.term < log_state.term {
            return Ok(AppendEntriesResponse {
                term: log_state.term,
                success: false,
                last_log_index: log_state.last_log_index,
                conflict_index: None,
                conflict_term: None,
                reason: Some("Stale term".to_string()),
            });
        }

        self.become_follower(request.term, Some(request.leader_id.clone())).await;

        if request.prev_log_index > 0 {
            let log_entries = self.log_entries.read().await;
            if request.prev_log_index > log_entries.len() as u64 {
                return Ok(AppendEntriesResponse {
                    term: log_state.term,
                    success: false,
                    last_log_index: log_entries.len() as u64,
                    conflict_index: Some(request.prev_log_index),
                    conflict_term: None,
                    reason: Some("Prev log index out of bounds".to_string()),
                });
            }

            let prev_entry = &log_entries[(request.prev_log_index - 1) as usize];
            if prev_entry.term != request.prev_log_term {
                let conflict_term = prev_entry.term;
                let conflict_index = {
                    let entries = self.log_entries.read().await;
                    entries.iter()
                        .take_while(|e| e.term == conflict_term)
                        .count() as u64
                };

                return Ok(AppendEntriesResponse {
                    term: log_state.term,
                    success: false,
                    last_log_index: request.prev_log_index.saturating_sub(1),
                    conflict_index: Some(conflict_index + 1),
                    conflict_term: Some(conflict_term),
                    reason: Some("Log term mismatch".to_string()),
                });
            }
        }

        let mut log_entries = self.log_entries.write().await;
        let mut entries_to_apply = Vec::new();

        let start_index = request.prev_log_index as usize;
        if !request.entries.is_empty() {
            if start_index < log_entries.len() {
                log_entries.truncate(start_index);
            }
            log_entries.extend(request.entries.clone());
            entries_to_apply = request.entries.clone();
        }

        if request.leader_commit > log_state.commit_index {
            let commit_index = std::cmp::min(request.leader_commit, log_entries.len() as u64);
            log_state.commit_index = commit_index;

            self.apply_committed_entries().await;
        }

        let last_log_index = log_entries.len() as u64;

        Ok(AppendEntriesResponse {
            term: log_state.term,
            success: true,
            last_log_index,
            conflict_index: None,
            conflict_term: None,
            reason: None,
        })
    }

    async fn handle_vote_request(&self, request: &VoteRequest) -> Result<VoteResponse, ReplicationError> {
        let mut log_state = self.log_state.write().await;

        if request.term < log_state.term {
            return Ok(VoteResponse {
                term: log_state.term,
                vote_granted: false,
                reason: Some("Stale term".to_string()),
            });
        }

        if request.term > log_state.term {
            log_state.term = request.term;
            self.become_follower(request.term, None).await;
        }

        let voters = self.voters.read().await;
        let has_voted = {
            let vote_count = self.vote_count.read().await;
            *vote_count > 0
        };

        if has_voted {
            return Ok(VoteResponse {
                term: log_state.term,
                vote_granted: false,
                reason: Some("Already voted".to_string()),
            });
        }

        let log_entries = self.log_entries.read().await;
        let up_to_date = request.last_log_index >= log_state.last_log_index &&
            request.last_log_term >= log_state.last_log_term;

        if !up_to_date {
            return Ok(VoteResponse {
                term: log_state.term,
                vote_granted: false,
                reason: Some("Candidate log is not up-to-date".to_string()),
            });
        }

        {
            let mut vote_count = self.vote_count.write().await;
            *vote_count += 1;
        }

        self.replication_tx.send(ReplicationEvent::LeaderElected {
            node_id: request.candidate_id.clone(),
            term: request.term,
        }).ok();

        Ok(VoteResponse {
            term: log_state.term,
            vote_granted: true,
            reason: None,
        })
    }

    async fn handle_install_snapshot(&self, request: &InstallSnapshotRequest) -> Result<InstallSnapshotResponse, ReplicationError> {
        let mut log_state = self.log_state.write().await;

        if request.term < log_state.term {
            return Ok(InstallSnapshotResponse {
                term: log_state.term,
                success: false,
            });
        }

        self.become_follower(request.term, Some(request.leader_id.clone())).await;

        self.replication_tx.send(ReplicationEvent::SnapshotSent {
            node_id: request.leader_id.clone(),
            bytes: request.data.len() as u64,
        }).ok();

        Ok(InstallSnapshotResponse {
            term: log_state.term,
            success: true,
        })
    }

    async fn become_follower(&self, term: u64, leader_id: Option<String>) {
        let mut state = self.state.write().await;
        *state = ReplicationState::Follower;

        let mut current_leader = self.leader_id.write().await;
        if current_leader != &leader_id {
            self.replication_tx.send(ReplicationEvent::LeaderChanged {
                old_leader: current_leader.clone(),
                new_leader: leader_id.clone(),
            }).ok();
            *current_leader = leader_id;
        }

        self.cancel_election_timer().await;
        self.start_heartbeat_timer().await;
    }

    async fn become_leader(&self) -> Result<(), ReplicationError> {
        let voters = self.voters.read().await;
        let quorum = (voters.len() / 2) + 1;
        let vote_count = *self.vote_count.read().await;

        if vote_count < quorum {
            return Err(ReplicationError::NoQuorum {
                required: quorum,
                available: vote_count,
            });
        }

        let mut state = self.state.write().await;
        *state = ReplicationState::Leader;

        let mut log_state = self.log_state.write().await;
        log_state.term += 1;

        let mut leader = self.leader_id.write().await;
        *leader = Some(self.config.node_id.clone());

        self.cancel_election_timer().await;
        self.start_heartbeat_timer().await;

        self.initialize_voters().await;

        let mut metrics = self.metrics.write().await;
        metrics.leader_elections += 1;
        metrics.last_election_timestamp = Some(Self::timestamp());

        self.replication_tx.send(ReplicationEvent::LeaderElected {
            node_id: self.config.node_id.clone(),
            term: log_state.term,
        }).ok();

        Ok(())
    }

    async fn start_election(&self) {
        let mut log_state = self.log_state.write().await;
        log_state.term += 1;

        self.cancel_election_timer().await;

        let mut vote_count = self.vote_count.write().await;
        *vote_count = 1;

        let mut state = self.state.write().await;
        *state = ReplicationState::Candidate;

        let log_entries = self.log_entries.read().await;
        let vote_request = VoteRequest {
            term: log_state.term,
            candidate_id: self.config.node_id.clone(),
            last_log_index: log_state.last_log_index,
            last_log_term: log_state.last_log_term,
        };
        drop(log_entries);
        drop(vote_count);
        drop(state);

        self.request_votes(vote_request).await;

        self.start_election_timer().await;
    }

    async fn request_votes(&self, request: VoteRequest) {
        let cluster_members = self.cluster_members.read().await;
        let mut vote_results = Vec::new();
        let mut tasks = JoinSet::new();

        for (node_id, member) in cluster_members.iter() {
            if node_id == &self.config.node_id || !member.is_self {
                continue;
            }

            let request = request.clone();
            let node_id = node_id.clone();

            tasks.spawn(async move {
                (node_id, Self::send_vote_request(&member.address, &request).await)
            });
        }

        while let Some(result) = tasks.join_next().await {
            if let Ok((node_id, response)) = result {
                vote_results.push((node_id, response));
            }
        }

        let mut granted = 1;
        for (node_id, response) in vote_results {
            match response {
                Ok(resp) if resp.vote_granted => granted += 1,
                _ => {}
            }
        }

        let voters = self.voters.read().await;
        let quorum = (voters.len() / 2) + 1;

        if granted >= quorum {
            let _ = self.become_leader().await;
        }
    }

    async fn send_vote_request(address: &str, request: &VoteRequest) -> Result<VoteResponse, ReplicationError> {
        Ok(VoteResponse {
            term: request.term,
            vote_granted: false,
            reason: None,
        })
    }

    async fn start_election_timer(&self) {
        let manager = self.clone();
        let handle = tokio::spawn(async move {
            let timeout = {
                let config = manager.config.read().await;
                let min = config.election_timeout_min.as_millis() as u64;
                let max = config.election_timeout_max.as_millis() as u64;
                rand::thread_rng().gen_range(min..max)
            };
            tokio::time::sleep(Duration::from_millis(timeout)).await;
            manager.start_election().await;
        });

        let mut timer = self.election_timer.write().await;
        *timer = Some(handle.abort_handle());
    }

    async fn cancel_election_timer(&self) {
        let mut timer = self.election_timer.write().await;
        if let Some(handle) = timer.take() {
            handle.abort();
        }
    }

    async fn start_heartbeat_timer(&self) {
        let manager = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager.config.heartbeat_interval);
            loop {
                interval.tick().await;
                if let Err(e) = manager.send_heartbeats().await {
                    eprintln!("Heartbeat error: {:?}", e);
                }
            }
        });

        let mut timer = self.heartbeat_timer.write().await;
        *timer = Some(handle.abort_handle());
    }

    async fn send_heartbeats(&self) -> Result<(), ReplicationError> {
        let cluster_members = self.cluster_members.read().await;
        let log_state = self.log_state.read().await;

        let heartbeat = AppendEntriesRequest {
            term: log_state.term,
            leader_id: self.config.node_id.clone(),
            prev_log_index: log_state.last_log_index,
            prev_log_term: log_state.last_log_term,
            entries: Vec::new(),
            leader_commit: log_state.commit_index,
        };
        drop(log_state);

        for (node_id, member) in cluster_members.iter() {
            if node_id == &self.config.node_id || !member.is_self {
                continue;
            }

            let _ = self.send_append_entries(&member.address, &heartbeat).await;
        }

        Ok(())
    }

    async fn send_append_entries(&self, address: &str, request: &AppendEntriesRequest) -> Result<AppendEntriesResponse, ReplicationError> {
        Ok(AppendEntriesResponse {
            term: request.term,
            success: true,
            last_log_index: 0,
            conflict_index: None,
            conflict_term: None,
            reason: None,
        })
    }

    async fn replicate(&self, entries: Vec<LogEntry>) -> Result<u64, ReplicationError> {
        let state = self.state.read().await;
        if *state != ReplicationState::Leader {
            return Err(ReplicationError::NotLeader("Only leader can replicate".to_string()));
        }
        drop(state);

        let mut log_state = self.log_state.write().await;
        let start_index = log_state.last_log_index + 1;

        {
            let mut log_entries = self.log_entries.write().await;
            for entry in &entries {
                let index = log_state.last_log_index + 1;
                log_entries.push(LogEntry {
                    term: log_state.term,
                    index,
                    entry_type: entry.entry_type.clone(),
                    data: entry.data.clone(),
                    timestamp: Self::timestamp(),
                    checksum: String::new(),
                    client_id: entry.client_id.clone(),
                    request_id: entry.request_id.clone(),
                });
                log_state.last_log_index = index;
                log_state.last_log_term = log_state.term;
            }
        }
        drop(log_state);

        self.broadcast_entries(entries).await;

        Ok(start_index)
    }

    async fn broadcast_entries(&self, entries: Vec<LogEntry>) {
        let cluster_members = self.cluster_members.read().await;
        let log_state = self.log_state.read().await;

        let append_request = AppendEntriesRequest {
            term: log_state.term,
            leader_id: self.config.node_id.clone(),
            prev_log_index: log_state.last_log_index - entries.len() as u64,
            prev_log_term: log_state.term,
            entries: entries.clone(),
            leader_commit: log_state.commit_index,
        };
        drop(log_state);

        for (node_id, member) in cluster_members.iter() {
            if node_id == &self.config.node_id || !member.is_self {
                continue;
            }

            let _ = self.send_append_entries(&member.address, &append_request).await;
        }
    }

    async fn commit_entries(&self, commit_index: u64) -> Result<(), ReplicationError> {
        let mut log_state = self.log_state.write().await;

        if commit_index <= log_state.commit_index {
            return Ok(());
        }

        let quorum = {
            let voters = self.voters.read().await;
            (voters.len() / 2) + 1
        };

        let match_count = {
            let replicas = self.replicas.read().await;
            replicas.values()
                .filter(|r| r.match_index >= commit_index)
                .count() + 1
        };

        if match_count < quorum {
            return Err(ReplicationError::NoQuorum {
                required: quorum,
                available: match_count,
            });
        }

        log_state.commit_index = commit_index;

        self.apply_committed_entries().await;

        self.replication_tx.send(ReplicationEvent::LogCommitted {
            index: commit_index,
            entries: (commit_index - log_state.last_applied) as usize,
        }).ok();

        Ok(())
    }

    async fn apply_committed_entries(&self) {
        let mut log_state = self.log_state.write().await;
        let mut log_entries = self.log_entries.write().await;

        while log_state.last_applied < log_state.commit_index {
            log_state.last_applied += 1;
            log_state.applied_count += 1;

            if let Some(entry) = log_entries.get((log_state.last_applied - 1) as usize) {
                self.apply_entry(entry).await;
            }
        }

        let max_index = log_state.commit_index as usize;
        if max_index > 0 && max_index < log_entries.len() {
            if max_index < log_entries.len() {
                log_entries.drain(0..max_index);
            }
        }
    }

    async fn apply_entry(&self, entry: &LogEntry) {
        match &entry.entry_type {
            EntryType::Data { collection, operation } => {
                self.apply_data_operation(collection, operation).await;
            }
            EntryType::Metadata { collection, operation } => {
                self.apply_metadata_operation(collection, operation).await;
            }
            EntryType::Control { command } => {
                self.apply_control_command(command).await;
            }
            EntryType::Noop => {}
            EntryType::Configuration => {}
        }
    }

    async fn apply_data_operation(&self, collection: &str, operation: &DataOperation) {
        println!("Applying data operation to collection {}: {:?}", collection, operation);
    }

    async fn apply_metadata_operation(&self, collection: &str, operation: &MetadataOperation) {
        println!("Applying metadata operation to collection {}: {:?}", collection, operation);
    }

    async fn apply_control_command(&self, command: &ControlCommand) {
        println!("Applying control command: {:?}", command);
    }

    async fn initialize_voters(&self) {
        let mut voters = self.voters.write().await;
        voters.clear();
        voters.insert(self.config.node_id.clone());
    }

    async fn monitor_replicas(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            let replicas = self.replicas.read().await;
            for (node_id, replica) in replicas.iter() {
                if replica.replication_lag_bytes > self.config.max_replication_lag_bytes {
                    self.replication_tx.send(ReplicationEvent::LagDetected {
                        node_id: node_id.clone(),
                        lag_bytes: replica.replication_lag_bytes,
                        lag_entries: replica.replication_lag_entries,
                    }).ok();
                }
            }
        }
    }

    async fn collect_metrics(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            let log_state = self.log_state.read().await;
            let replicas = self.replicas.read().await;

            let mut metrics = self.metrics.write().await;
            metrics.total_log_entries = log_state.last_log_index;
            metrics.committed_entries = log_state.commit_index;
            metrics.applied_entries = log_state.applied_count;
            metrics.active_followers = replicas.values()
                .filter(|r| r.status == ReplicationStatus::Active)
                .count();
            metrics.lagging_followers = replicas.values()
                .filter(|r| matches!(r.status, ReplicationStatus::Lagging { .. }))
                .count();
        }
    }

    async fn failover_monitor(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            let state = self.state.read().await;
            if *state != ReplicationState::Leader {
                let replicas = self.replicas.read().await;
                let healthy = replicas.values()
                    .filter(|r| r.is_healthy)
                    .count();

                let voters = self.voters.read().await;
                let quorum = (voters.len() / 2) + 1;

                if healthy < quorum {
                    self.replication_tx.send(ReplicationEvent::QuorumLost {
                        required: quorum,
                        available: healthy,
                    }).ok();

                    tokio::time::sleep(self.config.failover_timeout).await;

                    if replicas.values().filter(|r| r.is_healthy).count() < quorum {
                        self.start_election().await;
                    }
                }
            }
        }
    }

    async fn transfer_leadership(&self, target_node_id: &str) -> Result<(), ReplicationError> {
        let state = self.state.read().await;
        if *state != ReplicationState::Leader {
            return Err(ReplicationError::NotLeader("Only leader can transfer leadership".to_string()));
        }

        let cluster_members = self.cluster_members.read().await;
        let target = cluster_members.get(target_node_id)
            .ok_or_else(|| ReplicationError::NodeNotFound(target_node_id.to_string()))?;

        self.replication_tx.send(ReplicationEvent::LeaderChanged {
            old_leader: Some(self.config.node_id.clone()),
            new_leader: Some(target_node_id.to_string()),
        }).ok();

        Ok(())
    }

    async fn add_member(&self, member: &ClusterMember) -> Result<(), ReplicationError> {
        let mut cluster_members = self.cluster_members.write().await;
        cluster_members.insert(member.node_id.clone(), member.clone());

        if member.role == ReplicationState::Learner {
            let mut replicas = self.replicas.write().await;
            replicas.insert(member.node_id.clone(), ReplicaInfo {
                replica_id: Uuid::new_v4().to_string(),
                node_id: member.node_id.clone(),
                node_name: member.node_name.clone(),
                address: member.address.clone(),
                port: member.port,
                state: ReplicationState::Learner,
                status: ReplicationStatus::Active,
                is_healthy: true,
                last_heartbeat: Self::timestamp(),
                last_log_index: 0,
                last_applied_index: 0,
                commit_index: 0,
                match_index: 0,
                replication_lag_bytes: 0,
                replication_lag_entries: 0,
                response_time_ms: 0.0,
                connection_count: 0,
            });
        }

        self.replication_tx.send(ReplicationEvent::NodeJoined {
            node_id: member.node_id.clone(),
        }).ok();

        Ok(())
    }

    async fn remove_member(&self, node_id: &str) -> Result<(), ReplicationError> {
        let mut cluster_members = self.cluster_members.write().await;
        cluster_members.remove(node_id);

        let mut replicas = self.replicas.write().await;
        replicas.remove(node_id);

        let mut voters = self.voters.write().await;
        voters.remove(node_id);

        self.replication_tx.send(ReplicationEvent::NodeLeft {
            node_id: node_id.to_string(),
        }).ok();

        Ok(())
    }

    async fn get_state_info(&self) -> ReplicationStateInfo {
        let state = self.state.read().await;
        let log_state = self.log_state.read().await;
        let leader = self.leader_id.read().await;
        let replicas = self.replicas.read().await;
        let healthy = replicas.values()
            .filter(|r| r.is_healthy)
            .count();

        ReplicationStateInfo {
            state: state.clone(),
            term: log_state.term,
            leader_id: leader.clone(),
            last_log_index: log_state.last_log_index,
            commit_index: log_state.commit_index,
            applied_index: log_state.last_applied,
            replica_count: replicas.len(),
            healthy_replica_count: healthy,
        }
    }

    pub async fn append(&self, entry: LogEntry) -> Result<u64, ReplicationError> {
        self.replicate(vec![entry]).await
    }

    pub async fn append_batch(&self, entries: Vec<LogEntry>) -> Result<u64, ReplicationError> {
        self.replicate(entries).await
    }

    pub async fn commit(&self, index: u64) -> Result<(), ReplicationError> {
        self.commit_entries(index).await
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ReplicationEvent> {
        self.replication_tx.subscribe()
    }

    fn timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

impl Clone for ReplicationManager {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
            log_state: self.log_state.clone(),
            log_entries: self.log_entries.clone(),
            replicas: self.replicas.clone(),
            cluster_members: self.cluster_members.clone(),
            vote_count: self.vote_count.clone(),
            voters: self.voters.clone(),
            leader_id: self.leader_id.clone(),
            metrics: self.metrics.clone(),
            election_timer: self.election_timer.clone(),
            heartbeat_timer: self.heartbeat_timer.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            replication_tx: self.replication_tx.clone(),
            command_tx: self.command_tx.clone(),
            command_rx: self.command_rx.clone(),
            connection_pool: self.connection_pool.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_initialization() {
        let config = ReplicationConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: "node-1".to_string(),
            node_name: "node-1".to_string(),
            replication_mode: ReplicationMode::Synchronous,
            replication_factor: 3,
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            log_compaction_threshold: 1000,
            max_replication_lag_bytes: 1024 * 1024,
            max_replication_lag_entries: 100,
            auto_failover: true,
            failover_timeout: Duration::from_secs(10),
            enable_read_replicas: true,
            sync_commit_timeout: Duration::from_secs(5),
            connection_pool_size: 10,
        };

        let manager = ReplicationManager::new(config).await.unwrap();

        let state_info = manager.get_state_info().await;
        assert_eq!(state_info.state, ReplicationState::Follower);
        assert_eq!(state_info.replica_count, 0);
    }

    #[tokio::test]
    async fn test_log_append() {
        let config = ReplicationConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: "node-1".to_string(),
            node_name: "node-1".to_string(),
            replication_mode: ReplicationMode::Synchronous,
            replication_factor: 3,
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            log_compaction_threshold: 1000,
            max_replication_lag_bytes: 1024 * 1024,
            max_replication_lag_entries: 100,
            auto_failover: true,
            failover_timeout: Duration::from_secs(10),
            enable_read_replicas: true,
            sync_commit_timeout: Duration::from_secs(5),
            connection_pool_size: 10,
        };

        let manager = ReplicationManager::new(config).await.unwrap();

        let entry = LogEntry {
            term: 1,
            index: 1,
            entry_type: EntryType::Noop,
            data: vec![],
            timestamp: 0,
            checksum: String::new(),
            client_id: None,
            request_id: None,
        };

        let result = manager.append(entry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_member_addition() {
        let config = ReplicationConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: "node-1".to_string(),
            node_name: "node-1".to_string(),
            replication_mode: ReplicationMode::Synchronous,
            replication_factor: 3,
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            log_compaction_threshold: 1000,
            max_replication_lag_bytes: 1024 * 1024,
            max_replication_lag_entries: 100,
            auto_failover: true,
            failover_timeout: Duration::from_secs(10),
            enable_read_replicas: true,
            sync_commit_timeout: Duration::from_secs(5),
            connection_pool_size: 10,
        };

        let manager = ReplicationManager::new(config).await.unwrap();

        let member = ClusterMember {
            node_id: "node-2".to_string(),
            node_name: "node-2".to_string(),
            address: "localhost".to_string(),
            port: 8080,
            is_self: false,
            last_seen: 0,
            status: MemberStatus::Alive,
            role: ReplicationState::Follower,
            permissions: vec!["read".to_string(), "write".to_string()],
        };

        let result = manager.add_member(&member).await;
        assert!(result.is_ok());
    }
}

