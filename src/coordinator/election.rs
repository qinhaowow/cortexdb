use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex, broadcast, Barrier};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum ElectionError {
    #[error("No cluster members available")]
    NoMembersAvailable,
    #[error("Election not started")]
    ElectionNotStarted,
    #[error("Already participating in election")]
    AlreadyParticipating,
    #[error("Not leader")]
    NotLeader,
    #[error("Leader already exists")]
    LeaderExists,
    #[error("Session expired")]
    SessionExpired,
    #[error("Lock acquisition failed")]
    LockAcquisitionFailed,
    #[error("Invalid term")]
    InvalidTerm,
    #[error("Vote request denied")]
    VoteDenied,
    #[error("Heartbeat timeout")]
    HeartbeatTimeout,
    #[error("Shutdown")]
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionConfig {
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub leader_lease_duration: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub quorum_size: usize,
    pub enable_pre_vote: bool,
    pub min_election_round_timeout: Duration,
    pub max_election_round_timeout: Duration,
    pub auto_refresh_lease: bool,
    pub priority: u32,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            leader_lease_duration: Duration::from_secs(9),
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
            quorum_size: 2,
            enable_pre_vote: true,
            min_election_round_timeout: Duration::from_secs(10),
            max_election_round_timeout: Duration::from_secs(20),
            auto_refresh_lease: true,
            priority: 100,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ElectionState {
    Follower,
    Candidate,
    Leader,
    Observer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionMember {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub priority: u32,
    pub last_heartbeat: u64,
    pub term: u64,
    pub votes: u64,
    pub status: MemberStatus,
}

impl ElectionMember {
    pub fn new(id: &str, address: &str, port: u16, priority: u32) -> Self {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        Self {
            id: id.to_string(),
            address: address.to_string(),
            port,
            priority,
            last_heartbeat: now,
            term: 0,
            votes: 0,
            status: MemberStatus::Active,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MemberStatus {
    Active,
    Suspected,
    Failed,
    Leaving,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionStats {
    pub elections_started: AtomicU64,
    pub elections_won: AtomicU64,
    pub elections_lost: AtomicU64,
    pub terms_incremented: AtomicU64,
    pub votes_received: AtomicU64,
    pub votes_granted: AtomicU64,
    pub votes_denied: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub heartbeats_received: AtomicU64,
    pub leader_changes: AtomicU64,
    pub state_transitions: AtomicUsize,
    pub avg_election_time_ms: AtomicU64,
    pub avg_leadership_time_ms: AtomicU64,
    pub current_leadership_duration_ms: AtomicU64,
    pub last_leader_change: AtomicU64,
}

impl Default for ElectionStats {
    fn default() -> Self {
        Self {
            elections_started: AtomicU64::new(0),
            elections_won: AtomicU64::new(0),
            elections_lost: AtomicU64::new(0),
            terms_incremented: AtomicU64::new(0),
            votes_received: AtomicU64::new(0),
            votes_granted: AtomicU64::new(0),
            votes_denied: AtomicU64::new(0),
            heartbeats_sent: AtomicU64::new(0),
            heartbeats_received: AtomicU64::new(0),
            leader_changes: AtomicU64::new(0),
            state_transitions: AtomicUsize::new(0),
            avg_election_time_ms: AtomicU64::new(0),
            avg_leadership_time_ms: AtomicU64::new(0),
            current_leadership_duration_ms: AtomicU64::new(0),
            last_leader_change: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ElectionEvent {
    pub event_type: ElectionEventType,
    pub leader_id: Option<String>,
    pub term: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ElectionEventType {
    ElectionStarted,
    ElectionWon,
    ElectionLost,
    LeaderElected,
    LeaderChanged,
    HeartbeatReceived,
    HeartbeatTimeout,
    StateChanged,
    LeaseExpired,
}

pub struct LeaderElection {
    config: ElectionConfig,
    member: ElectionMember,
    members: Arc<RwLock<Vec<ElectionMember>>>,
    state: Arc<AtomicUsize>,
    current_term: Arc<AtomicU64>,
    voted_for: Arc<Mutex<Option<String>>>,
    leader_id: Arc<Mutex<Option<String>>>,
    stats: Arc<ElectionStats>,
    election_tx: broadcast::Sender<ElectionEvent>,
    event_subscribers: Arc<RwLock<Vec<mpsc::Sender<ElectionEvent>>>>,
    shutdown: broadcast::Sender<()>,
    is_running: Arc<AtomicBool>,
    last_election_time: Arc<Mutex<Instant>>,
    leadership_start_time: Arc<Mutex<Option<Instant>>>,
    barrier: Arc<Barrier>,
}

impl LeaderElection {
    pub fn new(
        member_id: &str,
        address: &str,
        port: u16,
        config: Option<ElectionConfig>,
    ) -> Self {
        let config = config.unwrap_or_default();
        let member = ElectionMember::new(member_id, address, port, config.priority);
        let members = Arc::new(RwLock::new(Vec::new()));
        let state = Arc::new(AtomicUsize::new(ElectionState::Follower as usize));
        let current_term = Arc::new(AtomicU64::new(0));
        let voted_for = Arc::new(Mutex::new(None));
        let leader_id = Arc::new(Mutex::new(None));
        let stats = Arc::new(ElectionStats::default());
        let (election_tx, _) = broadcast::channel(100);
        let event_subscribers = Arc::new(RwLock::new(Vec::new()));
        let (shutdown, _) = broadcast::channel(1);
        let is_running = Arc::new(AtomicBool::new(false));
        let last_election_time = Arc::new(Mutex::new(Instant::now()));
        let leadership_start_time = Arc::new(Mutex::new(None));
        let barrier = Arc::new(Barrier::new(2));

        Self {
            config,
            member,
            members,
            state,
            current_term,
            voted_for,
            leader_id,
            stats,
            election_tx,
            event_subscribers,
            shutdown,
            is_running,
            last_election_time,
            leadership_start_time,
            barrier,
        }
    }

    pub fn add_member(&mut self, member: ElectionMember) {
        let mut members = self.members.write();
        if !members.iter().any(|m| m.id == member.id) {
            members.push(member);
            debug!("Added member: {} ({}:{})", member.id, member.address, member.port);
        }
    }

    pub fn remove_member(&self, member_id: &str) {
        let mut members = self.members.write();
        members.retain(|m| m.id != member_id);
        debug!("Removed member: {}", member_id);
    }

    pub async fn start(&mut self) -> Result<(), ElectionError> {
        if self.is_running.load(Ordering::SeqCst) {
            return Err(ElectionError::AlreadyParticipating);
        }

        self.is_running.store(true, Ordering::SeqCst);
        self.set_state(ElectionState::Follower);


        self.start_heartbeat_monitor();

        let election_events = self.election_tx.subscribe();
        let shutdown_rx = self.shutdown.subscribe();
        let is_running = self.is_running.clone();
        let members = self.members.clone();
        let current_term = self.current_term.clone();
        let voted_for = self.voted_for.clone();
        let leader_id = self.leader_id.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();
        let last_election_time = self.last_election_time.clone();

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(config.election_timeout / 3);

            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if !is_running.load(Ordering::SeqCst) {
                            break;
                        }

                        let election_timeout = config.election_timeout;
                        let last_election = *last_election_time.lock().await;

                        if last_election.elapsed() > election_timeout {
                            if let Err(e) = Self::start_election(
                                &members,
                                &current_term,
                                &voted_for,
                                &leader_id,
                                &stats,
                                &config,
                            ).await {
                                error!("Election failed: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        info!("LeaderElection started for member: {}", self.member.id);
        Ok(())
    }

    async fn start_election(
        members: &Arc<RwLock<Vec<ElectionMember>>>,
        current_term: &Arc<AtomicU64>,
        voted_for: &Arc<Mutex<Option<String>>>,
        leader_id: &Arc<Mutex<Option<String>>>,
        stats: &Arc<ElectionStats>,
        config: &ElectionConfig,
    ) -> Result<(), ElectionError> {
        let term = current_term.fetch_add(1, Ordering::SeqCst) + 1;
        stats.elections_started.fetch_add(1, Ordering::SeqCst);
        stats.terms_incremented.fetch_add(1, Ordering::SeqCst);

        let mut request_votes = Vec::new();
        let mut votes_granted = 0;
        let mut total_members = members.read().await.len();

        if total_members == 0 {
            return Err(ElectionError::NoMembersAvailable);
        }

        let quorum = ((total_members / 2) + 1).max(config.quorum_size);

        for member in members.read().await.iter() {
            let vote_response = Self::request_vote(member, term, true, stats).await;
            request_votes.push(vote_response);
        }

        for response in request_votes {
            stats.votes_received.fetch_add(1, Ordering::SeqCst);
            if response.granted {
                votes_granted += 1;
                stats.votes_granted.fetch_add(1, Ordering::SeqCst);
            } else {
                stats.votes_denied.fetch_add(1, Ordering::SeqCst);
            }
        }

        if votes_granted >= quorum as u64 {
            stats.elections_won.fetch_add(1, Ordering::SeqCst);

            let mut leader = leader_id.lock().await;
            *leader = Some(String::new());
            stats.leader_changes.fetch_add(1, Ordering::SeqCst);
            stats.last_leader_change.store(std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64, Ordering::SeqCst);

            Ok(())
        } else {
            stats.elections_lost.fetch_add(1, Ordering::SeqCst);
            Err(ElectionError::VoteDenied)
        }
    }

    async fn request_vote(member: &ElectionMember, term: u64, log_ok: bool, stats: &Arc<ElectionStats>) -> VoteResponse {
        stats.votes_received.fetch_add(1, Ordering::SeqCst);

        VoteResponse { granted: true, term, debug_message: "Vote granted".to_string() }
    }

    pub fn is_leader(&self) -> bool {
        self.state.load(Ordering::SeqCst) == ElectionState::Leader as usize
    }

    pub fn get_state(&self) -> ElectionState {
        ElectionState::from_usize(self.state.load(Ordering::SeqCst))
    }

    fn set_state(&self, new_state: ElectionState) {
        self.state.store(new_state as usize, Ordering::SeqCst);
        self.stats.state_transitions.fetch_add(1, Ordering::SeqCst);

        let event = ElectionEvent {
            event_type: ElectionEventType::StateChanged,
            leader_id: None,
            term: self.current_term.load(Ordering::SeqCst),
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
        };
        let _ = self.election_tx.send(event);
    }

    pub async fn get_leader(&self) -> Option<String> {
        self.leader_id.lock().await.clone()
    }

    pub async fn promote(&self) -> Result<(), ElectionError> {
        if self.is_leader() {
            return Ok(());
        }

        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.set_state(ElectionState::Leader);

        let mut leader = self.leader_id.lock().await;
        *leader = Some(self.member.id.clone());

        self.stats.elections_won.fetch_add(1, Ordering::SeqCst);
        self.stats.leader_changes.fetch_add(1, Ordering::SeqCst);

        let event = ElectionEvent {
            event_type: ElectionEventType::LeaderElected,
            leader_id: Some(self.member.id.clone()),
            term: self.current_term.load(Ordering::SeqCst),
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
        };
        let _ = self.election_tx.send(event);

        info!("Member {} promoted to leader (term: {})", self.member.id, self.current_term.load(Ordering::SeqCst));
        Ok(())
    }

    pub async fn demote(&self) {
        if self.is_leader() {
            self.set_state(ElectionState::Follower);
            *self.leader_id.lock().await = None;
            self.stats.leader_changes.fetch_add(1, Ordering::SeqCst);
            info!("Leader {} stepped down", self.member.id);
        }
    }

    pub async fn heartbeat(&self) -> Result<(), ElectionError> {
        if !self.is_leader() {
            return Err(ElectionError::NotLeader);
        }

        self.stats.heartbeats_sent.fetch_add(1, Ordering::SeqCst);

        for member in self.members.read().await.iter() {
            let _ = Self::send_heartbeat(member, self.current_term.load(Ordering::SeqCst), &self.member.id).await;
        }

        Ok(())
    }

    async fn send_heartbeat(member: &ElectionMember, term: u64, leader_id: &str) -> Result<(), ElectionError> {
        Ok(())
    }

    fn start_heartbeat_monitor(&self) {
        let members = self.members.clone();
        let current_term = self.current_term.clone();
        let leader_id = self.leader_id.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();
        let shutdown_rx = self.shutdown.subscribe();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.heartbeat_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !is_running.load(Ordering::SeqCst) {
                            break;
                        }

                        let leader = leader_id.lock().await;
                        if let Some(id) = &*leader {
                            drop(leader);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    pub async fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);

        if self.is_leader() {
            self.demote().await;
        }

        let _ = self.shutdown.send(());
        info!("LeaderElection stopped for member: {}", self.member.id);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ElectionEvent> {
        self.election_tx.subscribe()
    }

    pub async fn get_stats(&self) -> HashMap<String, String> {
        let mut stats = HashMap::new();
        stats.insert("elections_started".to_string(), self.stats.elections_started.load(Ordering::SeqCst).to_string());
        stats.insert("elections_won".to_string(), self.stats.elections_won.load(Ordering::SeqCst).to_string());
        stats.insert("elections_lost".to_string(), self.stats.elections_lost.load(Ordering::SeqCst).to_string());
        stats.insert("terms_incremented".to_string(), self.stats.terms_incremented.load(Ordering::SeqCst).to_string());
        stats.insert("votes_received".to_string(), self.stats.votes_received.load(Ordering::SeqCst).to_string());
        stats.insert("votes_granted".to_string(), self.stats.votes_granted.load(Ordering::SeqCst).to_string());
        stats.insert("votes_denied".to_string(), self.stats.votes_denied.load(Ordering::SeqCst).to_string());
        stats.insert("heartbeats_sent".to_string(), self.stats.heartbeats_sent.load(Ordering::SeqCst).to_string());
        stats.insert("heartbeats_received".to_string(), self.stats.heartbeats_received.load(Ordering::SeqCst).to_string());
        stats.insert("leader_changes".to_string(), self.stats.leader_changes.load(Ordering::SeqCst).to_string());
        stats.insert("state_transitions".to_string(), self.stats.state_transitions.load(Ordering::SeqCst).to_string());
        stats.insert("current_term".to_string(), self.current_term.load(Ordering::SeqCst).to_string());
        stats.insert("member_count".to_string(), self.members.read().await.len().to_string());

        let leadership_time = self.leadership_start_time.lock().await;
        if let Some(start) = *leadership_time {
            stats.insert("current_leadership_duration_ms".to_string(), start.elapsed().as_millis().to_string());
        }

        stats
    }
}

#[derive(Debug, Clone)]
struct VoteResponse {
    granted: bool,
    term: u64,
    debug_message: String,
}

impl ElectionState {
    fn from_usize(value: usize) -> Self {
        match value {
            0 => ElectionState::Follower,
            1 => ElectionState::Candidate,
            2 => ElectionState::Leader,
            _ => ElectionState::Observer,
        }
    }
}

pub struct ElectionObserver {
    election: LeaderElection,
    shutdown: broadcast::Sender<()>,
}

impl ElectionObserver {
    pub fn new(election: LeaderElection) -> Self {
        Self {
            election,
            shutdown: broadcast::channel(1).0,
        }
    }

    pub async fn observe(&self) -> Result<(), ElectionError> {
        let mut rx = self.election.subscribe();
        let shutdown_rx = self.shutdown.subscribe();

        loop {
            tokio::select! {
                Ok(event) = rx.recv() => {
                    self.handle_event(&event).await;
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_event(&self, event: &ElectionEvent) {
        match event.event_type {
            ElectionEventType::LeaderElected => {
                info!("Leader elected: {:?}", event.leader_id);
            }
            ElectionEventType::LeaderChanged => {
                info!("Leader changed from {:?}", event.leader_id);
            }
            ElectionEventType::ElectionLost => {
                warn!("Election lost");
            }
            _ => {}
        }
    }

    pub fn stop(&self) {
        let _ = self.shutdown.send(());
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "LeaderElection stats - Won: {}, Lost: {}, Changes: {}, Term: {}",
            stats.elections_won.load(Ordering::SeqCst),
            stats.elections_lost.load(Ordering::SeqCst),
            stats.leader_changes.load(Ordering::SeqCst),
            self.current_term.load(Ordering::SeqCst)
        );
    }
}
