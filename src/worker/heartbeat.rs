use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use tokio::sync::{mpsc, broadcast, Mutex};
use tokio::time::{interval, timeout};
use uuid::Uuid;
use dashmap::DashMap;
use rand::Rng;

#[derive(Error, Debug)]
pub enum HeartbeatError {
    #[error("Worker error: {0}")]
    WorkerError(String),
    
    #[error("Registration error: {0}")]
    RegistrationError(String),
    
    #[error("Timeout error: {0}")]
    TimeoutError(String),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub miss_count_threshold: u32,
    pub enable_auto_register: bool,
    pub registration_retries: u32,
    pub registration_delay: Duration,
    pub enable_leader_election: bool,
    pub lease_duration: Duration,
    pub renew_interval: Duration,
    pub enable_monitoring: bool,
    pub metrics_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub worker_id: String,
    pub name: String,
    pub endpoint: String,
    pub status: WorkerStatus,
    pub capabilities: Vec<String>,
    pub resources: WorkerResources,
    pub labels: HashMap<String, String>,
    pub metadata: WorkerMetadata,
    pub last_heartbeat: u64,
    pub registered_at: u64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WorkerStatus {
    Online,
    Offline,
    Busy,
    Draining,
    Maintenance,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerResources {
    pub cpu_cores: u32,
    pub cpu_frequency_mhz: u64,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub gpu_count: u32,
    pub gpu_memory_bytes: u64,
    pub network_bandwidth_mbps: u64,
    pub disk_io_ps: u64,
    pub max_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetadata {
    pub hostname: String,
    pub ip_address: String,
    pub mac_address: String,
    pub os_version: String,
    pub kernel_version: String,
    pub uptime_seconds: u64,
    pub timezone: String,
    pub tags: Vec<String>,
    pub custom_fields: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub worker_id: String,
    pub timestamp: u64,
    pub sequence: u64,
    pub status: WorkerStatus,
    pub load: WorkerLoad,
    pub metrics: WorkerMetrics,
    pub metadata: HeartbeatMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLoad {
    pub cpu_percent: f32,
    pub memory_percent: f32,
    pub disk_percent: f32,
    pub network_percent: f32,
    pub active_tasks: u32,
    pub queued_tasks: u32,
    pub load_average_1m: f64,
    pub load_average_5m: f64,
    pub load_average_15m: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetrics {
    pub tasks_processed: u64,
    pub tasks_succeeded: u64,
    pub tasks_failed: u64,
    pub tasks_retried: u64,
    pub total_execution_time_ms: u64,
    pub avg_execution_time_ms: f64,
    pub total_bytes_processed: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_recv: u64,
    pub disk_bytes_read: u64,
    pub disk_bytes_written: u64,
    pub gc_pause_ns: u64,
    pub gc_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMetadata {
    pub version: String,
    pub uptime_seconds: u64,
    pub process_id: u32,
    pub thread_count: u32,
    pub open_files: u32,
    pub connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub worker_id: String,
    pub name: String,
    pub endpoint: String,
    pub capabilities: Vec<String>,
    pub resources: WorkerResources,
    pub labels: HashMap<String, String>,
    pub metadata: WorkerMetadata,
    pub registered_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerDeregistration {
    pub worker_id: String,
    pub reason: DeregistrationReason,
    pub graceful: bool,
    pub timestamp: u64,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DeregistrationReason {
    Shutdown,
    Maintenance,
    Failure,
    Manual,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLease {
    pub lease_id: String,
    pub worker_id: String,
    pub holder_id: String,
    pub lease_duration: Duration,
    pub acquired_at: u64,
    pub expires_at: u64,
    pub renewed_at: u64,
    pub status: LeaseStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum LeaseStatus {
    Active,
    Expired,
    Revoked,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEvent {
    pub event_type: WorkerEventType,
    pub worker_id: String,
    pub timestamp: u64,
    pub details: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WorkerEventType {
    Registered,
    Deregistered,
    HeartbeatReceived,
    StatusChanged,
    LeaseAcquired,
    LeaseExpired,
    LeaseRevoked,
    TaskAssigned,
    TaskCompleted,
    TaskFailed,
    ResourceWarning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStats {
    pub total_workers: u64,
    pub online_workers: u64,
    pub offline_workers: u64,
    pub busy_workers: u64,
    pub draining_workers: u64,
    pub avg_cpu_percent: f32,
    pub avg_memory_percent: f32,
    pub total_tasks_processed: u64,
    pub total_tasks_succeeded: u64,
    pub total_tasks_failed: u64,
    pub avg_tasks_per_worker: f64,
    pub heartbeat_rate: f64,
}

#[derive(Debug, Clone)]
pub struct WorkerState {
    worker: Worker,
    missed_heartbeats: u32,
    consecutive_failures: u32,
    last_successful_heartbeat: u64,
}

pub struct HeartbeatManager {
    config: WorkerConfig,
    workers: Arc<DashMap<String, WorkerState>>,
    leases: Arc<DashMap<String, WorkerLease>>,
    local_worker: Arc<RwLock<Option<Worker>>>,
    event_sender: Arc<Mutex<Option<mpsc::Sender<WorkerEvent>>>>,
    broadcast_sender: Arc<broadcast::Sender<WorkerEvent>>,
    shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
    heartbeat_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    monitor_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    metrics: Arc<HeartbeatMetrics>,
}

struct HeartbeatMetrics {
    total_heartbeats: std::sync::atomic::AtomicU64,
    successful_heartbeats: std::sync::atomic::AtomicU64,
    missed_heartbeats: std::sync::atomic::AtomicU64,
    worker_registrations: std::sync::atomic::AtomicU64,
    worker_deregistrations: std::sync::atomic::AtomicU64,
    lease_acquisitions: std::sync::atomic::AtomicU64,
    lease_expirations: std::sync::atomic::AtomicU64,
    last_heartbeat_time: Mutex<Option<Instant>>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(30),
            miss_count_threshold: 3,
            enable_auto_register: true,
            registration_retries: 3,
            registration_delay: Duration::from_secs(5),
            enable_leader_election: false,
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            enable_monitoring: true,
            metrics_interval: Duration::from_secs(60),
        }
    }
}

impl Default for WorkerResources {
    fn default() -> Self {
        Self {
            cpu_cores: 4,
            cpu_frequency_mhz: 2400,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            storage_bytes: 100 * 1024 * 1024 * 1024,
            gpu_count: 0,
            gpu_memory_bytes: 0,
            network_bandwidth_mbps: 1000,
            disk_io_ps: 10000,
            max_tasks: 100,
        }
    }
}

impl Default for WorkerMetadata {
    fn default() -> Self {
        Self {
            hostname: String::new(),
            ip_address: String::new(),
            mac_address: String::new(),
            os_version: String::new(),
            kernel_version: String::new(),
            uptime_seconds: 0,
            timezone: String::new(),
            tags: Vec::new(),
            custom_fields: HashMap::new(),
        }
    }
}

impl Default for WorkerLoad {
    fn default() -> Self {
        Self {
            cpu_percent: 0.0,
            memory_percent: 0.0,
            disk_percent: 0.0,
            network_percent: 0.0,
            active_tasks: 0,
            queued_tasks: 0,
            load_average_1m: 0.0,
            load_average_5m: 0.0,
            load_average_15m: 0.0,
        }
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self {
            tasks_processed: 0,
            tasks_succeeded: 0,
            tasks_failed: 0,
            tasks_retried: 0,
            total_execution_time_ms: 0,
            avg_execution_time_ms: 0.0,
            total_bytes_processed: 0,
            network_bytes_sent: 0,
            network_bytes_recv: 0,
            disk_bytes_read: 0,
            disk_bytes_written: 0,
            gc_pause_ns: 0,
            gc_count: 0,
        }
    }
}

impl Default for HeartbeatMetadata {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            uptime_seconds: 0,
            process_id: 0,
            thread_count: 0,
            open_files: 0,
            connections: 0,
        }
    }
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self {
            total_workers: 0,
            online_workers: 0,
            offline_workers: 0,
            busy_workers: 0,
            draining_workers: 0,
            avg_cpu_percent: 0.0,
            avg_memory_percent: 0.0,
            total_tasks_processed: 0,
            total_tasks_succeeded: 0,
            total_tasks_failed: 0,
            avg_tasks_per_worker: 0.0,
            heartbeat_rate: 0.0,
        }
    }
}

impl HeartbeatManager {
    pub async fn new(config: Option<WorkerConfig>) -> Result<Self, HeartbeatError> {
        let config = config.unwrap_or_default();
        
        let workers = Arc::new(DashMap::new());
        let leases = Arc::new(DashMap::new());
        let local_worker = Arc::new(RwLock::new(None));
        
        let (event_sender, _) = mpsc::channel(1000);
        let (broadcast_sender, _) = broadcast::channel(100);
        
        let metrics = Arc::new(HeartbeatMetrics {
            total_heartbeats: std::sync::atomic::AtomicU64::new(0),
            successful_heartbeats: std::sync::atomic::AtomicU64::new(0),
            missed_heartbeats: std::sync::atomic::AtomicU64::new(0),
            worker_registrations: std::sync::atomic::AtomicU64::new(0),
            worker_deregistrations: std::sync::atomic::AtomicU64::new(0),
            lease_acquisitions: std::sync::atomic::AtomicU64::new(0),
            lease_expirations: std::sync::atomic::AtomicU64::new(0),
            last_heartbeat_time: Mutex::new(None),
        });
        
        Ok(Self {
            config,
            workers,
            leases,
            local_worker,
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            broadcast_sender: Arc::new(broadcast_sender),
            shutdown_signal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            heartbeat_task: Arc::new(Mutex::new(None)),
            monitor_task: Arc::new(Mutex::new(None)),
            metrics,
        })
    }
    
    pub async fn register_worker(&self, worker: Worker) -> Result<(), HeartbeatError> {
        let worker_id = worker.worker_id.clone();
        
        let state = WorkerState {
            worker: worker.clone(),
            missed_heartbeats: 0,
            consecutive_failures: 0,
            last_successful_heartbeat: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        self.workers.insert(worker_id, state);
        self.metrics.worker_registrations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        if let Some(ref mut local) = *self.local_worker.write() {
            if local.worker_id == worker_id {
                *local = worker;
            }
        } else {
            *self.local_worker.write().unwrap() = Some(worker);
        }
        
        self.send_event(WorkerEvent {
            event_type: WorkerEventType::Registered,
            worker_id,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            details: HashMap::new(),
        });
        
        info!("Worker registered: {} ({})", worker.name, worker_id);
        
        Ok(())
    }
    
    pub async fn deregister_worker(&self, worker_id: &str, reason: DeregistrationReason, graceful: bool) 
        -> Result<(), HeartbeatError> {
        if let Some((_, state)) = self.workers.remove(worker_id) {
            self.metrics.worker_deregistrations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            
            self.leases.remove(worker_id);
            
            self.send_event(WorkerEvent {
                event_type: WorkerEventType::Deregistered,
                worker_id: worker_id.to_string(),
                timestamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                details: HashMap::from([
                    ("reason".to_string(), format!("{:?}", reason)),
                    ("graceful".to_string(), graceful.to_string()),
                ]),
            });
            
            info!("Worker deregistered: {} (reason: {:?})", worker_id, reason);
        }
        
        Ok(())
    }
    
    pub async fn receive_heartbeat(&self, heartbeat: Heartbeat) -> Result<bool, HeartbeatError> {
        self.metrics.total_heartbeats.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Some(mut state) = self.workers.get_mut(&heartbeat.worker_id) {
            state.missed_heartbeats = 0;
            state.last_successful_heartbeat = now;
            state.worker.last_heartbeat = now;
            state.worker.last_seen = now;
            state.worker.status = heartbeat.status;
            
            self.metrics.successful_heartbeats.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.metrics.last_heartbeat_time.lock().unwrap() = Some(Instant::now());
            
            self.send_event(WorkerEvent {
                event_type: WorkerEventType::HeartbeatReceived,
                worker_id: heartbeat.worker_id,
                timestamp: now,
                details: HashMap::from([
                    ("status".to_string(), format!("{:?}", heartbeat.status)),
                    ("load_cpu".to_string(), heartbeat.load.cpu_percent.to_string()),
                    ("load_memory".to_string(), heartbeat.load.memory_percent.to_string()),
                ]),
            });
            
            if state.worker.status != heartbeat.status {
                self.send_event(WorkerEvent {
                    event_type: WorkerEventType::StatusChanged,
                    worker_id: heartbeat.worker_id,
                    timestamp: now,
                    details: HashMap::from([
                        ("old_status".to_string(), format!("{:?}", state.worker.status)),
                        ("new_status".to_string(), format!("{:?}", heartbeat.status)),
                    ]),
                });
            }
            
            return Ok(true);
        }
        
        self.metrics.missed_heartbeats.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        warn!("Heartbeat received from unknown worker: {}", heartbeat.worker_id);
        
        Ok(false)
    }
    
    pub async fn start_heartbeat(&self, heartbeat: Heartbeat) -> Result<(), HeartbeatError> {
        let shutdown = Arc::clone(&self.shutdown_signal);
        let worker_id = heartbeat.worker_id.clone();
        
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        });
        
        *self.heartbeat_task.lock().unwrap() = Some(task);
        
        info!("Heartbeat started for worker: {}", worker_id);
        
        Ok(())
    }
    
    pub async fn check_worker_health(&self, worker_id: &str) -> Result<WorkerHealth, HeartbeatError> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Some(state) = self.workers.get(worker_id) {
            let last_heartbeat_age = now - state.worker.last_heartbeat;
            
            if last_heartbeat_age > self.config.heartbeat_timeout.as_secs() {
                return Ok(WorkerHealth {
                    healthy: false,
                    status: HealthStatus::Stale,
                    last_heartbeat_seconds_ago: last_heartbeat_age,
                    missed_heartbeats: state.missed_heartbeats,
                    load: None,
                });
            }
            
            let is_overloaded = state.worker.status == WorkerStatus::Busy &&
                state.worker.last_seen < now.saturating_sub(60);
            
            return Ok(WorkerHealth {
                healthy: true,
                status: if is_overloaded { HealthStatus::Degraded } else { HealthStatus::Healthy },
                last_heartbeat_seconds_ago: last_heartbeat_age,
                missed_heartbeats: state.missed_heartbeats,
                load: None,
            });
        }
        
        Ok(WorkerHealth {
            healthy: false,
            status: HealthStatus::Unknown,
            last_heartbeat_seconds_ago: 0,
            missed_heartbeats: 0,
            load: None,
        })
    }
    
    pub async fn start_monitoring(&self) {
        let shutdown = Arc::clone(&self.shutdown_signal);
        
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        });
        
        *self.monitor_task.lock().unwrap() = Some(task);
        
        info!("Worker monitoring started");
    }
    
    pub async fn stop(&mut self) {
        self.shutdown_signal.store(true, std::sync::atomic::Ordering::SeqCst);
        
        if let Some(task) = self.heartbeat_task.lock().unwrap().take() {
            task.abort();
        }
        
        if let Some(task) = self.monitor_task.lock().unwrap().take() {
            task.abort();
        }
        
        info!("Heartbeat manager stopped");
    }
    
    pub async fn acquire_lease(&self, worker_id: &str, holder_id: &str) -> Result<WorkerLease, HeartbeatError> {
        let lease_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let lease = WorkerLease {
            lease_id: lease_id.clone(),
            worker_id: worker_id.to_string(),
            holder_id: holder_id.to_string(),
            lease_duration: self.config.lease_duration,
            acquired_at: now,
            expires_at: now + self.config.lease_duration.as_secs(),
            renewed_at: now,
            status: LeaseStatus::Active,
        };
        
        self.leases.insert(worker_id.to_string(), lease.clone());
        self.metrics.lease_acquisitions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        self.send_event(WorkerEvent {
            event_type: WorkerEventType::LeaseAcquired,
            worker_id: worker_id.to_string(),
            timestamp: now,
            details: HashMap::from([
                ("lease_id".to_string(), lease_id),
                ("holder_id".to_string(), holder_id.to_string()),
            ]),
        });
        
        info!("Lease acquired for worker {} by {}", worker_id, holder_id);
        
        Ok(lease)
    }
    
    pub async fn renew_lease(&self, worker_id: &str) -> Result<bool, HeartbeatError> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Some(mut lease) = self.leases.get_mut(worker_id) {
            if lease.status == LeaseStatus::Active {
                lease.renewed_at = now;
                lease.expires_at = now + self.config.lease_duration.as_secs();
                return Ok(true);
            }
        }
        
        Ok(false)
    }
    
    pub async fn release_lease(&self, worker_id: &str) -> Result<bool, HeartbeatError> {
        if let Some((_, lease)) = self.leases.remove(worker_id) {
            self.send_event(WorkerEvent {
                event_type: WorkerEventType::LeaseExpired,
                worker_id: worker_id.to_string(),
                timestamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                details: HashMap::new(),
            });
            return Ok(true);
        }
        
        Ok(false)
    }
    
    pub async fn get_workers(&self, status_filter: Option<WorkerStatus>) -> Vec<Worker> {
        self.workers.iter()
            .filter(|s| {
                if let Some(filter) = status_filter {
                    s.worker.status == filter
                } else {
                    true
                }
            })
            .map(|s| s.worker.clone())
            .collect()
    }
    
    pub async fn get_online_workers(&self) -> Vec<Worker> {
        self.get_workers(Some(WorkerStatus::Online)).await
    }
    
    pub async fn get_worker(&self, worker_id: &str) -> Option<Worker> {
        self.workers.get(worker_id).map(|s| s.worker.clone())
    }
    
    pub async fn get_stats(&self) -> WorkerStats {
        let states: Vec<_> = self.workers.iter().collect();
        
        let online = states.iter().filter(|s| s.worker.status == WorkerStatus::Online).count() as u64;
        let offline = states.iter().filter(|s| s.worker.status == WorkerStatus::Offline).count() as u64;
        let busy = states.iter().filter(|s| s.worker.status == WorkerStatus::Busy).count() as u64;
        let draining = states.iter().filter(|s| s.worker.status == WorkerStatus::Draining).count() as u64;
        
        WorkerStats {
            total_workers: states.len() as u64,
            online_workers: online,
            offline_workers: offline,
            busy_workers: busy,
            draining_workers: draining,
            avg_cpu_percent: 0.0,
            avg_memory_percent: 0.0,
            total_tasks_processed: 0,
            total_tasks_succeeded: 0,
            total_tasks_failed: 0,
            avg_tasks_per_worker: if !states.is_empty() {
                states.len() as f64 / states.len() as f64
            } else {
                0.0
            },
            heartbeat_rate: 0.0,
        }
    }
    
    pub fn subscribe_events(&self) -> broadcast::Receiver<WorkerEvent> {
        self.broadcast_sender.subscribe()
    }
    
    fn send_event(&self, event: WorkerEvent) {
        let _ = self.broadcast_sender.send(event.clone());
        if let Some(ref sender) = *self.event_sender.lock().unwrap() {
            let _ = sender.send(event);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHealth {
    pub healthy: bool,
    pub status: HealthStatus,
    pub last_heartbeat_seconds_ago: u64,
    pub missed_heartbeats: u32,
    pub load: Option<WorkerLoad>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Stale,
    Unknown,
}

impl Default for WorkerRegistration {
    fn default() -> Self {
        Self {
            worker_id: Uuid::new_v4().to_string(),
            name: String::new(),
            endpoint: String::new(),
            capabilities: Vec::new(),
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            registered_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

impl Default for WorkerDeregistration {
    fn default() -> Self {
        Self {
            worker_id: String::new(),
            reason: DeregistrationReason::Unknown,
            graceful: true,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            message: String::new(),
        }
    }
}

impl Default for WorkerLease {
    fn default() -> Self {
        Self {
            lease_id: String::new(),
            worker_id: String::new(),
            holder_id: String::new(),
            lease_duration: Duration::from_secs(30),
            acquired_at: 0,
            expires_at: 0,
            renewed_at: 0,
            status: LeaseStatus::Active,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_manager_creation() {
        let manager = HeartbeatManager::new(None).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_worker_registration() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let worker = Worker {
            worker_id: "worker_1".to_string(),
            name: "Test Worker".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec!["cpu".to_string(), "memory".to_string()],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            registered_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let result = manager.register_worker(worker).await;
        assert!(result.is_ok());
        
        let workers = manager.get_workers(None).await;
        assert_eq!(workers.len(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_reception() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let worker = Worker {
            worker_id: "worker_2".to_string(),
            name: "Heartbeat Worker".to_string(),
            endpoint: "http://localhost:8081".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec!["cpu".to_string()],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: 0,
            registered_at: 0,
            last_seen: 0,
        };
        
        let _ = manager.register_worker(worker).await;
        
        let heartbeat = Heartbeat {
            worker_id: "worker_2".to_string(),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sequence: 1,
            status: WorkerStatus::Online,
            load: WorkerLoad::default(),
            metrics: WorkerMetrics::default(),
            metadata: HeartbeatMetadata::default(),
        };
        
        let result = manager.receive_heartbeat(heartbeat).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_worker_deregistration() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let worker = Worker {
            worker_id: "worker_3".to_string(),
            name: "Deregister Worker".to_string(),
            endpoint: "http://localhost:8082".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec![],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: 0,
            registered_at: 0,
            last_seen: 0,
        };
        
        let _ = manager.register_worker(worker).await;
        
        let result = manager.deregister_worker(
            "worker_3", 
            DeregistrationReason::Shutdown, 
            true
        ).await;
        
        assert!(result.is_ok());
        
        let workers = manager.get_workers(None).await;
        assert!(workers.is_empty());
    }

    #[tokio::test]
    async fn test_health_check() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let worker = Worker {
            worker_id: "worker_4".to_string(),
            name: "Health Check Worker".to_string(),
            endpoint: "http://localhost:8083".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec![],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            registered_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let _ = manager.register_worker(worker).await;
        
        let health = manager.check_worker_health("worker_4").await;
        assert!(health.is_ok());
        
        let health = health.unwrap();
        assert!(health.healthy);
        assert_eq!(health.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_lease_acquisition() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let lease = manager.acquire_lease("worker_5", "leader_1").await;
        assert!(lease.is_ok());
        
        let lease = lease.unwrap();
        assert_eq!(lease.holder_id, "leader_1");
        assert_eq!(lease.status, LeaseStatus::Active);
    }

    #[tokio::test]
    async fn test_lease_renewal() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let _ = manager.acquire_lease("worker_6", "leader_2").await.unwrap();
        
        let renewed = manager.renew_lease("worker_6").await;
        assert!(renewed.is_ok());
        assert!(renewed.unwrap());
    }

    #[tokio::test]
    async fn test_lease_release() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let _ = manager.acquire_lease("worker_7", "leader_3").await.unwrap();
        
        let released = manager.release_lease("worker_7").await;
        assert!(released.is_ok());
        assert!(released.unwrap());
    }

    #[tokio::test]
    async fn test_worker_stats() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_workers, 0);
        assert_eq!(stats.online_workers, 0);
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let mut subscription = manager.subscribe_events();
        
        let worker = Worker {
            worker_id: "event_worker".to_string(),
            name: "Event Worker".to_string(),
            endpoint: "http://localhost:8084".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec![],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: 0,
            registered_at: 0,
            last_seen: 0,
        };
        
        let _ = manager.register_worker(worker).await;
        
        tokio::spawn(async move {
            let event = subscription.recv().await;
            assert!(event.is_ok());
        });
    }

    #[tokio::test]
    async fn test_get_online_workers() {
        let manager = HeartbeatManager::new(None).await.unwrap();
        
        let worker1 = Worker {
            worker_id: "online_1".to_string(),
            name: "Online Worker 1".to_string(),
            endpoint: "http://localhost:8085".to_string(),
            status: WorkerStatus::Online,
            capabilities: vec![],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: 0,
            registered_at: 0,
            last_seen: 0,
        };
        
        let worker2 = Worker {
            worker_id: "offline_1".to_string(),
            name: "Offline Worker".to_string(),
            endpoint: "http://localhost:8086".to_string(),
            status: WorkerStatus::Offline,
            capabilities: vec![],
            resources: WorkerResources::default(),
            labels: HashMap::new(),
            metadata: WorkerMetadata::default(),
            last_heartbeat: 0,
            registered_at: 0,
            last_seen: 0,
        };
        
        let _ = manager.register_worker(worker1).await;
        let _ = manager.register_worker(worker2).await;
        
        let online_workers = manager.get_online_workers().await;
        assert_eq!(online_workers.len(), 1);
        assert_eq!(online_workers[0].worker_id, "online_1");
    }
}
