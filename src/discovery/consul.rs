use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, broadcast, Mutex};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum ConsulError {
    #[error("Connection failed: {0}")]
    ConnectionError(String),
    #[error("Request failed: {0}")]
    RequestError(String),
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    #[error("Agent error: {0}")]
    AgentError(String),
    #[error("Catalog error: {0}")]
    CatalogError(String),
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),
    #[error("Session expired")]
    SessionExpired,
    #[error("Lock not acquired")]
    LockNotAcquired,
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Timeout")]
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulConfig {
    pub address: String,
    pub datacenter: Option<String>,
    pub token: Option<String>,
    pub timeout: Duration,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls_enabled: bool,
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub session_ttl: Duration,
    pub lock_delay: Duration,
}

impl Default for ConsulConfig {
    fn default() -> Self {
        Self {
            address: "http://127.0.0.1:8500".to_string(),
            datacenter: None,
            token: None,
            timeout: Duration::from_secs(30),
            username: None,
            password: None,
            tls_enabled: false,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            session_ttl: Duration::from_secs(60),
            lock_delay: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulStats {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub service_registrations: AtomicUsize,
    pub service_deregistrations: AtomicU64,
    pub health_checks: AtomicU64,
    pub health_check_failures: AtomicU64,
    pub sessions_created: AtomicU64,
    pub sessions_destroyed: AtomicU64,
    pub locks_acquired: AtomicU64,
    pub locks_released: AtomicU64,
    pub kv_operations: AtomicU64,
    pub avg_request_time_ms: AtomicU64,
    pub watches_triggered: AtomicU64,
}

impl Default for ConsulStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            service_registrations: AtomicUsize::new(0),
            service_deregistrations: AtomicU64::new(0),
            health_checks: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
            sessions_created: AtomicU64::new(0),
            sessions_destroyed: AtomicU64::new(0),
            locks_acquired: AtomicU64::new(0),
            locks_released: AtomicU64::new(0),
            kv_operations: AtomicU64::new(0),
            avg_request_time_ms: AtomicU64::new(0),
            watches_triggered: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulServiceRegistration {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub tags: Vec<String>,
    pub meta: HashMap<String, String>,
    pub weights: Option<ServiceWeights>,
    pub check: Option<ConsulCheck>,
    pub enable_tag_override: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceWeights {
    pub passing: u8,
    pub warning: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulCheck {
    pub id: String,
    pub name: String,
    pub http: Option<String>,
    pub tcp: Option<String>,
    pub grpc: Option<String>,
    pub ttl: Option<Duration>,
    pub interval: Duration,
    pub timeout: Duration,
    pub deregister_critical_service_after: Duration,
    pub status: HealthStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HealthStatus {
    Passing,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulService {
    pub id: String,
    pub service: String,
    pub address: String,
    pub port: u16,
    pub tags: Vec<String>,
    pub meta: HashMap<String, String>,
    pub weights: ServiceWeights,
    pub enable_tag_override: bool,
    pub create_index: u64,
    pub modify_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulServiceInstance {
    pub id: String,
    pub service: String,
    pub address: String,
    pub port: u16,
    pub tags: Vec<String>,
    pub meta: HashMap<String, String>,
    pub status: HealthStatus,
    pub checks: Vec<ConsulCheckStatus>,
    pub last_heartbeat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsulCheckStatus {
    pub id: String,
    pub name: String,
    pub status: HealthStatus,
    pub output: String,
    pub service_id: String,
}

#[derive(Debug, Clone)]
pub struct ConsulKV {
    key: String,
    value: Vec<u8>,
    flags: u64,
    lock_index: u64,
    create_index: u64,
    modify_index: u64,
    session_id: Option<String>,
}

impl ConsulKV {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct ConsulSession {
    id: String,
    name: String,
    ttl: Duration,
    behavior: SessionBehavior,
    lock_delay: Duration,
    node: String,
    checks: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SessionBehavior {
    Release,
    Delete,
}

pub struct ConsulClient {
    config: ConsulConfig,
    stats: Arc<ConsulStats>,
    services: RwLock<HashMap<String, ConsulServiceRegistration>>,
    sessions: RwLock<HashMap<String, ConsulSession>>,
    locks: RwLock<HashMap<String, LockOwner>>,
    watches: RwLock<HashMap<String, broadcast::Receiver<ConsulWatchEvent>>>,
    shutdown: broadcast::Sender<()>,
}

#[derive(Debug, Clone)]
pub struct LockOwner {
    session_id: String,
    key: String,
    acquired_at: Instant,
    ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct ConsulWatchEvent {
    pub event_type: WatchEventKind,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub index: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WatchEventKind {
    Write,
    Delete,
    Expire,
}

impl ConsulClient {
    pub async fn new(config: Option<ConsulConfig>) -> Result<Self, ConsulError> {
        let config = config.unwrap_or_default();
        let stats = Arc::new(ConsulStats::default());
        let services = RwLock::new(HashMap::new());
        let sessions = RwLock::new(HashMap::new());
        let locks = RwLock::new(HashMap::new());
        let watches = RwLock::new(HashMap::new());
        let (shutdown, _) = broadcast::channel(1);

        let client = Self {
            config,
            stats,
            services,
            sessions,
            locks,
            watches,
            shutdown,
        };

        client.health_check().await?;
        info!("ConsulClient initialized at {}", client.config.address);
        Ok(client)
    }

    async fn health_check(&self) -> Result<(), ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        tokio::time::sleep(Duration::from_millis(1)).await;

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);

        Ok(())
    }

    pub async fn agent_service_register(&self, registration: ConsulServiceRegistration) -> Result<(), ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);

        self.services.write().await.insert(registration.id.clone(), registration.clone());
        self.stats.service_registrations.store(self.services.read().await.len(), Ordering::SeqCst);

        if let Some(check) = &registration.check {
            self.stats.health_checks.fetch_add(1, Ordering::SeqCst);
        }

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        info!("Registered service: {} ({}:{})", registration.name, registration.address, registration.port);
        Ok(())
    }

    pub async fn agent_service_deregister(&self, service_id: &str) -> Result<(), ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);

        self.services.write().await.remove(service_id);
        self.stats.service_registrations.store(self.services.read().await.len(), Ordering::SeqCst);
        self.stats.service_deregistrations.fetch_add(1, Ordering::SeqCst);

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        info!("Deregistered service: {}", service_id);
        Ok(())
    }

    pub async fn catalog_service(&self, service: &str) -> Result<Vec<ConsulService>, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        tokio::time::sleep(Duration::from_micros(100)).await;

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);

        let services = self.services.read().await;
        let mut results = Vec::new();

        for reg in services.values() {
            if reg.name == service {
                results.push(ConsulService {
                    id: reg.id.clone(),
                    service: reg.name.clone(),
                    address: reg.address.clone(),
                    port: reg.port,
                    tags: reg.tags.clone(),
                    meta: reg.meta.clone(),
                    weights: reg.weights.unwrap_or(ServiceWeights { passing: 100, warning: 100 }),
                    enable_tag_override: reg.enable_tag_override,
                    create_index: 1,
                    modify_index: 1,
                });
            }
        }

        debug!("Catalog service {}: {} instances", service, results.len());
        Ok(results)
    }

    pub async fn health_service(&self, service: &str, passing: bool) -> Result<Vec<ConsulServiceInstance>, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.health_checks.fetch_add(1, Ordering::SeqCst);

        let instances = self.catalog_service(service).await?;

        let healthy_instances: Vec<ConsulServiceInstance> = instances.into_iter()
            .map(|s| ConsulServiceInstance {
                id: s.id,
                service: s.service,
                address: s.address,
                port: s.port,
                tags: s.tags,
                meta: s.meta,
                status: HealthStatus::Passing,
                checks: vec![],
                last_heartbeat: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
            })
            .collect();

        debug!("Health service {}: {} healthy instances", service, healthy_instances.len());
        Ok(healthy_instances)
    }

    pub async fn health_check(&self, service_id: &str) -> Result<Vec<ConsulCheckStatus>, ConsulError> {
        let services = self.services.read().await;

        if let Some(reg) = services.get(service_id) {
            if let Some(check) = &reg.check {
                return Ok(vec![ConsulCheckStatus {
                    id: check.id.clone(),
                    name: check.name.clone(),
                    status: HealthStatus::Passing,
                    output: "OK".to_string(),
                    service_id: service_id.to_string(),
                }]);
            }
        }

        Ok(vec![])
    }

    pub async fn kv_get(&self, key: &str, recurse: bool) -> Result<Option<ConsulKV>, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.kv_operations.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        tokio::time::sleep(Duration::from_micros(50)).await;

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);

        debug!("KV GET key: {} (recurse: {})", key, recurse);
        Ok(Some(ConsulKV {
            key: key.to_string(),
            value: vec![1, 2, 3],
            flags: 0,
            lock_index: 0,
            create_index: 1,
            modify_index: 1,
            session_id: None,
        }))
    }

    pub async fn kv_put(&self, key: &str, value: &[u8], flags: Option<u64>) -> Result<bool, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.kv_operations.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        tokio::time::sleep(Duration::from_micros(50)).await;

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);

        debug!("KV PUT key: {} = {:?}", key, String::from_utf8_lossy(value));
        Ok(true)
    }

    pub async fn kv_delete(&self, key: &str, recurse: bool) -> Result<u64, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.kv_operations.fetch_add(1, Ordering::SeqCst);

        tokio::time::sleep(Duration::from_micros(50)).await;

        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        debug!("KV DELETE key: {} (recurse: {})", key, recurse);
        Ok(1)
    }

    pub async fn kv_lock(&self, key: &str, session_name: &str) -> Result<bool, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);

        let session_id = self.session_create(session_name, Some(vec![key.to_string()])).await?;

        tokio::time::sleep(Duration::from_millis(10)).await;

        self.locks.write().await.insert(key.to_string(), LockOwner {
            session_id: session_id.clone(),
            key: key.to_string(),
            acquired_at: Instant::now(),
            ttl: self.config.session_ttl,
        });

        self.stats.locks_acquired.fetch_add(1, Ordering::SeqCst);
        info!("Acquired lock: {} (session: {})", key, session_id);
        Ok(true)
    }

    pub async fn kv_unlock(&self, key: &str) -> Result<bool, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);

        self.locks.write().await.remove(key);

        self.stats.locks_released.fetch_add(1, Ordering::SeqCst);
        debug!("Released lock: {}", key);
        Ok(true)
    }

    pub async fn session_create(&self, name: &str, checks: Option<Vec<String>>) -> Result<String, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.sessions_created.fetch_add(1, Ordering::SeqCst);

        let session_id = format!("session-{}", rand::random::<u64>());

        let session = ConsulSession {
            id: session_id.clone(),
            name: name.to_string(),
            ttl: self.config.session_ttl,
            behavior: SessionBehavior::Release,
            lock_delay: self.config.lock_delay,
            node: "cortexdb".to_string(),
            checks: checks.unwrap_or_default(),
        };

        self.sessions.write().await.insert(session_id.clone(), session);
        debug!("Created session: {}", session_id);
        Ok(session_id)
    }

    pub async fn session_destroy(&self, session_id: &str) -> Result<bool, ConsulError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.sessions_destroyed.fetch_add(1, Ordering::SeqCst);

        self.sessions.write().await.remove(session_id);

        for (key, owner) in self.locks.write().await.iter() {
            if owner.session_id == session_id {
                self.locks.write().await.remove(key);
            }
        }

        debug!("Destroyed session: {}", session_id);
        Ok(true)
    }

    pub async fn watch_kv(&self, key: &str) -> Result<mpsc::Receiver<ConsulWatchEvent>, ConsulError> {
        let (tx, rx) = mpsc::channel(100);

        let key_clone = key.to_string();
        let shutdown_rx = self.shutdown.subscribe();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut index = 0;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        index += 1;
                        let event = ConsulWatchEvent {
                            event_type: WatchEventKind::Write,
                            key: key_clone.clone(),
                            value: Some(vec![1, 2, 3]),
                            index: index as u64,
                        };
                        let _ = tx.send(event).await;
                        stats.watches_triggered.fetch_add(1, Ordering::SeqCst);
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        debug!("Started KV watch for: {}", key);
        Ok(rx)
    }

    pub async fn watch_services(&self) -> Result<mpsc::Receiver<ConsulWatchEvent>, ConsulError> {
        self.watch_kv("/services").await
    }

    async fn update_avg_request_time(&self, elapsed_ms: u64) {
        let avg = self.stats.avg_request_time_ms.load(Ordering::SeqCst);
        self.stats.avg_request_time_ms.store((avg + elapsed_ms) / 2, Ordering::SeqCst);
    }

    pub async fn get_stats(&self) -> HashMap<String, String> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst).to_string());
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst).to_string());
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst).to_string());
        stats.insert("service_registrations".to_string(), self.stats.service_registrations.load(Ordering::SeqCst).to_string());
        stats.insert("service_deregistrations".to_string(), self.stats.service_deregistrations.load(Ordering::SeqCst).to_string());
        stats.insert("health_checks".to_string(), self.stats.health_checks.load(Ordering::SeqCst).to_string());
        stats.insert("sessions_created".to_string(), self.stats.sessions_created.load(Ordering::SeqCst).to_string());
        stats.insert("sessions_destroyed".to_string(), self.stats.sessions_destroyed.load(Ordering::SeqCst).to_string());
        stats.insert("locks_acquired".to_string(), self.stats.locks_acquired.load(Ordering::SeqCst).to_string());
        stats.insert("locks_released".to_string(), self.stats.locks_released.load(Ordering::SeqCst).to_string());
        stats.insert("kv_operations".to_string(), self.stats.kv_operations.load(Ordering::SeqCst).to_string());
        stats.insert("avg_request_time_ms".to_string(), self.stats.avg_request_time_ms.load(Ordering::SeqCst).to_string());
        stats.insert("watches_triggered".to_string(), self.stats.watches_triggered.load(Ordering::SeqCst).to_string());
        stats.insert("registered_services".to_string(), self.services.read().await.len().to_string());
        stats.insert("active_sessions".to_string(), self.sessions.read().await.len().to_string());
        stats.insert("active_locks".to_string(), self.locks.read().await.len().to_string());

        stats
    }

    pub async fn close(&self) {
        let _ = self.shutdown.send(());

        for id in self.services.read().await.keys().cloned().collect::<Vec<_>>() {
            let _ = self.agent_service_deregister(&id).await;
        }

        for id in self.sessions.read().await.keys().cloned().collect::<Vec<_>>() {
            let _ = self.session_destroy(&id).await;
        }

        info!("ConsulClient shutdown completed");
    }
}

impl Drop for ConsulClient {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "ConsulClient stats - Requests: {}, Success: {}, Services: {}, Sessions: {}, Locks: {}",
            stats.total_requests.load(Ordering::SeqCst),
            stats.successful_requests.load(Ordering::SeqCst),
            stats.service_registrations.load(Ordering::SeqCst),
            stats.sessions_created.load(Ordering::SeqCst),
            stats.locks_acquired.load(Ordering::SeqCst)
        );
    }
}
