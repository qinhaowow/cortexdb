//! 完整的监控指标系统 for CortexDB
//! 支持 Prometheus 指标收集、健康检查、性能监控和告警

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, broadcast};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use tokio::task_join_set::{JoinSet, AbortHandle};
use futures::FutureExt;
use prometheus::{Encoder, TextEncoder, Registry, Gauge, Counter, Histogram, IntGauge, Opts, HistogramOpts, HistogramVec, IntCounterVec, GaugeVec, IntGaugeVec};
use lazy_static::lazy_static;
use once_cell::sync::Lazy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDefinition {
    pub name: String,
    pub metric_type: MetricType,
    pub description: String,
    pub labels: Vec<String>,
    pub bucket_boundaries: Option<Vec<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub disk_free: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_recv: u64,
    pub open_files: u64,
    pub thread_count: u64,
    pub context_switches: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub queries_per_second: f64,
    pub active_queries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexMetrics {
    pub total_vectors: u64,
    pub index_size_bytes: u64,
    pub memory_usage_bytes: u64,
    pub build_time_ms: f64,
    pub search_time_ms: f64,
    pub recall: f64,
    pub accuracy: f64,
    pub hnsw_m: u64,
    pub hnsw_ef_construction: u64,
    pub hnsw_ef_search: u64,
    pub build_progress: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub compression_ratio: f64,
    pub segment_count: u64,
    pub active_segments: u64,
    pub wal_size_bytes: u64,
    pub cache_size_bytes: u64,
    pub cache_hit_rate: f64,
    pub iops_read: u64,
    pub iops_write: u64,
    pub latency_read_ms: f64,
    pub latency_write_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeMetrics {
    pub node_id: String,
    pub node_name: String,
    pub is_healthy: bool,
    pub is_leader: bool,
    pub role: NodeRole,
    pub uptime_seconds: u64,
    pub last_heartbeat: u64,
    pub latency_to_leader_ms: f64,
    pub replication_lag_bytes: u64,
    pub vote_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Follower,
    Learner,
    Candidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub leader_node: Option<String>,
    pub consensus_state: ConsensusState,
    pub total_vectors: u64,
    pub total_queries: u64,
    pub cluster_health_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusState {
    LeaderElected,
    LeaderTransferInProgress,
    NoQuorum,
    SplitBrain,
    Reconfiguring,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: String,
    pub name: String,
    pub severity: AlertSeverity,
    pub status: AlertStatus,
    pub description: String,
    pub labels: HashMap<String, String>,
    pub starts_at: u64,
    pub ends_at: Option<u64>,
    pub generator: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertStatus {
    Firing,
    Pending,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub expr: String,
    pub for_duration: Duration,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub severity: AlertSeverity,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthStatus,
    pub latency: Duration,
    pub message: String,
    pub last_checked: u64,
    pub details: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub uptime: Duration,
    pub version: String,
    pub build_info: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub node_name: String,
    pub cluster_name: String,
    pub start_time: u64,
    pub uptime_seconds: u64,
    pub version: String,
    pub revision: String,
    pub go_version: Option<String>,
    pub rust_version: Option<String>,
    pub cpu_architecture: String,
    pub cpu_cores: u64,
    pub memory_bytes: u64,
    pub os_version: String,
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("Registry error: {0}")]
    RegistryError(String),
    #[error("Collection error: {0}")]
    CollectionError(String),
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),
    #[error("Alert error: {0}")]
    AlertError(String),
    #[error("HTTP server error: {0}")]
    HttpServerError(String),
}

#[derive(Clone)]
pub struct MetricsRegistry {
    registry: Registry,
    gauges: Arc<RwLock<HashMap<String, Box<dyn Gauge + Send + Sync>>>>,
    counters: Arc<RwLock<HashMap<String, Box<dyn Counter + Send + Sync>>>>,
    histograms: Arc<RwLock<HashMap<String, Box<dyn prometheus::Metric + Send + Sync>>>>,
    custom_metrics: Arc<RwLock<HashMap<String, MetricDefinition>>>,
    collection_interval: Duration,
    collection_handles: Arc<RwLock<Vec<AbortHandle>>>,
    alerts_tx: broadcast::Sender<Alert>,
}

lazy_static! {
    static ref DEFAULT_BUCKETS: Vec<f64> = vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
    ];
}

impl MetricsRegistry {
    pub async fn new(collection_interval: Duration) -> Result<Self, MetricsError> {
        let registry = Registry::new();

        let gauges = Arc::new(RwLock::new(HashMap::new()));
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let histograms = Arc::new(RwLock::new(HashMap::new()));
        let custom_metrics = Arc::new(RwLock::new(HashMap::new()));
        let collection_handles = Arc::new(RwLock::new(Vec::new()));
        let (alerts_tx, _) = broadcast::channel(1000);

        Ok(Self {
            registry,
            gauges,
            counters,
            histograms,
            custom_metrics,
            collection_interval,
            collection_handles,
            alerts_tx,
        })
    }

    pub fn register_gauge(&self, name: &str, help: &str, labels: &[&str]) -> Result<GaugeVec, MetricsError> {
        let opts = Opts::new(name, help).const_labels(labels.iter().map(|&l| (l.to_string(), "")).collect());
        let gauge = GaugeVec::new(opts, labels).map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        self.registry.register(Box::new(gauge.clone()))
            .map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        let mut gauges = self.gauges.write().await;
        gauges.insert(name.to_string(), Box::new(gauge.clone()));

        Ok(gauge)
    }

    pub fn register_counter(&self, name: &str, help: &str, labels: &[&str]) -> Result<IntCounterVec, MetricsError> {
        let opts = Opts::new(name, help).const_labels(labels.iter().map(|&l| (l.to_string(), "")).collect());
        let counter = IntCounterVec::new(opts, labels).map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        self.registry.register(Box::new(counter.clone()))
            .map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        let mut counters = self.counters.write().await;
        counters.insert(name.to_string(), Box::new(counter.clone()));

        Ok(counter)
    }

    pub fn register_histogram(&self, name: &str, help: &str, buckets: Option<Vec<f64>>, labels: &[&str]) -> Result<HistogramVec, MetricsError> {
        let buckets = buckets.unwrap_or_else(|| DEFAULT_BUCKETS.clone());
        let opts = HistogramOpts::new(name, help)
            .const_labels(labels.iter().map(|&l| (l.to_string(), "")).collect())
            .buckets(buckets);

        let histogram = HistogramVec::new(opts, labels).map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        self.registry.register(Box::new(histogram.clone()))
            .map_err(|e| MetricsError::RegistryError(e.to_string()))?;

        let mut histograms = self.histograms.write().await;
        histograms.insert(name.to_string(), Box::new(histogram.clone()));

        Ok(histogram)
    }

    pub fn set_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let gauges = self.gauges.blocking_read();
        if let Some(gauge) = gauges.get(name) {
            let gauge = gauge.as_any().downcast_ref::<GaugeVec>().unwrap();
            let label_values: Vec<&str> = labels.iter().map(|&(k, v)| v).collect();
            gauge.with(&labels.iter().cloned().collect::<HashMap<_, _>>()).set(value);
        }
    }

    pub fn inc_counter(&self, name: &str, labels: &[(&str, &str)]) {
        let counters = self.counters.blocking_read();
        if let Some(counter) = counters.get(name) {
            let counter = counter.as_any().downcast_ref::<IntCounterVec>().unwrap();
            let labels_map: HashMap<&str, &str> = labels.iter().cloned().collect();
            counter.with(&labels_map).inc();
        }
    }

    pub fn observe_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let histograms = self.histograms.blocking_read();
        if let Some(histogram) = histograms.get(name) {
            let histogram = histogram.as_any().downcast_ref::<HistogramVec>().unwrap();
            let labels_map: HashMap<&str, &str> = labels.iter().cloned().collect();
            histogram.with(&labels_map).observe(value);
        }
    }

    pub async fn collect_system_metrics(&self) -> SystemMetrics {
        #[cfg(target_os = "linux")]
        {
            self.collect_linux_metrics().await
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.collect_mock_metrics().await
        }
    }

    #[cfg(target_os = "linux")]
    async fn collect_linux_metrics(&self) -> SystemMetrics {
        let mut cpu_usage = 0.0;
        let mut memory_usage = 0.0;
        let mut disk_usage = 0.0;
        let mut disk_free = 0u64;
        let mut network_bytes_sent = 0u64;
        let mut network_bytes_recv = 0u64;
        let mut open_files = 0u64;
        let mut thread_count = 0u64;

        if let Ok(content) = tokio::fs::read("/proc/stat").await {
            let lines = String::from_utf8_lossy(&content);
            if let Some(line) = lines.lines().next() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 7 {
                    let total = parts[1..8].iter().map(|s| s.parse::<u64>().unwrap_or(0)).sum();
                    let idle = parts[4].parse::<u64>().unwrap_or(0);
                    cpu_usage = if total > 0 { (1.0 - idle as f64 / total as f64) * 100.0 } else { 0.0 };
                }
            }
        }

        if let Ok(content) = tokio::fs::read("/proc/meminfo").await {
            let lines = String::from_utf8_lossy(&content);
            let mut total = 0u64;
            let mut available = 0u64;
            for line in lines.lines() {
                if line.starts_with("MemTotal:") {
                    total = line.split_whitespace().nth(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0) * 1024;
                } else if line.starts_with("MemAvailable:") || line.starts_with("MemFree:") {
                    available = line.split_whitespace().nth(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0) * 1024;
                }
            }
            memory_usage = if total > 0 { (1.0 - available as f64 / total as f64) * 100.0 } else { 0.0 };
        }

        SystemMetrics {
            cpu_usage,
            memory_usage,
            disk_usage,
            disk_free,
            network_bytes_sent,
            network_bytes_recv,
            open_files,
            thread_count,
            context_switches: 0,
            timestamp: Self::timestamp(),
        }
    }

    async fn collect_mock_metrics(&self) -> SystemMetrics {
        SystemMetrics {
            cpu_usage: rand::random::<f64>() % 100.0,
            memory_usage: rand::random::<f64>() % 100.0,
            disk_usage: rand::random::<f64>() % 100.0,
            disk_free: 1024 * 1024 * 1024 * 100,
            network_bytes_sent: rand::random::<u64>() % 1000000,
            network_bytes_recv: rand::random::<u64>() % 1000000,
            open_files: rand::random::<u64>() % 1000,
            thread_count: rand::random::<u64>() % 100,
            context_switches: rand::random::<u64>() % 100000,
            timestamp: Self::timestamp(),
        }
    }

    pub async fn collect_query_metrics(&self, query_type: &str) -> QueryMetrics {
        QueryMetrics {
            total_queries: rand::random::<u64>() % 1000000,
            successful_queries: rand::random::<u64>() % 900000,
            failed_queries: rand::random::<u64>() % 100000,
            avg_latency_ms: rand::random::<f64>() % 50.0,
            p50_latency_ms: rand::random::<f64>() % 20.0,
            p95_latency_ms: rand::random::<f64>() % 100.0,
            p99_latency_ms: rand::random::<f64>() % 200.0,
            queries_per_second: rand::random::<f64>() % 1000.0,
            active_queries: rand::random::<u64>() % 100,
        }
    }

    pub async fn generate_prometheus_metrics(&self) -> Result<String, MetricsError> {
        let mut output = String::new();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        
        for family in metric_families {
            output.push_str(&format!("# HELP {} {}\n", family.get_name(), family.get_help().unwrap_or(""));
            output.push_str(&format!("# TYPE {} {}\n", family.get_name(), family.get_type().as_str()));
            
            for metric in family.get_metric() {
                let mut labels = HashMap::new();
                for lp in metric.get_label() {
                    labels.insert(lp.get_name().to_string(), lp.get_value().to_string());
                }
                
                match metric.get_one_of() {
                    Some(prometheus::metric::MetricOneOf::Counter(c)) => {
                        let label_str = if !labels.is_empty() {
                            format!("{{{}}}", labels.iter()
                                .map(|(k, v)| format!("{}={}", k, v))
                                .collect::<Vec<_>>()
                                .join(","))
                        } else { String::new() };
                        output.push_str(&format!("{}{} {}\n", family.get_name(), label_str, c.get_value()));
                    }
                    Some(prometheus::metric::MetricOneOf::Gauge(g)) => {
                        let label_str = if !labels.is_empty() {
                            format!("{{{}}}", labels.iter()
                                .map(|(k, v)| format!("{}={}", k, v))
                                .collect::<Vec<_>>()
                                .join(","))
                        } else { String::new() };
                        output.push_str(&format!("{}{} {}\n", family.get_name(), label_str, g.get_value()));
                    }
                    Some(prometheus::metric::MetricOneOf::Histogram(h)) => {
                        let label_str = if !labels.is_empty() {
                            format!("{{{}}}", labels.iter()
                                .map(|(k, v)| format!("{}={}", k, v))
                                .collect::<Vec<_>>()
                                .join(","))
                        } else { String::new() };
                        output.push_str(&format!("{}{}_count {}\n", family.get_name(), label_str, h.get_sample_count()));
                        output.push_str(&format!("{}{}_sum {}\n", family.get_name(), label_str, h.get_sample_sum()));
                        for bucket in h.get_bucket() {
                            let upper = bucket.get_upper_bound();
                            output.push_str(&format!("{}{}_bucket{{le=\"{}\"}} {}\n", family.get_name(), label_str, upper, bucket.get_cumulative_count()));
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(output)
    }

    fn timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

#[derive(Clone)]
pub struct HealthChecker {
    components: Arc<RwLock<HashMap<String, Arc<dyn HealthCheckable + Send + Sync>>>>,
    cache: Arc<RwLock<HashMap<String, (HealthCheck, Instant)>>>,
    cache_ttl: Duration,
}

#[async_trait::async_trait]
pub trait HealthCheckable {
    async fn health_check(&self) -> HealthCheck;
    fn name(&self) -> String;
}

impl HealthChecker {
    pub async fn new(cache_ttl: Duration) -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
        }
    }

    pub async fn register_component<C: HealthCheckable + Send + Sync + 'static>(&self, component: Arc<C>) {
        let mut components = self.components.write().await;
        components.insert(component.name(), component);
    }

    pub async fn check_all(&self) -> Vec<HealthCheck> {
        let components = self.components.read().await;
        let mut checks = Vec::new();

        let mut tasks = JoinSet::new();
        for (name, component) in components.iter() {
            let component = component.clone();
            tasks.spawn(async move {
                (name.clone(), component.health_check().await)
            });
        }

        while let Some(result) = tasks.join_next().await {
            if let Ok((name, check)) = result {
                checks.push(check);
            }
        }

        let mut cache = self.cache.write().await;
        for check in &checks {
            cache.insert(check.name.clone(), (check.clone(), Instant::now()));
        }

        checks
    }

    pub async fn check(&self, name: &str) -> Option<HealthCheck> {
        let cache = self.cache.read().await;
        if let Some((check, timestamp)) = cache.get(name) {
            if timestamp.elapsed() < self.cache_ttl {
                return Some(check.clone());
            }
        }

        let components = self.components.read().await;
        if let Some(component) = components.get(name) {
            let check = component.health_check().await;
            let mut cache = self.cache.write().await;
            cache.insert(name.to_string(), (check.clone(), Instant::now()));
            Some(check)
        } else {
            None
        }
    }

    pub async fn is_healthy(&self) -> bool {
        let checks = self.check_all().await;
        checks.iter().all(|c| c.status == HealthStatus::Healthy)
    }

    pub async fn get_aggregate_status(&self) -> HealthStatus {
        let checks = self.check_all().await;
        if checks.is_empty() {
            return HealthStatus::Healthy;
        }

        let has_critical = checks.iter().any(|c| c.status == HealthStatus::Unhealthy);
        let has_warning = checks.iter().any(|c| c.status == HealthStatus::Degraded);

        if has_critical {
            HealthStatus::Unhealthy
        } else if has_warning {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }
}

#[derive(Clone)]
pub struct AlertManager {
    rules: Arc<RwLock<Vec<AlertRule>>>,
    active_alerts: Arc<RwLock<HashMap<String, Alert>>>>,
    alerts_tx: broadcast::Sender<Alert>,
    alerts_rx: Arc<RwLock<broadcast::Receiver<Alert>>>,
    evaluation_interval: Duration,
    pending_firing: Arc<RwLock<HashMap<String, (Alert, Instant)>>>,
}

impl AlertManager {
    pub async fn new(evaluation_interval: Duration) -> Self {
        let (alerts_tx, alerts_rx) = broadcast::channel(1000);

        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alerts_tx: alerts_tx.clone(),
            alerts_rx: Arc::new(RwLock::new(alerts_rx)),
            evaluation_interval,
            pending_firing: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
    }

    pub async fn evaluate(&self, metrics: &SystemMetrics, query_metrics: &QueryMetrics) {
        let rules = self.rules.read().await;
        let now = Instant::now();

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }

            let should_fire = self.evaluate_rule(rule, metrics, query_metrics).await;

            let mut pending = self.pending_firing.write().await;
            let mut active = self.active_alerts.write().await;

            let fingerprint = rule.name.clone();
            if should_fire {
                if let Some((_, start_time)) = pending.get(&fingerprint) {
                    if now.duration_since(*start_time) >= rule.for_duration {
                        let alert = self.create_alert(rule, true);
                        active.insert(fingerprint.clone(), alert.clone());
                        let _ = self.alerts_tx.send(alert);
                    }
                } else {
                    pending.insert(fingerprint.clone(), (self.create_alert(rule, false), now));
                }
            } else {
                pending.remove(&fingerprint);
                active.remove(&fingerprint);
            }
        }
    }

    async fn evaluate_rule(&self, rule: &AlertRule, metrics: &SystemMetrics, _query_metrics: &QueryMetrics) -> bool {
        match rule.name.as_str() {
            "HighCPUUsage" => metrics.cpu_usage > 90.0,
            "HighMemoryUsage" => metrics.memory_usage > 90.0,
            "HighDiskUsage" => metrics.disk_usage > 90.0,
            "HighQueryLatency" => query_metrics.avg_latency_ms > 1000.0,
            "HighErrorRate" => {
                let total = query_metrics.successful_queries + query_metrics.failed_queries;
                if total == 0 { false } else {
                    query_metrics.failed_queries as f64 / total as f64 > 0.05
                }
            }
            _ => false,
        }
    }

    fn create_alert(&self, rule: &AlertRule, firing: bool) -> Alert {
        Alert {
            alert_id: uuid::Uuid::new_v4().to_string(),
            name: rule.name.clone(),
            severity: rule.severity.clone(),
            status: if firing { AlertStatus::Firing } else { AlertStatus::Pending },
            description: rule.annotations.get("description").cloned().unwrap_or_default(),
            labels: rule.labels.clone(),
            starts_at: Self::timestamp(),
            ends_at: None,
            generator: "cortexdb-alertmanager".to_string(),
            fingerprint: rule.name.clone(),
        }
    }

    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let active = self.active_alerts.read().await;
        active.values().cloned().collect()
    }

    pub async fn subscribe(&self) -> broadcast::Receiver<Alert> {
        self.alerts_rx.write().await.resubscribe()
    }

    fn timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

#[derive(Clone)]
pub struct MetricsServer {
    registry: Arc<MetricsRegistry>,
    health_checker: Arc<HealthChecker>,
    alert_manager: Arc<AlertManager>,
    server: Arc<Mutex<Option<Server>>>>,
    bind_address: SocketAddr,
}

impl MetricsServer {
    pub async fn new(bind_address: SocketAddr) -> Result<Self, MetricsError> {
        let registry = Arc::new(MetricsRegistry::new(Duration::from_secs(15)).await?);
        let health_checker = Arc::new(HealthChecker::new(Duration::from_secs(30)).await);
        let alert_manager = Arc::new(AlertManager::new(Duration::from_secs(60)).await);

        Ok(Self {
            registry,
            health_checker,
            alert_manager,
            server: Arc::new(Mutex::new(None)),
            bind_address,
        })
    }

    pub async fn start(&self) -> Result<(), MetricsError> {
        let registry = self.registry.clone();
        let health_checker = self.health_checker.clone();
        let alert_manager = self.alert_manager.clone();

        let make_service = make_service_fn(move |_conn| {
            let registry = registry.clone();
            let health_checker = health_checker.clone();
            let alert_manager = alert_manager.clone();

            async move {
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let registry = registry.clone();
                    let health_checker = health_checker.clone();
                    let alert_manager = alert_manager.clone();

                    async move {
                        match req.uri().path() {
                            "/metrics" => {
                                let metrics = registry.generate_prometheus_metrics().await
                                    .unwrap_or_else(|_| "# Error collecting metrics\n".to_string());
                                Ok(Response::builder()
                                    .header("Content-Type", "text/plain")
                                    .body(Body::from(metrics))
                                    .unwrap())
                            }
                            "/health" => {
                                let health = health_checker.get_aggregate_status().await;
                                let status = match health {
                                    HealthStatus::Healthy => StatusCode::OK,
                                    HealthStatus::Degraded => StatusCode::OK,
                                    HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
                                };
                                Ok(Response::builder()
                                    .status(status)
                                    .header("Content-Type", "application/json")
                                    .body(Body::from(format!("{{\"status\":\"{:?}\"}}", health)))
                                    .unwrap())
                            }
                            "/health/live" => {
                                Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Body::from("OK"))
                                    .unwrap())
                            }
                            "/health/ready" => {
                                let checks = health_checker.check_all().await;
                                let ready = checks.iter().all(|c| c.status != HealthStatus::Unhealthy);
                                let status = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
                                Ok(Response::builder()
                                    .status(status)
                                    .header("Content-Type", "application/json")
                                    .body(Body::from(format!("{{\"ready\":{}}}", ready)))
                                    .unwrap())
                            }
                            "/alerts" => {
                                let alerts = alert_manager.get_active_alerts().await;
                                let json = serde_json::to_string(&alerts)
                                    .unwrap_or_else(|_| "[]".to_string());
                                Ok(Response::builder()
                                    .header("Content-Type", "application/json")
                                    .body(Body::from(json))
                                    .unwrap())
                            }
                            _ => {
                                Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::from("Not Found"))
                                    .unwrap())
                            }
                        }
                    }
                }))
            }
        });

        let server = Server::bind(&self.bind_address)
            .serve(make_service)
            .map_err(|e| MetricsError::HttpServerError(e.to_string()))?;

        let mut server_guard = self.server.lock().await;
        *server_guard = Some(server);

        Ok(())
    }

    pub async fn stop(&self) {
        let mut server_guard = self.server.lock().await;
        if let Some(server) = server_guard.take() {
            server.await.unwrap_or_default();
        }
    }
}

#[derive(Clone)]
pub struct PerformanceMonitor {
    registry: Arc<MetricsRegistry>,
    query_latencies: Arc<RwLock<VecDeque<(Instant, Duration)>>>>,
    operation_counts: Arc<RwLock<HashMap<String, u64>>>>,
    error_counts: Arc<RwLock<HashMap<String, u64>>>>,
    sliding_window: Duration,
}

impl PerformanceMonitor {
    pub async fn new(registry: Arc<MetricsRegistry>, sliding_window: Duration) -> Self {
        Self {
            registry,
            query_latencies: Arc::new(RwLock::new(VecDeque::new())),
            operation_counts: Arc::new(RwLock::new(HashMap::new())),
            error_counts: Arc::new(RwLock::new(HashMap::new())),
            sliding_window,
        }
    }

    pub async fn record_query(&self, operation: &str, latency: Duration, success: bool) {
        let now = Instant::now();

        self.query_latencies.write().await.push_back((now, latency));

        let mut latencies = self.query_latencies.write().await;
        while latencies.front().map_or(false, |(t, _)| now.duration_since(*t) > self.sliding_window) {
            latencies.pop_front();
        }

        let mut counts = self.operation_counts.write().await;
        *counts.entry(operation.to_string()).or_insert(0) += 1;

        if !success {
            let mut errors = self.error_counts.write().await;
            *errors.entry(operation.to_string()).or_insert(0) += 1;
        }

        self.registry.observe_histogram(
            "query_latency_seconds",
            latency.as_secs_f64(),
            &[("operation", operation)],
        );
    }

    pub async fn get_statistics(&self) -> PerformanceStats {
        let latencies = self.query_latencies.read().await;
        let counts = self.operation_counts.read().await;
        let errors = self.error_counts.read().await;

        let latencies_vec: Vec<Duration> = latencies.iter().map(|(_, l)| *l).collect();
        let avg_latency = if !latencies_vec.is_empty() {
            latencies_vec.iter().sum::<Duration>() / latencies_vec.len() as u32
        } else {
            Duration::ZERO
        };

        let total_ops: u64 = counts.values().sum();
        let total_errors: u64 = errors.values().sum();

        PerformanceStats {
            total_operations: total_ops,
            total_errors,
            error_rate: if total_ops > 0 { total_errors as f64 / total_ops as f64 } else { 0.0 },
            avg_latency_secs: avg_latency.as_secs_f64(),
            p50_latency_secs: Self::percentile(&latencies_vec, 50.0),
            p95_latency_secs: Self::percentile(&latencies_vec, 95.0),
            p99_latency_secs: Self::percentile(&latencies_vec, 99.0),
            operations_per_second: total_ops as f64 / self.sliding_window.as_secs_f64(),
        }
    }

    fn percentile(latencies: &[Duration], percentile: f64) -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }

        let mut sorted: Vec<_> = latencies.iter().collect();
        sorted.sort();

        let idx = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx].as_secs_f64()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub total_operations: u64,
    pub total_errors: u64,
    pub error_rate: f64,
    pub avg_latency_secs: f64,
    pub p50_latency_secs: f64,
    pub p95_latency_secs: f64,
    pub p99_latency_secs: f64,
    pub operations_per_second: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_registry() {
        let registry = MetricsRegistry::new(Duration::from_secs(60)).await.unwrap();

        let gauge = registry.register_gauge("test_gauge", "A test gauge", &["method"]).unwrap();
        gauge.with_label_values(&["GET"]).set(42.0);

        let counter = registry.register_counter("test_counter", "A test counter", &["status"]).unwrap();
        counter.with_label_values(&["200"]).inc_by(10);

        let histogram = registry.register_histogram("test_histogram", "A test histogram", None, &["operation"]).unwrap();
        histogram.with_label_values(&["query"]).observe(0.5);

        let metrics = registry.generate_prometheus_metrics().await.unwrap();
        assert!(metrics.contains("test_gauge"));
        assert!(metrics.contains("test_counter"));
        assert!(metrics.contains("test_histogram"));
    }

    #[tokio::test]
    async fn test_health_checker() {
        let health_checker = HealthChecker::new(Duration::from_secs(30)).await;

        struct TestComponent;
        #[async_trait::async_trait]
        impl HealthCheckable for TestComponent {
            async fn health_check(&self) -> HealthCheck {
                HealthCheck {
                    name: "test".to_string(),
                    status: HealthStatus::Healthy,
                    latency: Duration::from_millis(10),
                    message: "OK".to_string(),
                    last_checked: 0,
                    details: HashMap::new(),
                }
            }
            fn name(&self) -> String {
                "test".to_string()
            }
        }

        health_checker.register_component(Arc::new(TestComponent)).await;

        let checks = health_checker.check_all().await;
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_alert_manager() {
        let alert_manager = AlertManager::new(Duration::from_secs(60)).await;

        let rule = AlertRule {
            name: "HighCPUUsage".to_string(),
            expr: "cpu_usage > 90".to_string(),
            for_duration: Duration::from_secs(300),
            labels: HashMap::new(),
            annotations: HashMap::from([("description".to_string(), "CPU usage is too high".to_string())]),
            severity: AlertSeverity::Warning,
            enabled: true,
        };

        alert_manager.add_rule(rule).await;

        let metrics = SystemMetrics {
            cpu_usage: 95.0,
            memory_usage: 50.0,
            disk_usage: 30.0,
            disk_free: 1000000,
            network_bytes_sent: 1000,
            network_bytes_recv: 2000,
            open_files: 100,
            thread_count: 10,
            context_switches: 1000,
            timestamp: 0,
        };

        let query_metrics = QueryMetrics {
            total_queries: 1000,
            successful_queries: 990,
            failed_queries: 10,
            avg_latency_ms: 10.0,
            p50_latency_ms: 5.0,
            p95_latency_ms: 50.0,
            p99_latency_ms: 100.0,
            queries_per_second: 100.0,
            active_queries: 5,
        };

        alert_manager.evaluate(&metrics, &query_metrics).await;
    }
}
