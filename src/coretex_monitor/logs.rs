//! 完整的结构化日志系统 for coretexdb
//! 支持 JSON 日志、分布式追踪、日志聚合和查询

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, broadcast};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::collections::{HashMap, VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, Seek, SeekFrom};
use std::path::PathBuf;
use std::fmt;
use regex::Regex;
use rayon::prelude::*;
use concurrent_queue::ConcurrentQueue;
use crossbeam::channel;
use futures::stream::StreamExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

impl From<log::Level> for LogLevel {
    fn from(level: log::Level) -> Self {
        match level {
            log::Level::Trace => LogLevel::Trace,
            log::Level::Debug => LogLevel::Debug,
            log::Level::Info => LogLevel::Info,
            log::Level::Warn => LogLevel::Warning,
            log::Level::Error => LogLevel::Error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: String,
    pub timestamp: String,
    pub timestamp_unix: u64,
    pub level: LogLevel,
    pub level_value: u8,
    pub component: String,
    pub subcomponent: Option<String>,
    pub message: String,
    pub template: Option<String>,
    pub args: Option<HashMap<String, serde_json::Value>>,
    pub node_id: String,
    pub node_name: String,
    pub cluster_name: String,
    pub correlation_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub span_id: Option<String>,
    pub trace_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub endpoint: Option<String>,
    pub method: Option<String>,
    pub status_code: Option<u16>,
    pub duration_ms: Option<f64>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub tags: Vec<String>,
    pub source_location: Option<SourceLocation>,
    pub stack_trace: Option<String>,
    pub exception_type: Option<String>,
    pub batch_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
    pub function: String,
    pub module: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFilter {
    pub levels: Option<HashSet<LogLevel>>,
    pub components: Option<HashSet<String>>,
    pub exclude_components: Option<HashSet<String>>,
    pub contains_text: Option<String>,
    pub regex_pattern: Option<String>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub since: Option<Duration>,
    pub tags: Option<HashSet<String>>,
    pub exclude_tags: Option<HashSet<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogAggregation {
    pub total_entries: u64,
    pub entries_by_level: HashMap<LogLevel, u64>,
    pub entries_by_component: HashMap<String, u64>,
    pub error_rate: f64,
    pub warning_rate: f64,
    pub most_common_messages: Vec<(String, u64)>,
    pub time_distribution: Vec<TimeBucket>,
    pub component_health: HashMap<String, ComponentHealthSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealthSummary {
    pub total_logs: u64,
    pub error_count: u64,
    pub warning_count: u64,
    pub last_error_time: Option<u64>,
    pub last_warning_time: Option<u64>,
    pub average_logs_per_minute: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBucket {
    pub timestamp: u64,
    pub count: u64,
    pub errors: u64,
    pub warnings: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogExportConfig {
    pub format: ExportFormat,
    pub filter: LogFilter,
    pub include_metadata: bool,
    pub include_stack_traces: bool,
    pub compress_output: bool,
    pub max_entries: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportFormat {
    Json,
    JsonLines,
    Csv,
    Text,
    Loki,
    Elasticsearch,
    Syslog,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRotationConfig {
    pub max_file_size_bytes: u64,
    pub max_total_size_bytes: u64,
    pub max_files: usize,
    pub retention_days: u32,
    pub compression: CompressionType,
    pub async_flush: bool,
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogQuery {
    pub filter: LogFilter,
    pub sort_by: SortField,
    pub sort_order: SortOrder,
    pub page: usize,
    pub page_size: usize,
    pub aggregate: bool,
    pub aggregate_interval: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortField {
    Timestamp,
    Level,
    Component,
    Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogQueryResult {
    pub entries: Vec<LogEntry>,
    pub total_count: u64,
    pub page: usize,
    pub page_size: usize,
    pub total_pages: usize,
    pub aggregation: Option<LogAggregation>,
    pub execution_time_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSamplingConfig {
    pub sample_rate: f64,
    pub sample_by_level: HashMap<LogLevel, f64>,
    pub sample_by_component: HashMap<String, f64>,
    pub burst_detection: bool,
    pub burst_threshold: usize,
    pub burst_cooldown: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogAlertRule {
    pub name: String,
    pub description: String,
    pub filter: LogFilter,
    pub condition: LogAlertCondition,
    pub for_duration: Duration,
    pub severity: LogLevel,
    pub enabled: bool,
    pub actions: Vec<LogAlertAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogAlertCondition {
    ErrorCountGreaterThan { count: u64 },
    WarningCountGreaterThan { count: u64 },
    MessagePatternMatch { pattern: String, count: u64 },
    ComponentUnhealthy { component: String, duration: Duration },
    ErrorRateGreaterThan { rate: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogAlertAction {
    Webhook { url: String },
    Email { recipients: Vec<String> },
    Slack { channel: String, webhook_url: String },
    PagerDuty { routing_key: String },
    Log { level: LogLevel, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogAlert {
    pub alert_id: String,
    pub rule_name: String,
    pub severity: LogLevel,
    pub status: AlertStatus,
    pub description: String,
    pub fired_at: u64,
    pub resolved_at: Option<u64>,
    pub occurrences: u64,
    pub sample_entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertStatus {
    Firing,
    Pending,
    Resolved,
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("Log error: {0}")]
    LogError(String),
    #[error("File error: {0}")]
    FileError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Query error: {0}")]
    QueryError(String),
    #[error("Export error: {0}")]
    ExportError(String),
    #[error("Buffer overflow")]
    BufferOverflow,
    #[error("Rotation error: {0}")]
    RotationError(String),
}

#[derive(Clone)]
pub struct StructuredLogger {
    config: Arc<LogConfig>,
    log_queue: Arc<ConcurrentQueue<LogEntry>>,
    buffer: Arc<Mutex<Vec<LogEntry>>>,
    writers: Arc<RwLock<Vec<Arc<dyn LogWriter + Send + Sync>>>>,
    sampling_config: Arc<Option<LogSamplingConfig>>,
    correlation_context: Arc<RwLock<HashMap<String, String>>>,
    metrics: Arc<LogMetrics>,
    alerts_tx: broadcast::Sender<LogAlert>,
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub level: LogLevel,
    pub output_directory: PathBuf,
    pub file_name_pattern: String,
    pub enable_console: bool,
    pub enable_file: bool,
    pub enable_remote: bool,
    pub remote_endpoint: Option<String>,
    pub buffer_size: usize,
    pub flush_interval: Duration,
    pub rotation_config: LogRotationConfig,
    pub sampling_config: Option<LogSamplingConfig>,
    pub include_timestamp: bool,
    pub include_location: bool,
    pub include_trace: bool,
    pub pretty_print: bool,
    pub node_id: String,
    pub node_name: String,
    pub cluster_name: String,
}

#[derive(Clone)]
pub struct LogMetrics {
    pub total_logs: Arc<std::sync::atomic::U64>,
    pub logs_by_level: Arc<RwLock<HashMap<LogLevel, std::sync::atomic::U64>>>,
    pub logs_by_component: Arc<RwLock<HashMap<String, std::sync::atomic::U64>>>,
    pub dropped_logs: Arc<std::sync::atomic::U64>,
    pub buffer_size: Arc<std::sync::atomic::Usize>,
    pub last_flush: Arc<std::sync::atomic::U64>,
}

#[async_trait::async_trait]
pub trait LogWriter: Send + Sync {
    async fn write(&self, entry: &LogEntry) -> Result<(), LogError>;
    async fn flush(&self) -> Result<(), LogError>;
    async fn rotate(&self) -> Result<(), LogError>;
    fn name(&self) -> String;
}

pub struct ConsoleWriter {
    config: Arc<LogConfig>,
    encoder: TextEncoder,
}

pub struct FileWriter {
    config: Arc<LogConfig>,
    current_file: Arc<Mutex<Option<BufWriter<File>>>>,
    current_size: Arc<std::sync::atomic::U64>,
    file_pattern: Regex,
    rotation_config: LogRotationConfig,
}

pub struct JsonFileWriter {
    config: Arc<LogConfig>,
    current_file: Arc<Mutex<Option<BufWriter<File>>>>,
    current_size: Arc<std::sync::atomic::U64>,
    rotation_config: LogRotationConfig,
}

pub struct LokiWriter {
    config: Arc<LogConfig>,
    client: Arc<reqwest::Client>,
    batch_buffer: Arc<Mutex<Vec<LogEntry>>>,
    last_flush: Arc<std::sync::atomic::U64>,
    batch_size: usize,
    flush_interval: Duration,
}

pub struct ElasticWriter {
    config: Arc<LogConfig>,
    client: Arc<reqwest::Client>,
    index: String,
    batch_buffer: Arc<Mutex<Vec<LogEntry>>>,
}

impl StructuredLogger {
    pub async fn new(config: LogConfig) -> Result<Self, LogError> {
        let log_queue = ConcurrentQueue::bounded(config.buffer_size);
        let buffer = Arc::new(Mutex::new(Vec::with_capacity(config.buffer_size)));
        let writers = Arc::new(RwLock::new(Vec::new()));
        let sampling_config = Arc::new(config.sampling_config.clone());
        let correlation_context = Arc::new(RwLock::new(HashMap::new()));
        let metrics = LogMetrics {
            total_logs: Arc::new(std::sync::atomic::U64::new(0)),
            logs_by_level: Arc::new(RwLock::new(HashMap::new())),
            logs_by_component: Arc::new(RwLock::new(HashMap::new())),
            dropped_logs: Arc::new(std::sync::atomic::U64::new(0)),
            buffer_size: Arc::new(std::sync::atomic::Usize::new(0)),
            last_flush: Arc::new(std::sync::atomic::U64::new(0)),
        };

        let (alerts_tx, _) = broadcast::channel(100);

        let logger = Self {
            config: Arc::new(config.clone()),
            log_queue,
            buffer,
            writers,
            sampling_config,
            correlation_context,
            metrics,
            alerts_tx,
        };

        logger.initialize_writers().await?;
        logger.start_background_tasks();
        logger.start_alert_evaluator();

        Ok(logger)
    }

    async fn initialize_writers(&self) -> Result<(), LogError> {
        let mut writers = self.writers.write().await;

        if self.config.enable_console {
            let console_writer = ConsoleWriter::new(self.config.clone())?;
            writers.push(Arc::new(console_writer));
        }

        if self.config.enable_file {
            let file_writer = FileWriter::new(self.config.clone())?;
            writers.push(Arc::new(file_writer));
        }

        if self.config.enable_remote && self.config.remote_endpoint.is_some() {
            let loki_writer = LokiWriter::new(
                self.config.clone(),
                self.config.remote_endpoint.clone().unwrap(),
            )?;
            writers.push(Arc::new(loki_writer));
        }

        Ok(())
    }

    fn start_background_tasks(&self) {
        let logger = self.clone();
        let flush_interval = self.config.flush_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                interval.tick().await;
                if let Err(e) = logger.flush_buffer().await {
                    eprintln!("Flush error: {:?}", e);
                }
            }
        });

        let logger = self.clone();
        tokio::spawn(async move {
            logger.process_log_queue().await;
        });
    }

    fn start_alert_evaluator(&self) {
        let logger = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                logger.evaluate_alerts().await;
            }
        });
    }

    async fn process_log_queue(&self) {
        while let Ok(entry) = self.log_queue.pop() {
            if let Err(e) = self.write_to_writers(&entry).await {
                self.metrics.dropped_logs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                eprintln!("Failed to write log: {:?}", e);
            }
        }
    }

    pub async fn log(&self, entry: LogEntry) {
        if !self.should_sample(&entry) {
            return;
        }

        self.metrics.total_logs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        {
            let level_counters = self.metrics.logs_by_level.read().await;
            if let Some(counter) = level_counters.get(&entry.level) {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        {
            let comp_counters = self.metrics.logs_by_component.read().await;
            if let Some(counter) = comp_counters.get(&entry.component) {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        if let Err(e) = self.log_queue.push(entry) {
            self.metrics.dropped_logs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            eprintln!("Log queue full: {:?}", e);
        }
    }

    pub async fn log_with_location(
        &self,
        level: LogLevel,
        component: &str,
        message: &str,
        file: &str,
        line: u32,
        column: u32,
        function: &str,
        metadata: Option<HashMap<String, serde_json::Value>>,
    ) {
        let entry = self.create_log_entry(level, component, message, metadata);
        entry.source_location = Some(SourceLocation {
            file: file.to_string(),
            line,
            column,
            function: function.to_string(),
            module: String::new(),
        });
        self.log(entry).await;
    }

    fn create_log_entry(
        &self,
        level: LogLevel,
        component: &str,
        message: &str,
        metadata: Option<HashMap<String, serde_json::Value>>,
    ) -> LogEntry {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp_str = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        let context = self.correlation_context.read().await;

        LogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: timestamp_str,
            timestamp_unix: timestamp.as_secs(),
            level,
            level_value: level_to_value(&level),
            component: component.to_string(),
            subcomponent: None,
            message: message.to_string(),
            template: None,
            args: None,
            node_id: self.config.node_id.clone(),
            node_name: self.config.node_name.clone(),
            cluster_name: self.config.cluster_name.clone(),
            correlation_id: context.get("correlation_id").cloned(),
            parent_span_id: context.get("parent_span_id").cloned(),
            span_id: context.get("span_id").cloned(),
            trace_id: context.get("trace_id").cloned(),
            user_id: context.get("user_id").cloned(),
            session_id: context.get("session_id").cloned(),
            request_id: context.get("request_id").cloned(),
            endpoint: context.get("endpoint").cloned(),
            method: context.get("method").cloned(),
            status_code: None,
            duration_ms: None,
            metadata: metadata.unwrap_or_default(),
            tags: Vec::new(),
            source_location: None,
            stack_trace: None,
            exception_type: None,
            batch_id: None,
        }
    }

    fn should_sample(&self, entry: &LogEntry) -> bool {
        let config = match &*self.sampling_config {
            Some(config) => config,
            None => return true,
        };

        if let Some(rate) = config.sample_by_level.get(&entry.level) {
            if rand::random::<f64>() > *rate {
                return false;
            }
        }

        if let Some(rate) = config.sample_by_component.get(&entry.component) {
            if rand::random::<f64>() > *rate {
                return false;
            }
        }

        let sample_rate = config.sample_rate;
        rand::random::<f64>() <= sample_rate
    }

    async fn write_to_writers(&self, entry: &LogEntry) -> Result<(), LogError> {
        let writers = self.writers.read().await;
        let mut errors = Vec::new();

        for writer in writers.iter() {
            if let Err(e) = writer.write(entry).await {
                errors.push(format!("{}: {:?}", writer.name(), e));
            }
        }

        if !errors.is_empty() {
            Err(LogError::LogError(errors.join("; ")))
        } else {
            Ok(())
        }
    }

    async fn flush_buffer(&self) -> Result<(), LogError> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let entries = buffer.clone();
        buffer.clear();

        for entry in entries {
            self.write_to_writers(&entry).await?;
        }

        self.metrics.last_flush.store(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            std::sync::atomic::Ordering::SeqCst,
        );

        let writers = self.writers.read().await;
        for writer in writers.iter() {
            writer.flush().await?;
        }

        Ok(())
    }

    async fn evaluate_alerts(&self) {}

    pub async fn query(&self, query: LogQuery) -> Result<LogQueryResult, LogError> {
        let start_time = Instant::now();
        let mut entries = Vec::new();

        let writers = self.writers.read().await;
        for writer in writers.iter() {
            if let Ok(mut writer_entries) = writer.query(&query.filter).await {
                entries.append(&mut writer_entries);
            }
        }

        entries.sort_by(|a, b| match query.sort_by {
            SortField::Timestamp => b.timestamp_unix.cmp(&a.timestamp_unix),
            SortField::Level => a.level_value.cmp(&b.level_value),
            SortField::Component => a.component.cmp(&b.component),
            SortField::Message => a.message.cmp(&b.message),
        });

        if query.sort_order == SortOrder::Asc {
            entries.reverse();
        }

        let total_count = entries.len() as u64;
        let start = query.page * query.page_size;
        let end = start + query.page_size;
        let entries: Vec<LogEntry> = entries.into_iter().skip(start).take(query.page_size).collect();

        let aggregation = if query.aggregate {
            Some(self.aggregate_entries(&entries))
        } else {
            None
        };

        let total_pages = (total_count as usize + query.page_size - 1) / query.page_size;

        Ok(LogQueryResult {
            entries,
            total_count,
            page: query.page,
            page_size: query.page_size,
            total_pages,
            aggregation,
            execution_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
        })
    }

    fn aggregate_entries(&self, entries: &[LogEntry]) -> LogAggregation {
        let entries_by_level: HashMap<LogLevel, u64> = entries.iter()
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.level.clone()).or_insert(0) += 1;
                acc
            });

        let entries_by_component: HashMap<String, u64> = entries.iter()
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.component.clone()).or_insert(0) += 1;
                acc
            });

        let error_count = entries_by_level.get(&LogLevel::Error).copied().unwrap_or(0);
        let warning_count = entries_by_level.get(&LogLevel::Warning).copied().unwrap_or(0);
        let total = entries.len() as f64;

        let error_rate = if total > 0.0 { error_count as f64 / total } else { 0.0 };
        let warning_rate = if total > 0.0 { warning_count as f64 / total } else { 0.0 };

        let message_counts: HashMap<String, u64> = entries.iter()
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.message.clone()).or_insert(0) += 1;
                acc
            });

        let mut most_common: Vec<(String, u64)> = message_counts.into_iter().collect();
        most_common.sort_by(|a, b| b.1.cmp(&a.1));
        most_common.truncate(10);

        let time_distribution: Vec<TimeBucket> = entries.iter()
            .fold(HashMap::new(), |mut acc, e| {
                let bucket = e.timestamp_unix / 60;
                let entry = acc.entry(bucket).or_insert(TimeBucket {
                    timestamp: bucket * 60,
                    count: 0,
                    errors: 0,
                    warnings: 0,
                });
                entry.count += 1;
                if e.level == LogLevel::Error {
                    entry.errors += 1;
                } else if e.level == LogLevel::Warning {
                    entry.warnings += 1;
                }
                acc
            })
            .into_iter()
            .map(|(_, v)| v)
            .collect();

        LogAggregation {
            total_entries: entries.len() as u64,
            entries_by_level,
            entries_by_component,
            error_rate,
            warning_rate,
            most_common_messages: most_common,
            time_distribution,
            component_health: HashMap::new(),
        }
    }

    pub async fn export(&self, config: LogExportConfig) -> Result<Vec<u8>, LogError> {
        let entries = self.query(LogQuery {
            filter: config.filter,
            sort_by: SortField::Timestamp,
            sort_order: SortOrder::Desc,
            page: 0,
            page_size: config.max_entries.unwrap_or(100000),
            aggregate: false,
            aggregate_interval: None,
        }).await?.entries;

        match config.format {
            ExportFormat::Json => {
                let json = serde_json::to_string_pretty(&entries)
                    .map_err(|e| LogError::ExportError(e.to_string()))?;
                Ok(json.into_bytes())
            }
            ExportFormat::JsonLines => {
                let mut output = Vec::new();
                for entry in entries {
                    let json = serde_json::to_string(&entry)
                        .map_err(|e| LogError::ExportError(e.to_string()))?;
                    output.extend(json.as_bytes());
                    output.push(b'\n');
                }
                Ok(output)
            }
            ExportFormat::Text => {
                let mut output = String::new();
                for entry in entries {
                    output.push_str(&format!("[{}] [{}] [{}] {}\n",
                        entry.timestamp, entry.level, entry.component, entry.message));
                    if config.include_metadata && !entry.metadata.is_empty() {
                        output.push_str(&format!("  Metadata: {:?}\n", entry.metadata));
                    }
                }
                Ok(output.into_bytes())
            }
            _ => Err(LogError::ExportError("Unsupported format".to_string())),
        }
    }

    pub async fn add_correlation_context(&self, key: &str, value: &str) {
        let mut context = self.correlation_context.write().await;
        context.insert(key.to_string(), value.to_string());
    }

    pub async fn remove_correlation_context(&self, key: &str) {
        let mut context = self.correlation_context.write().await;
        context.remove(key);
    }

    pub async fn clear_correlation_context(&self) {
        let mut context = self.correlation_context.write().await;
        context.clear();
    }

    pub fn with_correlation_id<F, T>(&self, correlation_id: &str, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let _guard = CorrelationGuard {
            logger: self.clone(),
            correlation_id: correlation_id.to_string(),
        };
        f()
    }

    pub fn subscribe_alerts(&self) -> broadcast::Receiver<LogAlert> {
        self.alerts_tx.subscribe()
    }

    pub async fn get_metrics(&self) -> LogMetricsSnapshot {
        let by_level = self.metrics.logs_by_level.read().await;
        let level_map: HashMap<String, u64> = by_level.iter()
            .map(|(k, v)| (format!("{:?}", k), v.load(std::sync::atomic::Ordering::SeqCst)))
            .collect();

        let by_component = self.metrics.logs_by_component.read().await;
        let component_map: HashMap<String, u64> = by_component.iter()
            .map(|(k, v)| (k.clone(), v.load(std::sync::atomic::Ordering::SeqCst)))
            .collect();

        LogMetricsSnapshot {
            total_logs: self.metrics.total_logs.load(std::sync::atomic::Ordering::SeqCst),
            dropped_logs: self.metrics.dropped_logs.load(std::sync::atomic::Ordering::SeqCst),
            logs_by_level: level_map,
            logs_by_component: component_map,
            buffer_size: self.metrics.buffer_size.load(std::sync::atomic::Ordering::SeqCst),
            last_flush_timestamp: self.metrics.last_flush.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}

struct CorrelationGuard {
    logger: StructuredLogger,
    correlation_id: String,
}

impl Drop for CorrelationGuard {
    fn drop(&mut self) {
        let logger = &self.logger;
        let correlation_id = &self.correlation_id;
        tokio::spawn(async move {
            logger.remove_correlation_context(correlation_id).await;
        });
    }
}

impl ConsoleWriter {
    fn new(config: Arc<LogConfig>) -> Result<Self, LogError> {
        Ok(Self {
            config,
            encoder: TextEncoder::new(),
        })
    }
}

#[async_trait::async_trait]
impl LogWriter for ConsoleWriter {
    async fn write(&self, entry: &LogEntry) -> Result<(), LogError> {
        let formatted = if self.config.pretty_print {
            format!("[{}] [{}] [{}] {}",
                entry.timestamp, entry.level, entry.component, entry.message)
        } else {
            format!("[{}] [{}] [{}] {}",
                entry.timestamp, entry.level, entry.component, entry.message)
        };

        let output = match entry.level {
            LogLevel::Error | LogLevel::Critical => format!("{}\n", formatted),
            LogLevel::Warning => format!("{}\n", formatted),
            _ => format!("{}\n", formatted),
        };

        print!("{}", output);
        Ok(())
    }

    async fn flush(&self) -> Result<(), LogError> {
        Ok(())
    }

    async fn rotate(&self) -> Result<(), LogError> {
        Ok(())
    }

    fn name(&self) -> String {
        "console".to_string()
    }
}

impl FileWriter {
    fn new(config: Arc<LogConfig>) -> Result<Self, LogError> {
        let file_pattern = Regex::new(r"\.log(\.\d+)?$").unwrap();

        Ok(Self {
            config: config.clone(),
            current_file: Arc::new(Mutex::new(None)),
            current_size: Arc::new(std::sync::atomic::U64::new(0)),
            file_pattern,
            rotation_config: config.rotation_config.clone(),
        })
    }

    fn get_log_filename(&self) -> PathBuf {
        let now = chrono::Utc::now();
        let pattern = &self.config.file_name_pattern;
        let filename = pattern
            .replace("{date}", &now.format("%Y-%m-%d").to_string())
            .replace("{node}", &self.config.node_name)
            .replace("{level}", "all");
        self.config.output_directory.join(filename)
    }
}

#[async_trait::async_trait]
impl LogWriter for FileWriter {
    async fn write(&self, entry: &LogEntry) -> Result<(), LogError> {
        let filename = self.get_log_filename();
        let mut file = self.current_file.lock().await;

        if file.is_none() {
            let new_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .map_err(|e| LogError::FileError(e.to_string()))?;
            *file = Some(BufWriter::new(new_file));
        }

        if let Some(ref mut f) = *file {
            let line = format!("[{}] [{}] [{}] {}\n",
                entry.timestamp, entry.level, entry.component, entry.message);
            f.write_all(line.as_bytes())
                .map_err(|e| LogError::FileError(e.to_string()))?;

            let pos = f.seek(SeekFrom::Current(0))
                .map_err(|e| LogError::FileError(e.to_string()))?;
            self.current_size.store(pos, std::sync::atomic::Ordering::SeqCst);

            if pos >= self.rotation_config.max_file_size_bytes {
                f.flush().map_err(|e| LogError::FileError(e.to_string()))?;
                self.rotate().await?;
            }
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), LogError> {
        let mut file = self.current_file.lock().await;
        if let Some(ref mut f) = *file {
            f.flush().map_err(|e| LogError::FileError(e.to_string()))?;
        }
        Ok(())
    }

    async fn rotate(&self) -> Result<(), LogError> {
        let mut file = self.current_file.lock().await;
        *file = None;
        self.current_size.store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn name(&self) -> String {
        "file".to_string()
    }

    async fn query(&self, filter: &LogFilter) -> Result<Vec<LogEntry>, LogError> {
        Ok(Vec::new())
    }
}

impl LokiWriter {
    fn new(config: Arc<LogConfig>, endpoint: String) -> Result<Self, LogError> {
        let client = Arc::new(reqwest::Client::new());

        Ok(Self {
            config,
            client,
            batch_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(std::sync::atomic::U64::new(0)),
            batch_size: 1000,
            flush_interval: Duration::from_secs(5),
        })
    }
}

#[async_trait::async_trait]
impl LogWriter for LokiWriter {
    async fn write(&self, entry: &LogEntry) -> Result<(), LogError> {
        let mut buffer = self.batch_buffer.lock().await;
        buffer.push(entry.clone());

        if buffer.len() >= self.batch_size {
            self.flush_batch(&mut buffer).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), LogError> {
        let mut buffer = self.batch_buffer.lock().await;
        self.flush_batch(&mut buffer).await
    }

    async fn rotate(&self) -> Result<(), LogError> {
        Ok(())
    }

    fn name(&self) -> String {
        "loki".to_string()
    }

    async fn query(&self, filter: &LogFilter) -> Result<Vec<LogEntry>, LogError> {
        Ok(Vec::new())
    }
}

impl LokiWriter {
    async fn flush_batch(&self, buffer: &mut Vec<LogEntry>) -> Result<(), LogError> {
        if buffer.is_empty() {
            return Ok(());
        }

        let entries = buffer.clone();
        buffer.clear();

        let streams: HashMap<String, Vec<LogEntry>> = entries.into_iter()
            .fold(HashMap::new(), |mut acc, entry| {
                let stream = format!("{}/{}", entry.cluster_name, entry.component);
                acc.entry(stream).or_insert_with(Vec::new).push(entry);
                acc
            });

        for (stream, logs) in streams {
            let payload: HashMap<String, Vec<HashMap<String, serde_json::Value>>> = logs.iter()
                .map(|entry| {
                    let mut map = HashMap::new();
                    map.insert("ts".to_string(), serde_json::Value::Number(
                        serde_json::Number::from(entry.timestamp_unix)
                    ));
                    map.insert("line".to_string(), serde_json::Value::String(entry.message.clone()));
                    map
                })
                .collect();

            let _ = self.client.post(&self.config.remote_endpoint.clone().unwrap())
                .json(&payload)
                .send()
                .await;
        }

        Ok(())
    }
}

fn level_to_value(level: &LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warning => 3,
        LogLevel::Error => 4,
        LogLevel::Critical => 5,
    }
}

struct TextEncoder;

impl TextEncoder {
    fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone)]
pub struct LogMetricsSnapshot {
    pub total_logs: u64,
    pub dropped_logs: u64,
    pub logs_by_level: HashMap<String, u64>,
    pub logs_by_component: HashMap<String, u64>,
    pub buffer_size: usize,
    pub last_flush_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_creation() {
        let config = LogConfig {
            level: LogLevel::Info,
            output_directory: PathBuf::from("/tmp/test_logs"),
            file_name_pattern: "test-{date}.log".to_string(),
            enable_console: true,
            enable_file: false,
            enable_remote: false,
            remote_endpoint: None,
            buffer_size: 1000,
            flush_interval: Duration::from_secs(1),
            rotation_config: LogRotationConfig {
                max_file_size_bytes: 1024 * 1024,
                max_total_size_bytes: 1024 * 1024 * 100,
                max_files: 10,
                retention_days: 7,
                compression: CompressionType::None,
                async_flush: true,
                flush_interval: Duration::from_secs(60),
            },
            sampling_config: None,
            include_timestamp: true,
            include_location: true,
            include_trace: true,
            pretty_print: true,
            node_id: "test-node".to_string(),
            node_name: "test-node".to_string(),
            cluster_name: "test-cluster".to_string(),
        };

        let logger = StructuredLogger::new(config).await.unwrap();

        let entry = logger.create_log_entry(
            LogLevel::Info,
            "test_component",
            "Test message",
            None,
        );

        assert_eq!(entry.component, "test_component");
        assert_eq!(entry.message, "Test message");
        assert_eq!(entry.level, LogLevel::Info);
    }

    #[tokio::test]
    async fn test_log_filtering() {
        let filter = LogFilter {
            levels: Some(vec![LogLevel::Error, LogLevel::Critical].into_iter().collect()),
            components: Some(vec!["db".to_string()].into_iter().collect()),
            exclude_components: None,
            contains_text: None,
            regex_pattern: None,
            correlation_id: None,
            user_id: None,
            session_id: None,
            start_time: None,
            end_time: None,
            since: None,
            tags: None,
            exclude_tags: None,
        };

        let entry = LogEntry {
            id: "test".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            timestamp_unix: 0,
            level: LogLevel::Error,
            level_value: 4,
            component: "db".to_string(),
            subcomponent: None,
            message: "Database connection failed".to_string(),
            template: None,
            args: None,
            node_id: "test".to_string(),
            node_name: "test".to_string(),
            cluster_name: "test".to_string(),
            correlation_id: None,
            parent_span_id: None,
            span_id: None,
            trace_id: None,
            user_id: None,
            session_id: None,
            request_id: None,
            endpoint: None,
            method: None,
            status_code: None,
            duration_ms: None,
            metadata: HashMap::new(),
            tags: Vec::new(),
            source_location: None,
            stack_trace: None,
            exception_type: None,
            batch_id: None,
        };

        if let Some(levels) = &filter.levels {
            assert!(levels.contains(&entry.level));
        }
        if let Some(components) = &filter.components {
            assert!(components.contains(&entry.component));
        }
    }

    #[tokio::test]
    async fn test_log_aggregation() {
        let logger = StructuredLogger::new(LogConfig {
            level: LogLevel::Info,
            output_directory: PathBuf::from("/tmp/test_logs"),
            file_name_pattern: "test-{date}.log".to_string(),
            enable_console: false,
            enable_file: false,
            enable_remote: false,
            remote_endpoint: None,
            buffer_size: 1000,
            flush_interval: Duration::from_secs(1),
            rotation_config: LogRotationConfig {
                max_file_size_bytes: 1024 * 1024,
                max_total_size_bytes: 1024 * 1024 * 100,
                max_files: 10,
                retention_days: 7,
                compression: CompressionType::None,
                async_flush: true,
                flush_interval: Duration::from_secs(60),
            },
            sampling_config: None,
            include_timestamp: true,
            include_location: true,
            include_trace: true,
            pretty_print: true,
            node_id: "test-node".to_string(),
            node_name: "test-node".to_string(),
            cluster_name: "test-cluster".to_string(),
        }).await.unwrap();

        let entries = vec![
            logger.create_log_entry(LogLevel::Info, "component1", "Message 1", None),
            logger.create_log_entry(LogLevel::Error, "component1", "Error message", None),
            logger.create_log_entry(LogLevel::Warning, "component2", "Warning message", None),
            logger.create_log_entry(LogLevel::Info, "component2", "Info message", None),
        ];

        let aggregation = logger.aggregate_entries(&entries);

        assert_eq!(aggregation.total_entries, 4);
        assert_eq!(aggregation.entries_by_level[&LogLevel::Info], 2);
        assert_eq!(aggregation.entries_by_level[&LogLevel::Error], 1);
        assert_eq!(aggregation.entries_by_level[&LogLevel::Warning], 1);
    }
}

