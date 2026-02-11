//! Optimized Storage Engine with WAL Batch Writing and Async Flush
//!
//! This module provides enhanced storage capabilities including:
//! - Write-Ahead Log (WAL) with batch optimization
//! - Asynchronous flush with configurable strategies
//! - Adaptive LSM Tree compaction policies
//! - Write buffer management with memory limits
//! - Background maintenance tasks

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use std::thread;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::{self, JoinHandle};
use tracing::{debug, error, info, warn};

/// Configuration for optimized storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedStorageConfig {
    /// RocksDB directory
    pub directory: PathBuf,
    /// Maximum write buffer size (bytes)
    pub max_write_buffer_size: usize,
    /// Maximum number of write buffers
    pub max_write_buffer_number: usize,
    /// Minimum write buffers to merge
    pub min_write_buffer_number_to_merge: usize,
    /// Enable WAL compression
    pub wal_compression: bool,
    /// WAL TTL in seconds (0 = no TTL)
    pub wal_ttl_seconds: u64,
    /// WAL size limit in bytes
    pub wal_size_limit_mb: usize,
    /// Enable async flush
    pub async_flush: bool,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Maximum concurrent flushes
    pub max_concurrent_flushes: usize,
    /// Memory budget for memtables (bytes)
    pub memtable_memory_budget_mb: usize,
    /// Compaction style
    pub compaction_style: CompactionStyle,
    /// Target file size for compaction (bytes)
    pub target_file_size_bytes: usize,
    /// Maximum background jobs
    pub max_background_jobs: usize,
    /// Rate limiter for writes (ops/sec, 0 = unlimited)
    pub write_rate_limit: u64,
    /// Enable rate limiting
    pub rate_limit_enabled: bool,
}

impl Default for OptimizedStorageConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./data"),
            max_write_buffer_size: 256 * 1024 * 1024, // 256MB
            max_write_buffer_number: 6,
            min_write_buffer_number_to_merge: 2,
            wal_compression: true,
            wal_ttl_seconds: 0,
            wal_size_limit_mb: 1024,
            async_flush: true,
            flush_interval_ms: 1000,
            max_concurrent_flushes: 4,
            memtable_memory_budget_mb: 512,
            compaction_style: CompactionStyle::Universal,
            target_file_size_bytes: 64 * 1024 * 1024, // 64MB
            max_background_jobs: 8,
            write_rate_limit: 0,
            rate_limit_enabled: false,
        }
    }
}

/// Compaction style options
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompactionStyle {
    /// Level-based compaction
    Level,
    /// Universal compaction
    Universal,
    /// FIFO compaction (oldest files first)
    Fifo,
    /// Adaptive compaction based on workload
    Adaptive,
}

/// Workload pattern detection
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WorkloadPattern {
    /// Write-heavy workload
    WriteHeavy,
    /// Read-heavy workload
    ReadHeavy,
    /// Balanced workload
    Balanced,
    /// Mixed burst workload
    Burst,
}

/// Write batch with metadata
#[derive(Debug, Clone)]
pub struct WriteBatch {
    /// Batch ID for tracking
    pub id: String,
    /// Operations in the batch
    pub operations: Vec<StorageOperation>,
    /// Created timestamp
    pub created_at: Instant,
    /// Priority level
    pub priority: BatchPriority,
    /// Expected size hint
    pub size_hint: usize,
}

/// Priority levels for write batches
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BatchPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Storage operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}

/// Flush strategy options
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FlushStrategy {
    /// Flush based on time interval
    Interval,
    /// Flush based on buffer size
    SizeBased,
    /// Flush based on both time and size
    Hybrid,
    /// Adaptive based on write patterns
    Adaptive,
}

/// Status of the optimized storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedStorageStatus {
    /// Number of pending writes
    pub pending_writes: usize,
    /// Current memory usage (bytes)
    pub memory_usage_bytes: usize,
    /// Maximum memory budget (bytes)
    pub max_memory_bytes: usize,
    /// Number of active flushes
    pub active_flushes: usize,
    /// Total flush operations
    pub total_flushes: u64,
    /// Average flush duration (ms)
    pub avg_flush_duration_ms: f64,
    /// Current workload pattern
    pub workload_pattern: WorkloadPattern,
    /// WAL size (bytes)
    pub wal_size_bytes: u64,
    /// Compaction status
    pub compaction_status: CompactionStatus,
    /// Write rate (ops/sec)
    pub write_rate: f64,
    /// Is healthy
    pub is_healthy: bool,
}

/// Compaction status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStatus {
    /// Is compacting currently
    pub is_compacting: bool,
    /// Current compaction type
    pub compaction_type: String,
    /// Files being compacted
    pub files_being_compacted: usize,
    /// Estimated time remaining (seconds)
    pub estimated_time_remaining: u64,
    /// Last compaction timestamp
    pub last_compaction_timestamp: Option<u64>,
    /// Compaction score
    pub compaction_score: f64,
}

/// Rate limiter for write operations
#[derive(Debug)]
pub struct WriteRateLimiter {
    /// Operations per second limit
    pub ops_per_second: u64,
    /// Current token count
    tokens: Arc<AtomicUsize>,
    /// Last refill timestamp
    last_refill: Arc<RwLock<Instant>>,
    /// Maximum tokens
    max_tokens: usize,
}

impl WriteRateLimiter {
    /// Create a new rate limiter
    pub fn new(ops_per_second: u64) -> Self {
        Self {
            ops_per_second,
            tokens: Arc::new(AtomicUsize::new(ops_per_second as usize)),
            last_refill: Arc::new(RwLock::new(Instant::now())),
            max_tokens: ops_per_second as usize,
        }
    }

    /// Try to acquire a token
    pub fn try_acquire(&self) -> bool {
        if self.ops_per_second == 0 {
            return true;
        }

        let now = Instant::now();
        let mut last_refill = self.last_refill.write().unwrap();

        if last_refill.elapsed() >= Duration::from_secs(1) {
            // Refill tokens
            self.tokens.store(self.max_tokens, Ordering::SeqCst);
            *last_refill = now;
        }

        let current = self.tokens.load(Ordering::SeqCst);
        if current > 0 {
            self.tokens.store(current - 1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Wait for a token (async)
    pub async fn acquire(&self) {
        while !self.try_acquire() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Write buffer pool for efficient batching
#[derive(Debug)]
pub struct WriteBufferPool {
    /// Buffer capacity
    capacity: usize,
    /// Current size
    current_size: Arc<AtomicUsize>,
    /// Pending batches
    pending_batches: Arc<RwLock<Vec<WriteBatch>>>,
    /// Memory limit
    memory_limit: Arc<AtomicUsize>,
    /// Flush trigger
    flush_trigger: Arc<Notify>,
    /// Total memory used
    memory_used: Arc<AtomicUsize>,
}

impl WriteBufferPool {
    /// Create a new write buffer pool
    pub fn new(capacity: usize, memory_limit_bytes: usize) -> Self {
        Self {
            capacity,
            current_size: Arc::new(AtomicUsize::new(0)),
            pending_batches: Arc::new(RwLock::new(Vec::new())),
            memory_limit: Arc::new(AtomicUsize::new(memory_limit_bytes)),
            flush_trigger: Arc::new(Notify::new()),
            memory_used: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add a write batch to the pool
    pub fn add_batch(&self, batch: WriteBatch) -> Result<(), ()> {
        let batch_size = batch.size_hint;
        let current_memory = self.memory_used.load(Ordering::SeqCst);
        let limit = self.memory_limit.load(Ordering::SeqCst);

        if current_memory + batch_size > limit {
            return Err(());
        }

        let mut batches = self.pending_batches.write().unwrap();
        batches.push(batch);

        self.memory_used.store(current_memory + batch_size, Ordering::SeqCst);
        self.current_size.store(batches.len(), Ordering::SeqCst);

        // Trigger flush if buffer is full
        if batches.len() >= self.capacity {
            self.flush_trigger.notify_one();
        }

        Ok(())
    }

    /// Get batches for flushing
    pub fn get_batches_for_flush(&self, max_batches: usize) -> Vec<WriteBatch> {
        let mut batches = self.pending_batches.write().unwrap();
        let taken = batches.drain(0..std::cmp::min(batches.len(), max_batches)).collect();

        let total_size: usize = taken.iter().map(|b| b.size_hint).sum();
        self.memory_used.fetch_sub(total_size, Ordering::SeqCst);
        self.current_size.store(batches.len(), Ordering::SeqCst);

        taken
    }

    /// Get batch count
    pub fn len(&self) -> usize {
        self.current_size.load(Ordering::SeqCst)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage
    pub fn memory_usage(&self) -> usize {
        self.memory_used.load(Ordering::SeqCst)
    }

    /// Get flush notifier
    pub fn flush_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.flush_trigger)
    }
}

/// Adaptive compaction manager
#[derive(Debug)]
pub struct AdaptiveCompactionManager {
    /// Current workload pattern
    current_pattern: Arc<RwLock<WorkloadPattern>>,
    /// Compaction style
    style: Arc<RwLock<CompactionStyle>>,
    /// Performance metrics
    metrics: Arc<RwLock<CompactionMetrics>>,
    /// Last adjustment timestamp
    last_adjustment: Arc<RwLock<Instant>>,
    /// Adjustment interval
    adjustment_interval: Duration,
}

#[derive(Debug, Default)]
struct CompactionMetrics {
    total_compactions: u64,
    total_bytes_compacted: u64,
    avg_compaction_time_ms: f64,
    read_amplification: f64,
    write_amplification: f64,
    space_amplification: f64,
}

impl AdaptiveCompactionManager {
    /// Create a new adaptive compaction manager
    pub fn new(initial_style: CompactionStyle) -> Self {
        Self {
            current_pattern: Arc::new(RwLock::new(WorkloadPattern::Balanced)),
            style: Arc::new(RwLock::new(initial_style)),
            metrics: Arc::new(RwLock::new(CompactionMetrics::default())),
            last_adjustment: Arc::new(RwLock::new(Instant::now())),
            adjustment_interval: Duration::from_secs(60),
        }
    }

    /// Analyze workload and adjust compaction
    pub fn analyze_and_adjust(&self, write_rate: f64, read_rate: f64, _io_stats: &IOStats) {
        let mut pattern = self.current_pattern.write().unwrap();
        let mut style = self.style.write().unwrap();
        let mut last_adjustment = self.last_adjustment.write().unwrap();

        if last_adjustment.elapsed() < self.adjustment_interval {
            return;
        }

        *last_adjustment = Instant::now();

        let ratio = if read_rate > 0 { write_rate / read_rate } else { write_rate };

        let new_pattern = if ratio > 5.0 {
            WorkloadPattern::WriteHeavy
        } else if ratio < 0.2 {
            WorkloadPattern::ReadHeavy
        } else if ratio > 2.0 || ratio < 0.5 {
            WorkloadPattern::Burst
        } else {
            WorkloadPattern::Balanced
        };

        if new_pattern != *pattern {
            info!("Workload pattern changed: {:?} -> {:?}", pattern, new_pattern);
            *pattern = new_pattern;
        }

        let new_style = match *pattern {
            WorkloadPattern::WriteHeavy => {
                info!("Adjusting compaction to UNIVERSAL for write-heavy workload");
                CompactionStyle::Universal
            }
            WorkloadPattern::ReadHeavy => {
                info!("Adjusting compaction to LEVEL for read-heavy workload");
                CompactionStyle::Level
            }
            WorkloadPattern::Burst => {
                info!("Adjusting compaction to FIFO for burst workload");
                CompactionStyle::Fifo
            }
            WorkloadPattern::Balanced => {
                info!("Adjusting compaction to ADAPTIVE for balanced workload");
                CompactionStyle::Adaptive
            }
        };

        if new_style != *style {
            *style = new_style;
        }
    }

    /// Get current compaction style
    pub fn current_style(&self) -> CompactionStyle {
        *self.style.read().unwrap()
    }

    /// Get current workload pattern
    pub fn current_pattern(&self) -> WorkloadPattern {
        *self.current_pattern.read().unwrap()
    }

    /// Record compaction metrics
    pub fn record_compaction(&self, bytes_compacted: u64, duration_ms: u64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.total_compactions += 1;
        metrics.total_bytes_compacted += bytes_compacted;

        let total = metrics.total_compactions as f64;
        metrics.avg_compaction_time_ms = (metrics.avg_compaction_time_ms * (total - 1.0) + duration_ms as f64) / total;
    }
}

/// IO statistics for compaction analysis
#[derive(Debug, Default, Clone)]
pub struct IOStats {
    /// Read bytes per second
    pub read_bps: f64,
    /// Write bytes per second
    pub write_bps: f64,
    /// Read IOPS
    pub read_iops: u64,
    /// Write IOPS
    pub write_iops: u64,
    /// Average read latency (ms)
    pub avg_read_latency_ms: f64,
    /// Average write latency (ms)
    pub avg_write_latency_ms: f64,
}

/// Background flush task handle
#[derive(Debug)]
pub struct BackgroundFlushTask {
    /// Task handle
    handle: JoinHandle<()>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Shutdown signal
    shutdown: Arc<Notify>,
}

impl BackgroundFlushTask {
    /// Create and start the background flush task
    pub fn new(
        buffer_pool: Arc<WriteBufferPool>,
        flush_handler: Arc<dyn AsyncFlushHandler + Send + Sync>,
        interval: Duration,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(Notify::new());

        let running_clone = Arc::clone(&running);
        let shutdown_clone = Arc::clone(&shutdown);
        let buffer_pool_clone = Arc::clone(&buffer_pool);
        let handler_clone = Arc::clone(&flush_handler);

        let handle = task::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if buffer_pool.len() > 0 {
                            debug!("Triggering background flush");
                            let batches = buffer_pool.get_batches_for_flush(10);
                            if !batches.is_empty() {
                                if let Err(e) = handler_clone.flush_batches(&batches).await {
                                    error!("Background flush failed: {}", e);
                                }
                            }
                        }
                    }
                    _ = shutdown_clone.notified() => {
                        info!("Shutdown signal received, stopping background flush");
                        break;
                    }
                }
            }

            running_clone.store(false, Ordering::SeqCst);
        });

        Self {
            handle,
            running,
            shutdown,
        }
    }

    /// Stop the background task
    pub async fn stop(&self) {
        self.shutdown.notify();
        self.handle.abort();
        self.handle.await.ok();
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Trait for handling async flush operations
#[async_trait]
pub trait AsyncFlushHandler: Send + Sync {
    /// Flush a batch of operations
    async fn flush_batches(&self, batches: &[WriteBatch]) -> Result<(), Box<dyn std::error::Error>>;
    /// Flush immediately
    async fn flush_immediate(&self) -> Result<(), Box<dyn std::error::Error>>;
    /// Get flush status
    fn get_status(&self) -> FlushStatus;
}

/// Status of flush operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushStatus {
    /// Is flushing currently
    pub is_flushing: bool,
    /// Pending operations
    pub pending_operations: usize,
    /// Last flush timestamp
    pub last_flush_timestamp: Option<u64>,
    /// Last flush duration (ms)
    pub last_flush_duration_ms: u64,
    /// Total flushes
    pub total_flushes: u64,
}

/// Optimized RocksDB storage with WAL batch writing
#[derive(Debug)]
pub struct OptimizedRocksDBStorage {
    /// RocksDB instance
    db: Arc<RwLock<rocksdb::DB>>,
    /// Configuration
    config: OptimizedStorageConfig,
    /// Write buffer pool
    buffer_pool: Arc<WriteBufferPool>,
    /// Rate limiter
    rate_limiter: Arc<WriteRateLimiter>,
    /// Background flush task
    background_flush: Option<BackgroundFlushTask>,
    /// Adaptive compaction manager
    compaction_manager: Arc<AdaptiveCompactionManager>,
    /// Metrics
    metrics: Arc<RwLock<StorageMetrics>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Status
    status: Arc<RwLock<OptimizedStorageStatus>>,
}

#[derive(Debug, Default)]
struct StorageMetrics {
    total_writes: u64,
    total_reads: u64,
    total_bytes_written: u64,
    total_bytes_read: u64,
    avg_write_latency_ms: f64,
    avg_read_latency_ms: f64,
    write_operations_pending: usize,
}

impl OptimizedRocksDBStorage {
    /// Create a new optimized storage
    pub fn new(config: OptimizedStorageConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Create directory
        std::fs::create_dir_all(&config.directory)?;

        // Configure RocksDB with optimizations
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        // Memory and buffer settings
        options.set_max_write_buffer_number(config.max_write_buffer_number as i32);
        options.set_max_write_buffer_size(config.max_write_buffer_size as u64);
        options.set_min_write_buffer_number_to_merge(config.min_write_buffer_number_to_merge as i32);
        options.set_memtable_memory_budget(config.memtable_memory_budget_mb * 1024 * 1024);

        // WAL settings
        options.set_wal_compression(if config.wal_compression {
            rocksdb::DBWALCompressionType::Lz4
        } else {
            rocksdb::DBWALCompressionType::NoCompression
        });

        if config.wal_ttl_seconds > 0 {
            options.set_wal_ttl_duration(Duration::from_secs(config.wal_ttl_seconds));
        }

        if config.wal_size_limit_mb > 0 {
            options.set_wal_size_limit_mb(config.wal_size_limit_mb as u64);
        }

        // Compaction settings based on style
        match config.compaction_style {
            CompactionStyle::Level => {
                options.set_compaction_style(rocksdb::CompactionStyle::Level);
            }
            CompactionStyle::Universal => {
                options.set_compaction_style(rocksdb::CompactionStyle::Universal);
            }
            CompactionStyle::Fifo => {
                options.set_compaction_style(rocksdb::CompactionStyle::Fifo);
            }
            CompactionStyle::Adaptive => {
                options.set_compaction_style(rocksdb::CompactionStyle::Level);
            }
        }

        options.set_target_file_size_base(config.target_file_size_bytes as u64);
        options.set_max_background_jobs(config.max_background_jobs as i32);

        // Compression
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Open database
        let db = rocksdb::DB::open(&options, &config.directory)?;

        // Initialize components
        let buffer_pool = Arc::new(WriteBufferPool::new(
            config.max_write_buffer_number,
            config.max_write_buffer_size * config.max_write_buffer_number,
        ));

        let rate_limiter = Arc::new(WriteRateLimiter::new(config.write_rate_limit));

        let compaction_manager = Arc::new(AdaptiveCompactionManager::new(config.compaction_style));

        let metrics = Arc::new(RwLock::new(StorageMetrics::default()));

        let shutdown = Arc::new(AtomicBool::new(false));

        let status = Arc::new(RwLock::new(OptimizedStorageStatus {
            pending_writes: 0,
            memory_usage_bytes: 0,
            max_memory_bytes: config.memtable_memory_budget_mb as usize * 1024 * 1024,
            active_flushes: 0,
            total_flushes: 0,
            avg_flush_duration_ms: 0.0,
            workload_pattern: WorkloadPattern::Balanced,
            wal_size_bytes: 0,
            compaction_status: CompactionStatus {
                is_compacting: false,
                compaction_type: "none".to_string(),
                files_being_compacted: 0,
                estimated_time_remaining: 0,
                last_compaction_timestamp: None,
                compaction_score: 0.0,
            },
            write_rate: 0.0,
            is_healthy: true,
        }));

        // Create background flush task if enabled
        let background_flush = if config.async_flush {
            let handler: Arc<dyn AsyncFlushHandler + Send + Sync> = Arc::new(Self {
                db: Arc::new(RwLock::new(db)),
                config: config.clone(),
                buffer_pool: Arc::clone(&buffer_pool),
                rate_limiter: Arc::clone(&rate_limiter),
                background_flush: None,
                compaction_manager: Arc::clone(&compaction_manager),
                metrics: Arc::clone(&metrics),
                shutdown: Arc::clone(&shutdown),
                status: Arc::clone(&status),
            });

            Some(BackgroundFlushTask::new(
                Arc::clone(&buffer_pool),
                handler,
                Duration::from_millis(config.flush_interval_ms),
            ))
        } else {
            None
        };

        let storage = Self {
            db: Arc::new(RwLock::new(db)),
            config,
            buffer_pool,
            rate_limiter,
            background_flush,
            compaction_manager,
            metrics,
            shutdown,
            status,
        };

        Ok(storage)
    }

    /// Write a single operation
    pub fn write(&self, operation: StorageOperation) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        // Apply rate limiting
        if self.config.rate_limit_enabled {
            self.rate_limiter.try_acquire();
        }

        // Create batch and add to pool
        let batch = WriteBatch {
            id: uuid::Uuid::new_v4().to_string(),
            operations: vec![operation],
            created_at: start,
            priority: BatchPriority::Normal,
            size_hint: 1024,
        };

        // Try to add to buffer pool
        if self.buffer_pool.add_batch(batch).is_err() {
            // Buffer full, flush synchronously
            self.flush_immediate()?;
        }

        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        metrics.total_writes += 1;
        metrics.total_bytes_written += start.elapsed().as_millis() as u64;

        Ok(())
    }

    /// Write a batch of operations
    pub fn write_batch(&self, operations: &[StorageOperation], priority: BatchPriority) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        // Apply rate limiting
        if self.config.rate_limit_enabled {
            self.rate_limiter.try_acquire();
        }

        let total_size: usize = operations
            .iter()
            .map(|op| match op {
                StorageOperation::Put { key, value } => key.len() + value.len(),
                StorageOperation::Delete { key } => key.len(),
                StorageOperation::DeleteRange { start, end } => start.len() + end.len(),
            })
            .sum();

        let batch = WriteBatch {
            id: uuid::Uuid::new_v4().to_string(),
            operations: operations.to_vec(),
            created_at: start,
            priority,
            size_hint: total_size,
        };

        if self.buffer_pool.add_batch(batch).is_err() {
            self.flush_immediate()?;
        }

        Ok(())
    }

    /// Flush immediately (synchronous)
    pub fn flush_immediate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        if self.buffer_pool.is_empty() {
            return Ok(());
        }

        let batches = self.buffer_pool.get_batches_for_flush(100);

        let mut db = self.db.write().unwrap();
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);
        write_options.set_disable WAL(false);

        for batch in &batches {
            let mut batch_ops = rocksdb::WriteBatch::default();

            for op in &batch.operations {
                match op {
                    StorageOperation::Put { key, value } => {
                        batch_ops.put(key, value);
                    }
                    StorageOperation::Delete { key } => {
                        batch_ops.delete(key);
                    }
                    StorageOperation::DeleteRange { start, end } => {
                        batch_ops.delete_range(start, end);
                    }
                }
            }

            db.write_opt(batch_ops, &write_options)?;
        }

        // Flush WAL
        db.flush()?;

        let duration_ms = start.elapsed().as_millis() as u64;

        // Update status
        {
            let mut status = self.status.write().unwrap();
            status.total_flushes += 1;
            status.last_flush_timestamp = Some(chrono::Utc::now().timestamp() as u64);
            status.avg_flush_duration_ms =
                (status.avg_flush_duration_ms * (status.total_flushes as f64 - 1.0) + duration_ms as f64)
                    / status.total_flushes as f64;
        }

        Ok(())
    }

    /// Get storage status
    pub fn status(&self) -> OptimizedStorageStatus {
        self.status.read().unwrap().clone()
    }

    /// Analyze and adjust compaction
    pub fn analyze_and_compact(&self, io_stats: &IOStats) {
        let write_rate = self.metrics.read().unwrap().total_writes as f64;
        let read_rate = self.metrics.read().unwrap().total_reads as f64;

        self.compaction_manager.analyze_and_adjust(write_rate, read_rate, io_stats);
    }

    /// Get compaction manager
    pub fn compaction_manager(&self) -> Arc<AdaptiveCompactionManager> {
        Arc::clone(&self.compaction_manager)
    }
}

#[async_trait]
impl AsyncFlushHandler for OptimizedRocksDBStorage {
    async fn flush_batches(&self, batches: &[WriteBatch]) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        let mut db = self.db.write().unwrap();
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);

        for batch in batches {
            let mut rocks_batch = rocksdb::WriteBatch::default();

            for op in &batch.operations {
                match op {
                    StorageOperation::Put { key, value } => {
                        rocks_batch.put(key, value);
                    }
                    StorageOperation::Delete { key } => {
                        rocks_batch.delete(key);
                    }
                    StorageOperation::DeleteRange { start, end } => {
                        rocks_batch.delete_range(start, end);
                    }
                }
            }

            db.write_opt(rocks_batch, &write_options)?;
        }

        // Flush
        db.flush()?;

        let duration_ms = start.elapsed().as_millis() as u64;

        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        metrics.write_operations_pending = 0;

        Ok(())
    }

    async fn flush_immediate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush_immediate()
    }

    fn get_status(&self) -> FlushStatus {
        FlushStatus {
            is_flushing: self.background_flush.as_ref().map(|t| t.is_running()).unwrap_or(false),
            pending_operations: self.buffer_pool.len(),
            last_flush_timestamp: Some(chrono::Utc::now().timestamp() as u64),
            last_flush_duration_ms: 0,
            total_flushes: self.metrics.read().unwrap().total_writes,
        }
    }
}

impl Drop for OptimizedRocksDBStorage {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Stop background flush
        if let Some(task) = &self.background_flush {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                task.stop().await;
            });
        }

        // Final flush
        let _ = self.flush_immediate();
    }
}

/// Builder for optimized storage
#[derive(Debug)]
pub struct OptimizedStorageBuilder {
    config: OptimizedStorageConfig,
}

impl OptimizedStorageBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: OptimizedStorageConfig::default(),
        }
    }

    /// Set directory
    pub fn with_directory<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.config.directory = path.into();
        self
    }

    /// Set max write buffer size
    pub fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.config.max_write_buffer_size = size;
        self
    }

    /// Set memtable budget
    pub fn with_memtable_budget(mut self, mb: usize) -> Self {
        self.config.memtable_memory_budget_mb = mb;
        self
    }

    /// Enable async flush
    pub fn with_async_flush(mut self, enabled: bool) -> Self {
        self.config.async_flush = enabled;
        self
    }

    /// Set compaction style
    pub fn with_compaction_style(mut self, style: CompactionStyle) -> Self {
        self.config.compaction_style = style;
        self
    }

    /// Enable rate limiting
    pub fn with_rate_limit(mut self, ops_per_second: u64) -> Self {
        self.config.write_rate_limit = ops_per_second;
        self.config.rate_limit_enabled = ops_per_second > 0;
        self
    }

    /// Build the storage
    pub fn build(self) -> Result<OptimizedRocksDBStorage, Box<dyn std::error::Error>> {
        OptimizedRocksDBStorage::new(self.config)
    }
}

impl Default for OptimizedStorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_buffer_pool() {
        let pool = WriteBufferPool::new(10, 1024 * 1024);

        let batch = WriteBatch {
            id: "test".to_string(),
            operations: vec![StorageOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            }],
            created_at: Instant::now(),
            priority: BatchPriority::Normal,
            size_hint: 100,
        };

        assert!(pool.add_batch(batch).is_ok());
        assert_eq!(pool.len(), 1);
    }

    #[tokio::test]
    async fn test_write_rate_limiter() {
        let limiter = WriteRateLimiter::new(10);

        // Should be able to acquire tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire());
        }

        // Should be rate limited
        assert!(!limiter.try_acquire());
    }

    #[tokio::test]
    async fn test_optimized_storage_basic() {
        let temp_dir = TempDir::new().unwrap();

        let storage = OptimizedStorageBuilder::new()
            .with_directory(temp_dir.path())
            .with_async_flush(false)
            .with_max_buffer_size(1024 * 1024)
            .build()
            .unwrap();

        // Write some data
        storage
            .write(StorageOperation::Put {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            })
            .unwrap();

        // Check status
        let status = storage.status();
        assert!(status.is_healthy);
    }

    #[test]
    fn test_adaptive_compaction_manager() {
        let manager = AdaptiveCompactionManager::new(CompactionStyle::Level);

        let io_stats = IOStats::default();

        // Simulate write-heavy workload
        manager.analyze_and_adjust(1000.0, 10.0, &io_stats);

        let pattern = manager.current_pattern();
        assert!(matches!(pattern, WorkloadPattern::WriteHeavy));
    }
}
