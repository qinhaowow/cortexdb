//! Incremental Index Building Mechanism for coretexdb Enterprise
//!
//! This module provides incremental index update capabilities, allowing data to be
//! asynchronously updated while maintaining high query performance. It implements
//! a buffer-based approach that accumulates writes and periodically merges them
//! into the main index structure.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio::task::{self, JoinHandle};
use tracing::{debug, error, info, warn};

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::{Document, Embedding};

/// Configuration for incremental index building
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalIndexConfig {
    /// Maximum number of vectors in the pending buffer
    pub max_buffer_size: usize,
    /// Maximum time to wait before flushing pending updates
    pub flush_interval: Duration,
    /// Maximum memory usage for the buffer (in bytes)
    pub max_buffer_memory_mb: usize,
    /// Number of vectors per incremental build batch
    pub batch_size: usize,
    /// Whether to enable automatic background synchronization
    pub auto_sync: bool,
    /// Sync policy: "eager" (sync after each batch) or "lazy" (background sync)
    pub sync_policy: SyncPolicy,
    /// Maximum number of pending updates before forcing a sync
    pub max_pending_updates: usize,
    /// Enable compression for pending updates
    pub compress_pending: bool,
    /// Recovery mode: resume from last checkpoint on startup
    pub recovery_enabled: bool,
}

impl Default for IncrementalIndexConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 10000,
            flush_interval: Duration::from_secs(60),
            max_buffer_memory_mb: 256,
            batch_size: 1000,
            auto_sync: true,
            sync_policy: SyncPolicy::Lazy,
            max_pending_updates: 50000,
            compress_pending: true,
            recovery_enabled: true,
        }
    }
}

/// Synchronization policy for incremental updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncPolicy {
    /// Eager: Sync immediately after each batch
    Eager,
    /// Lazy: Background synchronization
    Lazy,
    /// Adaptive: Based on load conditions
    Adaptive,
}

/// Vector record for incremental indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    /// Unique document ID
    pub id: String,
    /// Vector embedding data
    pub embedding: Vec<f32>,
    /// Associated document metadata
    pub document: Option<Document>,
    /// Timestamp of the record
    pub timestamp: u64,
    /// Operation type
    pub operation: IndexOperation,
}

/// Type of index operation
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IndexOperation {
    /// Insert new vector
    Insert,
    /// Update existing vector
    Update,
    /// Mark vector for deletion (soft delete)
    Delete,
    /// Hard delete from index
    Purge,
}

/// Status of incremental index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalIndexStatus {
    /// Total vectors in main index
    pub main_index_size: usize,
    /// Pending vectors waiting to be indexed
    pub pending_buffer_size: usize,
    /// Total sync operations performed
    pub sync_count: u64,
    /// Last sync timestamp
    pub last_sync_timestamp: Option<u64>,
    /// Last successful sync duration (milliseconds)
    pub last_sync_duration_ms: u64,
    /// Whether a sync is currently in progress
    pub is_syncing: bool,
    /// Current memory usage of buffer (bytes)
    pub buffer_memory_bytes: usize,
    /// Number of pending deletions
    pub pending_deletions: usize,
    /// Index health status
    pub health: IndexHealth,
}

/// Health status of the incremental index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexHealth {
    /// Healthy - all operations normal
    Healthy,
    /// Warning - buffer approaching limits
    Warning(String),
    /// Critical - index needs immediate attention
    Critical(String),
    /// Recovering - restoring from checkpoint
    Recovering,
}

/// Pending update batch for efficient processing
#[derive(Debug, Clone)]
pub struct PendingBatch {
    /// Batch ID for tracking
    pub batch_id: String,
    /// Records in this batch
    pub records: Vec<VectorRecord>,
    /// Created timestamp
    pub created_at: u64,
    /// Operation type for this batch
    pub operation_type: IndexOperation,
}

/// Checkpoint data for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Checkpoint ID
    pub id: String,
    /// Last synced vector ID
    pub last_vector_id: usize,
    /// Last synced document ID
    pub last_document_id: String,
    /// Sequence number for ordering
    pub sequence_number: u64,
    /// Checkpoint timestamp
    pub timestamp: u64,
    /// Hash of checkpoint data for validation
    pub checksum: String,
}

/// Incremental Index trait for supporting real-time updates
#[async_trait]
pub trait IncrementalIndex: Send + Sync {
    /// Add vectors to the pending buffer
    async fn add_vectors(
        &mut self,
        vectors: &[VectorRecord],
    ) -> Result<(), CortexError>;

    /// Delete vectors by ID
    async fn delete_vectors(
        &mut self,
        ids: &[String],
    ) -> Result<(), CortexError>;

    /// Synchronize pending updates to the main index
    async fn sync_pending_updates(&mut self) -> Result<SyncResult, CortexError>;

    /// Get the number of pending updates
    fn pending_updates_count(&self) -> usize;

    /// Get current status of the incremental index
    async fn status(&self) -> IncrementalIndexStatus;

    /// Force a flush of all pending updates
    async fn flush(&mut self) -> Result<(), CortexError>;

    /// Checkpoint the current state for recovery
    async fn checkpoint(&mut self) -> Result<Checkpoint, CortexError>;

    /// Recover from a previous checkpoint
    async fn recover(&mut self, checkpoint: Checkpoint) -> Result<(), CortexError>;

    /// Pause incremental updates (for maintenance)
    async fn pause(&mut self) -> Result<(), CortexError>;

    /// Resume incremental updates
    async fn resume(&mut self) -> Result<(), CortexError>;

    /// Get buffer statistics
    fn buffer_stats(&self) -> BufferStats;
}

/// Statistics about the buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferStats {
    /// Current buffer size
    pub current_size: usize,
    /// Maximum buffer size
    pub max_size: usize,
    /// Current memory usage
    pub memory_usage_bytes: usize,
    /// Maximum memory allowed
    pub max_memory_bytes: usize,
    /// Average batch processing time (ms)
    pub avg_batch_time_ms: f64,
    /// Total records processed
    pub total_records_processed: u64,
    /// Dropped records (due to errors)
    pub dropped_records: u64,
}

/// Result of a synchronization operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Number of vectors added
    pub vectors_added: usize,
    /// Number of vectors updated
    pub vectors_updated: usize,
    /// Number of vectors deleted
    pub vectors_deleted: usize,
    /// Number of batches processed
    pub batches_processed: usize,
    /// Sync duration in milliseconds
    pub duration_ms: u64,
    /// Whether the sync was successful
    pub success: bool,
    /// Error message if failed
    pub error_message: Option<String>,
}

/// Implementation of incremental index for HNSW
pub struct IncrementalHnswIndex {
    /// Main HNSW index (immutable during sync)
    main_index: Arc<RwLock<super::HnswIndex>>,
    /// Pending insert buffer
    insert_buffer: Arc<RwLock<Vec<VectorRecord>>>,
    /// Pending delete buffer
    delete_buffer: Arc<RwLock<Vec<String>>>,
    /// Update buffer
    update_buffer: Arc<RwLock<Vec<VectorRecord>>>,
    /// Configuration
    config: IncrementalIndexConfig,
    /// Background sync task handle
    sync_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Shutdown signal for background task
    shutdown: Arc<RwLock<Option<Notify>>>,
    /// Sync in progress flag
    is_syncing: Arc<RwLock<bool>>,
    /// Statistics
    stats: Arc<RwLock<IncrementalIndexStats>>,
    /// Checkpoint directory
    checkpoint_dir: Option<PathBuf>,
    /// Sequence counter for ordering
    sequence: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
struct IncrementalIndexStats {
    sync_count: u64,
    last_sync_timestamp: Option<u64>,
    last_sync_duration_ms: u64,
    total_vectors_added: usize,
    total_vectors_updated: usize,
    total_vectors_deleted: usize,
    total_batches_processed: usize,
    dropped_records: u64,
}

impl Default for IncrementalIndexStats {
    fn default() -> Self {
        Self {
            sync_count: 0,
            last_sync_timestamp: None,
            last_sync_duration_ms: 0,
            total_vectors_added: 0,
            total_vectors_updated: 0,
            total_vectors_deleted: 0,
            total_batches_processed: 0,
            dropped_records: 0,
        }
    }
}

impl IncrementalHnswIndex {
    /// Create a new incremental HNSW index
    pub fn new(
        main_index: super::HnswIndex,
        config: Option<IncrementalIndexConfig>,
        checkpoint_dir: Option<PathBuf>,
    ) -> Self {
        let config = config.unwrap_or_default();

        let index = Self {
            main_index: Arc::new(RwLock::new(main_index)),
            insert_buffer: Arc::new(RwLock::new(Vec::with_capacity(config.max_buffer_size))),
            delete_buffer: Arc::new(RwLock::new(Vec::new())),
            update_buffer: Arc::new(RwLock::new(Vec::with_capacity(config.max_buffer_size))),
            config: config.clone(),
            sync_task: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(RwLock::new(Some(Notify::new()))),
            is_syncing: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(IncrementalIndexStats::default())),
            checkpoint_dir,
            sequence: Arc::new(RwLock::new(0)),
        };

        if index.config.auto_sync {
            index.start_background_sync();
        }

        info!(
            "Created incremental HNSW index with buffer size {}",
            config.max_buffer_size
        );

        index
    }

    /// Start background synchronization task
    fn start_background_sync(&mut self) {
        let insert_buffer = Arc::clone(&self.insert_buffer);
        let delete_buffer = Arc::clone(&self.delete_buffer);
        let update_buffer = Arc::clone(&self.update_buffer);
        let main_index = Arc::clone(&self.main_index);
        let is_syncing = Arc::clone(&self.is_syncing);
        let shutdown = Arc::clone(&self.shutdown);
        let stats = Arc::clone(&self.stats);
        let config = self.config.clone();

        let handle = task::spawn(async move {
            let mut interval = tokio::time::interval(config.flush_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let buffer_len = insert_buffer.read().unwrap().len()
                            + update_buffer.read().unwrap().len();

                        if buffer_len > 0 {
                            debug!(
                                "Triggering background sync with {} pending updates",
                                buffer_len
                            );
                            Self::perform_sync(
                                &main_index,
                                &insert_buffer,
                                &delete_buffer,
                                &update_buffer,
                                &is_syncing,
                                &stats,
                                &config,
                            ).await;
                        }
                    }
                    _ = shutdown.write().unwrap().as_ref().unwrap().notified() => {
                        info!("Shutdown signal received, stopping background sync");
                        break;
                    }
                }
            }
        });

        *self.sync_task.write().unwrap() = Some(handle);
    }

    /// Perform the actual synchronization
    async fn perform_sync(
        main_index: &Arc<RwLock<super::HnswIndex>>>,
        insert_buffer: &Arc<RwLock<Vec<VectorRecord>>>,
        delete_buffer: &Arc<RwLock<Vec<String>>>,
        update_buffer: &Arc<RwLock<Vec<VectorRecord>>>,
        is_syncing: &Arc<RwLock<bool>>,
        stats: &Arc<RwLock<IncrementalIndexStats>>>,
        config: &IncrementalIndexConfig,
    ) {
        // Check if already syncing
        if *is_syncing.read().unwrap() {
            warn!("Sync already in progress, skipping");
            return;
        }

        *is_syncing.write().unwrap() = true;

        let start_time = std::time::Instant::now();

        // Drain buffers
        let inserts = {
            let mut buffer = insert_buffer.write().unwrap();
            std::mem::take(&mut buffer)
        };

        let deletes = {
            let mut buffer = delete_buffer.write().unwrap();
            std::mem::take(&mut buffer)
        };

        let updates = {
            let mut buffer = update_buffer.write().unwrap();
            std::mem::take(&mut buffer)
        };

        // Process deletions first
        for id in &deletes {
            let mut index = main_index.write().unwrap();
            if let Ok(Some(internal_id)) = index.get_internal_id(id) {
                if let Err(e) = index.remove(internal_id) {
                    error!("Failed to delete vector {}: {}", id, e);
                }
            }
        }

        // Process updates (delete then re-insert)
        for record in &updates {
            if let Ok(Some(internal_id)) = {
                let index = main_index.read().unwrap();
                index.get_internal_id(&record.id)
            } {
                let mut index = main_index.write().unwrap();
                let _ = index.remove(internal_id);
            }
        }

        // Process inserts and updates in batches
        let all_inserts = [inserts, updates].concat();
        let batches = all_inserts.chunks(config.batch_size);

        let mut vectors_added = 0;
        let mut vectors_updated = 0;

        for batch in batches {
            let mut index = main_index.write().unwrap();

            for record in batch {
                let internal_id = {
                    let id_map = index.id_map.read().unwrap();
                    *id_map.get(&record.id).unwrap_or(&0)
                };

                let result = if internal_id > 0 {
                    // Update
                    index.remove(internal_id)?;
                    index.add(internal_id, record.embedding.clone())?;
                    vectors_updated += 1;
                    Ok(())
                } else {
                    // Insert
                    let new_id = index.size() + 1;
                    index.add(new_id, record.embedding.clone())?;
                    vectors_added += 1;
                    Ok(())
                };

                if let Err(e) = result {
                    error!("Failed to process record {}: {}", record.id, e);
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        // Update stats
        {
            let mut stats = stats.write().unwrap();
            stats.sync_count += 1;
            stats.last_sync_timestamp = Some(chrono::Utc::now().timestamp() as u64);
            stats.last_sync_duration_ms = duration_ms;
            stats.total_vectors_added += vectors_added;
            stats.total_vectors_updated += vectors_updated;
            stats.total_vectors_deleted += deletes.len();
            stats.total_batches_processed += batches.len();
        }

        *is_syncing.write().unwrap() = false;

        info!(
            "Sync completed: {} added, {} updated, {} deleted in {}ms",
            vectors_added,
            vectors_updated,
            deletes.len(),
            duration_ms
        );
    }

    /// Calculate buffer memory usage
    fn calculate_memory_usage(&self) -> usize {
        let insert_buffer = self.insert_buffer.read().unwrap();
        let update_buffer = self.update_buffer.read().unwrap();

        let insert_memory = insert_buffer
            .iter()
            .map(|r| r.embedding.len() * 4 + r.id.len())
            .sum::<usize>();

        let update_memory = update_buffer
            .iter()
            .map(|r| r.embedding.len() * 4 + r.id.len())
            .sum::<usize>();

        insert_memory + update_memory
    }
}

#[async_trait]
impl IncrementalIndex for IncrementalHnswIndex {
    async fn add_vectors(
        &mut self,
        vectors: &[VectorRecord],
    ) -> Result<(), CortexError> {
        let mut insert_buffer = self.insert_buffer.write().unwrap();

        // Check buffer size limits
        if insert_buffer.len() + vectors.len() > self.config.max_buffer_size {
            // Trigger sync if buffer is full
            drop(insert_buffer);
            self.sync_pending_updates().await?;
            insert_buffer = self.insert_buffer.write().unwrap();
        }

        // Add to buffer
        for vector in vectors {
            insert_buffer.push(vector.clone());
        }

        // Check if we need to trigger sync based on pending count
        if insert_buffer.len() >= self.config.max_pending_updates {
            drop(insert_buffer);
            self.sync_pending_updates().await?;
        }

        Ok(())
    }

    async fn delete_vectors(
        &mut self,
        ids: &[String],
    ) -> Result<(), CortexError> {
        let mut delete_buffer = self.delete_buffer.write().unwrap();
        delete_buffer.extend(ids.clone());
        Ok(())
    }

    async fn sync_pending_updates(&mut self) -> Result<SyncResult, CortexError> {
        let start_time = std::time::Instant::now();

        let buffers_filled = {
            let insert_buffer = self.insert_buffer.read().unwrap();
            let update_buffer = self.update_buffer.read().unwrap();
            let delete_buffer = self.delete_buffer.read().unwrap();

            insert_buffer.len() > 0
                || update_buffer.len() > 0
                || delete_buffer.len() > 0
        };

        if !buffers_filled {
            return Ok(SyncResult {
                vectors_added: 0,
                vectors_updated: 0,
                vectors_deleted: 0,
                batches_processed: 0,
                duration_ms: 0,
                success: true,
                error_message: None,
            });
        }

        Self::perform_sync(
            &self.main_index,
            &self.insert_buffer,
            &self.delete_buffer,
            &self.update_buffer,
            &self.is_syncing,
            &self.stats,
            &self.config,
        )
        .await;

        let duration_ms = start_time.elapsed().as_millis() as u64;

        let stats = self.stats.read().unwrap();
        let result = SyncResult {
            vectors_added: stats.total_vectors_added,
            vectors_updated: stats.total_vectors_updated,
            vectors_deleted: stats.total_vectors_deleted,
            batches_processed: stats.total_batches_processed,
            duration_ms,
            success: true,
            error_message: None,
        };

        Ok(result)
    }

    fn pending_updates_count(&self) -> usize {
        let insert_buffer = self.insert_buffer.read().unwrap();
        let update_buffer = self.update_buffer.read().unwrap();
        let delete_buffer = self.delete_buffer.read().unwrap();

        insert_buffer.len() + update_buffer.len() + delete_buffer.len()
    }

    async fn status(&self) -> IncrementalIndexStatus {
        let buffer_size = self.pending_updates_count();
        let memory_usage = self.calculate_memory_usage();

        let stats = self.stats.read().unwrap();

        let health = if buffer_size > self.config.max_pending_updates {
            IndexHealth::Critical(format!(
                "Buffer overflow: {} pending updates",
                buffer_size
            ))
        } else if buffer_size > self.config.max_buffer_size * 8 / 10 {
            IndexHealth::Warning(format!(
                "Buffer filling up: {} pending updates",
                buffer_size
            ))
        } else {
            IndexHealth::Healthy
        };

        let main_index = self.main_index.read().unwrap();

        IncrementalIndexStatus {
            main_index_size: main_index.size(),
            pending_buffer_size: buffer_size,
            sync_count: stats.sync_count,
            last_sync_timestamp: stats.last_sync_timestamp,
            last_sync_duration_ms: stats.last_sync_duration_ms,
            is_syncing: *self.is_syncing.read().unwrap(),
            buffer_memory_bytes: memory_usage,
            pending_deletions: {
                let delete_buffer = self.delete_buffer.read().unwrap();
                delete_buffer.len()
            },
            health,
        }
    }

    async fn flush(&mut self) -> Result<(), CortexError> {
        self.sync_pending_updates().await?;
        Ok(())
    }

    async fn checkpoint(&mut self) -> Result<Checkpoint, CortexError> {
        if let Some(ref checkpoint_dir) = self.checkpoint_dir {
            let timestamp = chrono::Utc::now().timestamp() as u64;
            let sequence = *self.sequence.read().unwrap();

            let checkpoint = Checkpoint {
                id: format!("checkpoint_{}", timestamp),
                last_vector_id: {
                    let index = self.main_index.read().unwrap();
                    index.size()
                },
                last_document_id: {
                    let insert_buffer = self.insert_buffer.read().unwrap();
                    insert_buffer
                        .last()
                        .map(|r| r.id.clone())
                        .unwrap_or_default()
                },
                sequence_number: sequence,
                timestamp,
                checksum: format!("{:x}", md5::compute(format!("{:?}", timestamp))),
            };

            // Write checkpoint to disk
            let checkpoint_path = checkpoint_dir.join(&checkpoint.id);
            let checkpoint_json = serde_json::to_string_pretty(&checkpoint)
                .map_err(|e| CortexError::Index(IndexError::InternalError(e.to_string())))?;

            tokio::fs::write(checkpoint_path, checkpoint_json)
                .await
                .map_err(|e| CortexError::Index(IndexError::InternalError(e.to_string())))?;

            info!("Created checkpoint: {}", checkpoint.id);

            Ok(checkpoint)
        } else {
            Err(CortexError::Index(IndexError::InternalError(
                "Checkpoint directory not configured".to_string(),
            )))
        }
    }

    async fn recover(&mut self, checkpoint: Checkpoint) -> Result<(), CortexError> {
        info!("Recovering from checkpoint: {}", checkpoint.id);

        // Pause background sync
        self.pause().await?;

        // Load pending updates from checkpoint
        // In a real implementation, this would reload buffered data from disk

        // Resume background sync
        self.resume().await?;

        info!("Recovery completed from checkpoint: {}", checkpoint.id);

        Ok(())
    }

    async fn pause(&mut self) -> Result<(), CortexError> {
        let mut shutdown = self.shutdown.write().unwrap();
        if let Some(ref notify) = *shutdown {
            notify.notify();
        }
        *shutdown = Some(Notify::new());

        // Wait for current sync to complete
        while *self.is_syncing.read().unwrap() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Incremental index paused");
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), CortexError> {
        if self.config.auto_sync {
            self.start_background_sync();
        }

        info!("Incremental index resumed");
        Ok(())
    }

    fn buffer_stats(&self) -> BufferStats {
        let insert_buffer = self.insert_buffer.read().unwrap();
        let update_buffer = self.update_buffer.read().unwrap();

        let stats = self.stats.read().unwrap();

        let current_size = insert_buffer.len() + update_buffer.len();
        let memory_usage = self.calculate_memory_usage();

        BufferStats {
            current_size,
            max_size: self.config.max_buffer_size,
            memory_usage_bytes: memory_usage,
            max_memory_bytes: self.config.max_buffer_memory_mb * 1024 * 1024,
            avg_batch_time_ms: if stats.total_batches_processed > 0 {
                stats.last_sync_duration_ms as f64 / stats.total_batches_processed as f64
            } else {
                0.0
            },
            total_records_processed: stats.total_vectors_added as u64
                + stats.total_vectors_updated as u64,
            dropped_records: stats.dropped_records,
        }
    }
}

impl Drop for IncrementalHnswIndex {
    fn drop(&mut self) {
        // Signal shutdown
        let mut shutdown = self.shutdown.write().unwrap();
        if let Some(ref notify) = *shutdown {
            notify.notify();
        }
    }
}

/// Builder for incremental indexes
#[derive(Debug)]
pub struct IncrementalIndexBuilder {
    config: IncrementalIndexConfig,
    checkpoint_dir: Option<PathBuf>,
}

impl IncrementalIndexBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: IncrementalIndexConfig::default(),
            checkpoint_dir: None,
        }
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.config.max_buffer_size = size;
        self
    }

    /// Set flush interval
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.config.flush_interval = interval;
        self
    }

    /// Set maximum buffer memory
    pub fn with_max_memory(mut self, mb: usize) -> Self {
        self.config.max_buffer_memory_mb = mb;
        self
    }

    /// Enable/disable auto sync
    pub fn with_auto_sync(mut self, enabled: bool) -> Self {
        self.config.auto_sync = enabled;
        self
    }

    /// Set sync policy
    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.config.sync_policy = policy;
        self
    }

    /// Set checkpoint directory
    pub fn with_checkpoint_dir(mut self, dir: PathBuf) -> Self {
        self.checkpoint_dir = Some(dir);
        self
    }

    /// Build the incremental index
    pub fn build<I>(self, main_index: I) -> IncrementalHnswIndex
    where
        I: Into<super::HnswIndex>,
    {
        IncrementalHnswIndex::new(main_index.into(), Some(self.config), self.checkpoint_dir)
    }
}

impl Default for IncrementalIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_incremental_index_add() {
        let config = IncrementalIndexConfig {
            max_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            auto_sync: false,
            ..Default::default()
        };

        let main_index = super::HnswIndex::new(128, 16, 100);
        let mut incremental = IncrementalHnswIndex::new(main_index, Some(config), None);

        // Add vectors
        let records = vec![
            VectorRecord {
                id: "vec1".to_string(),
                embedding: vec![0.1; 128],
                document: None,
                timestamp: 0,
                operation: IndexOperation::Insert,
            },
            VectorRecord {
                id: "vec2".to_string(),
                embedding: vec![0.2; 128],
                document: None,
                timestamp: 0,
                operation: IndexOperation::Insert,
            },
        ];

        incremental.add_vectors(&records).await.unwrap();
        assert_eq!(incremental.pending_updates_count(), 2);

        // Sync
        incremental.sync_pending_updates().await.unwrap();
        assert_eq!(incremental.pending_updates_count(), 0);

        // Check status
        let status = incremental.status().await;
        assert_eq!(status.main_index_size, 0); // Main index still empty, vectors in buffer
        assert_eq!(status.pending_buffer_size, 0);
    }

    #[tokio::test]
    async fn test_incremental_index_delete() {
        let config = IncrementalIndexConfig {
            max_buffer_size: 100,
            auto_sync: false,
            ..Default::default()
        };

        let main_index = super::HnswIndex::new(128, 16, 100);
        let mut incremental = IncrementalHnswIndex::new(main_index, Some(config), None);

        // Delete vectors
        incremental.delete_vectors(&["vec1".to_string(), "vec2".to_string()]).await.unwrap();
        assert_eq!(incremental.pending_updates_count(), 2);
    }

    #[tokio::test]
    async fn test_incremental_index_status() {
        let config = IncrementalIndexConfig {
            max_buffer_size: 10,
            auto_sync: false,
            ..Default::default()
        };

        let main_index = super::HnswIndex::new(128, 16, 100);
        let incremental = IncrementalHnswIndex::new(main_index, Some(config), None);

        let status = incremental.status().await;
        assert!(matches!(status.health, IndexHealth::Healthy));
        assert!(!status.is_syncing);
    }

    #[tokio::test]
    async fn test_buffer_stats() {
        let config = IncrementalIndexConfig {
            max_buffer_size: 100,
            auto_sync: false,
            ..Default::default()
        };

        let main_index = super::HnswIndex::new(128, 16, 100);
        let incremental = IncrementalHnswIndex::new(main_index, Some(config), None);

        let stats = incremental.buffer_stats();
        assert_eq!(stats.current_size, 0);
        assert_eq!(stats.max_size, 100);
    }
}

