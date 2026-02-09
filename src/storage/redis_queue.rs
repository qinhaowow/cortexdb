use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, broadcast};
use tokio::time::timeout;
use redis::{AsyncCommands, Client, RedisResult, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum RedisQueueError {
    #[error("Redis connection failed: {0}")]
    ConnectionError(String),
    #[error("Queue operation failed: {0}")]
    OperationError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    #[error("Timeout error")]
    TimeoutError,
    #[error("Queue not found: {0}")]
    QueueNotFound(String),
    #[error("Invalid message format")]
    InvalidMessageFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMessage {
    pub id: String,
    pub payload: Vec<u8>,
    pub priority: u8,
    pub created_at: u64,
    pub attempts: u32,
    pub metadata: HashMap<String, String>,
}

impl QueueMessage {
    pub fn new<T: Serialize>(payload: &T, priority: u8) -> Result<Self, RedisQueueError> {
        let id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(payload).map_err(|e| RedisQueueError::SerializationError(e.to_string()))?;
        
        Ok(Self {
            id,
            payload,
            priority,
            created_at: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
            attempts: 0,
            metadata: HashMap::new(),
        })
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_queue_size: usize,
    pub message_ttl: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
    pub consumer_timeout: Duration,
    pub batch_size: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 100000,
            message_ttl: Duration::from_secs(86400),
            retry_delay: Duration::from_secs(5),
            max_retries: 3,
            consumer_timeout: Duration::from_secs(30),
            batch_size: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub messages_in: AtomicU64,
    pub messages_out: AtomicU64,
    pub messages_failed: AtomicU64,
    pub messages_requeued: AtomicU64,
    pub current_size: AtomicUsize,
    pub avg_processing_time_ms: AtomicU64,
    pub last_activity: AtomicU64,
}

impl Default for QueueStats {
    fn default() -> Self {
        Self {
            messages_in: AtomicU64::new(0),
            messages_out: AtomicU64::new(0),
            messages_failed: AtomicU64::new(0),
            messages_requeued: AtomicU64::new(0),
            current_size: AtomicUsize::new(0),
            avg_processing_time_ms: AtomicU64::new(0),
            last_activity: AtomicU64::new(0),
        }
    }
}

pub struct RedisQueue {
    client: Client,
    config: QueueConfig,
    stats: Arc<QueueStats>,
    producers: Arc<RwLock<HashMap<String, mpsc::Sender<QueueMessage>>>>,
    consumers: Arc<RwLock<HashMap<String, mpsc::Receiver<QueueMessage>>>>,
    shutdown: broadcast::Sender<()>,
}

impl RedisQueue {
    pub async fn new(connection_string: &str, config: Option<QueueConfig>) -> Result<Self, RedisQueueError> {
        let client = Client::open(connection_string)
            .map_err(|e| RedisQueueError::ConnectionError(e.to_string()))?;
        
        let config = config.unwrap_or_default();
        let stats = Arc::new(QueueStats::default());
        let producers = Arc::new(RwLock::new(HashMap::new()));
        let consumers = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown, _) = broadcast::channel(1);
        
        let queue = Self {
            client,
            config,
            stats,
            producers,
            consumers,
            shutdown,
        };
        
        info!("RedisQueue initialized with connection: {}", connection_string);
        Ok(queue)
    }

    async fn get_connection(&self) -> Result<redis::aio::Connection, RedisQueueError> {
        self.client.get_async_connection()
            .await
            .map_err(|e| RedisQueueError::ConnectionError(e.to_string()))
    }

    pub async fn enqueue(&self, queue_name: &str, message: QueueMessage) -> Result<(), RedisQueueError> {
        let start = Instant::now();
        let mut conn = self.get_connection().await?;
        
        let queue_key = format!("queue:{}", queue_name);
        let priority_key = format!("queue:{}:priority", queue_name);
        
        let size: usize = conn.zcard(&queue_key).await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))?;
        
        if size >= self.config.max_queue_size {
            error!("Queue {} is full (max: {})", queue_name, self.config.max_queue_size);
            return Err(RedisQueueError::OperationError("Queue is full".to_string()));
        }
        
        let score = -(message.created_at as f64 + (message.priority as f64 * 1e10));
        let message_json = serde_json::to_string(&message)
            .map_err(|e| RedisQueueError::SerializationError(e.to_string()))?;
        
        let _: () = conn.zadd(&queue_key, message_json.clone(), score).await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))?;
        
        conn.setex(format!("msg:{}", message.id), self.config.message_ttl.as_secs(), &message_json).await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))?;
        
        self.stats.messages_in.fetch_add(1, Ordering::SeqCst);
        self.stats.current_size.fetch_add(1, Ordering::SeqCst);
        self.stats.last_activity.store(std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64, Ordering::SeqCst);
        
        debug!("Enqueued message {} to queue {} in {:?}", message.id, queue_name, start.elapsed());
        Ok(())
    }

    pub async fn dequeue<T: for<'de> Deserialize<'de>>(&self, queue_name: &str) -> Result<Option<(QueueMessage, T)>, RedisQueueError> {
        let start = Instant::now();
        let mut conn = self.get_connection().await?;
        
        let queue_key = format!("queue:{}", queue_name);
        
        let result: RedisResult<Value> = conn.zpopmax(&queue_key, 1).await;
        
        match result {
            Ok(Value::BulkString(values)) if !values.is_empty() => {
                if let Some(bytes) = values.first() {
                    let message_json = String::from_utf8_lossy(bytes);
                    let message: QueueMessage = serde_json::from_str(&message_json)
                        .map_err(|e| RedisQueueError::DeserializationError(e.to_string()))?;
                    
                    let payload: T = serde_json::from_slice(&message.payload)
                        .map_err(|e| RedisQueueError::DeserializationError(e.to_string()))?;
                    
                    self.stats.messages_out.fetch_add(1, Ordering::SeqCst);
                    self.stats.current_size.fetch_sub(1, Ordering::SeqCst);
                    
                    debug!("Dequeued message {} from queue {} in {:?}", message.id, queue_name, start.elapsed());
                    return Ok(Some((message, payload)));
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!("Dequeue failed: {}", e);
                return Err(RedisQueueError::OperationError(e.to_string()));
            }
        }
        
        Ok(None)
    }

    pub async fn acknowledge(&self, queue_name: &str, message_id: &str) -> Result<bool, RedisQueueError> {
        let mut conn = self.get_connection().await?;
        let msg_key = format!("msg:{}", message_id);
        
        let deleted: bool = conn.del(&msg_key).await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))?;
        
        if deleted {
            debug!("Acknowledged message {} from queue {}", message_id, queue_name);
        }
        
        Ok(deleted)
    }

    pub async fn requeue(&self, queue_name: &str, message: QueueMessage) -> Result<(), RedisQueueError> {
        let mut message = message;
        message.attempts += 1;
        
        if message.attempts > self.config.max_retries {
            error!("Message {} exceeded max retries", message.id);
            self.stats.messages_failed.fetch_add(1, Ordering::SeqCst);
            return Err(RedisQueueError::OperationError("Max retries exceeded".to_string()));
        }
        
        let delay = self.config.retry_delay * message.attempts;
        tokio::time::sleep(delay).await;
        
        self.enqueue(queue_name, message).await?;
        self.stats.messages_requeued.fetch_add(1, Ordering::SeqCst);
        
        Ok(())
    }

    pub async fn get_queue_size(&self, queue_name: &str) -> Result<usize, RedisQueueError> {
        let mut conn = self.get_connection().await?;
        let queue_key = format!("queue:{}", queue_name);
        
        conn.zcard(&queue_key)
            .await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))
    }

    pub async fn get_queue_stats(&self) -> HashMap<String, usize> {
        let mut conn = match self.get_connection().await {
            Ok(c) => c,
            Err(_) => return HashMap::new(),
        };
        
        let mut stats = HashMap::new();
        let queue_names = vec!["default", "high", "low", "critical"];
        
        for name in queue_names {
            if let Ok(size) = self.get_queue_size(name).await {
                stats.insert(name.to_string(), size);
            }
        }
        
        stats
    }

    pub async fn subscribe(&self, channel: &str) -> Result<mpsc::Receiver<QueueMessage>, RedisQueueError> {
        let mut conn = self.get_connection().await?;
        let pubsub = conn.as_pubsub();
        
        pubsub.subscribe(channel).await
            .map_err(|e| RedisQueueError::ConnectionError(e.to_string()))?;
        
        let (tx, rx) = mpsc::channel(1000);
        let channel = channel.to_string();
        let shutdown_rx = self.shutdown.subscribe();
        
        tokio::spawn(async move {
            let mut msg_stream = pubsub.on_message();
            
            loop {
                tokio::select! {
                    Some(msg) = msg_stream.next() => {
                        if let Ok(payload) = msg.get_payload::<String>() {
                            if let Ok(message) = serde_json::from_str::<QueueMessage>(&payload) {
                                let _ = tx.send(message).await;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        
        Ok(rx)
    }

    pub async fn publish(&self, channel: &str, message: &QueueMessage) -> Result<u64, RedisQueueError> {
        let mut conn = self.get_connection().await?;
        let message_json = serde_json::to_string(message)
            .map_err(|e| RedisQueueError::SerializationError(e.to_string()))?;
        
        conn.publish(channel, message_json)
            .await
            .map_err(|e| RedisQueueError::OperationError(e.to_string()))
    }

    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        info!("RedisQueue shutdown initiated");
    }
}

impl Drop for RedisQueue {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "RedisQueue stats - In: {}, Out: {}, Failed: {}, Requeued: {}",
            stats.messages_in.load(Ordering::SeqCst),
            stats.messages_out.load(Ordering::SeqCst),
            stats.messages_failed.load(Ordering::SeqCst),
            stats.messages_requeued.load(Ordering::SeqCst)
        );
    }
}

pub struct RedisQueueConfig {
    pub connection_string: String,
    pub queue_config: QueueConfig,
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            connection_string: "redis://127.0.0.1/".to_string(),
            queue_config: QueueConfig::default(),
        }
    }
}
