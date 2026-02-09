use std::collections::{VecDeque, HashMap, BTreeMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::cmp::{Ordering, PartialOrd};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{debug, warn};
use dashmap::DashMap;
use indexmap::IndexMap;

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("Queue is full: {0}")]
    QueueFull(String),
    
    #[error("Queue is empty")]
    QueueEmpty,
    
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_size: usize,
    pub max_priority: u32,
    pub enable_blocking: bool,
    pub blocking_timeout: Duration,
    pub enable_backpressure: bool,
    pub backpressure_threshold: f64,
    pub enable_priority_inheritance: bool,
    pub fair_shares: Vec<String>,
    pub enable_dead_letter: bool,
    pub dead_letter_queue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry<T> {
    pub id: String,
    pub data: T,
    pub priority: u32,
    pub enqueued_at: u64,
    pub dequeued_at: Option<u64>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub deadline: Option<u64>,
    pub metadata: EntryMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub message_type: Option<String>,
    pub headers: HashMap<String, String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub total_enqueued: u64,
    pub total_dequeued: u64,
    pub total_failed: u64,
    pub current_size: usize,
    pub peak_size: usize,
    pub avg_processing_time_ms: f64,
    pub avg_wait_time_ms: f64,
    pub throughput_per_sec: f64,
    pub utilization_percent: f64,
    pub priority_distribution: HashMap<u32, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityBucket<T> {
    pub priority: u32,
    pub entries: VecDeque<Arc<QueueEntry<T>>>,
    pub processing: usize,
    pub completed: u64,
    pub failed: u64,
}

impl<T> PriorityBucket<T> {
    pub fn new(priority: u32) -> Self {
        Self {
            priority,
            entries: VecDeque::new(),
            processing: 0,
            completed: 0,
            failed: 0,
        }
    }
    
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    pub fn is_full(&self, max_size: usize) -> bool {
        self.entries.len() >= max_size
    }
}

pub struct PriorityQueue<T> {
    buckets: BTreeMap<u32, PriorityBucket<T>>,
    max_size: usize,
    fair_shares: Vec<String>,
    fair_share_index: Arc<RwLock<usize>>,
    total_size: Arc<std::sync::atomic::AtomicUsize>,
    stats: Arc<QueueStatsInternal>,
    config: QueueConfig,
}

struct QueueStatsInternal {
    total_enqueued: std::sync::atomic::AtomicU64,
    total_dequeued: std::sync::atomic::AtomicU64,
    total_failed: std::sync::atomic::AtomicU64,
    peak_size: std::sync::atomic::AtomicUsize,
    processing_times: dashmap::DashMap<String, Vec<u64>>,
    wait_times: dashmap::DashMap<String, Vec<u64>>,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_size: 10000,
            max_priority: 100,
            enable_blocking: true,
            blocking_timeout: Duration::from_secs(30),
            enable_backpressure: true,
            backpressure_threshold: 0.8,
            enable_priority_inheritance: false,
            fair_shares: Vec::new(),
            enable_dead_letter: false,
            dead_letter_queue: None,
        }
    }
}

impl Default for EntryMetadata {
    fn default() -> Self {
        Self {
            correlation_id: None,
            reply_to: None,
            message_type: None,
            headers: HashMap::new(),
            tags: Vec::new(),
        }
    }
}

impl Default for QueueStats {
    fn default() -> Self {
        Self {
            total_enqueued: 0,
            total_dequeued: 0,
            total_failed: 0,
            current_size: 0,
            peak_size: 0,
            avg_processing_time_ms: 0.0,
            avg_wait_time_ms: 0.0,
            throughput_per_sec: 0.0,
            utilization_percent: 0.0,
            priority_distribution: HashMap::new(),
        }
    }
}

impl<T> PriorityQueue<T> {
    pub fn new(config: Option<QueueConfig>) -> Self {
        let config = config.unwrap_or_default();
        
        let mut buckets = BTreeMap::new();
        for priority in 0..=config.max_priority {
            buckets.insert(priority, PriorityBucket::new(priority));
        }
        
        Self {
            buckets,
            max_size: config.max_size,
            fair_shares: config.fair_shares.clone(),
            fair_share_index: Arc::new(RwLock::new(0)),
            total_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            stats: Arc::new(QueueStatsInternal {
                total_enqueued: std::sync::atomic::AtomicU64::new(0),
                total_dequeued: std::sync::atomic::AtomicU64::new(0),
                total_failed: std::sync::atomic::AtomicU64::new(0),
                peak_size: std::sync::atomic::AtomicUsize::new(0),
                processing_times: DashMap::new(),
                wait_times: DashMap::new(),
            }),
            config,
        }
    }
    
    pub fn enqueue(&mut self, id: String, data: T, priority: u32, metadata: Option<EntryMetadata>) 
        -> Result<(), QueueError> {
        let current_size = self.total_size.load(std::sync::atomic::Ordering::SeqCst);
        
        if current_size >= self.max_size {
            if self.config.enable_backpressure {
                return Err(QueueError::QueueFull(format!("Queue is full (max: {})", self.max_size)));
            }
        }
        
        let priority = priority.min(self.config.max_priority);
        
        let entry = Arc::new(QueueEntry {
            id,
            data,
            priority,
            enqueued_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            dequeued_at: None,
            attempts: 0,
            max_attempts: 3,
            deadline: None,
            metadata: metadata.unwrap_or_default(),
        });
        
        if let Some(bucket) = self.buckets.get_mut(&priority) {
            bucket.entries.push_back(entry);
        }
        
        self.total_size.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let peak = self.stats.peak_size.load(std::sync::atomic::Ordering::SeqCst);
        if current_size + 1 > peak {
            self.stats.peak_size.store(current_size + 1, std::sync::atomic::Ordering::SeqCst);
        }
        
        self.stats.total_enqueued.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        Ok(())
    }
    
    pub fn dequeue(&mut self) -> Result<Option<Arc<QueueEntry<T>>>, QueueError> {
        let mut fair_index = self.fair_share_index.write().unwrap();
        
        for _ in 0..self.buckets.len() {
            let current_index = *fair_index % self.buckets.len().max(1);
            *fair_index = (current_index + 1) % self.buckets.len().max(1);
            
            for (offset, (priority, bucket)) in self.buckets.iter_mut().enumerate() {
                if offset == current_index {
                    if let Some(entry) = bucket.entries.pop_front() {
                        let mut entry = (*entry).clone();
                        entry.dequeued_at = Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs()
                        );
                        
                        self.total_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        self.stats.total_dequeued.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        
                        bucket.processing += 1;
                        
                        return Ok(Some(Arc::new(entry)));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    pub fn peek(&self) -> Option<&Arc<QueueEntry<T>>> {
        for bucket in self.buckets.values() {
            if let Some(entry) = bucket.entries.front() {
                return Some(entry);
            }
        }
        None
    }
    
    pub fn len(&self) -> usize {
        self.total_size.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    pub fn capacity(&self) -> usize {
        self.max_size
    }
    
    pub fn contains(&self, id: &str) -> bool {
        for bucket in self.buckets.values() {
            for entry in &bucket.entries {
                if entry.id == id {
                    return true;
                }
            }
        }
        false
    }
    
    pub fn remove(&mut self, id: &str) -> Result<Option<Arc<QueueEntry<T>>>, QueueError> {
        for bucket in self.buckets.values_mut() {
            if let Some(pos) = bucket.entries.iter().position(|e| e.id == id) {
                if let Some(entry) = bucket.entries.remove(pos) {
                    self.total_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }
    
    pub fn clear(&mut self) {
        for bucket in self.buckets.values_mut() {
            bucket.entries.clear();
            bucket.processing = 0;
        }
        self.total_size.store(0, std::sync::atomic::Ordering::SeqCst);
    }
    
    pub fn get_stats(&self) -> QueueStats {
        let current_size = self.len();
        
        let mut priority_distribution = HashMap::new();
        for (priority, bucket) in &self.buckets {
            if bucket.entries.len() > 0 {
                priority_distribution.insert(*priority, bucket.entries.len());
            }
        }
        
        QueueStats {
            total_enqueued: self.stats.total_enqueued.load(std::sync::atomic::Ordering::SeqCst),
            total_dequeued: self.stats.total_dequeued.load(std::sync::atomic::Ordering::SeqCst),
            total_failed: self.stats.total_failed.load(std::sync::atomic::Ordering::SeqCst),
            current_size,
            peak_size: self.stats.peak_size.load(std::sync::atomic::Ordering::SeqCst),
            avg_processing_time_ms: 0.0,
            avg_wait_time_ms: 0.0,
            throughput_per_sec: 0.0,
            utilization_percent: if self.max_size > 0 {
                (current_size as f64 / self.max_size as f64) * 100.0
            } else {
                0.0
            },
            priority_distribution,
        }
    }
    
    pub fn get_by_priority(&self, priority: u32) -> Vec<Arc<QueueEntry<T>>> {
        if let Some(bucket) = self.buckets.get(&priority) {
            bucket.entries.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }
    
    pub fn get_all(&self) -> Vec<Arc<QueueEntry<T>>> {
        let mut all = Vec::new();
        for bucket in self.buckets.values() {
            all.extend(bucket.entries.iter().cloned());
        }
        all
    }
}

pub struct DelayQueue<T> {
    entries: BTreeMap<u64, Vec<Arc<QueueEntry<T>>>>,
    max_size: usize,
    total_size: Arc<std::sync::atomic::AtomicUsize>,
    clock: Arc<std::sync::atomic::AtomicU64>,
}

impl<T> DelayQueue<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            max_size,
            total_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            clock: Arc::new(std::sync::atomic::AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            )),
        }
    }
    
    pub fn enqueue(&mut self, delay: Duration, entry: Arc<QueueEntry<T>>) -> Result<(), QueueError> {
        if self.total_size.load(std::sync::atomic::Ordering::SeqCst) >= self.max_size {
            return Err(QueueError::QueueFull("Delay queue is full".to_string()));
        }
        
        let now = self.clock.load(std::sync::atomic::Ordering::SeqCst);
        let ready_at = now + delay.as_secs();
        
        self.entries.entry(ready_at)
            .or_insert_with(Vec::new)
            .push(entry);
        
        self.total_size.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        Ok(())
    }
    
    pub fn poll_ready(&mut self) -> Vec<Arc<QueueEntry<T>>> {
        let now = self.clock.load(std::sync::atomic::Ordering::SeqCst);
        let mut ready = Vec::new();
        
        let ready_times: Vec<u64> = self.entries.keys()
            .filter(|&&time| time <= now)
            .cloned()
            .collect();
        
        for time in ready_times {
            if let Some(entries) = self.entries.remove(&time) {
                ready.extend(entries);
                self.total_size.fetch_sub(entries.len(), std::sync::atomic::Ordering::SeqCst);
            }
        }
        
        ready
    }
    
    pub fn len(&self) -> usize {
        self.total_size.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    pub fn next_ready_time(&self) -> Option<u64> {
        self.entries.keys().next().cloned()
    }
}

pub struct RingBuffer<T> {
    buffer: Vec<Option<T>>,
    head: usize,
    tail: usize,
    capacity: usize,
    mask: usize,
}

impl<T: Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        Self {
            buffer: vec![None; capacity],
            head: 0,
            tail: 0,
            capacity,
            mask: capacity - 1,
        }
    }
    
    pub fn push(&mut self, item: T) -> Result<(), QueueError> {
        let next_tail = (self.tail + 1) & self.mask;
        if next_tail == self.head {
            return Err(QueueError::QueueFull("Ring buffer is full".to_string()));
        }
        
        self.buffer[self.tail] = Some(item);
        self.tail = next_tail;
        
        Ok(())
    }
    
    pub fn pop(&mut self) -> Result<Option<T>, QueueError> {
        if self.head == self.tail {
            return Err(QueueError::QueueEmpty);
        }
        
        let item = self.buffer[self.head].take();
        self.head = (self.head + 1) & self.mask;
        
        Ok(item)
    }
    
    pub fn len(&self) -> usize {
        (self.tail - self.head) & self.mask
    }
    
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }
    
    pub fn capacity(&self) -> usize {
        self.capacity - 1
    }
    
    pub fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
        self.buffer.fill(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_priority_queue_creation() {
        let queue: PriorityQueue<i32> = PriorityQueue::new(None);
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_priority_queue_enqueue_dequeue() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("1".to_string(), 100, 10, None).unwrap();
        queue.enqueue("2".to_string(), 200, 5, None).unwrap();
        queue.enqueue("3".to_string(), 300, 15, None).unwrap();
        
        assert_eq!(queue.len(), 3);
        
        let result = queue.dequeue();
        assert!(result.is_ok());
        let entry = result.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().priority, 15);
    }

    #[tokio::test]
    async fn test_priority_queue_contains() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("test1".to_string(), 100, 5, None).unwrap();
        
        assert!(queue.contains("test1"));
        assert!(!queue.contains("test2"));
    }

    #[tokio::test]
    async fn test_priority_queue_remove() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("remove_me".to_string(), 100, 5, None).unwrap();
        let removed = queue.remove("remove_me");
        assert!(removed.is_ok());
        assert!(removed.unwrap().is_some());
        assert!(!queue.contains("remove_me"));
    }

    #[tokio::test]
    async fn test_priority_queue_clear() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("1".to_string(), 100, 5, None).unwrap();
        queue.enqueue("2".to_string(), 200, 5, None).unwrap();
        
        queue.clear();
        
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_priority_queue_stats() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("1".to_string(), 100, 5, None).unwrap();
        queue.enqueue("2".to_string(), 200, 10, None).unwrap();
        
        let stats = queue.get_stats();
        assert_eq!(stats.current_size, 2);
        assert_eq!(stats.total_enqueued, 2);
    }

    #[tokio::test]
    async fn test_ring_buffer() {
        let mut buffer: RingBuffer<i32> = RingBuffer::new(8);
        
        for i in 0..5 {
            buffer.push(i).unwrap();
        }
        
        assert_eq!(buffer.len(), 5);
        
        for i in 0..5 {
            let item = buffer.pop().unwrap();
            assert!(item.is_some());
            assert_eq!(item.unwrap(), i);
        }
        
        assert!(buffer.pop().is_err());
    }

    #[tokio::test]
    async fn test_ring_buffer_wrap_around() {
        let mut buffer: RingBuffer<i32> = RingBuffer::new(4);
        
        buffer.push(1).unwrap();
        buffer.push(2).unwrap();
        buffer.push(3).unwrap();
        
        buffer.pop().unwrap();
        buffer.push(4).unwrap();
        
        assert_eq!(buffer.len(), 3);
        
        buffer.push(5).unwrap();
        
        assert_eq!(buffer.len(), 4);
    }

    #[tokio::test]
    async fn test_delay_queue() {
        let mut delay_queue: DelayQueue<i32> = DelayQueue::new(100);
        
        let entry = Arc::new(QueueEntry {
            id: "delayed".to_string(),
            data: 42,
            priority: 5,
            enqueued_at: 0,
            dequeued_at: None,
            attempts: 0,
            max_attempts: 3,
            deadline: None,
            metadata: EntryMetadata::default(),
        });
        
        delay_queue.enqueue(Duration::from_secs(1), entry).unwrap();
        
        assert_eq!(delay_queue.len(), 1);
        
        let ready = delay_queue.poll_ready();
        assert!(ready.is_empty());
    }

    #[tokio::test]
    async fn test_queue_with_metadata() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        let metadata = EntryMetadata {
            correlation_id: Some("corr-123".to_string()),
            reply_to: Some("response_queue".to_string()),
            message_type: Some("command".to_string()),
            headers: HashMap::from([
                ("version".to_string(), "1.0".to_string()),
            ]),
            tags: vec!["important".to_string()],
        };
        
        queue.enqueue("msg1".to_string(), 100, 10, Some(metadata)).unwrap();
        
        let entry = queue.dequeue().unwrap().unwrap();
        assert_eq!(entry.metadata.correlation_id, Some("corr-123".to_string()));
        assert_eq!(entry.metadata.tags, vec!["important".to_string()]);
    }

    #[tokio::test]
    async fn test_priority_distribution() {
        let mut queue: PriorityQueue<i32> = PriorityQueue::new(None);
        
        queue.enqueue("1".to_string(), 100, 1, None).unwrap();
        queue.enqueue("2".to_string(), 200, 1, None).unwrap();
        queue.enqueue("3".to_string(), 300, 5, None).unwrap();
        queue.enqueue("4".to_string(), 400, 10, None).unwrap();
        
        let stats = queue.get_stats();
        assert_eq!(stats.priority_distribution.get(&1), Some(&2));
        assert_eq!(stats.priority_distribution.get(&5), Some(&1));
        assert_eq!(stats.priority_distribution.get(&10), Some(&1));
    }
}
