use std::collections::{HashMap, HashSet, LinkedList};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use dashmap::DashMap;
use lru::LruCache;
use xxhash_rust::xxh3;
use crossbeam::epoch::{self, Guard};
use rayon::prelude::*;
use parking_lot::Mutex;
use once_cell::sync::Lazy;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Cache error: {0}")]
    CacheError(String),
    
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Capacity exceeded: {0}")]
    CapacityExceeded(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("IO error: {0}")]
    IoError(String),
    
    #[error("Eviction error: {0}")]
    EvictionError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub max_capacity: usize,
    pub max_memory_bytes: u64,
    pub ttl_seconds: u64,
    pub ttl_check_interval_seconds: u64,
    pub eviction_policy: EvictionPolicy,
    pub enable_compression: bool,
    pub compression_threshold_bytes: usize,
    pub compression_type: CompressionType,
    pub enable_persistence: bool,
    pub persistence_path: Option<String>,
    pub persistence_interval_seconds: u64,
    pub warm_on_start: bool,
    pub enable_statistics: bool,
    pub enable_bloom_filter: bool,
    pub bloom_filter_fp_rate: f64,
    pub enable_tiered_storage: bool,
    pub l1_cache_size: usize,
    pub l2_cache_size: usize,
    pub l3_cache_size: usize,
    pub write_buffer_size: usize,
    pub enable_memory_mapped: bool,
    pub memory_mapped_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionPolicy {
    LRU,
    LFU,
    LRU_K,
    ARC,
    FIFO,
    LIFO,
    MRU,
    Random,
    SizeBased,
    TTLBased,
    SegmentedLRU,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
    Snappy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry<T> {
    pub key: String,
    pub value: T,
    pub created_at: u64,
    pub accessed_at: u64,
    pub modified_at: u64,
    pub access_count: u64,
    pub size_bytes: usize,
    pub frequency: f64,
    pub priority: CachePriority,
    pub flags: EntryFlags,
    pub metadata: EntryMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryFlags {
    pub pinned: bool,
    pub dirty: bool,
    pub compressed: bool,
    pub encrypted: bool,
    pub shared: bool,
    pub temporary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub version: u64,
    pub checksum: String,
    pub tags: HashSet<String>,
    pub source: Option<String>,
    pub expire_at: Option<u64>,
    pub last_refresh: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CachePriority {
    Critical,
    High,
    Medium,
    Low,
    Background,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub evictions: u64,
    pub expirations: u64,
    pub insertions: u64,
    pub deletions: u64,
    pub current_size: usize,
    pub current_items: u64,
    pub memory_usage_bytes: u64,
    pub peak_memory_bytes: u64,
    pub avg_access_time_ns: u64,
    pub avg_insert_time_ns: u64,
    pub compression_ratio: f64,
    pub bloom_filter_stats: Option<BloomFilterStats>,
    pub tier_stats: TierStats,
    pub last_updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterStats {
    pub items_count: u64,
    pub false_positives: u64,
    pub memory_bytes: u64,
    pub fp_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStats {
    pub l1_hits: u64,
    pub l1_misses: u64,
    pub l2_hits: u64,
    pub l2_misses: u64,
    pub l3_hits: u64,
    pub l3_misses: u64,
    pub promotions: u64,
    pub demotions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePolicy {
    pub max_entries: usize,
    pub max_memory: u64,
    pub default_ttl: Duration,
    pub refresh_interval: Option<Duration>,
    pub stale_while_revalidate: bool,
    pub stale_ttl: Duration,
    pub write_back: bool,
    pub write_through: bool,
    pub read_back: bool,
    pub async_writes: bool,
    pub batch_inserts: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheEvent {
    Hit(CacheHitEvent),
    Miss(CacheMissEvent),
    Insert(CacheInsertEvent),
    Evict(CacheEvictEvent),
    Expire(CacheExpireEvent),
    Error(CacheErrorEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheHitEvent {
    pub key: String,
    pub tier: u8,
    pub access_time_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMissEvent {
    pub key: String,
    pub reason: MissReason,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MissReason {
    NotFound,
    Expired,
    Evicted,
    Capacity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInsertEvent {
    pub key: String,
    pub size_bytes: usize,
    pub tier: u8,
    pub from_backfill: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEvictEvent {
    pub key: String,
    pub reason: EvictReason,
    pub evicted_size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictReason {
    Capacity,
    TTL,
    Manual,
    Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheExpireEvent {
    pub key: String,
    pub age_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheErrorEvent {
    pub key: Option<String>,
    pub error: String,
}

struct LRUCacheNode<K, V> {
    key: K,
    value: V,
    prev: Option<*mut LRUCacheNode<K, V>>,
    next: Option<*mut LRUCacheNode<K, V>>,
    access_time: Instant,
    access_count: u64,
    size_bytes: usize,
}

struct LRUCache<K, V> {
    capacity: usize,
    current_size: usize,
    map: HashMap<K, *mut LRUCacheNode<K, V>>,
    head: *mut LRUCacheNode<K, V>,
    tail: *mut LRUCacheNode<K, V>,
}

struct LFUCacheNode<K, V> {
    key: K,
    value: V,
    frequency: u64,
    access_time: Instant,
    size_bytes: usize,
}

struct LFUCache<K, V> {
    capacity: usize,
    current_size: usize,
    min_frequency: u64,
    freq_map: HashMap<u64, LinkedList<K>>,
    key_map: HashMap<K, LFUCacheNode<K, V>>,
}

struct ARCCache<K, V> {
    t1_size: usize,
    t2_size: usize,
    b1_size: usize,
    b2_size: usize,
    t1: HashMap<K, V>,
    t2: HashMap<K, V>,
    b1: HashMap<K, V>,
    b2: HashMap<K, V>,
}

struct TieredCache<K, V> {
    l1: Arc<dyn CacheTier<K, V>>,
    l2: Arc<dyn CacheTier<K, V>>,
    l3: Arc<dyn CacheTier<K, V>>,
    current_t1_size: usize,
    current_t2_size: usize,
    promotion_threshold: u32,
    demotion_threshold: u32,
}

#[async_trait::async_trait]
trait CacheTier<K, V>: Send + Sync {
    async fn get(&self, key: &K) -> Option<V>;
    async fn insert(&self, key: K, value: V, size_bytes: usize) -> Result<(), CacheError>;
    async fn remove(&self, key: &K) -> bool;
    async fn clear(&self) -> Result<(), CacheError>;
    async fn len(&self) -> usize;
    async fn is_empty(&self) -> bool;
    async fn capacity(&self) -> usize;
    async fn current_size(&self) -> usize;
    async fn contains(&self, key: &K) -> bool;
}

struct MemoryTier<K, V> {
    cache: DashMap<K, CacheEntry<V>>,
    max_capacity: usize,
    current_size: Arc<Mutex<usize>>,
    stats: Arc<CacheStats>,
}

struct DiskTier<K, V> {
    path: String,
    max_capacity: usize,
    current_size: Arc<Mutex<usize>>,
    index: DashMap<K, DiskEntry>,
    stats: Arc<CacheStats>,
}

struct DiskEntry {
    file_offset: u64,
    size_bytes: usize,
    created_at: u64,
}

struct BloomFilter {
    bit_array: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
    item_count: u64,
}

struct CacheWarmingTask {
    keys: Vec<String>,
    priority: CachePriority,
    scheduled_at: u64,
    execute_after: u64,
}

pub struct CacheManager {
    config: CacheConfig,
    l1_cache: Arc<dyn CacheTier<String, Vec<u8>>>,
    l2_cache: Arc<dyn CacheTier<String, Vec<u8>>>,
    l3_cache: Arc<dyn CacheTier<String, Vec<u8>>>,
    tiered_cache: Arc<RwLock<TieredCache<String, Vec<u8>>>>,
    bloom_filter: Arc<RwLock<Option<BloomFilter>>>,
    stats: Arc<CacheStats>,
    event_sender: Arc<Mutex<Option<tokio::sync::mpsc::Sender<CacheEvent>>>>,
    warming_tasks: Arc<Mutex<Vec<CacheWarmingTask>>>,
    shutdown_signal: Arc<AtomicBool>,
    persist_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    check_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::marker::PhantomData;

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 10000,
            max_memory_bytes: 1024 * 1024 * 1024,
            ttl_seconds: 3600,
            ttl_check_interval_seconds: 60,
            eviction_policy: EvictionPolicy::LRU,
            enable_compression: false,
            compression_threshold_bytes: 1024,
            compression_type: CompressionType::Zstd,
            enable_persistence: false,
            persistence_path: None,
            persistence_interval_seconds: 300,
            warm_on_start: false,
            enable_statistics: true,
            enable_bloom_filter: true,
            bloom_filter_fp_rate: 0.01,
            enable_tiered_storage: true,
            l1_cache_size: 1000,
            l2_cache_size: 5000,
            l3_cache_size: 10000,
            write_buffer_size: 4096,
            enable_memory_mapped: false,
            memory_mapped_path: None,
        }
    }
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            max_memory: 1024 * 1024 * 1024,
            default_ttl: Duration::from_secs(3600),
            refresh_interval: None,
            stale_while_revalidate: false,
            stale_ttl: Duration::from_secs(300),
            write_back: false,
            write_through: true,
            read_back: true,
            async_writes: false,
            batch_inserts: false,
            batch_size: 100,
        }
    }
}

impl Default for EntryFlags {
    fn default() -> Self {
        Self {
            pinned: false,
            dirty: false,
            compressed: false,
            encrypted: false,
            shared: false,
            temporary: false,
        }
    }
}

impl Default for EntryMetadata {
    fn default() -> Self {
        Self {
            version: 0,
            checksum: String::new(),
            tags: HashSet::new(),
            source: None,
            expire_at: None,
            last_refresh: None,
        }
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
            evictions: 0,
            expirations: 0,
            insertions: 0,
            deletions: 0,
            current_size: 0,
            current_items: 0,
            memory_usage_bytes: 0,
            peak_memory_bytes: 0,
            avg_access_time_ns: 0,
            avg_insert_time_ns: 0,
            compression_ratio: 1.0,
            bloom_filter_stats: None,
            tier_stats: TierStats::default(),
            last_updated: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

impl Default for TierStats {
    fn default() -> Self {
        Self {
            l1_hits: 0,
            l1_misses: 0,
            l2_hits: 0,
            l2_misses: 0,
            l3_hits: 0,
            l3_misses: 0,
            promotions: 0,
            demotions: 0,
        }
    }
}

impl<K, V> LRUCache<K, V> where K: Hash + Eq + Clone {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            current_size: 0,
            map: HashMap::new(),
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(node_ptr) = self.map.get(key) {
            let node = unsafe { &mut **node_ptr };
            self.move_to_front(node_ptr);
            node.access_time = Instant::now();
            node.access_count += 1;
            Some(&node.value)
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V, size_bytes: usize) -> Option<V> {
        if let Some(existing) = self.map.get(&key) {
            let node = unsafe { &mut **existing };
            self.move_to_front(existing);
            node.access_time = Instant::now();
            node.access_count += 1;
            return Some(node.value.clone());
        }

        if self.current_size >= self.capacity {
            self.evict_lru();
        }

        let node = Box::into_raw(Box::new(LRUCacheNode {
            key: key.clone(),
            value,
            prev: None,
            next: None,
            access_time: Instant::now(),
            access_count: 1,
            size_bytes,
        }));

        self.map.insert(key, node);
        self.add_to_front(node);
        self.current_size += size_bytes;

        None
    }

    fn remove(&mut self, key: &K) -> bool {
        if let Some(node_ptr) = self.map.remove(key) {
            self.remove_node(node_ptr);
            self.current_size -= unsafe { (*node_ptr).size_bytes };
            let value = unsafe { Box::from_raw(node_ptr).value };
            Some(value).is_some()
        } else {
            false
        }
    }

    fn add_to_front(&mut self, node: *mut LRUCacheNode<K, V>) {
        unsafe {
            (*node).prev = None;
            (*node).next = if !self.head.is_null() {
                Some(self.head)
            } else {
                None
            };

            if !self.head.is_null() {
                (*self.head).prev = Some(node);
            }
            self.head = node;

            if self.tail.is_null() {
                self.tail = node;
            }
        }
    }

    fn move_to_front(&mut self, node: *mut LRUCacheNode<K, V>) {
        unsafe {
            if node == self.head {
                return;
            }

            if let Some(prev) = (*node).prev {
                (*prev).next = (*node).next;
            }
            if let Some(next) = (*node).next {
                (*next).prev = (*node).prev;
            }
            if Some(node) == self.tail {
                self.tail = (*node).prev;
            }

            self.add_to_front(node);
        }
    }

    fn remove_node(&mut self, node: *mut LRUCacheNode<K, V>) {
        unsafe {
            if let Some(prev) = (*node).prev {
                (*prev).next = (*node).next;
            }
            if let Some(next) = (*node).next {
                (*next).prev = (*node).prev;
            }
            if Some(node) == self.head {
                self.head = (*node).next;
            }
            if Some(node) == self.tail {
                self.tail = (*node).prev;
            }
        }
    }

    fn evict_lru(&mut self) {
        if let Some(tail) = self.tail {
            self.remove_node(tail);
            if let Some(key) = self.map.remove(unsafe { &(*tail).key }) {
                self.current_size -= unsafe { (*tail).size_bytes };
            }
            unsafe { let _ = Box::from_raw(tail); }
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K, V> Drop for LRUCache<K, V> {
    fn drop(&mut self) {
        while let Some(tail) = self.tail {
            self.tail = unsafe { (*tail).prev };
            unsafe { let _ = Box::from_raw(tail); }
        }
    }
}

impl<K, V> LFUCache<K, V> where K: Hash + Eq + Clone {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            current_size: 0,
            min_frequency: 0,
            freq_map: HashMap::new(),
            key_map: HashMap::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(node) = self.key_map.get_mut(key) {
            let old_freq = node.frequency;
            node.frequency += 1;
            node.access_time = Instant::now();

            if let Some(list) = self.freq_map.get_mut(&old_freq) {
                list.remove(key);
                if list.is_empty() && old_freq == self.min_frequency {
                    self.min_frequency += 1;
                }
            }

            self.freq_map.entry(node.frequency)
                .or_insert_with(LinkedList::new())
                .push_back(key.clone());

            Some(&node.value)
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V, size_bytes: usize) -> Option<V> {
        if self.current_size >= self.capacity {
            self.evict_lfu();
        }

        let node = LFUCacheNode {
            key: key.clone(),
            value,
            frequency: 1,
            access_time: Instant::now(),
            size_bytes,
        };

        self.key_map.insert(key.clone(), node);
        self.freq_map.entry(1)
            .or_insert_with(LinkedList::new())
            .push_back(key);
        self.current_size += size_bytes;

        None
    }

    fn evict_lfu(&mut self) {
        if let Some(list) = self.freq_map.get_mut(&self.min_frequency) {
            if let Some(evict_key) = list.pop_front() {
                if let Some(node) = self.key_map.remove(&evict_key) {
                    self.current_size -= node.size_bytes;
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.key_map.len()
    }

    fn is_empty(&self) -> bool {
        self.key_map.is_empty()
    }
}

impl<K, V> ARCCache<K, V> where K: Hash + Eq + Clone {
    fn new(capacity: usize) -> Self {
        let half = capacity / 2;
        Self {
            t1_size: half,
            t2_size: half,
            b1_size: half,
            b2_size: half,
            t1: HashMap::new(),
            t2: HashMap::new(),
            b1: HashMap::new(),
            b2: HashMap::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(value) = self.t2.get(key) {
            self.replace(key, true);
            Some(value)
        } else if let Some(value) = self.t1.get(key) {
            self.replace(key, true);
            Some(value)
        } else if let Some(_) = self.b1.get(key) {
            self.replace(key, false);
            self.t2_size = (self.t1_size + self.t2_size + 1).min(self.t1_size + self.t2_size + self.b1_size + self.b2_size);
            self.b1_size = (self.t1_size + self.t2_size + 1).min(self.t1_size + self.t2_size + self.b1_size + self.b2_size) - self.t2_size;
            None
        } else if let Some(_) = self.b2.get(key) {
            self.replace(key, false);
            self.t1_size = (self.t1_size + self.t2_size + 1).min(self.t1_size + self.t2_size + self.b1_size + self.b2_size);
            self.t2_size = (self.t1_size + self.t2_size + 1).min(self.t1_size + self.t2_size + self.b1_size + self.b2_size) - self.t1_size;
            None
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.t1.contains_key(&key) || self.t2.contains_key(&key) {
            return Some(value);
        }

        if self.t1.len() + self.t2.len() >= self.t1_size + self.t2_size {
            if self.b1.len() > 0 {
                self.t2.extend(self.b1.clone());
                self.b1.clear();
            } else if self.b2.len() > 0 {
                self.t1.extend(self.b2.clone());
                self.b2.clear();
            } else {
                if self.t1.len() < self.t1_size {
                    if let Some((k, _)) = self.t1.iter().next() {
                        let k = k.clone();
                        self.b1.insert(k.clone(), self.t1.remove(&k).unwrap());
                    }
                } else {
                    if let Some((k, _)) = self.t2.iter().next() {
                        let k = k.clone();
                        self.b2.insert(k.clone(), self.t2.remove(&k).unwrap());
                    }
                }
            }
        }

        self.t1.insert(key.clone(), value);
        Some(value)
    }

    fn replace(&mut self, key: &K, hit: bool) {
        if self.t1.len() + self.t2.len() >= self.t1_size + self.t2_size {
            if hit {
                if self.b1.len() >= self.b2.len() * self.t2_size / self.t1_size.max(1) {
                    if let Some((k, _)) = self.b1.iter().next() {
                        let k = k.clone();
                        self.b1.remove(&k);
                    }
                }
            } else {
                if self.b2.len() > 0 {
                    if let Some((k, _)) = self.b2.iter().next() {
                        let k = k.clone();
                        self.b2.remove(&k);
                    }
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.t1.len() + self.t2.len()
    }

    fn is_empty(&self) -> bool {
        self.t1.is_empty() && self.t2.is_empty()
    }
}

impl BloomFilter {
    fn new(expected_items: u64, fp_rate: f64) -> Self {
        let num_bits = ((-1.0 * expected_items as f64 * fp_rate.ln()) / (2.0_f64.ln().powi(2))) as usize;
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()) as usize;

        Self {
            bit_array: vec![0u8; (num_bits + 7) / 8],
            num_bits,
            num_hashes,
            item_count: 0,
        }
    }

    fn contains(&self, item: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.double_hash(item, i);
            let pos = hash % self.num_bits as u64;
            let byte_pos = (pos / 8) as usize;
            let bit_pos = (pos % 8) as u8;
            if (self.bit_array[byte_pos] >> bit_pos) & 1 == 0 {
                return false;
            }
        }
        true
    }

    fn insert(&mut self, item: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.double_hash(item, i);
            let pos = hash % self.num_bits as u64;
            let byte_pos = (pos / 8) as usize;
            let bit_pos = (pos % 8) as u8;
            self.bit_array[byte_pos] |= 1 << bit_pos;
        }
        self.item_count += 1;
    }

    fn double_hash(&self, item: &[u8], i: u32) -> u64 {
        let h1 = xxh3::xxh3_64(item);
        let h2 = xxh3::xxh3_64_with_seed(item, 0x9e3779b97f4a7c15);
        ((h1 as u64).wrapping_add((i as u64).wrapping_mul(h2 as u64))) % self.num_bits as u64
    }

    fn memory_usage(&self) -> usize {
        self.bit_array.len()
    }
}

impl CacheManager {
    pub async fn new(config: Option<CacheConfig>) -> Result<Self, CacheError> {
        let config = config.unwrap_or_default();
        
        let l1_cache: Arc<dyn CacheTier<String, Vec<u8>>> = Arc::new(
            MemoryTier::new(config.l1_cache_size).await?
        );
        
        let l2_cache: Arc<dyn CacheTier<String, Vec<u8>>> = Arc::new(
            MemoryTier::new(config.l2_cache_size).await?
        );
        
        let l3_cache: Arc<dyn CacheTier<String, Vec<u8>>> = Arc::new(
            MemoryTier::new(config.l3_cache_size).await?
        );

        let bloom_filter = if config.enable_bloom_filter {
            Some(BloomFilter::new(config.max_capacity as u64, config.bloom_filter_fp_rate))
        } else {
            None
        };

        let stats = Arc::new(CacheStats::default());

        let (event_sender, _) = tokio::sync::mpsc::channel(1000);

        Ok(Self {
            config,
            l1_cache,
            l2_cache,
            l3_cache,
            tiered_cache: Arc::new(RwLock::new(TieredCache {
                l1: Arc::clone(&l1_cache),
                l2: Arc::clone(&l2_cache),
                l3: Arc::clone(&l3_cache),
                current_t1_size: 0,
                current_t2_size: 0,
                promotion_threshold: 3,
                demotion_threshold: 5,
            })),
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            stats: Arc::clone(&stats),
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            warming_tasks: Arc::new(Mutex::new(Vec::new())),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            persist_task: Arc::new(Mutex::new(None)),
            check_task: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let start = Instant::now();
        let key_str = key.to_string();

        if let Some(ref mut bloom) = *self.bloom_filter.write() {
            if !bloom.contains(key_str.as_bytes()) {
                self.record_miss(&key_str, MissReason::NotFound).await;
                return Ok(None);
            }
        }

        if let Some(value) = self.l1_cache.get(&key_str).await {
            self.record_hit(&key_str, 1, start.elapsed().as_nanos() as u64).await;
            self.tiered_cache.write().await.current_t1_size += 1;
            return Ok(Some(value));
        }

        if let Some(value) = self.l2_cache.get(&key_str).await {
            self.record_hit(&key_str, 2, start.elapsed().as_nanos() as u64).await;
            self.l2_cache.remove(&key_str).await;
            self.l1_cache.insert(key_str.clone(), value.clone(), value.len()).await?;
            self.tiered_cache.write().await.promotions += 1;
            return Ok(Some(value));
        }

        if let Some(value) = self.l3_cache.get(&key_str).await {
            self.record_hit(&key_str, 3, start.elapsed().as_nanos() as u64).await;
            self.l3_cache.remove(&key_str).await;
            self.l2_cache.insert(key_str.clone(), value.clone(), value.len()).await?;
            self.tiered_cache.write().await.promotions += 1;
            return Ok(Some(value));
        }

        self.record_miss(&key_str, MissReason::NotFound).await;
        Ok(None)
    }

    pub async fn insert(&self, key: &str, value: &[u8]) -> Result<(), CacheError> {
        self.insert_with_priority(key, value, CachePriority::Medium).await
    }

    pub async fn insert_with_priority(
        &self,
        key: &str,
        value: &[u8],
        priority: CachePriority,
    ) -> Result<(), CacheError> {
        let key_str = key.to_string();
        let value_vec = value.to_vec();
        let size_bytes = value_vec.len();

        self.insert_into_tier(&key_str, &value_vec, size_bytes).await?;

        if let Some(ref mut bloom) = *self.bloom_filter.write() {
            bloom.insert(key_str.as_bytes());
        }

        self.record_insertion(&key_str, size_bytes).await;
        Ok(())
    }

    async fn insert_into_tier(
        &self,
        key: &str,
        value: &[u8],
        size_bytes: usize,
    ) -> Result<(), CacheError> {
        let mut tiered = self.tiered_cache.write().await;

        if tiered.current_t1_size < self.config.l1_cache_size {
            self.l1_cache.insert(key.to_string(), value.to_vec(), size_bytes).await?;
            tiered.current_t1_size += 1;
        } else if tiered.current_t2_size < self.config.l2_cache_size {
            self.l2_cache.insert(key.to_string(), value.to_vec(), size_bytes).await?;
            tiered.current_t2_size += 1;
        } else {
            self.l3_cache.insert(key.to_string(), value.to_vec(), size_bytes).await?;
        }

        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<bool, CacheError> {
        let key_str = key.to_string();

        let removed = self.l1_cache.remove(&key_str).await
            || self.l2_cache.remove(&key_str).await
            || self.l3_cache.remove(&key_str).await;

        if removed {
            self.record_deletion(&key_str).await;
        }

        Ok(removed)
    }

    pub async fn clear(&self) -> Result<(), CacheError> {
        self.l1_cache.clear().await?;
        self.l2_cache.clear().await?;
        self.l3_cache.clear().await?;

        self.record_clear().await;
        Ok(())
    }

    pub async fn contains(&self, key: &str) -> bool {
        let key_str = key.to_string();

        if let Some(ref bloom) = *self.bloom_filter.read() {
            if !bloom.contains(key_str.as_bytes()) {
                return false;
            }
        }

        self.l1_cache.contains(&key_str).await
            || self.l2_cache.contains(&key_str).await
            || self.l3_cache.contains(&key_str).await
    }

    pub async fn len(&self) -> usize {
        self.l1_cache.len().await + self.l2_cache.len().await + self.l3_cache.len().await
    }

    pub async fn is_empty(&self) -> bool {
        self.l1_cache.is_empty().await && self.l2_cache.is_empty().await && self.l3_cache.is_empty().await
    }

    pub async fn get_stats(&self) -> CacheStats {
        let mut stats = (*self.stats).clone();

        stats.current_items = (self.l1_cache.len().await + self.l2_cache.len().await + self.l3_cache.len().await) as u64;

        if stats.hits + stats.misses > 0 {
            stats.hit_rate = stats.hits as f64 / (stats.hits + stats.misses) as f64;
        }

        stats.last_updated = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if let Some(ref bloom) = *self.bloom_filter.read() {
            stats.bloom_filter_stats = Some(BloomFilterStats {
                items_count: bloom.item_count,
                false_positives: 0,
                memory_bytes: bloom.memory_usage() as u64,
                fp_rate: bloom.fp_rate,
            });
        }

        stats
    }

    pub async fn warm_cache(&self, keys: Vec<String>) -> Result<(), CacheError> {
        for key in keys {
            let task = CacheWarmingTask {
                keys: vec![key],
                priority: CachePriority::Medium,
                scheduled_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                execute_after: 0,
            };
            self.warming_tasks.lock().await.push(task);
        }
        Ok(())
    }

    pub async fn schedule_warming(
        &self,
        keys: Vec<String>,
        priority: CachePriority,
        delay_seconds: u64,
    ) {
        let task = CacheWarmingTask {
            keys,
            priority,
            scheduled_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            execute_after: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() + delay_seconds,
        };
        self.warming_tasks.lock().await.push(task);
    }

    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<u32, CacheError> {
        let regex = regex::Regex::new(pattern).map_err(|e| CacheError::CacheError(e.to_string()))?;
        let mut invalidated = 0;

        let keys: Vec<String> = self.l1_cache.len().await as usize;
        let keys: Vec<String> = vec![];

        for key in keys {
            if regex.is_match(&key) {
                self.remove(&key).await?;
                invalidated += 1;
            }
        }

        Ok(invalidated)
    }

    pub async fn start_background_tasks(&self) {
        let ttl_check_interval = self.config.ttl_check_interval_seconds;
        let shutdown = Arc::clone(&self.shutdown_signal);

        let check_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(ttl_check_interval));
            loop {
                interval.tick().await;
                if shutdown.load(AtomicOrdering::Relaxed) {
                    break;
                }
            }
        });

        *self.check_task.lock().await = Some(check_task);

        if self.config.enable_persistence {
            let persist_interval = self.config.persistence_interval_seconds;
            let shutdown = Arc::clone(&self.shutdown_signal);

            let persist_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(persist_interval));
                loop {
                    interval.tick().await;
                    if shutdown.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                }
            });

            *self.persist_task.lock().await = Some(persist_task);
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown_signal.store(true, AtomicOrdering::Relaxed);

        if let Some(task) = self.check_task.lock().await.take() {
            task.abort();
        }

        if let Some(task) = self.persist_task.lock().await.take() {
            task.abort();
        }
    }

    async fn record_hit(&self, key: &str, tier: u8, access_time_ns: u64) {
        let mut stats = self.stats.clone();
        stats.hits += 1;

        self.update_avg_access_time(access_time_ns);

        if let Some(ref sender) = *self.event_sender.lock() {
            let _ = sender.send(CacheEvent::Hit(CacheHitEvent {
                key: key.to_string(),
                tier,
                access_time_ns,
            })).await;
        }
    }

    async fn record_miss(&self, key: &str, reason: MissReason) {
        let mut stats = self.stats.clone();
        stats.misses += 1;

        if let Some(ref sender) = *self.event_sender.lock() {
            let _ = sender.send(CacheEvent::Miss(CacheMissEvent {
                key: key.to_string(),
                reason,
            })).await;
        }
    }

    async fn record_insertion(&self, key: &str, size_bytes: usize) {
        let mut stats = self.stats.clone();
        stats.insertions += 1;
        stats.current_size += size_bytes;
        stats.memory_usage_bytes += size_bytes as u64;
        stats.peak_memory_bytes = stats.peak_memory_bytes.max(stats.memory_usage_bytes);

        if let Some(ref sender) = *self.event_sender.lock() {
            let _ = sender.send(CacheEvent::Insert(CacheInsertEvent {
                key: key.to_string(),
                size_bytes,
                tier: 1,
                from_backfill: false,
            })).await;
        }
    }

    async fn record_deletion(&self, key: &str) {
        self.stats.lock().unwrap().deletions += 1;
    }

    async fn record_eviction(&self, key: &str, reason: EvictReason, size_bytes: usize) {
        self.stats.lock().unwrap().evictions += 1;

        if let Some(ref sender) = *self.event_sender.lock() {
            let _ = sender.send(CacheEvent::Evict(CacheEvictEvent {
                key: key.to_string(),
                reason,
                evicted_size_bytes: size_bytes,
            })).await;
        }
    }

    async fn record_clear(&self) {
        *self.stats.lock().unwrap() = CacheStats::default();
    }

    fn update_avg_access_time(&self, new_time_ns: u64) {
        let mut stats = self.stats.lock().unwrap();
        let total_accesses = stats.hits;
        if total_accesses > 0 {
            stats.avg_access_time_ns = ((stats.avg_access_time_ns * (total_accesses - 1)) + new_time_ns) / total_accesses;
        } else {
            stats.avg_access_time_ns = new_time_ns;
        }
    }
}

#[async_trait::async_trait]
impl<K, V> CacheTier<K, V> for MemoryTier<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> Option<V> {
        if let Some(entry) = self.cache.get(key) {
            entry.accessed_at = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            entry.access_count += 1;
            Some(entry.value.clone())
        } else {
            None
        }
    }

    async fn insert(&self, key: K, value: V, size_bytes: usize) -> Result<(), CacheError> {
        let mut current = self.current_size.lock();

        while *current + size_bytes > self.max_capacity && !self.cache.is_empty() {
            self.evict_lru(current);
        }

        let entry = CacheEntry {
            key: key.clone(),
            value,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            accessed_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            modified_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            access_count: 0,
            size_bytes,
            frequency: 1.0,
            priority: CachePriority::Medium,
            flags: EntryFlags::default(),
            metadata: EntryMetadata::default(),
        };

        self.cache.insert(key, entry);
        *current += size_bytes;

        Ok(())
    }

    async fn remove(&self, key: &K) -> bool {
        if let Some((_, entry)) = self.cache.remove(key) {
            *self.current_size.lock() -= entry.size_bytes;
            true
        } else {
            false
        }
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.cache.clear();
        *self.current_size.lock() = 0;
        Ok(())
    }

    async fn len(&self) -> usize {
        self.cache.len()
    }

    async fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    async fn capacity(&self) -> usize {
        self.max_capacity
    }

    async fn current_size(&self) -> usize {
        *self.current_size.lock()
    }

    async fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }
}

impl<K, V> MemoryTier<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn new(max_capacity: usize) -> Result<Self, CacheError> {
        Ok(Self {
            cache: DashMap::new(),
            max_capacity,
            current_size: Arc::new(Mutex::new(0)),
            stats: Arc::new(CacheStats::default()),
        })
    }

    fn evict_lru(&self, _current: &mut parking_lot::MutexGuard<'_, usize>) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_manager_creation() {
        let manager = CacheManager::new(None).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let manager = CacheManager::new(None).await.unwrap();

        let result = manager.insert("key1", b"value1").await;
        assert!(result.is_ok());

        let retrieved = manager.get("key1").await;
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap(), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let manager = CacheManager::new(None).await.unwrap();

        let result = manager.get("nonexistent").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_remove() {
        let manager = CacheManager::new(None).await.unwrap();

        let _ = manager.insert("key1", b"value1").await;
        let removed = manager.remove("key1").await;
        assert!(removed.unwrap());

        let retrieved = manager.get("key1").await;
        assert_eq!(retrieved.unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let manager = CacheManager::new(None).await.unwrap();

        let _ = manager.insert("key1", b"value1").await;
        let _ = manager.insert("key2", b"value2").await;

        let cleared = manager.clear().await;
        assert!(cleared.is_ok());

        let is_empty = manager.is_empty().await;
        assert!(is_empty);
    }

    #[tokio::test]
    async fn test_cache_contains() {
        let manager = CacheManager::new(None).await.unwrap();

        let _ = manager.insert("key1", b"value1").await;

        let contains = manager.contains("key1").await;
        assert!(contains);

        let contains_missing = manager.contains("nonexistent").await;
        assert!(!contains_missing);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let manager = CacheManager::new(None).await.unwrap();

        let _ = manager.insert("key1", b"value1").await;
        let _ = manager.get("key1").await;
        let _ = manager.get("nonexistent").await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.insertions, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_cache_len() {
        let manager = CacheManager::new(None).await.unwrap();

        let len = manager.len().await;
        assert_eq!(len, 0);

        let _ = manager.insert("key1", b"value1").await;
        let _ = manager.insert("key2", b"value2").await;

        let len = manager.len().await;
        assert_eq!(len, 2);
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        let mut filter = BloomFilter::new(1000, 0.01);

        filter.insert(b"test");
        assert!(filter.contains(b"test"));
        assert!(!filter.contains(b"notpresent"));
    }

    #[tokio::test]
    async fn test_priority_insertion() {
        let manager = CacheManager::new(None).await.unwrap();

        let result = manager.insert_with_priority(
            "key1",
            b"value1",
            CachePriority::High,
        ).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_lru_cache() {
        let mut cache = LRUCache::new(3);

        cache.insert("a", "1", 1);
        cache.insert("b", "2", 1);
        cache.insert("c", "3", 1);
        cache.insert("d", "4", 1);

        assert!(cache.get(&"a").is_none());
        assert!(cache.get(&"b").is_some());
        assert!(cache.get(&"c").is_some());
        assert!(cache.get(&"d").is_some());
    }

    #[tokio::test]
    async fn test_lfu_cache() {
        let mut cache = LFUCache::new(3);

        cache.insert("a", "1", 1);
        cache.insert("b", "2", 1);
        cache.insert("c", "3", 1);

        cache.get(&"a");
        cache.get(&"a");
        cache.get(&"b");

        cache.insert("d", "4", 1);

        assert!(cache.get(&"c").is_none());
        assert!(cache.get(&"a").is_some());
    }

    #[tokio::test]
    async fn test_arc_cache() {
        let mut cache = ARCCache::new(3);

        cache.insert("a", "1");
        cache.insert("b", "2");
        cache.insert("c", "3");

        cache.get(&"a");
        cache.get(&"b");

        cache.insert("d", "4");

        assert!(cache.get(&"c").is_none());
    }
}
