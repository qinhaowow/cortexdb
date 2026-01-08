use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub struct LruCache<K: Hash + Eq + Clone, V: Clone> {
    capacity: usize,
    cache: Arc<Mutex<LruCacheInner<K, V>>>,
}

struct LruCacheInner<K: Hash + Eq + Clone, V: Clone> {
    entries: HashMap<K, V>,
    lru: VecDeque<K>,
    capacity: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let inner = LruCacheInner {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        };
        Self {
            capacity,
            cache: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut inner = self.cache.lock().unwrap();
        if let Some(value) = inner.entries.get(key) {
            inner.touch(key);
            Some(value.clone())
        } else {
            None
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let mut inner = self.cache.lock().unwrap();
        
        if inner.entries.contains_key(&key) {
            inner.touch(&key);
            let old_value = inner.entries.insert(key, value);
            old_value
        } else {
            if inner.entries.len() >= inner.capacity {
                self.evict(&mut inner);
            }
            inner.entries.insert(key.clone(), value);
            inner.lru.push_front(key);
            None
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut inner = self.cache.lock().unwrap();
        if inner.entries.contains_key(key) {
            inner.lru.retain(|k| k != key);
            inner.entries.remove(key)
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let mut inner = self.cache.lock().unwrap();
        inner.entries.clear();
        inner.lru.clear();
    }

    pub fn contains(&self, key: &K) -> bool {
        let inner = self.cache.lock().unwrap();
        inner.entries.contains_key(key)
    }

    pub fn size(&self) -> usize {
        let inner = self.cache.lock().unwrap();
        inner.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn evict(&self, inner: &mut LruCacheInner<K, V>) {
        if let Some(key) = inner.lru.pop_back() {
            inner.entries.remove(&key);
        }
    }
}

impl<K: Hash + Eq + Clone, V: Clone> LruCacheInner<K, V> {
    fn touch(&mut self, key: &K) {
        self.lru.retain(|k| k != key);
        self.lru.push_front(key.clone());
    }
}

pub struct ConcurrentLruCache<K: Hash + Eq + Clone, V: Clone> {
    shards: Vec<LruCache<K, V>>,
    capacity: usize,
    shard_count: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> ConcurrentLruCache<K, V> {
    pub fn new(capacity: usize, shard_count: usize) -> Self {
        let shard_capacity = capacity / shard_count;
        let mut shards = Vec::with_capacity(shard_count);
        
        for _ in 0..shard_count {
            shards.push(LruCache::new(shard_capacity));
        }
        
        Self {
            shards,
            capacity,
            shard_count,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let shard = self.get_shard(key);
        self.shards[shard].get(key)
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let shard = self.get_shard(&key);
        self.shards[shard].put(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let shard = self.get_shard(key);
        self.shards[shard].remove(key)
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    pub fn size(&self) -> usize {
        self.shards.iter().map(|s| s.size()).sum()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn get_shard(&self, key: &K) -> usize {
        let hash = std::hash::Hash::hash(key) as usize;
        hash % self.shard_count
    }
}
