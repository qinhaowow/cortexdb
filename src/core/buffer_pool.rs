use std::sync::{Arc, Mutex, Weak};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

pub trait Bufferable: Sized + Clone {
    type Key: Hash + Eq + Clone;
    fn key(&self) -> Self::Key;
    fn size(&self) -> usize;
}

pub struct BufferPool<T: Bufferable> {
    capacity: usize,
    current_size: usize,
    buffers: Arc<Mutex<BufferPoolInner<T>>>,
}

struct BufferPoolInner<T: Bufferable> {
    entries: HashMap<T::Key, Arc<Mutex<BufferEntry<T>>>>,
    lru: VecDeque<T::Key>,
    capacity: usize,
    current_size: usize,
}

struct BufferEntry<T: Bufferable> {
    data: T,
    pin_count: usize,
    dirty: bool,
    pool: Weak<Mutex<BufferPoolInner<T>>>,
    key: T::Key,
}

pub struct BufferHandle<T: Bufferable>(Arc<Mutex<BufferEntry<T>>>);

impl<T: Bufferable> BufferPool<T> {
    pub fn new(capacity: usize) -> Self {
        let inner = BufferPoolInner {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
            current_size: 0,
        };
        let buffers = Arc::new(Mutex::new(inner));
        Self {
            capacity,
            current_size: 0,
            buffers,
        }
    }

    pub fn get(&self, key: &T::Key) -> Option<BufferHandle<T>> {
        let mut inner = self.buffers.lock().unwrap();
        if let Some(entry) = inner.entries.get(key) {
            inner.touch(key);
            let entry = entry.clone();
            let mut entry_lock = entry.lock().unwrap();
            entry_lock.pin_count += 1;
            Some(BufferHandle(entry))
        } else {
            None
        }
    }

    pub fn put(&self, data: T) -> BufferHandle<T> {
        let key = data.key();
        let entry = Arc::new(Mutex::new(BufferEntry {
            data,
            pin_count: 1,
            dirty: false,
            pool: Arc::downgrade(&self.buffers),
            key: key.clone(),
        }));

        let mut inner = self.buffers.lock().unwrap();
        if let Some(existing) = inner.entries.remove(&key) {
            inner.current_size -= existing.lock().unwrap().data.size();
        }

        let size = entry.lock().unwrap().data.size();
        while inner.current_size + size > inner.capacity {
            if let Some(evict_key) = inner.lru.pop_back() {
                if let Some(evict_entry) = inner.entries.remove(&evict_key) {
                    let mut evict_entry_lock = evict_entry.lock().unwrap();
                    if evict_entry_lock.pin_count == 0 {
                        inner.current_size -= evict_entry_lock.data.size();
                    } else {
                        inner.entries.insert(evict_key.clone(), evict_entry);
                        inner.lru.push_back(evict_key);
                    }
                }
            } else {
                break;
            }
        }

        inner.entries.insert(key.clone(), entry.clone());
        inner.lru.push_front(key);
        inner.current_size += size;

        BufferHandle(entry)
    }

    pub fn contains(&self, key: &T::Key) -> bool {
        let inner = self.buffers.lock().unwrap();
        inner.entries.contains_key(key)
    }

    pub fn remove(&self, key: &T::Key) -> Option<T> {
        let mut inner = self.buffers.lock().unwrap();
        if let Some(entry) = inner.entries.remove(&key) {
            let mut entry_lock = entry.lock().unwrap();
            if entry_lock.pin_count == 0 {
                inner.current_size -= entry_lock.data.size();
                inner.lru.retain(|k| k != key);
                Some(entry_lock.data.clone())
            } else {
                inner.entries.insert(key.clone(), entry);
                None
            }
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let mut inner = self.buffers.lock().unwrap();
        inner.entries.retain(|_, entry| {
            let entry_lock = entry.lock().unwrap();
            entry_lock.pin_count > 0
        });
        inner.lru.retain(|key| inner.entries.contains_key(key));
        inner.current_size = inner.entries.values()
            .map(|entry| entry.lock().unwrap().data.size())
            .sum();
    }

    pub fn size(&self) -> usize {
        let inner = self.buffers.lock().unwrap();
        inner.current_size
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn entry_count(&self) -> usize {
        let inner = self.buffers.lock().unwrap();
        inner.entries.len()
    }
}

impl<T: Bufferable> BufferPoolInner<T> {
    fn touch(&mut self, key: &T::Key) {
        self.lru.retain(|k| k != key);
        self.lru.push_front(key.clone());
    }
}

impl<T: Bufferable> Drop for BufferEntry<T> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let mut pool = pool.lock().unwrap();
            pool.current_size -= self.data.size();
            pool.entries.remove(&self.key);
            pool.lru.retain(|k| k != &self.key);
        }
    }
}

impl<T: Bufferable> BufferHandle<T> {
    pub fn mark_dirty(&mut self) {
        let mut entry = self.0.lock().unwrap();
        entry.dirty = true;
    }

    pub fn is_dirty(&self) -> bool {
        let entry = self.0.lock().unwrap();
        entry.dirty
    }

    pub fn unpin(self) {
        drop(self);
    }
}

impl<T: Bufferable> Deref for BufferHandle<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0.lock().unwrap().data
    }
}

impl<T: Bufferable> DerefMut for BufferHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let mut entry = self.0.lock().unwrap();
        entry.dirty = true;
        &mut entry.data
    }
}

impl<T: Bufferable> Drop for BufferHandle<T> {
    fn drop(&mut self) {
        let mut entry = self.0.lock().unwrap();
        entry.pin_count -= 1;
        if entry.pin_count == 0 && entry.dirty {
            self.flush();
        }
    }
}

impl<T: Bufferable> BufferHandle<T> {
    fn flush(&self) {
        let entry = self.0.lock().unwrap();
        if entry.dirty {
        }
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    page_id: u64,
    data: Vec<u8>,
}

impl Bufferable for Page {
    type Key = u64;
    fn key(&self) -> Self::Key {
        self.page_id
    }
    fn size(&self) -> usize {
        self.data.len()
    }
}

impl Page {
    pub fn new(page_id: u64, data: Vec<u8>) -> Self {
        Self { page_id, data }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            page_id: 0,
            data: Vec::new(),
        }
    }
}
