use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};

pub struct HashIndex<K: Eq + std::hash::Hash + Clone, V: Clone> {
    data: Arc<RwLock<HashMap<K, HashSet<V>>>>,
    unique: bool,
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> HashIndex<K, V> {
    pub fn new(unique: bool) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            unique,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), HashIndexError> {
        let mut data = self.data.write().unwrap();
        let values = data.entry(key).or_insert_with(HashSet::new());
        
        if self.unique && !values.is_empty() {
            return Err(HashIndexError::DuplicateKey);
        }
        
        values.insert(value);
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<HashSet<V>>, HashIndexError> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<HashSet<V>>, HashIndexError> {
        let mut data = self.data.write().unwrap();
        Ok(data.remove(key))
    }

    pub fn remove_value(&mut self, key: &K, value: &V) -> Result<bool, HashIndexError> {
        let mut data = self.data.write().unwrap();
        if let Some(values) = data.get_mut(key) {
            let removed = values.remove(value);
            if values.is_empty() {
                data.remove(key);
            }
            Ok(removed)
        } else {
            Ok(false)
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        let data = self.data.read().unwrap();
        data.contains_key(key)
    }

    pub fn contains_value(&self, key: &K, value: &V) -> bool {
        let data = self.data.read().unwrap();
        data.get(key).map(|values| values.contains(value)).unwrap_or(false)
    }

    pub fn size(&self) -> usize {
        let data = self.data.read().unwrap();
        data.len()
    }

    pub fn clear(&mut self) {
        self.data.write().unwrap().clear();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HashIndexError {
    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> Default for HashIndex<K, V> {
    fn default() -> Self {
        Self::new(false)
    }
}
