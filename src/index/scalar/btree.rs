use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;

pub struct BTreeIndex<K: Ord + Clone, V: Clone> {
    data: Arc<RwLock<BTreeMap<K, V>>>,
    unique: bool,
}

impl<K: Ord + Clone, V: Clone> BTreeIndex<K, V> {
    pub fn new(unique: bool) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            unique,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), BTreeIndexError> {
        let mut data = self.data.write().unwrap();
        
        if self.unique && data.contains_key(&key) {
            return Err(BTreeIndexError::DuplicateKey);
        }
        
        data.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, BTreeIndexError> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, BTreeIndexError> {
        let mut data = self.data.write().unwrap();
        Ok(data.remove(key))
    }

    pub fn range(&self, start: Option<&K>, end: Option<&K>) -> Result<Vec<(K, V)>, BTreeIndexError> {
        let data = self.data.read().unwrap();
        let mut results = Vec::new();

        match (start, end) {
            (Some(s), Some(e)) => {
                for (k, v) in data.range(s..=e) {
                    results.push((k.clone(), v.clone()));
                }
            },
            (Some(s), None) => {
                for (k, v) in data.range(s..) {
                    results.push((k.clone(), v.clone()));
                }
            },
            (None, Some(e)) => {
                for (k, v) in data.range(..=e) {
                    results.push((k.clone(), v.clone()));
                }
            },
            (None, None) => {
                for (k, v) in data.iter() {
                    results.push((k.clone(), v.clone()));
                }
            },
        }

        Ok(results)
    }

    pub fn contains(&self, key: &K) -> bool {
        let data = self.data.read().unwrap();
        data.contains_key(key)
    }

    pub fn first(&self) -> Result<Option<(K, V)>, BTreeIndexError> {
        let data = self.data.read().unwrap();
        Ok(data.first_key_value().map(|(k, v)| (k.clone(), v.clone())))
    }

    pub fn last(&self) -> Result<Option<(K, V)>, BTreeIndexError> {
        let data = self.data.read().unwrap();
        Ok(data.last_key_value().map(|(k, v)| (k.clone(), v.clone())))
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
pub enum BTreeIndexError {
    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl<K: Ord + Clone, V: Clone> Default for BTreeIndex<K, V> {
    fn default() -> Self {
        Self::new(false)
    }
}
