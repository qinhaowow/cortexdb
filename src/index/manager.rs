use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use crate::index::{vector, scalar};

pub enum IndexType {
    Vector { algorithm: String },
    Scalar { algorithm: String },
}

pub trait Index: Send + Sync {
    fn name(&self) -> &str;
    fn index_type(&self) -> IndexType;
    fn size(&self) -> usize;
    fn clear(&mut self);
}

pub struct IndexManager {
    indexes: Arc<RwLock<HashMap<String, Arc<dyn Index>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_vector_index(
        &self,
        name: String,
        algorithm: String,
        dimension: usize,
        params: serde_json::Value,
    ) -> Result<Arc<dyn Index>, IndexManagerError> {
        let index: Arc<dyn Index> = match algorithm.as_str() {
            "hnsw" => {
                let max_connections = params.get("max_connections").and_then(|v| v.as_u64()).unwrap_or(16) as usize;
                let ef_construction = params.get("ef_construction").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
                Arc::new(vector::HnswIndex::new(dimension, max_connections, ef_construction))
            },
            "diskann" => {
                let r = params.get("r").and_then(|v| v.as_u64()).unwrap_or(32) as usize;
                let l = params.get("l").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
                Arc::new(vector::DiskAnnIndex::new(dimension, r, l))
            },
            "brute_force" => {
                Arc::new(vector::BruteForceIndex::new(dimension))
            },
            _ => {
                return Err(IndexManagerError::UnsupportedAlgorithm(algorithm));
            }
        };

        let mut indexes = self.indexes.write().unwrap();
        if indexes.contains_key(&name) {
            return Err(IndexManagerError::IndexAlreadyExists(name));
        }

        indexes.insert(name, index.clone());
        Ok(index)
    }

    pub fn create_scalar_index(
        &self,
        name: String,
        algorithm: String,
        unique: bool,
    ) -> Result<Arc<dyn Index>, IndexManagerError> {
        let index: Arc<dyn Index> = match algorithm.as_str() {
            "hash" => {
                Arc::new(scalar::HashIndex::new(unique))
            },
            "btree" => {
                Arc::new(scalar::BTreeIndex::new(unique))
            },
            _ => {
                return Err(IndexManagerError::UnsupportedAlgorithm(algorithm));
            }
        };

        let mut indexes = self.indexes.write().unwrap();
        if indexes.contains_key(&name) {
            return Err(IndexManagerError::IndexAlreadyExists(name));
        }

        indexes.insert(name, index.clone());
        Ok(index)
    }

    pub fn get_index(&self, name: &str) -> Result<Arc<dyn Index>, IndexManagerError> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name)
            .ok_or_else(|| IndexManagerError::IndexNotFound(name.to_string()))
            .map(|i| i.clone())
    }

    pub fn drop_index(&self, name: &str) -> Result<(), IndexManagerError> {
        let mut indexes = self.indexes.write().unwrap();
        if !indexes.contains_key(name) {
            return Err(IndexManagerError::IndexNotFound(name.to_string()));
        }
        indexes.remove(name);
        Ok(())
    }

    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read().unwrap();
        indexes.keys().cloned().collect()
    }

    pub fn index_count(&self) -> usize {
        let indexes = self.indexes.read().unwrap();
        indexes.len()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexManagerError {
    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),

    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
