use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MVCCManager {
    data: Arc<RwLock<HashMap<String, VecDeque<VersionedValue>>>>,
    next_timestamp: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
pub struct VersionedValue {
    value: Vec<u8>,
    timestamp: u64,
    is_deleted: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum MVCCError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Version conflict")]
    VersionConflict,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
}

impl MVCCManager {
    pub async fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            next_timestamp: Arc::new(RwLock::new(1)),
        }
    }

    pub async fn get(&self, key: &str, timestamp: u64) -> Result<Vec<u8>, MVCCError> {
        let data = self.data.read().await;
        if let Some(versions) = data.get(key) {
            for version in versions.iter().rev() {
                if version.timestamp <= timestamp && !version.is_deleted {
                    return Ok(version.value.clone());
                }
            }
            Err(MVCCError::KeyNotFound)
        } else {
            Err(MVCCError::KeyNotFound)
        }
    }

    pub async fn put(&self, key: String, value: Vec<u8>, timestamp: u64) -> Result<(), MVCCError> {
        let mut data = self.data.write().await;
        let versions = data.entry(key).or_insert_with(VecDeque::new);
        
        // Check for version conflicts
        for version in versions.iter().rev() {
            if version.timestamp == timestamp {
                return Err(MVCCError::VersionConflict);
            }
            if version.timestamp < timestamp {
                break;
            }
        }
        
        versions.push_back(VersionedValue {
            value,
            timestamp,
            is_deleted: false,
        });
        
        // Limit version history
        self.trim_versions(versions).await;
        
        Ok(())
    }

    pub async fn delete(&self, key: &str, timestamp: u64) -> Result<(), MVCCError> {
        let mut data = self.data.write().await;
        if let Some(versions) = data.get_mut(key) {
            versions.push_back(VersionedValue {
                value: Vec::new(),
                timestamp,
                is_deleted: true,
            });
            Ok(())
        } else {
            Err(MVCCError::KeyNotFound)
        }
    }

    pub async fn get_next_timestamp(&self) -> u64 {
        let mut next_ts = self.next_timestamp.write().await;
        let ts = *next_ts;
        *next_ts += 1;
        ts
    }

    async fn trim_versions(&self, versions: &mut VecDeque<VersionedValue>) {
        const MAX_VERSIONS: usize = 10;
        while versions.len() > MAX_VERSIONS {
            versions.pop_front();
        }
    }
}