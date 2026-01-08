use std::collections::BTreeMap;

pub struct MemTable {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    size: usize,
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            size: 0,
            max_size,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemTableError> {
        let entry_size = key.len() + value.len();
        if self.size + entry_size > self.max_size {
            return Err(MemTableError::MemTableFull);
        }

        if let Some(old_value) = self.data.insert(key, value) {
            self.size -= old_value.len();
        }
        self.size += entry_size;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemTableError> {
        Ok(self.data.get(key).cloned())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), MemTableError> {
        if let Some(old_value) = self.data.remove(key) {
            self.size -= key.len() + old_value.len();
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn to_immutable(&mut self) -> Result<Arc<SSTable>, MemTableError> {
        let sstable = SSTable::from_memtable(self)?;
        Ok(Arc::new(sstable))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter()
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.size = 0;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemTableError {
    #[error("MemTable is full")]
    MemTableFull,

    #[error("Internal error: {0}")]
    InternalError(String),
}

use std::sync::Arc;
use crate::storage::lsm::sstable::SSTable;
