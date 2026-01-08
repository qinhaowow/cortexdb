use std::sync::{Arc, Mutex, RwLock};
use std::path::Path;
use crate::storage::lsm::{MemTable, SSTable, CompactionStrategy};
use crate::core::buffer_pool::{BufferPool, Bufferable};

pub struct LsmTree {
    memtable: Arc<Mutex<MemTable>>,
    immutable_memtables: Arc<RwLock<Vec<Arc<SSTable>>>>,
    sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
    buffer_pool: Arc<BufferPool<Page>>,
    compaction_strategy: Box<dyn CompactionStrategy>,
    max_memtable_size: usize,
    min_compaction_size: usize,
}

impl LsmTree {
    pub fn new<P: AsRef<Path>>(
        path: P,
        max_memtable_size: usize,
        min_compaction_size: usize,
        buffer_pool_capacity: usize,
    ) -> Self {
        let buffer_pool = Arc::new(BufferPool::new(buffer_pool_capacity));
        let memtable = Arc::new(Mutex::new(MemTable::new(max_memtable_size)));
        
        Self {
            memtable,
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(Vec::new())),
            buffer_pool,
            compaction_strategy: Box::new(DefaultCompactionStrategy),
            max_memtable_size,
            min_compaction_size,
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), LsmError> {
        let mut memtable = self.memtable.lock().unwrap();
        if memtable.size() >= self.max_memtable_size {
            self.flush_memtable()?;
            memtable = self.memtable.lock().unwrap();
        }
        memtable.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, LsmError> {
        {
            let memtable = self.memtable.lock().unwrap();
            if let Some(value) = memtable.get(key)? {
                return Ok(Some(value));
            }
        }

        for memtable in self.immutable_memtables.read().unwrap().iter() {
            if let Some(value) = memtable.get(key)? {
                return Ok(Some(value));
            }
        }

        for sstable in self.sstables.read().unwrap().iter() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), LsmError> {
        self.put(key, vec![])
    }

    pub fn flush_memtable(&self) -> Result<(), LsmError> {
        let mut memtable = self.memtable.lock().unwrap();
        let immutable = memtable.to_immutable()?;
        drop(memtable);

        let mut immutable_memtables = self.immutable_memtables.write().unwrap();
        immutable_memtables.push(immutable);

        let mut memtable = self.memtable.lock().unwrap();
        *memtable = MemTable::new(self.max_memtable_size);

        self.check_compaction()
    }

    pub fn check_compaction(&self) -> Result<(), LsmError> {
        let immutable_memtables = self.immutable_memtables.read().unwrap();
        if immutable_memtables.len() >= 4 {
            self.compact()?;
        }
        Ok(())
    }

    pub fn compact(&self) -> Result<(), LsmError> {
        let mut immutable_memtables = self.immutable_memtables.write().unwrap();
        if immutable_memtables.is_empty() {
            return Ok(());
        }

        let mut sstables = self.sstables.write().unwrap();
        let compacted = self.compaction_strategy.compact(
            &mut immutable_memtables,
            &mut sstables,
            self.min_compaction_size,
        )?;

        for sstable in compacted {
            sstables.push(sstable);
        }

        Ok(())
    }

    pub fn size(&self) -> usize {
        let memtable_size = self.memtable.lock().unwrap().size();
        let immutable_size = self.immutable_memtables.read().unwrap()
            .iter().map(|m| m.size()).sum::<usize>();
        let sstable_size = self.sstables.read().unwrap()
            .iter().map(|s| s.size()).sum::<usize>();
        memtable_size + immutable_size + sstable_size
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LsmError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Memtable error: {0}")]
    MemtableError(String),

    #[error("SSTable error: {0}")]
    SSTableError(String),

    #[error("Compaction error: {0}")]
    CompactionError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub trait CompactionStrategy {
    fn compact(
        &self,
        immutable_memtables: &mut Vec<Arc<SSTable>>,
        sstables: &mut Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, LsmError>;
}

pub struct DefaultCompactionStrategy;

impl CompactionStrategy for DefaultCompactionStrategy {
    fn compact(
        &self,
        immutable_memtables: &mut Vec<Arc<SSTable>>,
        sstables: &mut Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, LsmError> {
        let mut compacted = Vec::new();
        if immutable_memtables.len() >= 4 {
            let to_compact = immutable_memtables.drain(..4).collect();
            let result = self.compact_sstables(to_compact, min_compaction_size)?;
            compacted.extend(result);
        }
        Ok(compacted)
    }
}

impl DefaultCompactionStrategy {
    fn compact_sstables(
        &self,
        sstables: Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, LsmError> {
        Ok(Vec::new())
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
