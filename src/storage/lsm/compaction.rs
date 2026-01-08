use std::sync::Arc;
use crate::storage::lsm::SSTable;

pub trait CompactionStrategy {
    fn compact(
        &self,
        immutable_memtables: &mut Vec<Arc<SSTable>>,
        sstables: &mut Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, CompactionError>;
}

pub struct LeveledCompactionStrategy {
    level_thresholds: Vec<usize>,
}

impl LeveledCompactionStrategy {
    pub fn new(level_thresholds: Vec<usize>) -> Self {
        Self { level_thresholds }
    }
}

impl CompactionStrategy for LeveledCompactionStrategy {
    fn compact(
        &self,
        immutable_memtables: &mut Vec<Arc<SSTable>>,
        sstables: &mut Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, CompactionError> {
        let mut compacted = Vec::new();
        
        if !immutable_memtables.is_empty() {
            let mut to_compact = Vec::new();
            while let Some(memtable) = immutable_memtables.pop() {
                to_compact.push(memtable);
                if to_compact.len() >= 4 {
                    break;
                }
            }
            
            if !to_compact.is_empty() {
                let merged = self.merge_sstables(to_compact)?;
                compacted.push(merged);
            }
        }
        
        Ok(compacted)
    }
}

impl LeveledCompactionStrategy {
    fn merge_sstables(&self, sstables: Vec<Arc<SSTable>>) -> Result<Arc<SSTable>, CompactionError> {
        Ok(Arc::new(SSTable::new("/tmp/merged.sst")?))
    }
}

pub struct SizeTieredCompactionStrategy {
    max_files_per_tier: usize,
}

impl SizeTieredCompactionStrategy {
    pub fn new(max_files_per_tier: usize) -> Self {
        Self { max_files_per_tier }
    }
}

impl CompactionStrategy for SizeTieredCompactionStrategy {
    fn compact(
        &self,
        immutable_memtables: &mut Vec<Arc<SSTable>>,
        sstables: &mut Vec<Arc<SSTable>>,
        min_compaction_size: usize,
    ) -> Result<Vec<Arc<SSTable>>, CompactionError> {
        let mut compacted = Vec::new();
        
        if immutable_memtables.len() >= self.max_files_per_tier {
            let mut to_compact = Vec::new();
            while let Some(memtable) = immutable_memtables.pop() {
                to_compact.push(memtable);
            }
            
            let merged = self.merge_sstables(to_compact)?;
            compacted.push(merged);
        }
        
        Ok(compacted)
    }
}

impl SizeTieredCompactionStrategy {
    fn merge_sstables(&self, sstables: Vec<Arc<SSTable>>) -> Result<Arc<SSTable>, CompactionError> {
        Ok(Arc::new(SSTable::new("/tmp/merged.sst")?))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Compaction failed: {0}")]
    CompactionFailed(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}
