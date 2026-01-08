use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use crate::storage::lsm::memtable::MemTable;

pub struct SSTable {
    path: String,
    size: usize,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl SSTable {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, SSTableError> {
        let path = path.as_ref().to_str().ok_or(SSTableError::InvalidPath)?;
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len() as usize;

        Ok(Self {
            path: path.to_string(),
            size,
            min_key: Vec::new(),
            max_key: Vec::new(),
        })
    }

    pub fn from_memtable(memtable: &MemTable) -> Result<Self, SSTableError> {
        let temp_path = format!("/tmp/sstable_{}.tmp", uuid::Uuid::new_v4());
        let mut file = File::create(&temp_path)?;

        let mut min_key = Vec::new();
        let mut max_key = Vec::new();
        let mut size = 0;

        for (key, value) in memtable.iter() {
            if min_key.is_empty() {
                min_key = key.clone();
            }
            max_key = key.clone();

            let key_len = key.len() as u32;
            let value_len = value.len() as u32;

            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(key)?;
            file.write_all(&value_len.to_le_bytes())?;
            file.write_all(value)?;

            size += 4 + key.len() + 4 + value.len();
        }

        Ok(Self {
            path: temp_path,
            size,
            min_key,
            max_key,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SSTableError> {
        let mut file = File::open(&self.path)?;
        let mut buffer = Vec::new();

        file.seek(SeekFrom::Start(0))?;
        while file.read_to_end(&mut buffer)? > 0 {
            let mut offset = 0;
            while offset < buffer.len() {
                if offset + 4 > buffer.len() {
                    break;
                }
                let key_len = u32::from_le_bytes(buffer[offset..offset+4].try_into()?);
                offset += 4;

                if offset + key_len as usize > buffer.len() {
                    break;
                }
                let current_key = &buffer[offset..offset+key_len as usize];
                offset += key_len as usize;

                if offset + 4 > buffer.len() {
                    break;
                }
                let value_len = u32::from_le_bytes(buffer[offset..offset+4].try_into()?);
                offset += 4;

                if offset + value_len as usize > buffer.len() {
                    break;
                }
                let value = &buffer[offset..offset+value_len as usize];
                offset += value_len as usize;

                if current_key == key {
                    return Ok(Some(value.to_vec()));
                } else if current_key > key {
                    return Ok(None);
                }
            }
            buffer.clear();
        }

        Ok(None)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SSTableError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid path")]
    InvalidPath,

    #[error("Corrupted SSTable")]
    Corrupted,

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Drop for SSTable {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
