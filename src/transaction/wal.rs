use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum WALEntry {
    Begin { tx_id: u64 },
    Commit { tx_id: u64 },
    Rollback { tx_id: u64 },
    Put { tx_id: u64, key: String, value: Vec<u8> },
    Delete { tx_id: u64, key: String },
}

#[derive(Debug, thiserror::Error)]
pub enum WALError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Invalid WAL entry")]
    InvalidEntry,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

pub struct WAL {
    file: Arc<Mutex<BufWriter<File>>>,
    path: String,
}

impl WAL {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(path)?;
        
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            path: path.as_ref().to_str().unwrap().to_string(),
        })
    }

    pub async fn write_entry(&self, entry: &WALEntry) -> Result<(), WALError> {
        let mut file = self.file.lock().await;
        let serialized = self.serialize_entry(entry)?;
        file.write_all(&serialized)?;
        file.flush()?;
        Ok(())
    }

    pub async fn read_all_entries(&self) -> Result<Vec<WALEntry>, WALError> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        
        let mut entries = Vec::new();
        let mut buffer = Vec::new();
        
        loop {
            let mut size_buffer = [0; 8];
            match file.read_exact(&mut size_buffer) {
                Ok(_) => {
                    let size = u64::from_le_bytes(size_buffer);
                    buffer.resize(size as usize, 0);
                    file.read_exact(&mut buffer)?;
                    
                    let entry = self.deserialize_entry(&buffer)?;
                    entries.push(entry);
                },
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WALError::IoError(e)),
            }
        }
        
        Ok(entries)
    }

    pub async fn truncate(&self) -> Result<(), WALError> {
        let file = File::create(&self.path)?;
        *self.file.lock().await = BufWriter::new(file);
        Ok(())
    }

    fn serialize_entry(&self, entry: &WALEntry) -> Result<Vec<u8>, WALError> {
        let mut buffer = Vec::new();
        
        match entry {
            WALEntry::Begin { tx_id } => {
                buffer.push(0);
                buffer.extend_from_slice(&tx_id.to_le_bytes());
            },
            WALEntry::Commit { tx_id } => {
                buffer.push(1);
                buffer.extend_from_slice(&tx_id.to_le_bytes());
            },
            WALEntry::Rollback { tx_id } => {
                buffer.push(2);
                buffer.extend_from_slice(&tx_id.to_le_bytes());
            },
            WALEntry::Put { tx_id, key, value } => {
                buffer.push(3);
                buffer.extend_from_slice(&tx_id.to_le_bytes());
                buffer.extend_from_slice(&key.len().to_le_bytes());
                buffer.extend_from_slice(key.as_bytes());
                buffer.extend_from_slice(&value.len().to_le_bytes());
                buffer.extend_from_slice(value);
            },
            WALEntry::Delete { tx_id, key } => {
                buffer.push(4);
                buffer.extend_from_slice(&tx_id.to_le_bytes());
                buffer.extend_from_slice(&key.len().to_le_bytes());
                buffer.extend_from_slice(key.as_bytes());
            },
        }
        
        let checksum = self.calculate_checksum(&buffer);
        buffer.extend_from_slice(&checksum.to_le_bytes());
        
        let size = buffer.len() as u64;
        let mut result = Vec::new();
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&buffer);
        
        Ok(result)
    }

    fn deserialize_entry(&self, buffer: &[u8]) -> Result<WALEntry, WALError> {
        if buffer.len() < 9 {
            return Err(WALError::InvalidEntry);
        }
        
        let checksum = u32::from_le_bytes(buffer[buffer.len() - 4..].try_into().unwrap());
        let data = &buffer[..buffer.len() - 4];
        
        if self.calculate_checksum(data) != checksum {
            return Err(WALError::ChecksumMismatch);
        }
        
        match data[0] {
            0 if data.len() == 9 => {
                let tx_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok(WALEntry::Begin { tx_id })
            },
            1 if data.len() == 9 => {
                let tx_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok(WALEntry::Commit { tx_id })
            },
            2 if data.len() == 9 => {
                let tx_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok(WALEntry::Rollback { tx_id })
            },
            3 if data.len() > 17 => {
                let tx_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
                let key_len = u64::from_le_bytes(data[9..17].try_into().unwrap()) as usize;
                let key = String::from_utf8_lossy(&data[17..17 + key_len]).to_string();
                let value_len_start = 17 + key_len;
                let value_len = u64::from_le_bytes(data[value_len_start..value_len_start + 8].try_into().unwrap()) as usize;
                let value = data[value_len_start + 8..value_len_start + 8 + value_len].to_vec();
                Ok(WALEntry::Put { tx_id, key, value })
            },
            4 if data.len() > 17 => {
                let tx_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
                let key_len = u64::from_le_bytes(data[9..17].try_into().unwrap()) as usize;
                let key = String::from_utf8_lossy(&data[17..17 + key_len]).to_string();
                Ok(WALEntry::Delete { tx_id, key })
            },
            _ => Err(WALError::InvalidEntry),
        }
    }

    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        let mut checksum = 0u32;
        for &byte in data {
            checksum = checksum.wrapping_add(byte as u32);
        }
        checksum
    }
}