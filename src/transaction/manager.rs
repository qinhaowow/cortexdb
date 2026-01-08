use std::collections::HashMap;
use std::path::Path;
use crate::transaction::{mvcc::MVCCManager, wal::WAL, consistency::ConsistencyManager, Transaction, TransactionState, TransactionError};

pub struct TransactionManagerInner {
    mvcc: MVCCManager,
    wal: Option<WAL>,
    consistency: ConsistencyManager,
    active_transactions: HashMap<u64, Transaction>,
    next_tx_id: u64,
}

impl TransactionManagerInner {
    pub async fn new() -> Self {
        let mvcc = MVCCManager::new().await;
        let consistency = ConsistencyManager::new(crate::transaction::consistency::ConsistencyLevel::ReadCommitted);
        
        Self {
            mvcc,
            wal: None,
            consistency,
            active_transactions: HashMap::new(),
            next_tx_id: 1,
        }
    }

    pub async fn init_wal<P: AsRef<Path>>(&mut self, path: P) -> Result<(), TransactionError> {
        let wal = WAL::new(path)?;
        self.wal = Some(wal);
        Ok(())
    }

    pub async fn begin_transaction(&mut self) -> Result<Transaction, TransactionError> {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        
        let timestamp = self.mvcc.get_next_timestamp().await;
        
        let tx = Transaction {
            id: tx_id,
            timestamp,
            state: TransactionState::Active,
        };
        
        // Write begin entry to WAL if enabled
        if let Some(wal) = &self.wal {
            wal.write_entry(&crate::transaction::wal::WALEntry::Begin { tx_id }).await?;
        }
        
        self.active_transactions.insert(tx_id, tx.clone());
        Ok(tx)
    }

    pub async fn commit_transaction(&mut self, tx: &Transaction) -> Result<(), TransactionError> {
        if tx.state != TransactionState::Active {
            return Err(TransactionError::AlreadyCommitted);
        }
        
        // Write commit entry to WAL if enabled
        if let Some(wal) = &self.wal {
            wal.write_entry(&crate::transaction::wal::WALEntry::Commit { tx_id: tx.id }).await?;
        }
        
        // Update transaction state
        if let Some(active_tx) = self.active_transactions.get_mut(&tx.id) {
            active_tx.state = TransactionState::Committed;
        }
        
        // Remove from active transactions after commit
        self.active_transactions.remove(&tx.id);
        
        Ok(())
    }

    pub async fn rollback_transaction(&mut self, tx: &Transaction) -> Result<(), TransactionError> {
        if tx.state != TransactionState::Active {
            return Err(TransactionError::AlreadyRolledBack);
        }
        
        // Write rollback entry to WAL if enabled
        if let Some(wal) = &self.wal {
            wal.write_entry(&crate::transaction::wal::WALEntry::Rollback { tx_id: tx.id }).await?;
        }
        
        // Update transaction state
        if let Some(active_tx) = self.active_transactions.get_mut(&tx.id) {
            active_tx.state = TransactionState::RolledBack;
        }
        
        // Remove from active transactions after rollback
        self.active_transactions.remove(&tx.id);
        
        Ok(())
    }

    pub async fn put(&mut self, tx: &Transaction, key: String, value: Vec<u8>) -> Result<(), TransactionError> {
        if tx.state != TransactionState::Active {
            return Err(TransactionError::Aborted("Transaction is not active".to_string()));
        }
        
        // Write to WAL if enabled
        if let Some(wal) = &self.wal {
            wal.write_entry(&crate::transaction::wal::WALEntry::Put {
                tx_id: tx.id,
                key: key.clone(),
                value: value.clone(),
            }).await?;
        }
        
        // Write to MVCC
        self.mvcc.put(key, value, tx.timestamp).await?;
        Ok(())
    }

    pub async fn get(&self, tx: &Transaction, key: &str) -> Result<Vec<u8>, TransactionError> {
        if tx.state != TransactionState::Active {
            return Err(TransactionError::Aborted("Transaction is not active".to_string()));
        }
        
        self.mvcc.get(key, tx.timestamp).await?;
        Ok(vec![])
    }

    pub async fn delete(&mut self, tx: &Transaction, key: String) -> Result<(), TransactionError> {
        if tx.state != TransactionState::Active {
            return Err(TransactionError::Aborted("Transaction is not active".to_string()));
        }
        
        // Write to WAL if enabled
        if let Some(wal) = &self.wal {
            wal.write_entry(&crate::transaction::wal::WALEntry::Delete {
                tx_id: tx.id,
                key: key.clone(),
            }).await?;
        }
        
        // Delete from MVCC
        self.mvcc.delete(&key, tx.timestamp).await?;
        Ok(())
    }

    pub async fn recover(&mut self) -> Result<(), TransactionError> {
        if let Some(wal) = &self.wal {
            let entries = wal.read_all_entries().await?;
            self.apply_wal_entries(entries).await?;
        }
        Ok(())
    }

    async fn apply_wal_entries(&mut self, entries: Vec<crate::transaction::wal::WALEntry>) -> Result<(), TransactionError> {
        for entry in entries {
            match entry {
                crate::transaction::wal::WALEntry::Begin { tx_id } => {
                    // Track active transactions
                    if !self.active_transactions.contains_key(&tx_id) {
                        let timestamp = self.mvcc.get_next_timestamp().await;
                        self.active_transactions.insert(tx_id, Transaction {
                            id: tx_id,
                            timestamp,
                            state: TransactionState::Active,
                        });
                    }
                },
                crate::transaction::wal::WALEntry::Commit { tx_id } => {
                    // Commit transaction
                    if let Some(tx) = self.active_transactions.get_mut(&tx_id) {
                        tx.state = TransactionState::Committed;
                    }
                    self.active_transactions.remove(&tx_id);
                },
                crate::transaction::wal::WALEntry::Rollback { tx_id } => {
                    // Rollback transaction
                    if let Some(tx) = self.active_transactions.get_mut(&tx_id) {
                        tx.state = TransactionState::RolledBack;
                    }
                    self.active_transactions.remove(&tx_id);
                },
                crate::transaction::wal::WALEntry::Put { tx_id, key, value } => {
                    // Apply put operation
                    if let Some(tx) = self.active_transactions.get(&tx_id) {
                        self.mvcc.put(key, value, tx.timestamp).await?;
                    }
                },
                crate::transaction::wal::WALEntry::Delete { tx_id, key } => {
                    // Apply delete operation
                    if let Some(tx) = self.active_transactions.get(&tx_id) {
                        self.mvcc.delete(&key, tx.timestamp).await?;
                    }
                },
            }
        }
        Ok(())
    }
}