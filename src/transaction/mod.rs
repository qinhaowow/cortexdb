pub mod mvcc;
pub mod wal;
pub mod consistency;
pub mod manager;

use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TransactionManager {
    inner: Arc<RwLock<manager::TransactionManagerInner>>,
}

impl TransactionManager {
    pub async fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(manager::TransactionManagerInner::new().await)),
        }
    }

    pub async fn begin_transaction(&self) -> Result<Transaction, TransactionError> {
        let mut inner = self.inner.write().await;
        inner.begin_transaction().await
    }

    pub async fn commit_transaction(&self, tx: &Transaction) -> Result<(), TransactionError> {
        let mut inner = self.inner.write().await;
        inner.commit_transaction(tx).await
    }

    pub async fn rollback_transaction(&self, tx: &Transaction) -> Result<(), TransactionError> {
        let mut inner = self.inner.write().await;
        inner.rollback_transaction(tx).await
    }
}

pub struct Transaction {
    id: u64,
    timestamp: u64,
    state: TransactionState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Transaction already committed")]
    AlreadyCommitted,
    #[error("Transaction already rolled back")]
    AlreadyRolledBack,
    #[error("Transaction aborted: {0}")]
    Aborted(String),
    #[error("WAL error: {0}")]
    WalError(#[from] wal::WALError),
    #[error("MVCC error: {0}")]
    MvccError(#[from] mvcc::MVCCError),
    #[error("Consistency error: {0}")]
    ConsistencyError(#[from] consistency::ConsistencyError),
}

impl Transaction {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn state(&self) -> &TransactionState {
        &self.state
    }
}