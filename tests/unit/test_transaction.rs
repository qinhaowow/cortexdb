use cortexdb::transaction::{TransactionManager, TransactionError};

#[tokio::test]
async fn test_begin_transaction() {
    let tx_manager = TransactionManager::new().await;
    let tx = tx_manager.begin_transaction().await;
    assert!(tx.is_ok());
}

#[tokio::test]
async fn test_commit_transaction() {
    let tx_manager = TransactionManager::new().await;
    let tx = tx_manager.begin_transaction().await.unwrap();
    let result = tx_manager.commit_transaction(&tx).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_rollback_transaction() {
    let tx_manager = TransactionManager::new().await;
    let tx = tx_manager.begin_transaction().await.unwrap();
    let result = tx_manager.rollback_transaction(&tx).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_double_commit() {
    let tx_manager = TransactionManager::new().await;
    let tx = tx_manager.begin_transaction().await.unwrap();
    tx_manager.commit_transaction(&tx).await.unwrap();
    let result = tx_manager.commit_transaction(&tx).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        TransactionError::AlreadyCommitted => (),
        _ => panic!("Expected AlreadyCommitted error"),
    }
}