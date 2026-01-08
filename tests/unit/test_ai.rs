use cortexdb::ai::{AIManager, embedding::{EmbeddingManager, EmbeddingRequest, EmbeddingError}};

#[tokio::test]
async fn test_embedding_manager() {
    let embedding_manager = EmbeddingManager::new().await;
    let models = embedding_manager.list_models().await;
    assert!(!models.is_empty());
}

#[tokio::test]
async fn test_get_embedding() {
    let embedding_manager = EmbeddingManager::new().await;
    let request = EmbeddingRequest {
        texts: vec!["Hello world".to_string()],
        model_name: "text-embedding-ada-002".to_string(),
        batch_size: None,
    };
    let result = embedding_manager.get_embedding(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.embeddings.is_empty());
}

#[tokio::test]
async fn test_ai_manager() {
    let ai_manager = AIManager::new().await;
    let embedding_manager = ai_manager.get_embedding_manager().await;
    assert!(embedding_manager.try_read().is_ok());
}

#[tokio::test]
async fn test_invalid_model() {
    let embedding_manager = EmbeddingManager::new().await;
    let request = EmbeddingRequest {
        texts: vec!["Hello world".to_string()],
        model_name: "invalid-model".to_string(),
        batch_size: None,
    };
    let result = embedding_manager.get_embedding(request).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        EmbeddingError::ModelNotFound(_) => (),
        _ => panic!("Expected ModelNotFound error"),
    }
}