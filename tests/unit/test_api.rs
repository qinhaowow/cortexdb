use cortexdb::api::{ApiManager, ApiError};

#[tokio::test]
async fn test_api_manager() {
    let api_manager = ApiManager::new().await;
    assert!(api_manager.inner.try_read().is_ok());
}

#[tokio::test]
async fn test_rest_server() {
    let api_manager = ApiManager::new().await;
    let result = api_manager.start_rest_server("127.0.0.1:8000").await;
    // This might fail if port is in use, but we're just testing the API
    match result {
        Ok(_) => {
            api_manager.stop_all_servers().await.unwrap();
        },
        Err(ApiError::RestError(_)) => {
            // Expected if port is in use
        },
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_grpc_server() {
    let api_manager = ApiManager::new().await;
    let result = api_manager.start_grpc_server("127.0.0.1:9000").await;
    // This might fail if port is in use, but we're just testing the API
    match result {
        Ok(_) => {
            api_manager.stop_all_servers().await.unwrap();
        },
        Err(ApiError::GrpcError(_)) => {
            // Expected if port is in use
        },
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_graphql_server() {
    let api_manager = ApiManager::new().await;
    let result = api_manager.start_graphql_server("127.0.0.1:8080").await;
    // This might fail if port is in use, but we're just testing the API
    match result {
        Ok(_) => {
            api_manager.stop_all_servers().await.unwrap();
        },
        Err(ApiError::GraphqlError(_)) => {
            // Expected if port is in use
        },
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }
}