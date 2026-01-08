use cortexdb::{CortexDB, CollectionSchema, DistanceMetric, IndexConfig, IndexType, Vector};
use std::time::Duration;

#[tokio::test]
async fn test_basic_workflow() {
    // Test basic database workflow
    let db = CortexDB::new().await.unwrap();
    
    // Store a vector
    let vector = vec![1.0, 2.0, 3.0];
    let metadata = serde_json::json!({ "name": "test" });
    db.store("test1", &vector, &metadata).await.unwrap();
    
    // Retrieve the vector
    let result = db.retrieve("test1").await.unwrap();
    assert!(result.is_some());
    let (retrieved_vector, retrieved_metadata) = result.unwrap();
    assert_eq!(retrieved_vector, vector);
    assert_eq!(retrieved_metadata, metadata);
    
    // List vectors
    let vectors = db.list().await.unwrap();
    assert!(vectors.contains(&"test1".to_string()));
    
    // Count vectors
    let count = db.count().await.unwrap();
    assert_eq!(count, 1);
    
    // Delete vector
    let deleted = db.delete("test1").await.unwrap();
    assert!(deleted);
    
    // Verify deletion
    let result = db.retrieve("test1").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_collection_operations() {
    // Test collection creation, insertion, and search
    let db = CortexDB::new().await.unwrap();
    
    // Create an index
    let index_config = IndexConfig {
        distance_metric: DistanceMetric::Euclidean,
        dimension: 3,
        index_type: IndexType::BruteForce,
        params: serde_json::json!({
            "name": "test_index"
        }),
    };
    
    let index_manager = db.index_manager();
    let index = index_manager.create_index("test_index", index_config).await.unwrap();
    
    // Add vectors to index
    let vectors = vec![
        (vec![1.0, 2.0, 3.0], "vec1"),
        (vec![4.0, 5.0, 6.0], "vec2"),
        (vec![7.0, 8.0, 9.0], "vec3"),
    ];
    
    for (vector, id) in vectors {
        index.add(id, &vector).await.unwrap();
    }
    
    // Search vectors
    let query = vec![1.1, 2.1, 3.1];
    let results = index.search(&query, 2).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec1");
}

#[tokio::test]
async fn test_index_performance() {
    // Test index performance with different configurations
    let db = CortexDB::new().await.unwrap();
    
    // Create HNSW index
    let hnsw_config = IndexConfig {
        distance_metric: DistanceMetric::Cosine,
        dimension: 128,
        index_type: IndexType::HNSW,
        params: serde_json::json!({
            "name": "hnsw_index",
            "m": 16,
            "ef_construction": 100,
            "ef_search": 10
        }),
    };
    
    let index_manager = db.index_manager();
    let hnsw_index = index_manager.create_index("hnsw_index", hnsw_config).await.unwrap();
    
    // Add random vectors
    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    let start_time = std::time::Instant::now();
    
    for i in 0..1000 {
        let vector: Vec<f32> = (0..128).map(|_| rng.gen_range(-1.0..1.0)).collect();
        hnsw_index.add(&format!("vec{}", i), &vector).await.unwrap();
    }
    
    let index_time = start_time.elapsed();
    println!("Indexing time: {:?}", index_time);
    
    // Test search performance
    let query: Vec<f32> = (0..128).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let search_start = std::time::Instant::now();
    
    let results = hnsw_index.search(&query, 10).await.unwrap();
    
    let search_time = search_start.elapsed();
    println!("Search time: {:?}", search_time);
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn test_error_handling() {
    // Test error handling
    let db = CortexDB::new().await.unwrap();
    
    // Try to retrieve non-existent vector
    let result = db.retrieve("non_existent").await;
    assert!(result.is_ok());
    let retrieved = result.unwrap();
    assert!(retrieved.is_none());
    
    // Try to delete non-existent vector
    let deleted = db.delete("non_existent").await.unwrap();
    assert!(!deleted);
}