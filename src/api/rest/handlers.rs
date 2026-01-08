use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use warp::reply::Json;
use warp::Reply;

#[derive(Debug, Deserialize)]
pub struct CreateCollectionRequest {
    name: String,
    dimension: usize,
    metric: String,
    index_type: Option<String>,
    index_params: Option<HashMap<String, f64>>,
}

#[derive(Debug, Serialize)]
pub struct CreateCollectionResponse {
    id: String,
    name: String,
    status: String,
}

#[derive(Debug, Deserialize)]
pub struct InsertDocumentRequest {
    collection_name: String,
    documents: Vec<Document>,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    id: Option<String>,
    vector: Vec<f32>,
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct InsertDocumentResponse {
    inserted_count: usize,
    ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    collection_name: String,
    query: Vec<f32>,
    limit: usize,
    filters: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    id: String,
    score: f32,
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    results: Vec<SearchResult>,
    took_ms: f64,
}

pub struct Handlers {
    collections: HashMap<String, Collection>,
}

#[derive(Debug, Clone)]
pub struct Collection {
    id: String,
    name: String,
    dimension: usize,
    metric: String,
    index_type: String,
    documents: HashMap<String, Document>,
}

impl Handlers {
    pub async fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    pub async fn create_collection(&mut self, req: CreateCollectionRequest) -> Result<Json, String> {
        let id = format!("col_{}", uuid::Uuid::new_v4());
        let collection = Collection {
            id: id.clone(),
            name: req.name.clone(),
            dimension: req.dimension,
            metric: req.metric,
            index_type: req.index_type.unwrap_or("hnsw".to_string()),
            documents: HashMap::new(),
        };
        
        self.collections.insert(req.name.clone(), collection);
        
        let response = CreateCollectionResponse {
            id,
            name: req.name,
            status: "created".to_string(),
        };
        
        Ok(warp::reply::json(&response))
    }

    pub async fn insert_document(&mut self, req: InsertDocumentRequest) -> Result<Json, String> {
        if let Some(collection) = self.collections.get_mut(&req.collection_name) {
            let mut inserted_count = 0;
            let mut ids = Vec::new();
            
            for doc in req.documents {
                let id = doc.id.unwrap_or_else(|| format!("doc_{}", uuid::Uuid::new_v4()));
                collection.documents.insert(id.clone(), doc);
                inserted_count += 1;
                ids.push(id);
            }
            
            let response = InsertDocumentResponse {
                inserted_count,
                ids,
            };
            
            Ok(warp::reply::json(&response))
        } else {
            Err("Collection not found".to_string())
        }
    }

    pub async fn search(&self, req: SearchRequest) -> Result<Json, String> {
        if let Some(collection) = self.collections.get(&req.collection_name) {
            // Mock search results
            let mut results = Vec::new();
            
            for (id, doc) in collection.documents.iter().take(req.limit) {
                results.push(SearchResult {
                    id: id.clone(),
                    score: rand::random::<f32>(),
                    metadata: doc.metadata.clone(),
                });
            }
            
            let response = SearchResponse {
                results,
                took_ms: rand::random::<f64>() * 100.0,
            };
            
            Ok(warp::reply::json(&response))
        } else {
            Err("Collection not found".to_string())
        }
    }

    pub async fn get_collections(&self) -> Result<Json, String> {
        let collection_list: Vec<HashMap<String, String>> = self.collections.values()
            .map(|col| {
                let mut map = HashMap::new();
                map.insert("id".to_string(), col.id.clone());
                map.insert("name".to_string(), col.name.clone());
                map.insert("dimension".to_string(), col.dimension.to_string());
                map.insert("metric".to_string(), col.metric.clone());
                map.insert("index_type".to_string(), col.index_type.clone());
                map.insert("document_count".to_string(), col.documents.len().to_string());
                map
            })
            .collect();
        
        Ok(warp::reply::json(&collection_list))
    }

    pub async fn health_check(&self) -> Result<Json, String> {
        let health_status = HashMap::from([
            ("status".to_string(), "healthy".to_string()),
            ("service".to_string(), "cortexdb".to_string()),
            ("version".to_string(), "0.1.0".to_string()),
        ]);
        
        Ok(warp::reply::json(&health_status))
    }
}