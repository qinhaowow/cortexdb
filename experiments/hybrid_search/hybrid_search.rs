use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HybridSearchRequest {
    query: String,
    vector: Option<Vec<f32>>,
    filters: Option<HashMap<String, String>>,
    limit: usize,
    weights: HybridWeights,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HybridWeights {
    text: f32,
    vector: f32,
    lexical: f32,
}

#[derive(Debug, Serialize)]
pub struct HybridSearchResult {
    id: String,
    score: f32,
    text_score: f32,
    vector_score: f32,
    lexical_score: f32,
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, thiserror::Error)]
pub enum HybridSearchError {
    #[error("Search failed: {0}")]
    SearchFailed(String),
}

pub struct HybridSearchEngine {
    // Internal state
}

impl HybridSearchEngine {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn search(&self, request: HybridSearchRequest) -> Result<Vec<HybridSearchResult>, HybridSearchError> {
        println!("Performing hybrid search with query: {}", request.query);
        println!("Weights: text={}, vector={}, lexical={}", request.weights.text, request.weights.vector, request.weights.lexical);

        // Mock hybrid search
        let mut results = Vec::new();
        
        for i in 1..=request.limit {
            let result = HybridSearchResult {
                id: format!("doc_{}", i),
                score: 1.0 - (i as f32 / request.limit as f32),
                text_score: 0.8 - (i as f32 / request.limit as f32 * 0.3),
                vector_score: 0.9 - (i as f32 / request.limit as f32 * 0.2),
                lexical_score: 0.7 - (i as f32 / request.limit as f32 * 0.4),
                metadata: Some(HashMap::from([
                    ("title".to_string(), serde_json::Value::String(format!("Document {}", i))),
                    ("relevance".to_string(), serde_json::Value::Number(serde_json::Number::from_f64((1.0 - (i as f64 / request.limit as f64)) * 100.0).unwrap())),
                ])),
            };
            results.push(result);
        }

        Ok(results)
    }

    pub async fn index(&self, id: String, text: String, vector: Vec<f32>, metadata: Option<HashMap<String, serde_json::Value>>) -> Result<(), HybridSearchError> {
        // Mock index operation
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let engine = HybridSearchEngine::new();
    
    let request = HybridSearchRequest {
        query: "machine learning".to_string(),
        vector: Some(vec![0.1, 0.2, 0.3, 0.4, 0.5]),
        filters: None,
        limit: 5,
        weights: HybridWeights {
            text: 0.4,
            vector: 0.5,
            lexical: 0.1,
        },
    };
    
    match engine.search(request).await {
        Ok(results) => {
            println!("Hybrid search results:");
            for result in results {
                println!("{}: score={:.3}", result.id, result.score);
            }
        },
        Err(e) => {
            eprintln!("Search failed: {}", e);
        }
    }
}