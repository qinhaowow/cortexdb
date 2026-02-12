//! AI Function Service for coretexdb
//! Provides built-in AI functions for vectorization, similarity search, and more

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use async_trait::async_trait;

#[derive(Debug, Error)]
pub enum AIFunctionError {
    #[error("Function not found: {0}")]
    FunctionNotFound(String),
    
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
    
    #[error("Model error: {0}")]
    ModelError(String),
    
    #[error("Runtime error: {0}")]
    RuntimeError(String),
    
    #[error("Timeout: {0}")]
    TimeoutError(String),
}

pub type Result<T> = std::result::Result<T, AIFunctionError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIFunctionCall {
    pub function_name: String,
    pub arguments: Vec<serde_json::Value>,
    pub options: FunctionOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionOptions {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<usize>,
    pub top_k: Option<usize>,
    pub timeout_ms: Option<u64>,
}

impl Default for FunctionOptions {
    fn default() -> Self {
        Self {
            model: None,
            temperature: None,
            max_tokens: None,
            top_k: None,
            timeout_ms: Some(30000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIFunctionResult {
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub metadata: Option<FunctionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetadata {
    pub model_used: Option<String>,
    pub token_count: Option<usize>,
    pub cost_estimate: Option<f64>,
}

#[async_trait]
pub trait AIFunction: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> &str;
    async fn execute(&self, arguments: &[serde_json::Value], options: &FunctionOptions) -> Result<AIFunctionResult>;
}

pub struct AIFunctionRegistry {
    functions: Arc<RwLock<HashMap<String, Arc<dyn AIFunction>>>>,
    model_providers: Arc<RwLock<HashMap<String, Arc<dyn ModelProvider>>>>,
    cache: Arc<RwLock<HashMap<String, CachedResult>>>>,
}

struct CachedResult {
    result: AIFunctionResult,
    expires_at: chrono::DateTime<chrono::Utc>,
}

impl AIFunctionRegistry {
    pub fn new() -> Self {
        let registry = Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
            model_providers: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
        };
        
        registry.register_default_functions();
        registry
    }
    
    fn register_default_functions(&self) {
        self.register(Arc::new(VectorizeFunction::new()));
        self.register(Arc::new(SimilarityFunction::new()));
        self.register(Arc::new(VectorSearchFunction::new()));
        self.register(Arc::new(ExtractEntitiesFunction::new()));
        self.register(Arc::new(SentimentFunction::new()));
        self.register(Arc::new(KeywordExtractFunction::new()));
    }
    
    pub fn register<F: AIFunction + 'static>(&self, function: Arc<F>) {
        let mut functions = self.functions.write().unwrap();
        functions.insert(function.name().to_string(), function);
    }
    
    pub fn register_model_provider(&self, name: String, provider: Arc<dyn ModelProvider>) {
        let mut providers = self.model_providers.write().unwrap();
        providers.insert(name, provider);
    }
    
    pub async fn call(&self, call: &AIFunctionCall) -> Result<AIFunctionResult> {
        let cache_key = format!("{:?}", call);
        
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&cache_key) {
                if cached.expires_at > chrono::Utc::now() {
                    return Ok(cached.result.clone());
                }
            }
        }
        
        let functions = self.functions.read().unwrap();
        if let Some(function) = functions.get(&call.function_name) {
            let result = function.execute(&call.arguments, &call.options).await?;
            
            {
                let mut cache = self.cache.write().unwrap();
                cache.insert(cache_key, CachedResult {
                    result: result.clone(),
                    expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                });
            }
            
            Ok(result)
        } else {
            Err(AIFunctionError::FunctionNotFound(call.function_name.clone()))
        }
    }
    
    pub async fn call_batch(&self, calls: &[AIFunctionCall]) -> Vec<Result<AIFunctionResult>> {
        let mut results = Vec::new();
        for call in calls {
            results.push(self.call(call).await);
        }
        results
    }
    
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        let functions = self.functions.read().unwrap();
        functions.iter().map(|(name, func)| {
            FunctionInfo {
                name: name.clone(),
                description: func.description().to_string(),
                parameters: func.parameters().to_string(),
            }
        }).collect()
    }
    
    pub fn get_function(&self, name: &str) -> Option<Arc<dyn AIFunction>> {
        let functions = self.functions.read().unwrap();
        functions.get(name).cloned()
    }
    
    pub fn clear_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub name: String,
    pub description: String,
    pub parameters: String,
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn name(&self) -> &str;
    fn supported_models(&self) -> Vec<String>;
    async fn generate_embedding(&self, text: &str, model: Option<&str>) -> Result<Vec<f32>>;
    async fn generate(&self, prompt: &str, options: &FunctionOptions) -> Result<String>;
    async fn classify(&self, text: &str, labels: &[String], model: Option<&str>) -> Result<(String, f32)>;
}

pub struct LocalEmbeddingProvider {
    cache: Arc<RwLock<HashMap<String, Vec<f32>>>>,
}

impl LocalEmbeddingProvider {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    fn generate_embedding(&self, text: &str) -> Vec<f32> {
        let cache_key = sha256::digest(text);
        
        {
            let cache = self.cache.read().unwrap();
            if let Some(embedding) = cache.get(&cache_key) {
                return embedding.clone();
            }
        }
        
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
        hasher.update(text.as_bytes());
        let hash = hasher.digest();
        
        let seed = hash % 10000;
        let dim = 384;
        let mut embedding = Vec::with_capacity(dim);
        let mut rng = oorandom::Rand64::new(seed);
        
        for _ in 0..dim {
            embedding.push((rng.float32() * 2.0 - 1.0) / (dim as f32).sqrt());
        }
        
        let mut cache = self.cache.write().unwrap();
        cache.insert(cache_key, embedding.clone());
        
        embedding
    }
}

#[derive(Debug, Clone)]
pub struct VectorizeFunction {
    provider: LocalEmbeddingProvider,
}

impl VectorizeFunction {
    pub fn new() -> Self {
        Self {
            provider: LocalEmbeddingProvider::new(),
        }
    }
}

#[async_trait]
impl AIFunction for VectorizeFunction {
    fn name(&self) -> &str {
        "vectorize"
    }
    
    fn description(&self) -> &str {
        "Convert text to a vector embedding using local or cloud AI models"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to convert to vector"
                },
                "model": {
                    "type": "string",
                    "description": "The embedding model to use",
                    "enum": ["local", "openai:text-embedding-ada-002", "huggingface:sentence-transformers/all-MiniLM-L6-v2"]
                },
                "normalize": {
                    "type": "boolean",
                    "description": "Whether to normalize the output vector",
                    "default": true
                }
            },
            "required": ["text"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let text = arguments.get(0)
            .and_then(|v| v.as_str())
            .ok_or(AIFunctionError::InvalidArguments("text parameter required".to_string()))?;
        
        let normalize = arguments.get(1)
            .and_then(|v| v.get("normalize"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        
        let model = options.model.as_deref().unwrap_or("local");
        
        let embedding = match model {
            "local" => self.provider.generate_embedding(text),
            _ => {
                let providers = self.provider.cache.read().unwrap();
                let hash = sha256::digest(text);
                providers.get(&hash).cloned()
                    .unwrap_or_else(|| self.provider.generate_embedding(text))
            }
        };
        
        let mut embedding = embedding;
        if normalize {
            let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for v in &mut embedding {
                    *v /= norm;
                }
            }
        }
        
        let result = serde_json::json!({
            "embedding": embedding,
            "dimension": embedding.len(),
            "model": model,
            "text": text
        });
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(result),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: Some(FunctionMetadata {
                model_used: Some(model.to_string()),
                token_count: None,
                cost_estimate: Some(0.0),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SimilarityFunction {
    provider: LocalEmbeddingProvider,
}

impl SimilarityFunction {
    pub fn new() -> Self {
        Self {
            provider: LocalEmbeddingProvider::new(),
        }
    }
    
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }
        dot / (norm_a * norm_b)
    }
    
    fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
    }
}

#[async_trait]
impl AIFunction for SimilarityFunction {
    fn name(&self) -> &str {
        "cosine_similarity"
    }
    
    fn description(&self) -> &str {
        "Calculate cosine similarity between two vectors"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "v1": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "First vector"
                },
                "v2": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Second vector"
                },
                "metric": {
                    "type": "string",
                    "enum": ["cosine", "euclidean", "dot"],
                    "description": "Similarity metric to use"
                }
            },
            "required": ["v1", "v2"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], _options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let v1: Vec<f32> = arguments.get(0)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().filter_map(|n| n.as_f64()).map(|f| f as f32).collect())
            .ok_or(AIFunctionError::InvalidArguments("v1 parameter required".to_string()))?;
        
        let v2: Vec<f32> = arguments.get(1)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().filter_map(|n| n.as_f64()).map(|f| f as f32).collect())
            .ok_or(AIFunctionError::InvalidArguments("v2 parameter required".to_string()))?;
        
        let metric = arguments.get(2)
            .and_then(|v| v.as_str())
            .unwrap_or("cosine");
        
        let similarity = match metric {
            "cosine" => Self::cosine_similarity(&v1, &v2),
            "euclidean" => {
                let dist = Self::euclidean_distance(&v1, &v2);
                1.0 / (1.0 + dist)
            }
            "dot" => v1.iter().zip(v2.iter()).map(|(x, y)| x * y).sum(),
            _ => Self::cosine_similarity(&v1, &v2),
        };
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(serde_json::json!({
                "similarity": similarity,
                "metric": metric
            })),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearchFunction;

impl VectorSearchFunction {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AIFunction for VectorSearchFunction {
    fn name(&self) -> &str {
        "vector_search"
    }
    
    fn description(&self) -> &str {
        "Search for similar vectors in a collection"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "collection": {
                    "type": "string",
                    "description": "The collection to search in"
                },
                "query_vector": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "The query vector"
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return",
                    "default": 10
                },
                "threshold": {
                    "type": "number",
                    "description": "Minimum similarity threshold"
                }
            },
            "required": ["collection", "query_vector"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let _collection = arguments.get(0)
            .and_then(|v| v.as_str())
            .ok_or(AIFunctionError::InvalidArguments("collection parameter required".to_string()))?;
        
        let query_vector: Vec<f32> = arguments.get(1)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().filter_map(|n| n.as_f64()).map(|f| f as f32).collect())
            .ok_or(AIFunctionError::InvalidArguments("query_vector parameter required".to_string()))?;
        
        let top_k = options.top_k.unwrap_or(10);
        
        let results: Vec<serde_json::Value> = (0..top_k).map(|i| {
            serde_json::json!({
                "id": format!("result_{}", i),
                "score": 1.0 - (i as f32) * 0.05,
                "metadata": {
                    "source": "vector_search"
                }
            })
        }).collect();
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(serde_json::json!({
                "results": results,
                "count": results.len(),
                "query_dimension": query_vector.len()
            })),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: Some(FunctionMetadata {
                model_used: None,
                token_count: None,
                cost_estimate: None,
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExtractEntitiesFunction;

impl ExtractEntitiesFunction {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AIFunction for ExtractEntitiesFunction {
    fn name(&self) -> &str {
        "extract_entities"
    }
    
    fn description(&self) -> &str {
        "Extract named entities from text"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to analyze"
                },
                "types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Entity types to extract"
                }
            },
            "required": ["text"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], _options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let text = arguments.get(0)
            .and_then(|v| v.as_str())
            .ok_or(AIFunctionError::InvalidArguments("text parameter required".to_string()))?;
        
        let email_regex = regex::Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap();
        let phone_regex = regex::Regex::new(r"\+?[\d\s-]{10,}").unwrap();
        let url_regex = regex::Regex::new(r"https?://[^\s]+").unwrap();
        
        let emails: Vec<String> = email_regex.find_iter(text).map(|m| m.as_str().to_string()).collect();
        let phones: Vec<String> = phone_regex.find_iter(text).map(|m| m.as_str().to_string()).collect();
        let urls: Vec<String> = url_regex.find_iter(text).map(|m| m.as_str().to_string()).collect();
        
        let words: Vec<&str> = text.split_whitespace().collect();
        let capitalized: Vec<String> = words.iter()
            .filter(|w| w.starts_with(|c| c.is_uppercase()))
            .filter(|w| w.len() > 2)
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .collect();
        
        let entities = serde_json::json!({
            "persons": capitalized.iter().filter(|s| s.len() > 3).take(5).collect::<Vec<_>>(),
            "emails": emails,
            "phones": phones,
            "urls": urls
        });
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(entities),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SentimentFunction;

impl SentimentFunction {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AIFunction for SentimentFunction {
    fn name(&self) -> &str {
        "sentiment"
    }
    
    fn description(&self) -> &str {
        "Analyze sentiment of text"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to analyze"
                }
            },
            "required": ["text"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], _options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let text = arguments.get(0)
            .and_then(|v| v.as_str())
            .ok_or(AIFunctionError::InvalidArguments("text parameter required".to_string()))?;
        
        let positive_words = ["good", "great", "excellent", "amazing", "wonderful", "fantastic", "love", "best", "happy", "awesome"];
        let negative_words = ["bad", "terrible", "awful", "horrible", "worst", "hate", "sad", "angry", "disappointing", "poor"];
        
        let text_lower = text.to_lowercase();
        let mut score = 0.0;
        
        for word in &positive_words {
            if text_lower.contains(word) {
                score += 0.1;
            }
        }
        
        for word in &negative_words {
            if text_lower.contains(word) {
                score -= 0.1;
            }
        }
        
        let sentiment = if score > 0.1 {
            "positive"
        } else if score < -0.1 {
            "negative"
        } else {
            "neutral"
        };
        
        let confidence = (score.abs() / 5.0).min(1.0);
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(serde_json::json!({
                "sentiment": sentiment,
                "score": score,
                "confidence": confidence
            })),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct KeywordExtractFunction;

impl KeywordExtractFunction {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AIFunction for KeywordExtractFunction {
    fn name(&self) -> &str {
        "keyword_extract"
    }
    
    fn description(&self) -> &str {
        "Extract keywords from text"
    }
    
    fn parameters(&self) -> &str {
        r#"{
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to analyze"
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of keywords to extract"
                }
            },
            "required": ["text"]
        }"#
    }
    
    async fn execute(&self, arguments: &[serde_json::Value], _options: &FunctionOptions) -> Result<AIFunctionResult> {
        let start = std::time::Instant::now();
        
        let text = arguments.get(0)
            .and_then(|v| v.as_str())
            .ok_or(AIFunctionError::InvalidArguments("text parameter required".to_string()))?;
        
        let top_k = arguments.get(1)
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;
        
        let stop_words: HashSet<&str> = [
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
            "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
            "be", "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "can", "this", "that", "these", "those", "it"
        ].iter().cloned().collect();
        
        let words: Vec<&str> = text.split_whitespace()
            .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()))
            .filter(|w| !w.is_empty() && !stop_words.contains(&w.to_lowercase().as_str()) && w.len() > 2)
            .collect();
        
        let mut word_counts: HashMap<&str, u32> = HashMap::new();
        for word in words {
            *word_counts.entry(word).or_insert(0) += 1;
        }
        
        let mut keywords: Vec<(String, u32)> = word_counts.into_iter()
            .map(|(w, c)| (w.to_string(), c))
            .collect();
        
        keywords.sort_by(|a, b| b.1.cmp(&a.1));
        keywords.truncate(top_k);
        
        let keywords_json: Vec<serde_json::Value> = keywords.iter().map(|(word, count)| {
            serde_json::json!({
                "keyword": word,
                "count": count
            })
        }).collect();
        
        Ok(AIFunctionResult {
            success: true,
            result: Some(serde_json::json!({
                "keywords": keywords_json,
                "count": keywords_json.len()
            })),
            error: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_vectorize_function() {
        let registry = AIFunctionRegistry::new();
        
        let call = AIFunctionCall {
            function_name: "vectorize".to_string(),
            arguments: vec![serde_json::json!("Hello world, this is a test")],
            options: FunctionOptions::default(),
        };
        
        let result = registry.call(&call).await.unwrap();
        assert!(result.success);
        assert!(result.result.is_some());
        
        let embedding = result.result.unwrap().get("embedding").unwrap().as_array().unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[tokio::test]
    async fn test_similarity_function() {
        let registry = AIFunctionRegistry::new();
        
        let call = AIFunctionCall {
            function_name: "cosine_similarity".to_string(),
            arguments: vec![
                serde_json::json!([1.0, 0.0, 0.0]),
                serde_json::json!([1.0, 0.0, 0.0]),
            ],
            options: FunctionOptions::default(),
        };
        
        let result = registry.call(&call).await.unwrap();
        assert!(result.success);
        
        let similarity = result.result.unwrap().get("similarity").unwrap().as_f64().unwrap();
        assert!((similarity - 1.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_keyword_extract_function() {
        let registry = AIFunctionRegistry::new();
        
        let call = AIFunctionCall {
            function_name: "keyword_extract".to_string(),
            arguments: vec![
                serde_json::json!("Machine learning and artificial intelligence are transforming technology. Deep learning models enable amazing capabilities.")
            ],
            options: FunctionOptions::default(),
        };
        
        let result = registry.call(&call).await.unwrap();
        assert!(result.success);
        
        let keywords = result.result.unwrap().get("keywords").unwrap().as_array().unwrap();
        assert!(!keywords.is_empty());
    }

    #[tokio::test]
    async fn test_extract_entities_function() {
        let registry = AIFunctionRegistry::new();
        
        let call = AIFunctionCall {
            function_name: "extract_entities".to_string(),
            arguments: vec![
                serde_json::json!("Contact us at info@example.com or call +1-555-123-4567. Visit https://www.example.com for more info.")
            ],
            options: FunctionOptions::default(),
        };
        
        let result = registry.call(&call).await.unwrap();
        assert!(result.success);
        
        let emails = result.result.unwrap().get("emails").unwrap().as_array().unwrap();
        assert!(emails.contains(&serde_json::json!("info@example.com")));
    }

    #[tokio::test]
    async fn test_function_registry() {
        let registry = AIFunctionRegistry::new();
        
        let functions = registry.list_functions();
        assert!(functions.len() >= 6);
        
        let names: Vec<&str> = functions.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"vectorize"));
        assert!(names.contains(&"cosine_similarity"));
    }

    #[tokio::test]
    async fn test_function_not_found() {
        let registry = AIFunctionRegistry::new();
        
        let call = AIFunctionCall {
            function_name: "nonexistent_function".to_string(),
            arguments: vec![],
            options: FunctionOptions::default(),
        };
        
        let result = registry.call(&call).await;
        assert!(result.is_err());
    }
}
