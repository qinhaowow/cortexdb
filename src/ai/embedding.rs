use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum EmbeddingProvider {
    OpenAI,
    HuggingFace,
    Cohere,
    Local,
}

#[derive(Debug, Clone)]
pub struct EmbeddingModel {
    name: String,
    provider: EmbeddingProvider,
    dimension: usize,
    context_window: usize,
    max_batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct EmbeddingRequest {
    texts: Vec<String>,
    model_name: String,
    batch_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct EmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
    model_name: String,
    usage: Option<UsageStats>,
}

#[derive(Debug, Clone)]
pub struct UsageStats {
    total_tokens: usize,
    prompt_tokens: usize,
    completion_tokens: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

pub struct EmbeddingManager {
    models: Arc<RwLock<HashMap<String, EmbeddingModel>>>,
    cache: Arc<RwLock<HashMap<String, Vec<f32>>>>,
}

impl EmbeddingManager {
    pub async fn new() -> Self {
        let mut models = HashMap::new();
        
        // Register default models
        models.insert(
            "text-embedding-ada-002".to_string(),
            EmbeddingModel {
                name: "text-embedding-ada-002".to_string(),
                provider: EmbeddingProvider::OpenAI,
                dimension: 1536,
                context_window: 8191,
                max_batch_size: 2048,
            },
        );
        
        models.insert(
            "BAAI/bge-small-en".to_string(),
            EmbeddingModel {
                name: "BAAI/bge-small-en".to_string(),
                provider: EmbeddingProvider::HuggingFace,
                dimension: 384,
                context_window: 512,
                max_batch_size: 1024,
            },
        );
        
        Self {
            models: Arc::new(RwLock::new(models)),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_model(&self, model: EmbeddingModel) -> Result<(), EmbeddingError> {
        let mut models = self.models.write().await;
        models.insert(model.name.clone(), model);
        Ok(())
    }

    pub async fn get_embedding(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, EmbeddingError> {
        let models = self.models.read().await;
        let model = models.get(&request.model_name).ok_or_else(|| {
            EmbeddingError::ModelNotFound(request.model_name.clone())
        })?;
        
        let batch_size = request.batch_size.unwrap_or(model.max_batch_size);
        
        // Process in batches
        let mut embeddings = Vec::new();
        let mut total_tokens = 0;
        
        for batch in request.texts.chunks(batch_size) {
            let batch_embeddings = self.process_batch(batch, model).await?;
            embeddings.extend(batch_embeddings);
            
            // Calculate tokens (simplified)
            for text in batch {
                total_tokens += text.len() / 4; // Rough estimate
            }
        }
        
        Ok(EmbeddingResponse {
            embeddings,
            model_name: request.model_name,
            usage: Some(UsageStats {
                total_tokens,
                prompt_tokens: total_tokens,
                completion_tokens: 0,
            }),
        })
    }

    async fn process_batch(&self, texts: &[String], model: &EmbeddingModel) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let mut batch_embeddings = Vec::new();
        
        for text in texts {
            // Check cache first
            let cache_key = format!("{}:{}", model.name, text);
            {
                let cache = self.cache.read().await;
                if let Some(embedding) = cache.get(&cache_key) {
                    batch_embeddings.push(embedding.clone());
                    continue;
                }
            }
            
            // Generate embedding based on provider
            let embedding = match model.provider {
                EmbeddingProvider::OpenAI => self.generate_openai_embedding(text, model).await?,
                EmbeddingProvider::HuggingFace => self.generate_huggingface_embedding(text, model).await?,
                EmbeddingProvider::Cohere => self.generate_cohere_embedding(text, model).await?,
                EmbeddingProvider::Local => self.generate_local_embedding(text, model).await?,
            };
            
            // Cache the result
            {
                let mut cache = self.cache.write().await;
                cache.insert(cache_key, embedding.clone());
            }
            
            batch_embeddings.push(embedding);
        }
        
        Ok(batch_embeddings)
    }

    async fn generate_openai_embedding(&self, text: &str, model: &EmbeddingModel) -> Result<Vec<f32>, EmbeddingError> {
        // Mock implementation for now
        Ok(vec![0.0; model.dimension])
    }

    async fn generate_huggingface_embedding(&self, text: &str, model: &EmbeddingModel) -> Result<Vec<f32>, EmbeddingError> {
        // Mock implementation for now
        Ok(vec![0.0; model.dimension])
    }

    async fn generate_cohere_embedding(&self, text: &str, model: &EmbeddingModel) -> Result<Vec<f32>, EmbeddingError> {
        // Mock implementation for now
        Ok(vec![0.0; model.dimension])
    }

    async fn generate_local_embedding(&self, text: &str, model: &EmbeddingModel) -> Result<Vec<f32>, EmbeddingError> {
        // Mock implementation for now
        Ok(vec![0.0; model.dimension])
    }

    pub async fn get_model(&self, model_name: &str) -> Result<EmbeddingModel, EmbeddingError> {
        let models = self.models.read().await;
        models.get(model_name).cloned().ok_or_else(|| {
            EmbeddingError::ModelNotFound(model_name.to_string())
        })
    }

    pub async fn list_models(&self) -> Vec<EmbeddingModel> {
        let models = self.models.read().await;
        models.values().cloned().collect()
    }
}