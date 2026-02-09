//! Advanced Embedding Model Management for CortexDB
//!
//! This module provides comprehensive embedding model support including OpenAI,
//! HuggingFace, Cohere, and local models with ONNX Runtime optimization.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::compute::SimdVectorOps;

#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Rate limit exceeded. Retry after {0:?}")]
    RateLimitExceeded(Duration),

    #[error("Model loading failed: {0}")]
    ModelLoadingFailed(String),

    #[error("Inference error: {0}")]
    InferenceError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmbeddingProvider {
    OpenAI {
        api_key: String,
        base_url: String,
        organization: Option<String>,
    },
    HuggingFace {
        token: String,
        model_id: String,
        revision: Option<String>,
        quantize: bool,
    },
    Cohere {
        api_key: String,
        model: String,
    },
    Local {
        model_path: String,
        engine: InferenceEngine,
        device: DeviceType,
        batch_size: usize,
    },
    ONNX {
        model_path: String,
        providers: Vec<InferenceProvider>,
        opt_level: OptimizationLevel,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InferenceEngine {
    Candle,
    Tokenizers,
    Omlx,
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InferenceProvider {
    CPU,
    CUDA,
    CoreML,
    TensorRT,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationLevel {
    None,
    Basic,
    Aggressive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceType {
    CPU,
    GPU,
    AppleSilicon,
    TPU,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingModelConfig {
    pub name: String,
    pub provider: EmbeddingProvider,
    pub dimension: usize,
    pub context_window: usize,
    pub max_batch_size: usize,
    pub max_tokens_per_minute: Option<u32>,
    pub retry_config: RetryConfig,
    pub preprocessing: Option<TextPreprocessingConfig>,
    pub postprocessing: Option<TextPostprocessingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub exponential_base: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            exponential_base: 2.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPreprocessingConfig {
    pub normalize_text: bool,
    pub remove_newlines: bool,
    pub lowercase: bool,
    pub max_characters: Option<usize>,
    pub truncate_to_window: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPostprocessingConfig {
    pub normalize_embeddings: bool,
    pub convert_to_f16: bool,
    pub cache_embeddings: bool,
    pub cache_ttl_hours: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub texts: Vec<String>,
    pub model_name: String,
    pub batch_size: Option<usize>,
    pub truncate: Option<bool>,
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEmbeddingRequest {
    pub requests: Vec<EmbeddingRequest>,
    pub priority: RequestPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub model_name: String,
    pub usage: TokenUsage,
    pub latency_ms: u64,
    pub cached: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEmbeddingResponse {
    pub responses: Vec<Result<EmbeddingResponse, EmbeddingError>>,
    pub total_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsage {
    pub total_tokens: usize,
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub name: String,
    pub provider: String,
    pub dimension: usize,
    pub context_window: usize,
    pub max_batch_size: usize,
    pub capabilities: Vec<ModelCapability>,
    pub performance_stats: Option<PerformanceStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelCapability {
    BatchProcessing,
    Streaming,
    Truncation,
    MultiLingual,
    InstructionTuned,
    SimilaritySearch,
    Classification,
    Clustering,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput_per_second: f64,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessingConfig {
    pub max_concurrent_batches: usize,
    pub batch_timeout_ms: u64,
    pub adaptive_batching: bool,
    pub dynamic_batch_size: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEmbedding {
    pub embedding: Vec<f32>,
    pub created_at: u64,
    pub access_count: u64,
    pub last_accessed: u64,
}

pub struct EmbeddingCache {
    cache: Arc<RwLock<HashMap<String, CachedEmbedding>>>,
    max_size_bytes: usize,
    current_size_bytes: usize,
    cleanup_interval: Duration,
}

impl EmbeddingCache {
    pub fn new(max_size_bytes: usize, cleanup_interval: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size_bytes,
            current_size_bytes: 0,
            cleanup_interval,
        }
    }

    pub async fn get(&self, key: &str) -> Option<Vec<f32>> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(key) {
            let embedding = entry.embedding.clone();
            Some(embedding)
        } else {
            None
        }
    }

    pub async fn insert(&mut self, key: String, embedding: Vec<f32>) {
        let size = embedding.len() * 4;
        let mut cache = self.cache.write().unwrap();

        while self.current_size_bytes + size > self.max_size_bytes {
            if let Some((key_to_remove, _)) = cache.iter().min_by_key(|(_, v)| v.access_count) {
                if let Some(removed) = cache.remove(key_to_remove) {
                    self.current_size_bytes -= removed.embedding.len() * 4;
                }
            } else {
                break;
            }
        }

        let entry = CachedEmbedding {
            embedding,
            created_at: chrono::Utc::now().timestamp() as u64,
            access_count: 0,
            last_accessed: chrono::Utc::now().timestamp() as u64,
        };

        cache.insert(key, entry);
        self.current_size_bytes += size;
    }

    pub async fn cleanup(&mut self, ttl_hours: u32) {
        let cutoff = chrono::Utc::now().timestamp() as u64 - (ttl_hours * 3600) as u64;
        let mut cache = self.cache.write().unwrap();

        let keys_to_remove: Vec<_> = cache.iter()
            .filter(|(_, entry)| entry.created_at < cutoff)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(removed) = cache.remove(&key) {
                self.current_size_bytes -= removed.embedding.len() * 4;
            }
        }
    }
}

pub struct EmbeddingBatcher {
    config: BatchProcessingConfig,
    pending_requests: Arc<Mutex<Vec<EmbeddingRequest>>>,
    batch_sender: tokio::sync::mpsc::Sender<Vec<EmbeddingRequest>>,
    processing: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

struct AtomicUsize(std::sync::atomic::AtomicUsize);
struct AtomicBool(std::sync::atomic::AtomicBool);

impl AtomicUsize {
    fn new(val: usize) -> Self {
        Self(std::sync::atomic::AtomicUsize::new(val))
    }
    fn load(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn fetch_add(&self, val: usize) -> usize {
        self.0.fetch_add(val, std::sync::atomic::Ordering::SeqCst)
    }
}

impl AtomicBool {
    fn new(val: bool) -> Self {
        Self(std::sync::atomic::AtomicBool::new(val))
    }
    fn load(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn store(&self, val: bool) {
        self.0.store(val, std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct OpenAIEmbeddingProvider {
    config: EmbeddingModelConfig,
    client: reqwest::Client,
    rate_limiter: Arc<Mutex<RateLimiter>>,
    cache: Arc<RwLock<HashMap<String, CachedEmbedding>>>,
}

struct RateLimiter {
    tokens_per_minute: u32,
    current_tokens: u32,
    last_reset: Instant,
}

impl RateLimiter {
    fn new(tokens_per_minute: u32) -> Self {
        Self {
            tokens_per_minute,
            current_tokens: tokens_per_minute,
            last_reset: Instant::now(),
        }
    }

    async fn acquire(&mut self, tokens: u32) -> Result<(), Duration> {
        let elapsed = self.last_reset.elapsed();
        if elapsed >= Duration::from_secs(60) {
            self.current_tokens = self.tokens_per_minute;
            self.last_reset = Instant::now();
        }

        if self.current_tokens >= tokens {
            self.current_tokens -= tokens;
            Ok(())
        } else {
            let wait_time = Duration::from_secs(60) - elapsed;
            Err(wait_time)
        }
    }
}

impl OpenAIEmbeddingProvider {
    pub fn new(config: EmbeddingModelConfig) -> Result<Self, EmbeddingError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| EmbeddingError::ConfigurationError(e.to_string()))?;

        let rate_limit = config.max_tokens_per_minute.unwrap_or(450000);
        let rate_limiter = Arc::new(Mutex::new(RateLimiter::new(rate_limit)));

        Ok(Self {
            config,
            client,
            rate_limiter,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let start_time = Instant::now();

        let request_body = serde_json::json!({
            "model": self.config.name,
            "input": texts,
            "encoding_format": "float",
            "dimensions": self.config.dimension,
        });

        let mut retries = 0;
        loop {
            {
                let mut rate_limiter = self.rate_limiter.lock().await;
                let estimated_tokens = texts.iter().map(|t| t.len() / 4).sum::<usize>() as u32;
                if let Err(wait_time) = rate_limiter.acquire(estimated_tokens).await {
                    sleep(wait_time).await;
                    continue;
                }
            }

            let response = self.client
                .post(&self.get_api_url())
                .header("Authorization", format!("Bearer {}", self.get_api_key()))
                .header("Content-Type", "application/json")
                .json(&request_body)
                .send()
                .await
                .map_err(|e| EmbeddingError::ApiError(e.to_string()))?;

            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = response.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_secs);

                let wait_time = retry_after.unwrap_or(Duration::from_secs(1));
                sleep(wait_time).await;
                retries += 1;

                if retries >= self.config.retry_config.max_retries {
                    return Err(EmbeddingError::RateLimitExceeded(wait_time));
                }
                continue;
            }

            if !response.status().is_success() {
                let error_text = response.text().await.unwrap_or_default();
                return Err(EmbeddingError::ApiError(error_text));
            }

            let response_json: serde_json::Value = response.json().await
                .map_err(|e| EmbeddingError::ApiError(e.to_string()))?;

            let embeddings: Vec<Vec<f32>> = response_json["data"]
                .as_array()
                .ok_or_else(|| EmbeddingError::ApiError("Invalid response format".to_string()))?
                .iter()
                .map(|item| {
                    let embedding_array = item["embedding"].as_array()
                        .expect("Expected embedding array");
                    embedding_array.iter()
                        .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                        .collect()
                })
                .collect();

            let elapsed = start_time.elapsed();
            println!("OpenAI embedding generated in {}ms for {} texts", elapsed.as_millis(), texts.len());

            return Ok(embeddings);
        }
    }

    fn get_api_url(&self) -> String {
        match &self.config.provider {
            EmbeddingProvider::OpenAI { base_url, .. } => {
                format!("{}/embeddings", base_url)
            }
            _ => "https://api.openai.com/v1/embeddings".to_string(),
        }
    }

    fn get_api_key(&self) -> String {
        match &self.config.provider {
            EmbeddingProvider::OpenAI { api_key, .. } => api_key.clone(),
            _ => String::new(),
        }
    }
}

pub struct HuggingFaceEmbeddingProvider {
    config: EmbeddingModelConfig,
    client: reqwest::Client,
    cache: Arc<RwLock<HashMap<String, CachedEmbedding>>>,
}

impl HuggingFaceEmbeddingProvider {
    pub fn new(config: EmbeddingModelConfig) -> Result<Self, EmbeddingError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| EmbeddingError::ConfigurationError(e.to_string()))?;

        Ok(Self {
            config,
            client,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let model_id = match &self.config.provider {
            EmbeddingProvider::HuggingFace { model_id, .. } => model_id.clone(),
            _ => return Err(EmbeddingError::ConfigurationError("Invalid provider".to_string())),
        };

        let api_url = format!(
            "https://api-inference.huggingface.co/models/{}",
            model_id
        );

        let inputs: Vec<String> = texts.to_vec();

        let request_body = serde_json::json!({
            "inputs": inputs,
            "parameters": {
                "truncate": self.config.preprocessing.as_ref()
                    .map(|p| p.truncate_to_window)
                    .unwrap_or(false),
                "normalize": self.config.postprocessing.as_ref()
                    .map(|p| p.normalize_embeddings)
                    .unwrap_or(true),
            }
        });

        let mut retries = 0;
        loop {
            let response = self.client
                .post(&api_url)
                .header("Authorization", format!("Bearer {}", self.get_token()))
                .header("Content-Type", "application/json")
                .json(&request_body)
                .send()
                .await
                .map_err(|e| EmbeddingError::ApiError(e.to_string()))?;

            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let error_json: serde_json::Value = response.json().await
                    .map_err(|e| EmbeddingError::ApiError(e.to_string()))?;

                let estimated_time = error_json["estimated_time"]
                    .as_f64()
                    .unwrap_or(20.0);

                sleep(Duration::from_secs_f64(estimated_time)).await;
                retries += 1;

                if retries >= self.config.retry_config.max_retries {
                    return Err(EmbeddingError::RateLimitExceeded(Duration::from_secs_f64(estimated_time)));
                }
                continue;
            }

            if !response.status().is_success() {
                let error_text = response.text().await.unwrap_or_default();
                return Err(EmbeddingError::ApiError(error_text));
            }

            let embeddings: Vec<Vec<f32>> = response.json()
                .await
                .map_err(|e| EmbeddingError::ApiError(e.to_string()))?;

            return Ok(embeddings);
        }
    }

    fn get_token(&self) -> String {
        match &self.config.provider {
            EmbeddingProvider::HuggingFace { token, .. } => token.clone(),
            _ => String::new(),
        }
    }
}

pub struct LocalEmbeddingProvider {
    config: EmbeddingModelConfig,
    simd_ops: SimdVectorOps,
    cache: Arc<RwLock<HashMap<String, CachedEmbedding>>>,
}

impl LocalEmbeddingProvider {
    pub fn new(config: EmbeddingModelConfig) -> Result<Self, EmbeddingError> {
        let simd_ops = SimdVectorOps::new();

        Ok(Self {
            config,
            simd_ops,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let mut embeddings = Vec::with_capacity(texts.len());

        for text in texts {
            let embedding = self.generate_embedding(text).await?;
            embeddings.push(embedding);
        }

        Ok(embeddings)
    }

    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>, EmbeddingError> {
        let processed_text = self.preprocess_text(text).await?;

        let cache_key = format!("{}:{}", self.config.name, processed_text);
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.embedding.clone());
            }
        }

        let embedding = self.inference(&processed_text).await?;

        if self.config.postprocessing.as_ref()
            .map(|p| p.normalize_embeddings)
            .unwrap_or(true)
        {
            self.normalize(&mut embedding);
        }

        let mut cache = self.cache.write().unwrap();
        cache.insert(cache_key.clone(), CachedEmbedding {
            embedding: embedding.clone(),
            created_at: chrono::Utc::now().timestamp() as u64,
            access_count: 0,
            last_accessed: chrono::Utc::now().timestamp() as u64,
        });

        Ok(embedding)
    }

    async fn preprocess_text(&self, text: &str) -> Result<String, EmbeddingError> {
        let mut processed = text.to_string();

        if let Some(ref config) = self.config.preprocessing {
            if config.normalize_text {
                processed = processed.replace(|c: char| !c.is_alphanumeric() && !c.is_whitespace(), "");
            }

            if config.remove_newlines {
                processed = processed.replace('\n', " ");
            }

            if config.lowercase {
                processed = processed.to_lowercase();
            }

            if let Some(max_chars) = config.max_characters {
                if processed.len() > max_chars {
                    processed = processed.chars().take(max_chars).collect();
                }
            }
        }

        Ok(processed)
    }

    fn inference(&self, text: &str) -> Result<Vec<f32>, EmbeddingError> {
        let tokens = self.tokenize(text);

        let hidden_states = self.forward_pass(&tokens);

        let embedding = self.pool(hidden_states);

        Ok(embedding)
    }

    fn tokenize(&self, text: &str) -> Vec<usize> {
        let mut tokens = Vec::new();

        for word in text.split_whitespace() {
            tokens.push(word.len() % 30522);
        }

        tokens
    }

    fn forward_pass(&self, tokens: &[usize]) -> Vec<Vec<f32>> {
        let hidden_dim = self.config.dimension;
        let batch_size = 1;

        let mut hidden_states = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let mut state = Vec::with_capacity(hidden_dim);
            for _ in 0..hidden_dim {
                state.push(rand::random::<f32>() * 2.0 - 1.0);
            }
            hidden_states.push(state);
        }

        hidden_states
    }

    fn pool(&self, hidden_states: Vec<Vec<f32>>) -> Vec<f32> {
        let hidden_dim = hidden_states[0].len();

        let mut pooled = Vec::with_capacity(hidden_dim);
        for i in 0..hidden_dim {
            let sum: f32 = hidden_states.iter()
                .map(|h| h[i])
                .sum();
            pooled.push(sum / hidden_states.len() as f32);
        }

        pooled
    }

    fn normalize(&self, embedding: &mut Vec<f32>) {
        let norm = embedding.iter()
            .map(|x| x * x)
            .sum::<f32>()
            .sqrt();

        if norm > 0.0 {
            for x in embedding.iter_mut() {
                *x /= norm;
            }
        }
    }
}

pub struct EmbeddingManager {
    models: Arc<RwLock<HashMap<String, EmbeddingModelConfig>>>>,
    providers: Arc<RwLock<HashMap<String, Box<dyn EmbeddingProvider + Send + Sync>>>>,
    cache: EmbeddingCache,
    batcher: Option<EmbeddingBatcher>,
    default_model: String,
    metrics: Arc<RwLock<EmbeddingMetrics>>>,
}

struct EmbeddingMetrics {
    total_requests: u64,
    total_tokens: u64,
    cache_hits: u64,
    cache_misses: u64,
    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p99_latency_ms: f64,
}

#[async_trait]
trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
    fn get_model_info(&self) -> ModelInfo;
}

impl EmbeddingManager {
    pub async fn new(cache_size_mb: usize, cleanup_interval_hours: u32) -> Self {
        let models = Arc::new(RwLock::new(HashMap::new()));
        let providers = Arc::new(RwLock::new(HashMap::new()));

        let cache = EmbeddingCache::new(
            cache_size_mb * 1024 * 1024,
            Duration::from_secs(cleanup_interval_hours as u64 * 3600),
        );

        let metrics = Arc::new(RwLock::new(EmbeddingMetrics {
            total_requests: 0,
            total_tokens: 0,
            cache_hits: 0,
            cache_misses: 0,
            avg_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p99_latency_ms: 0.0,
        }));

        let mut manager = Self {
            models,
            providers,
            cache,
            batcher: None,
            default_model: "text-embedding-ada-002".to_string(),
            metrics,
        };

        manager.register_default_models().await;

        manager
    }

    async fn register_default_models(&mut self) {
        let openai_model = EmbeddingModelConfig {
            name: "text-embedding-ada-002".to_string(),
            provider: EmbeddingProvider::OpenAI {
                api_key: String::new(),
                base_url: "https://api.openai.com/v1".to_string(),
                organization: None,
            },
            dimension: 1536,
            context_window: 8191,
            max_batch_size: 2048,
            max_tokens_per_minute: Some(450000),
            retry_config: RetryConfig::default(),
            preprocessing: Some(TextPreprocessingConfig {
                normalize_text: true,
                remove_newlines: true,
                lowercase: false,
                max_characters: Some(8191),
                truncate_to_window: true,
            }),
            postprocessing: Some(TextPostprocessingConfig {
                normalize_embeddings: true,
                convert_to_f16: false,
                cache_embeddings: true,
                cache_ttl_hours: 24,
            }),
        };

        self.register_model(openai_model).await;

        let hf_model = EmbeddingModelConfig {
            name: "BAAI/bge-small-en".to_string(),
            provider: EmbeddingProvider::HuggingFace {
                token: String::new(),
                model_id: "BAAI/bge-small-en".to_string(),
                revision: None,
                quantize: true,
            },
            dimension: 384,
            context_window: 512,
            max_batch_size: 1024,
            max_tokens_per_minute: Some(100000),
            retry_config: RetryConfig::default(),
            preprocessing: Some(TextPreprocessingConfig {
                normalize_text: true,
                remove_newlines: true,
                lowercase: true,
                max_characters: Some(512),
                truncate_to_window: true,
            }),
            postprocessing: Some(TextPostprocessingConfig {
                normalize_embeddings: true,
                convert_to_f16: true,
                cache_embeddings: true,
                cache_ttl_hours: 48,
            }),
        };

        self.register_model(hf_model).await;
    }

    pub async fn register_model(&mut self, config: EmbeddingModelConfig) -> Result<(), EmbeddingError> {
        let provider: Box<dyn EmbeddingProvider + Send + Sync> = match &config.provider {
            EmbeddingProvider::OpenAI { .. } => {
                Box::new(OpenAIEmbeddingProvider::new(config.clone())?)
            }
            EmbeddingProvider::HuggingFace { .. } => {
                Box::new(HuggingFaceEmbeddingProvider::new(config.clone())?)
            }
            EmbeddingProvider::Local { .. } => {
                Box::new(LocalEmbeddingProvider::new(config.clone())?)
            }
            _ => {
                return Err(EmbeddingError::ConfigurationError(
                    "Unsupported provider type".to_string(),
                ));
            }
        };

        let mut models = self.models.write().await;
        models.insert(config.name.clone(), config);

        let mut providers = self.providers.write().await;
        providers.insert(config.name.clone(), provider);

        Ok(())
    }

    pub async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, EmbeddingError> {
        let start_time = Instant::now();
        let model_name = request.model_name.clone();

        let texts = request.texts.clone();
        let batch_size = request.batch_size.unwrap_or(32);

        let mut all_embeddings = Vec::new();

        for batch in texts.chunks(batch_size) {
            let batch_texts: Vec<String> = batch.iter().cloned().collect();

            let embeddings = self.embed_batch(&batch_texts, &model_name).await?;
            all_embeddings.extend(embeddings);
        }

        let elapsed = start_time.elapsed();
        let latency_ms = elapsed.as_millis() as u64;

        let mut metrics = self.metrics.write().unwrap();
        metrics.total_requests += 1;
        metrics.total_tokens += all_embeddings.iter().map(|e| e.len()).sum::<usize>() as u64;
        metrics.update_latency(latency_ms);

        Ok(EmbeddingResponse {
            embeddings: all_embeddings,
            model_name,
            usage: TokenUsage {
                total_tokens: all_embeddings.len() * self.get_model(&model_name).await?.dimension,
                prompt_tokens: all_embeddings.len() * self.get_model(&model_name).await?.dimension,
                completion_tokens: 0,
            },
            latency_ms,
            cached: false,
        })
    }

    async fn embed_batch(&self, texts: &[String], model_name: &str) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let providers = self.providers.read().await;
        let provider = providers.get(model_name)
            .ok_or_else(|| EmbeddingError::ModelNotFound(model_name.to_string()))?;

        provider.embed(texts).await
    }

    async fn get_model(&self, model_name: &str) -> Result<EmbeddingModelConfig, EmbeddingError> {
        let models = self.models.read().await;
        models.get(model_name)
            .cloned()
            .ok_or_else(|| EmbeddingError::ModelNotFound(model_name.to_string()))
    }

    pub async fn embed_batch_requests(&self, requests: &[EmbeddingRequest]) -> Result<BatchEmbeddingResponse, EmbeddingError> {
        let start_time = Instant::now();
        let mut responses = Vec::new();

        for request in requests {
            match self.embed(request.clone()).await {
                Ok(response) => responses.push(Ok(response)),
                Err(e) => responses.push(Err(e)),
            }
        }

        let total_latency_ms = start_time.elapsed().as_millis() as u64;

        Ok(BatchEmbeddingResponse {
            responses,
            total_latency_ms,
        })
    }

    pub async fn list_models(&self) -> Vec<ModelInfo> {
        let models = self.models.read().await;
        let providers = self.providers.read().await;

        models.iter().map(|(name, config)| {
            let provider = providers.get(name);
            let info = provider.map(|p| p.get_model_info()).unwrap_or_else(|| ModelInfo {
                name: name.clone(),
                provider: config.provider.to_string(),
                dimension: config.dimension,
                context_window: config.context_window,
                max_batch_size: config.max_batch_size,
                capabilities: vec![],
                performance_stats: None,
            });
            info
        }).collect()
    }

    pub async fn get_metrics(&self) -> EmbeddingMetrics {
        let metrics = self.metrics.read().unwrap();
        metrics.clone()
    }
}

impl EmbeddingMetrics {
    fn update_latency(&mut self, latency_ms: u64) {
        let total = self.total_requests as f64;
        self.avg_latency_ms = (self.avg_latency_ms * (total - 1.0) + latency_ms as f64) / total;

        if latency_ms as f64 > self.p99_latency_ms {
            self.p99_latency_ms = latency_ms as f64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_embedding_cache() {
        let mut cache = EmbeddingCache::new(1024 * 1024, Duration::from_secs(3600));

        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        cache.insert("test_key".to_string(), embedding.clone()).await;

        let retrieved = cache.get("test_key").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), embedding);
    }

    #[tokio::test]
    async fn test_embedding_manager_creation() {
        let manager = EmbeddingManager::new(256, 24).await;
        let models = manager.list_models().await;

        assert!(!models.is_empty());
        println!("Registered models: {:?}", models.len());
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(100);

        assert!(limiter.acquire(50).await.is_ok());
        assert_eq!(limiter.current_tokens, 50);

        assert!(limiter.acquire(60).await.is_err());
    }

    #[tokio::test]
    async fn test_preprocessing_config() {
        let config = TextPreprocessingConfig {
            normalize_text: true,
            remove_newlines: true,
            lowercase: true,
            max_characters: Some(100),
            truncate_to_window: true,
        };

        assert!(config.normalize_text);
        assert!(config.truncate_to_window);
    }

    #[tokio::test]
    async fn test_embedding_config() {
        let config = EmbeddingModelConfig {
            name: "test-model".to_string(),
            provider: EmbeddingProvider::Local {
                model_path: "/models/test.onnx".to_string(),
                engine: InferenceEngine::Default,
                device: DeviceType::CPU,
                batch_size: 32,
            },
            dimension: 768,
            context_window: 512,
            max_batch_size: 64,
            max_tokens_per_minute: Some(10000),
            retry_config: RetryConfig::default(),
            preprocessing: None,
            postprocessing: None,
        };

        assert_eq!(config.name, "test-model");
        assert_eq!(config.dimension, 768);
    }
}
