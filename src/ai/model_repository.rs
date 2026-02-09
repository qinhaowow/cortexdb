//! Model Repository for CortexDB AI Module
//!
//! This module provides comprehensive model management including version control,
//! automatic downloads, caching, and metadata management for AI models.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use futures::stream::{self, StreamExt, TryStreamExt};
use reqwest;
use sha2::{Sha256, Digest};
use hex;

#[derive(Debug, Error)]
pub enum ModelRepositoryError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Model download failed: {0}")]
    DownloadFailed(String),

    #[error("Model validation failed: {0}")]
    ValidationFailed(String),

    #[error("Version not found: {0}")]
    VersionNotFound(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Registry error: {0}")]
    RegistryError(String),

    #[error("Cache error: {0}")]
    CacheError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    pub version: String,
    pub created_at: u64,
    pub created_by: String,
    pub description: String,
    pub file_size: u64,
    pub checksum: String,
    pub download_url: String,
    pub metadata: ModelMetadata,
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub framework: String,
    pub format: ModelFormat,
    pub quantization: Option<QuantizationType>,
    pub device_target: Vec<DeviceType>,
    pub min_memory_mb: u32,
    pub max_batch_size: usize,
    pub input_shapes: Vec<Shape>,
    pub output_shapes: Vec<Shape>,
    pub preprocessing: Option<PreprocessingConfig>,
    pub performance: Option<PerformanceProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelFormat {
    Onnx,
    TensorflowSavedModel,
    TorchScript,
    Pytorch,
    Tflite,
    Jax,
    OpenVino,
    Engine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuantizationType {
    F16,
    Int8,
    Int4,
    Dynamic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceType {
    Cpu,
    NvidiaGpu,
    AmdGpu,
    AppleSilicon,
    Tpu,
    Vulkan,
    Dml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    pub dimensions: Vec<isize>,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessingConfig {
    pub normalize_mean: Option<Vec<f32>>,
    pub normalize_std: Option<Vec<f32>>,
    pub resize_to: Option<(usize, usize)>,
    pub to_rgb: bool,
    pub tokenization: Option<TokenizationConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizationConfig {
    pub vocab_size: usize,
    pub max_length: usize,
    pub padding_strategy: String,
    pub truncation_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceProfile {
    pub avg_inference_time_ms: f32,
    pub p50_inference_time_ms: f32,
    p99_inference_time_ms: f32,
    pub throughput_per_second: f32,
    pub peak_memory_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub name: String,
    pub publisher: String,
    pub category: ModelCategory,
    pub description: String,
    pub license: String,
    pub tags: Vec<String>,
    pub versions: Vec<ModelVersion>,
    pub latest_version: String,
    pub default_version: String,
    pub total_downloads: u64,
    pub rating: f32,
    pub created_at: u64,
    pub updated_at: u64,
    pub is_public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelCategory {
    Embedding,
    Classification,
    Detection,
    Segmentation,
    Generation,
    Translation,
    Summarization,
    QuestionAnswering,
    TextToImage,
    ImageToText,
    Multimodal,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRegistryEntry {
    pub local_path: PathBuf,
    pub loaded_at: u64,
    pub access_count: u64,
    pub last_accessed: u64,
    pub memory_footprint_mb: u32,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadProgress {
    pub model_id: String,
    pub version: String,
    pub bytes_downloaded: u64,
    pub total_bytes: u64,
    pub speed_bytes_per_sec: f64,
    pub eta_seconds: u64,
    pub status: DownloadStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownloadStatus {
    Pending,
    Downloading,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRegistryConfig {
    pub models_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub max_cache_size_bytes: u64,
    pub max_models_loaded: usize,
    pub download_timeout_secs: u64,
    pub verify_checksum: bool,
    pub auto_cleanup: bool,
    pub cleanup_interval_hours: u32,
    pub retention_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelLoadStats {
    pub total_loads: u64,
    pub successful_loads: u64,
    pub failed_loads: u64,
    pub avg_load_time_ms: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

pub struct ModelRepository {
    config: ModelRegistryConfig,
    models: Arc<RwLock<HashMap<String, ModelInfo>>>,
    registry: Arc<RwLock<HashMap<String, ModelRegistryEntry>>>>,
    download_queue: Arc<Mutex<Vec<DownloadTask>>>,
    active_downloads: Arc<RwLock<HashMap<String, DownloadProgress>>>>,
    stats: Arc<RwLock<ModelLoadStats>>,
    http_client: reqwest::Client,
}

struct DownloadTask {
    model_id: String,
    version: String,
    priority: u8,
    retry_count: u32,
}

impl Default for ModelRegistryConfig {
    fn default() -> Self {
        Self {
            models_dir: PathBuf::from("./models"),
            cache_dir: PathBuf::from("./cache/models"),
            max_cache_size_bytes: 10 * 1024 * 1024 * 1024,
            max_models_loaded: 10,
            download_timeout_secs: 300,
            verify_checksum: true,
            auto_cleanup: true,
            cleanup_interval_hours: 24,
            retention_days: 30,
        }
    }
}

impl ModelRepository {
    pub async fn new(config: ModelRegistryConfig) -> Result<Self, ModelRepositoryError> {
        let models_dir = &config.models_dir;
        let cache_dir = &config.cache_dir;

        if !models_dir.exists() {
            fs::create_dir_all(models_dir)
                .await
                .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;
        }

        if !cache_dir.exists() {
            fs::create_dir_all(cache_dir)
                .await
                .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.download_timeout_secs))
            .build()
            .map_err(|e| ModelRepositoryError::ConfigurationError(e.to_string()))?;

        Ok(Self {
            config,
            models: Arc::new(RwLock::new(HashMap::new())),
            registry: Arc::new(RwLock::new(HashMap::new())),
            download_queue: Arc::new(Mutex::new(Vec::new())),
            active_downloads: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ModelLoadStats {
                total_loads: 0,
                successful_loads: 0,
                failed_loads: 0,
                avg_load_time_ms: 0.0,
                cache_hits: 0,
                cache_misses: 0,
            })),
            http_client,
        })
    }

    pub async fn register_model(&self, model: ModelInfo) -> Result<(), ModelRepositoryError> {
        let mut models = self.models.write().unwrap();

        if models.contains_key(&model.id) {
            return Err(ModelRepositoryError::RegistryError(
                format!("Model {} already registered", model.id),
            ));
        }

        models.insert(model.id.clone(), model);

        Ok(())
    }

    pub async fn get_model(&self, model_id: &str) -> Result<ModelInfo, ModelRepositoryError> {
        let models = self.models.read().unwrap();
        models.get(model_id)
            .cloned()
            .ok_or_else(|| ModelRepositoryError::ModelNotFound(model_id.to_string()))
    }

    pub async fn get_model_version(
        &self,
        model_id: &str,
        version: &str,
    ) -> Result<ModelVersion, ModelRepositoryError> {
        let model = self.get_model(model_id).await?;

        model.versions.iter()
            .find(|v| v.version == version)
            .cloned()
            .ok_or_else(|| ModelRepositoryError::VersionNotFound(version.to_string()))
    }

    pub async fn add_version(
        &self,
        model_id: &str,
        version: ModelVersion,
    ) -> Result<(), ModelRepositoryError> {
        let mut models = self.models.write().unwrap();

        if let Some(model) = models.get_mut(model_id) {
            model.versions.push(version.clone());
            model.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if version.is_default {
                for v in model.versions.iter_mut() {
                    v.is_default = false;
                }
                version.is_default = true;
            }

            Ok(())
        } else {
            Err(ModelRepositoryError::ModelNotFound(model_id.to_string()))
        }
    }

    pub async fn list_models(&self) -> Vec<ModelInfo> {
        let models = self.models.read().unwrap();
        models.values().cloned().collect()
    }

    pub async fn search_models(
        &self,
        query: &str,
        category: Option<ModelCategory>,
        tags: Option<&[String]>,
    ) -> Vec<ModelInfo> {
        let models = self.models.read().unwrap();
        let query_lower = query.to_lowercase();

        models.values()
            .filter(|model| {
                let name_match = model.name.to_lowercase().contains(&query_lower)
                    || model.description.to_lowercase().contains(&query_lower);

                let category_match = match category {
                    Some(cat) => model.category == cat,
                    None => true,
                };

                let tag_match = match tags {
                    Some(required_tags) => required_tags.iter().all(|tag| {
                        model.tags.iter().any(|t| t.to_lowercase() == tag.to_lowercase())
                    }),
                    None => true,
                });

                name_match && category_match && tag_match
            })
            .cloned()
            .collect()
    }

    pub async fn download_model(
        &self,
        model_id: &str,
        version: &str,
        progress_callback: impl Fn(DownloadProgress),
    ) -> Result<PathBuf, ModelRepositoryError> {
        let model = self.get_model(model_id).await?;
        let model_version = self.get_model_version(model_id, version).await?;

        let model_dir = self.config.models_dir.join(model_id).join(version);
        if model_dir.exists() {
            self.update_registry_access(model_id).await?;
            return Ok(model_dir);
        }

        fs::create_dir_all(&model_dir)
            .await
            .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;

        let download_url = &model_version.download_url;
        let target_file = model_dir.join(format!("model{}", self.get_extension(&model_version.metadata.format)));

        self.update_download_progress(model_id, version, DownloadProgress {
            model_id: model_id.to_string(),
            version: version.to_string(),
            bytes_downloaded: 0,
            total_bytes: model_version.file_size,
            speed_bytes_per_sec: 0.0,
            eta_seconds: 0,
            status: DownloadStatus::Downloading,
        }).await;

        let response = self.http_client
            .get(download_url)
            .send()
            .await
            .map_err(|e| ModelRepositoryError::DownloadFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ModelRepositoryError::DownloadFailed(
                format!("HTTP {}", response.status()),
            ));
        }

        let total_size = response.content_length().unwrap_or(0);
        let mut downloaded: u64 = 0;
        let mut last_update = Instant::now();
        let mut file = BufWriter::new(
            File::create(&target_file)
                .await
                .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?,
        );

        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let start_time = Instant::now();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|e| ModelRepositoryError::DownloadFailed(e.to_string()))?;

            file.write_all(&chunk)
                .await
                .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;

            downloaded += chunk.len() as u64;
            hasher.update(&chunk);

            if last_update.elapsed() >= Duration::from_millis(500) {
                let elapsed = start_time.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 { downloaded as f64 / elapsed } else { 0.0 };
                let eta = if speed > 0.0 { ((total_size - downloaded) as f64 / speed) as u64 } else { 0 };

                let progress = DownloadProgress {
                    model_id: model_id.to_string(),
                    version: version.to_string(),
                    bytes_downloaded: downloaded,
                    total_bytes: total_size,
                    speed_bytes_per_sec: speed,
                    eta_seconds: eta,
                    status: DownloadStatus::Downloading,
                };

                progress_callback(progress.clone());
                self.update_download_progress(model_id, version, progress).await;

                last_update = Instant::now();
            }
        }

        file.flush()
            .await
            .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;

        if self.config.verify_checksum {
            let checksum = hex::encode(hasher.finalize());

            if checksum != model_version.checksum {
                return Err(ModelRepositoryError::ValidationFailed(
                    format!("Checksum mismatch: expected {}, got {}", model_version.checksum, checksum),
                ));
            }
        }

        let final_progress = DownloadProgress {
            model_id: model_id.to_string(),
            version: version.to_string(),
            bytes_downloaded: downloaded,
            total_bytes: total_size,
            speed_bytes_per_sec: 0.0,
            eta_seconds: 0,
            status: DownloadStatus::Completed,
        };

        progress_callback(final_progress.clone());
        self.update_download_progress(model_id, version, final_progress).await;

        self.register_downloaded_model(model_id, version, &model_dir).await?;

        Ok(model_dir)
    }

    async fn update_download_progress(&self, model_id: &str, version: &str, progress: DownloadProgress) {
        let mut downloads = self.active_downloads.write().unwrap();
        downloads.insert(format!("{}:{}", model_id, version), progress);
    }

    async fn register_downloaded_model(
        &self,
        model_id: &str,
        version: &str,
        path: &Path,
    ) -> Result<(), ModelRepositoryError> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| ModelRepositoryError::StorageError(e.to_string()))?;

        let entry = ModelRegistryEntry {
            local_path: path.to_path_buf(),
            loaded_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            access_count: 0,
            last_accessed: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            memory_footprint_mb: (metadata.len() / (1024 * 1024)) as u32,
            is_active: true,
        };

        let mut registry = self.registry.write().unwrap();
        registry.insert(format!("{}:{}", model_id, version), entry);

        Ok(())
    }

    async fn update_registry_access(&self, model_id: &str) -> Result<(), ModelRepositoryError> {
        let mut registry = self.registry.write().unwrap();

        if let Some(entry) = registry.get_mut(model_id) {
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        }

        Ok(())
    }

    pub async fn load_model(&self, model_id: &str, version: &str) -> Result<bool, ModelRepositoryError> {
        let mut stats = self.stats.write().unwrap();
        stats.total_loads += 1;
        stats.cache_misses += 1;

        let registry = self.registry.read().unwrap();
        let key = format!("{}:{}", model_id, version);

        if let Some(entry) = registry.get(&key) {
            if entry.is_active {
                let mut stats = self.stats.write().unwrap();
                stats.cache_hits += 1;
                stats.successful_loads += 1;
                return Ok(true);
            }
        }

        drop(registry);

        let start_time = Instant::now();

        let model_dir = self.config.models_dir.join(model_id).join(version);
        if !model_dir.exists() {
            return Err(ModelRepositoryError::ModelNotFound(format!("{}:{}", model_id, version)));
        }

        let load_time = start_time.elapsed().as_secs_f64();
        let mut stats = self.stats.write().unwrap();
        stats.successful_loads += 1;
        let total = stats.successful_loads as f64;
        stats.avg_load_time_ms = (stats.avg_load_time_ms * (total - 1.0) + load_time * 1000.0) / total;

        self.activate_model(model_id, version).await?;

        Ok(true)
    }

    async fn activate_model(&self, model_id: &str, version: &str) -> Result<(), ModelRepositoryError> {
        let mut registry = self.registry.write().unwrap();
        let key = format!("{}:{}", model_id, version);

        if let Some(entry) = registry.get_mut(&key) {
            entry.is_active = true;
            entry.loaded_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        }

        Ok(())
    }

    pub async fn unload_model(&self, model_id: &str, version: &str) -> Result<(), ModelRepositoryError> {
        let mut registry = self.registry.write().unwrap();
        let key = format!("{}:{}", model_id, version);

        if let Some(entry) = registry.get_mut(&key) {
            entry.is_active = false;
        }

        Ok(())
    }

    pub async fn list_loaded_models(&self) -> Vec<(String, String, u32)> {
        let registry = self.registry.read().unwrap();

        registry.iter()
            .filter(|(_, entry)| entry.is_active)
            .map(|(key, entry)| {
                let parts: Vec<&str> = key.split(':').collect();
                (
                    parts[0].to_string(),
                    parts[1].to_string(),
                    entry.memory_footprint_mb,
                )
            })
            .collect()
    }

    pub async fn cleanup_cache(&self) -> Result<u64, ModelRepositoryError> {
        let mut registry = self.registry.write().unwrap();
        let mut removed_size: u64 = 0;

        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (self.config.retention_days as u64 * 24 * 60 * 60);

        let keys_to_remove: Vec<String> = registry.iter()
            .filter(|(_, entry)| {
                !entry.is_active && entry.last_accessed < cutoff
            })
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = registry.remove(&key) {
                let _ = tokio::fs::remove_dir_all(&entry.local_path).await;
                removed_size += entry.memory_footprint_mb as u64 * 1024 * 1024;
            }
        }

        Ok(removed_size)
    }

    pub async fn get_cache_size(&self) -> Result<u64, ModelRepositoryError> {
        let mut total_size: u64 = 0;

        let mut dir = tokio::fs::read_dir(&self.config.cache_dir)
            .await
            .map_err(|e| ModelRepositoryError::CacheError(e.to_string()))?;

        while let Some(entry) = dir.next_entry()
            .await
            .map_err(|e| ModelRepositoryError::CacheError(e.to_string()))?
        {
            if entry.file_type().await.unwrap().is_file() {
                total_size += entry.metadata()
                    .await
                    .map_err(|e| ModelRepositoryError::CacheError(e.to_string()))?
                    .len();
            }
        }

        Ok(total_size)
    }

    pub async fn get_statistics(&self) -> ModelLoadStats {
        let stats = self.stats.read().unwrap();
        stats.clone()
    }

    pub async fn get_model_path(&self, model_id: &str, version: &str) -> Result<PathBuf, ModelRepositoryError> {
        let registry = self.registry.read().unwrap();
        let key = format!("{}:{}", model_id, version);

        registry.get(&key)
            .map(|e| e.local_path.clone())
            .ok_or_else(|| ModelRepositoryError::ModelNotFound(format!("{}:{}", model_id, version)))
    }

    fn get_extension(&self, format: &ModelFormat) -> &str {
        match format {
            ModelFormat::Onnx => ".onnx",
            ModelFormat::TensorflowSavedModel => "",
            ModelFormat::TorchScript => ".pt",
            ModelFormat::Pytorch => ".pt",
            ModelFormat::Tflite => ".tflite",
            ModelFormat::Jax => ".pb",
            ModelFormat::OpenVino => ".xml",
            ModelFormat::Engine => ".engine",
        }
    }

    pub async fn create_default_models(&self) {
        let embedding_models = vec![
            ModelInfo {
                id: "text-embedding-ada-002".to_string(),
                name: "text-embedding-ada-002".to_string(),
                publisher: "OpenAI".to_string(),
                category: ModelCategory::Embedding,
                description: "OpenAI's second generation text embedding model".to_string(),
                license: "proprietary".to_string(),
                tags: vec!["embedding", "text", "openai".to_string()],
                versions: vec![ModelVersion {
                    version: "1".to_string(),
                    created_at: 1672531200,
                    created_by: "system".to_string(),
                    description: "Initial version".to_string(),
                    file_size: 300_000_000,
                    checksum: "sha256:abc123".to_string(),
                    download_url: "https://models.example.com/text-embedding-ada-002".to_string(),
                    metadata: ModelMetadata {
                        framework: "ONNX".to_string(),
                        format: ModelFormat::Onnx,
                        quantization: Some(QuantizationType::F16),
                        device_target: vec![DeviceType::Cpu, DeviceType::NvidiaGpu],
                        min_memory_mb: 512,
                        max_batch_size: 2048,
                        input_shapes: vec![Shape {
                            dimensions: vec![-1, -1],
                            data_type: "int64".to_string(),
                        }],
                        output_shapes: vec![Shape {
                            dimensions: vec![-1, 1536],
                            data_type: "float32".to_string(),
                        }],
                        preprocessing: Some(PreprocessingConfig {
                            normalize_mean: None,
                            normalize_std: None,
                            resize_to: None,
                            to_rgb: false,
                            tokenization: Some(TokenizationConfig {
                                vocab_size: 50257,
                                max_length: 8191,
                                padding_strategy: "right".to_string(),
                                truncation_strategy: "longest_first".to_string(),
                            }),
                        }),
                        performance: Some(PerformanceProfile {
                            avg_inference_time_ms: 5.0,
                            p50_inference_time_ms: 4.5,
                            p99_inference_time_ms: 15.0,
                            throughput_per_second: 200.0,
                            peak_memory_mb: 1024,
                        }),
                    },
                    is_default: true,
                }],
                latest_version: "1".to_string(),
                default_version: "1".to_string(),
                total_downloads: 1000000,
                rating: 4.8,
                created_at: 1672531200,
                updated_at: 1672531200,
                is_public: true,
            },
            ModelInfo {
                id: "BAAI/bge-small-en".to_string(),
                name: "BAAI/bge-small-en".to_string(),
                publisher: "Beijing Academy of AI".to_string(),
                category: ModelCategory::Embedding,
                description: "BGE small English embedding model".to_string(),
                license: "mit".to_string(),
                tags: vec!["embedding", "text", "bge".to_string()],
                versions: vec![ModelVersion {
                    version: "1".to_string(),
                    created_at: 1672531200,
                    created_by: "system".to_string(),
                    description: "Initial version".to_string(),
                    file_size: 90_000_000,
                    checksum: "sha256:def456".to_string(),
                    download_url: "https://models.example.com/BAAI/bge-small-en".to_string(),
                    metadata: ModelMetadata {
                        framework: "ONNX".to_string(),
                        format: ModelFormat::Onnx,
                        quantization: Some(QuantizationType::Int8),
                        device_target: vec![DeviceType::Cpu],
                        min_memory_mb: 256,
                        max_batch_size: 1024,
                        input_shapes: vec![Shape {
                            dimensions: vec![-1, -1],
                            data_type: "int64".to_string(),
                        }],
                        output_shapes: vec![Shape {
                            dimensions: vec![-1, 512],
                            data_type: "float32".to_string(),
                        }],
                        preprocessing: Some(PreprocessingConfig {
                            normalize_mean: None,
                            normalize_std: None,
                            resize_to: None,
                            to_rgb: false,
                            tokenization: Some(TokenizationConfig {
                                vocab_size: 21128,
                                max_length: 512,
                                padding_strategy: "right".to_string(),
                                truncation_strategy: "longest_first".to_string(),
                            }),
                        }),
                        performance: Some(PerformanceProfile {
                            avg_inference_time_ms: 2.0,
                            p50_inference_time_ms: 1.8,
                            p99_inference_time_ms: 5.0,
                            throughput_per_second: 500.0,
                            peak_memory_mb: 512,
                        }),
                    },
                    is_default: true,
                }],
                latest_version: "1".to_string(),
                default_version: "1".to_string(),
                total_downloads: 500000,
                rating: 4.6,
                created_at: 1672531200,
                updated_at: 1672531200,
                is_public: true,
            },
        ];

        for model in embedding_models {
            let _ = self.register_model(model).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_model_repository_creation() {
        let config = ModelRegistryConfig::default();
        let repo = ModelRepository::new(config).await;

        assert!(repo.is_ok());
    }

    #[tokio::test]
    async fn test_register_model() {
        let config = ModelRegistryConfig::default();
        let repo = ModelRepository::new(config).await.unwrap();

        let model_info = ModelInfo {
            id: "test-model".to_string(),
            name: "Test Model".to_string(),
            publisher: "Test".to_string(),
            category: ModelCategory::Embedding,
            description: "A test model".to_string(),
            license: "mit".to_string(),
            tags: vec!["test".to_string()],
            versions: vec![],
            latest_version: "1".to_string(),
            default_version: "1".to_string(),
            total_downloads: 0,
            rating: 0.0,
            created_at: 1672531200,
            updated_at: 1672531200,
            is_public: true,
        };

        let result = repo.register_model(model_info).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_model() {
        let config = ModelRegistryConfig::default();
        let repo = ModelRepository::new(config).await.unwrap();

        let model_info = ModelInfo {
            id: "get-test-model".to_string(),
            name: "Get Test Model".to_string(),
            publisher: "Test".to_string(),
            category: ModelCategory::Embedding,
            description: "A test model for get".to_string(),
            license: "mit".to_string(),
            tags: vec!["test".to_string()],
            versions: vec![],
            latest_version: "1".to_string(),
            default_version: "1".to_string(),
            total_downloads: 0,
            rating: 0.0,
            created_at: 1672531200,
            updated_at: 1672531200,
            is_public: true,
        };

        repo.register_model(model_info).await.unwrap();

        let retrieved = repo.get_model("get-test-model").await;
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap().name, "Get Test Model");
    }

    #[tokio::test]
    async fn test_search_models() {
        let config = ModelRegistryConfig::default();
        let repo = ModelRepository::new(config).await.unwrap();

        let models = vec![
            ModelInfo {
                id: "search-test-1".to_string(),
                name: "Searchable Model 1".to_string(),
                publisher: "Test".to_string(),
                category: ModelCategory::Embedding,
                description: "First searchable model".to_string(),
                license: "mit".to_string(),
                tags: vec!["embedding".to_string()],
                versions: vec![],
                latest_version: "1".to_string(),
                default_version: "1".to_string(),
                total_downloads: 0,
                rating: 0.0,
                created_at: 1672531200,
                updated_at: 1672531200,
                is_public: true,
            },
            ModelInfo {
                id: "search-test-2".to_string(),
                name: "Searchable Model 2".to_string(),
                publisher: "Test".to_string(),
                category: ModelCategory::Classification,
                description: "Second searchable model".to_string(),
                license: "mit".to_string(),
                tags: vec!["classification".to_string()],
                versions: vec![],
                latest_version: "1".to_string(),
                default_version: "1".to_string(),
                total_downloads: 0,
                rating: 0.0,
                created_at: 1672531200,
                updated_at: 1672531200,
                is_public: true,
            },
        ];

        for model in models {
            repo.register_model(model).await.unwrap();
        }

        let results = repo.search_models("searchable", None, None).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_model_statistics() {
        let config = ModelRegistryConfig::default();
        let repo = ModelRepository::new(config).await.unwrap();

        let stats = repo.get_statistics().await;
        assert_eq!(stats.total_loads, 0);
        assert_eq!(stats.successful_loads, 0);
    }
}
