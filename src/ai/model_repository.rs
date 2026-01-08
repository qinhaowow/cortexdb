use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum ModelType {
    Embedding,
    LLM,
    Vision,
    Multimodal,
}

#[derive(Debug, Clone)]
pub struct ModelMetadata {
    name: String,
    version: String,
    model_type: ModelType,
    provider: String,
    size_bytes: Option<u64>,
    description: Option<String>,
    tags: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct ModelArtifact {
    path: String,
    metadata: ModelMetadata,
    is_local: bool,
    is_cached: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ModelRepositoryError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Invalid model: {0}")]
    InvalidModel(String),
    #[error("Download failed: {0}")]
    DownloadFailed(String),
}

pub struct ModelRepository {
    models: Arc<RwLock<HashMap<String, VecDeque<ModelArtifact>>>>,
    storage_path: String,
}

impl ModelRepository {
    pub async fn new() -> Self {
        let storage_path = "./models".to_string();
        
        // Create storage directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&storage_path) {
            eprintln!("Warning: Failed to create model storage directory: {}", e);
        }
        
        Self {
            models: Arc::new(RwLock::new(HashMap::new())),
            storage_path,
        }
    }

    pub async fn add_model(&self, artifact: ModelArtifact) -> Result<(), ModelRepositoryError> {
        let mut models = self.models.write().await;
        let model_queue = models.entry(artifact.metadata.name.clone()).or_insert_with(VecDeque::new);
        
        // Add to the front (most recent version first)
        model_queue.push_front(artifact);
        
        Ok(())
    }

    pub async fn get_model(&self, name: &str, version: Option<&str>) -> Result<ModelArtifact, ModelRepositoryError> {
        let models = self.models.read().await;
        if let Some(model_queue) = models.get(name) {
            if let Some(version) = version {
                // Find specific version
                for artifact in model_queue {
                    if artifact.metadata.version == version {
                        return Ok(artifact.clone());
                    }
                }
                Err(ModelRepositoryError::ModelNotFound(format!("Model {} version {} not found", name, version)))
            } else {
                // Return latest version
                model_queue.front().cloned().ok_or_else(|| {
                    ModelRepositoryError::ModelNotFound(name.to_string())
                })
            }
        } else {
            Err(ModelRepositoryError::ModelNotFound(name.to_string()))
        }
    }

    pub async fn list_models(&self, model_type: Option<ModelType>) -> Vec<ModelMetadata> {
        let models = self.models.read().await;
        let mut result = Vec::new();
        
        for model_queue in models.values() {
            if let Some(artifact) = model_queue.front() {
                if let Some(ref requested_type) = model_type {
                    if artifact.metadata.model_type == *requested_type {
                        result.push(artifact.metadata.clone());
                    }
                } else {
                    result.push(artifact.metadata.clone());
                }
            }
        }
        
        result
    }

    pub async fn download_model(&self, url: &str, name: &str, version: &str, model_type: ModelType) -> Result<ModelArtifact, ModelRepositoryError> {
        // Mock implementation for now
        let metadata = ModelMetadata {
            name: name.to_string(),
            version: version.to_string(),
            model_type,
            provider: "remote".to_string(),
            size_bytes: None,
            description: Some(format!("Downloaded from {}", url)),
            tags: vec!["downloaded".to_string()],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        
        let artifact = ModelArtifact {
            path: format!("{}/{}_{}", self.storage_path, name, version),
            metadata,
            is_local: true,
            is_cached: true,
        };
        
        self.add_model(artifact.clone()).await?;
        Ok(artifact)
    }

    pub async fn delete_model(&self, name: &str, version: Option<&str>) -> Result<(), ModelRepositoryError> {
        let mut models = self.models.write().await;
        if let Some(model_queue) = models.get_mut(name) {
            if let Some(version) = version {
                // Delete specific version
                let mut found = false;
                model_queue.retain(|artifact| {
                    if artifact.metadata.version == version {
                        found = true;
                        // Delete from filesystem
                        if let Err(e) = std::fs::remove_file(&artifact.path) {
                            eprintln!("Warning: Failed to delete model file: {}", e);
                        }
                        false
                    } else {
                        true
                    }
                });
                
                if !found {
                    return Err(ModelRepositoryError::ModelNotFound(format!("Model {} version {} not found", name, version)));
                }
            } else {
                // Delete all versions
                for artifact in model_queue {
                    if let Err(e) = std::fs::remove_file(&artifact.path) {
                        eprintln!("Warning: Failed to delete model file: {}", e);
                    }
                }
                model_queue.clear();
            }
            
            // Remove the entry if empty
            if model_queue.is_empty() {
                models.remove(name);
            }
            
            Ok(())
        } else {
            Err(ModelRepositoryError::ModelNotFound(name.to_string()))
        }
    }

    pub async fn cache_model(&self, artifact: &ModelArtifact) -> Result<(), ModelRepositoryError> {
        // Implementation would involve copying to cache directory
        Ok(())
    }
}