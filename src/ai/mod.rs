pub mod embedding;
pub mod model_repository;
pub mod autotune;
pub mod intelligent_features;

use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AIManager {
    inner: Arc<RwLock<AIManagerInner>>,
}

pub struct AIManagerInner {
    embedding_manager: embedding::EmbeddingManager,
    model_repository: model_repository::ModelRepository,
    autotune: autotune::AutoTune,
    intelligent_features: intelligent_features::IntelligentFeatures,
}

impl AIManagerInner {
    pub async fn new() -> Self {
        Self {
            embedding_manager: embedding::EmbeddingManager::new().await,
            model_repository: model_repository::ModelRepository::new().await,
            autotune: autotune::AutoTune::new().await,
            intelligent_features: intelligent_features::IntelligentFeatures::new().await,
        }
    }
}

impl AIManager {
    pub async fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(AIManagerInner::new().await)),
        }
    }

    pub async fn get_embedding_manager(&self) -> Arc<RwLock<embedding::EmbeddingManager>> {
        let inner = self.inner.read().await;
        Arc::new(RwLock::new(inner.embedding_manager.clone()))
    }

    pub async fn get_model_repository(&self) -> Arc<RwLock<model_repository::ModelRepository>> {
        let inner = self.inner.read().await;
        Arc::new(RwLock::new(inner.model_repository.clone()))
    }

    pub async fn get_autotune(&self) -> Arc<RwLock<autotune::AutoTune>> {
        let inner = self.inner.read().await;
        Arc::new(RwLock::new(inner.autotune.clone()))
    }

    pub async fn get_intelligent_features(&self) -> Arc<RwLock<intelligent_features::IntelligentFeatures>> {
        let inner = self.inner.read().await;
        Arc::new(RwLock::new(inner.intelligent_features.clone()))
    }
}