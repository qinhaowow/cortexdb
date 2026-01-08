use std::sync::Arc;
use std::path::Path;
use async_trait::async_trait;

#[async_trait]
pub trait ObjectStoreAdapter: Send + Sync {
    async fn put_object(&self, key: &str, data: &[u8]) -> Result<(), ObjectStoreError>;
    
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError>;
    
    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError>;
    
    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>, ObjectStoreError>;
    
    async fn exists(&self, key: &str) -> Result<bool, ObjectStoreError>;
    
    async fn get_object_size(&self, key: &str) -> Result<usize, ObjectStoreError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Authentication error: {0}")]
    AuthError(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub struct ObjectStore {
    adapter: Arc<dyn ObjectStoreAdapter>,
    bucket_name: String,
}

impl ObjectStore {
    pub fn new(adapter: Arc<dyn ObjectStoreAdapter>, bucket_name: String) -> Self {
        Self {
            adapter,
            bucket_name,
        }
    }

    pub async fn put(&self, key: &str, data: &[u8]) -> Result<(), ObjectStoreError> {
        let full_key = self.get_full_key(key);
        self.adapter.put_object(&full_key, data).await
    }

    pub async fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        let full_key = self.get_full_key(key);
        self.adapter.get_object(&full_key).await
    }

    pub async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let full_key = self.get_full_key(key);
        self.adapter.delete_object(&full_key).await
    }

    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>, ObjectStoreError> {
        let full_prefix = prefix.map(|p| self.get_full_key(p));
        let objects = self.adapter.list_objects(full_prefix.as_deref()).await?;
        
        let bucket_prefix = format!("{}/", self.bucket_name);
        Ok(objects.into_iter()
            .filter(|obj| obj.starts_with(&bucket_prefix))
            .map(|obj| obj.trim_start_matches(&bucket_prefix).to_string())
            .collect())
    }

    pub async fn exists(&self, key: &str) -> Result<bool, ObjectStoreError> {
        let full_key = self.get_full_key(key);
        self.adapter.exists(&full_key).await
    }

    pub async fn size(&self, key: &str) -> Result<usize, ObjectStoreError> {
        let full_key = self.get_full_key(key);
        self.adapter.get_object_size(&full_key).await
    }

    fn get_full_key(&self, key: &str) -> String {
        format!("{}/{}", self.bucket_name, key)
    }
}

pub struct ObjectStoreBuilder {
    adapter: Option<Arc<dyn ObjectStoreAdapter>>,
    bucket_name: Option<String>,
}

impl ObjectStoreBuilder {
    pub fn new() -> Self {
        Self {
            adapter: None,
            bucket_name: None,
        }
    }

    pub fn with_adapter(mut self, adapter: Arc<dyn ObjectStoreAdapter>) -> Self {
        self.adapter = Some(adapter);
        self
    }

    pub fn with_bucket(mut self, bucket_name: String) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn build(self) -> Result<ObjectStore, ObjectStoreError> {
        let adapter = self.adapter.ok_or_else(|| ObjectStoreError::InternalError("Adapter not set".to_string()))?;
        let bucket_name = self.bucket_name.ok_or_else(|| ObjectStoreError::InternalError("Bucket name not set".to_string()))?;
        
        Ok(ObjectStore::new(adapter, bucket_name))
    }
}
