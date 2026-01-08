use std::sync::{Arc, RwLock};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Catalog {
    collections: Arc<RwLock<HashMap<String, CollectionMetadata>>>,
    indexes: Arc<RwLock<HashMap<String, IndexMetadata>>>,
    functions: Arc<RwLock<HashMap<String, FunctionMetadata>>>,
}

#[derive(Debug, Clone)]
pub struct CollectionMetadata {
    name: String,
    schema: Schema,
    storage_params: StorageParams,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<Field>,
    primary_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Field {
    name: String,
    field_type: FieldType,
    nullable: bool,
    default_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Vector { dimension: usize },
    Object,
    Array(Box<FieldType>),
    Timestamp,
}

#[derive(Debug, Clone)]
pub struct StorageParams {
    engine: String,
    compression: Option<String>,
    encryption: Option<String>,
    other_params: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct IndexMetadata {
    name: String,
    collection_name: String,
    index_type: IndexType,
    fields: Vec<String>,
    params: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    Vector { algorithm: String },
    Scalar { algorithm: String },
    FullText,
    Graph,
}

#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    name: String,
    function_type: FunctionType,
    signature: String,
    implementation: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum FunctionType {
    UDF,
    UDAF,
    UDTF,
    Aggregate,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_collection(&self, metadata: CollectionMetadata) -> Result<(), CatalogError> {
        let mut collections = self.collections.write().unwrap();
        if collections.contains_key(&metadata.name) {
            return Err(CatalogError::CollectionAlreadyExists(metadata.name));
        }
        collections.insert(metadata.name.clone(), metadata);
        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> Option<CollectionMetadata> {
        let collections = self.collections.read().unwrap();
        collections.get(name).cloned()
    }

    pub fn list_collections(&self) -> Vec<CollectionMetadata> {
        let collections = self.collections.read().unwrap();
        collections.values().cloned().collect()
    }

    pub fn drop_collection(&self, name: &str) -> Result<(), CatalogError> {
        let mut collections = self.collections.write().unwrap();
        if !collections.contains_key(name) {
            return Err(CatalogError::CollectionNotFound(name.to_string()));
        }
        collections.remove(name);
        
        let mut indexes = self.indexes.write().unwrap();
        indexes.retain(|_, idx| idx.collection_name != name);
        
        Ok(())
    }

    pub fn create_index(&self, metadata: IndexMetadata) -> Result<(), CatalogError> {
        let collections = self.collections.read().unwrap();
        if !collections.contains_key(&metadata.collection_name) {
            return Err(CatalogError::CollectionNotFound(metadata.collection_name.clone()));
        }

        let mut indexes = self.indexes.write().unwrap();
        if indexes.contains_key(&metadata.name) {
            return Err(CatalogError::IndexAlreadyExists(metadata.name));
        }
        indexes.insert(metadata.name.clone(), metadata);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<IndexMetadata> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name).cloned()
    }

    pub fn list_indexes(&self, collection_name: Option<&str>) -> Vec<IndexMetadata> {
        let indexes = self.indexes.read().unwrap();
        match collection_name {
            Some(name) => indexes.values()
                .filter(|idx| idx.collection_name == name)
                .cloned()
                .collect(),
            None => indexes.values().cloned().collect(),
        }
    }

    pub fn drop_index(&self, name: &str) -> Result<(), CatalogError> {
        let mut indexes = self.indexes.write().unwrap();
        if !indexes.contains_key(name) {
            return Err(CatalogError::IndexNotFound(name.to_string()));
        }
        indexes.remove(name);
        Ok(())
    }

    pub fn create_function(&self, metadata: FunctionMetadata) -> Result<(), CatalogError> {
        let mut functions = self.functions.write().unwrap();
        if functions.contains_key(&metadata.name) {
            return Err(CatalogError::FunctionAlreadyExists(metadata.name));
        }
        functions.insert(metadata.name.clone(), metadata);
        Ok(())
    }

    pub fn get_function(&self, name: &str) -> Option<FunctionMetadata> {
        let functions = self.functions.read().unwrap();
        functions.get(name).cloned()
    }

    pub fn list_functions(&self) -> Vec<FunctionMetadata> {
        let functions = self.functions.read().unwrap();
        functions.values().cloned().collect()
    }

    pub fn drop_function(&self, name: &str) -> Result<(), CatalogError> {
        let mut functions = self.functions.write().unwrap();
        if !functions.contains_key(name) {
            return Err(CatalogError::FunctionNotFound(name.to_string()));
        }
        functions.remove(name);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Function already exists: {0}")]
    FunctionAlreadyExists(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
