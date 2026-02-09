//! Tantivy Full-Text Search Integration for CortexDB
//!
//! This module provides a high-performance full-text search implementation
//! using the Tantivy search engine library.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tantivy::collector::{Collector, TopDocs};
use tantivy::query::{Query, QueryParser, BooleanQuery, Occur, TermQuery};
use tantivy::schema::{Field, IndexableTextField, NumericOptions, Schema, Term, TextFieldIndexing, TextOptions, STORED, STRING};
use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TantivyFullTextError {
    #[error("Index creation failed: {0}")]
    IndexCreationFailed(String),

    #[error("Document indexing failed: {0}")]
    IndexingFailed(String),

    #[error("Search failed: {0}")]
    SearchFailed(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Configuration for the Tantivy full-text index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivyFullTextConfig {
    /// Path to store the index
    pub index_path: String,
    /// Maximum memory usage for indexing (in bytes)
    pub max_memory_usage: usize,
    /// Number of threads for indexing
    pub num_threads: usize,
    /// Schema fields configuration
    pub fields: Vec<FieldConfig>,
    /// Default search field
    pub default_search_field: String,
    /// Enable fuzzy search
    pub enable_fuzzy: bool,
    /// Enable phrase search
    pub enable_phrase: bool,
    /// Minimum word length for indexing
    pub min_word_length: usize,
    /// Stop words to filter out
    pub stop_words: Vec<String>,
}

impl Default for TantivyFullTextConfig {
    fn default() -> Self {
        Self {
            index_path: "./fulltext_index".to_string(),
            max_memory_usage: 256_000_000,
            num_threads: 4,
            fields: vec![
                FieldConfig {
                    name: "content".to_string(),
                    field_type: FieldType::Text(TextFieldConfig {
                        index: true,
                        stored: true,
                        tokenize: true,
                        fast: false,
                        analyzer: "en".to_string(),
                    }),
                },
                FieldConfig {
                    name: "title".to_string(),
                    field_type: FieldType::Text(TextFieldConfig {
                        index: true,
                        stored: true,
                        tokenize: true,
                        fast: false,
                        analyzer: "en".to_string(),
                    }),
                },
                FieldConfig {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                },
            ],
            default_search_field: "content".to_string(),
            enable_fuzzy: true,
            enable_phrase: true,
            min_word_length: 2,
            stop_words: vec![
                "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
                "of", "with", "by", "from", "up", "about", "into", "through", "during",
                "before", "after", "above", "below", "between", "under", "again", "further",
            ].into_iter().map(String::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    Text(TextFieldConfig),
    String,
    Numeric(NumericFieldConfig),
    Date,
    Boolean,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextFieldConfig {
    pub index: bool,
    pub stored: bool,
    pub tokenize: bool,
    pub fast: bool,
    pub analyzer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericFieldConfig {
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
}

/// Document representation for full-text indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextDocument {
    pub id: String,
    pub title: Option<String>,
    pub content: Option<String>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub boost: f32,
}

impl Default for FullTextDocument {
    fn default() -> Self {
        Self {
            id: String::new(),
            title: None,
            content: None,
            metadata: HashMap::new(),
            boost: 1.0,
        }
    }
}

/// Search result from full-text query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextSearchResult {
    pub document_id: String,
    pub score: f32,
    pub title: Option<String>,
    pub snippet: Option<String>,
    pub highlights: Vec<String>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Search parameters for full-text queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextSearchParams {
    pub query: String,
    pub fields: Vec<String>,
    pub filter: Option<HashMap<String, serde_json::Value>>,
    pub limit: usize,
    pub offset: usize,
    pub boost: HashMap<String, f32>,
    pub fuzzy: bool,
    pub phrase: bool,
    pub highlight: bool,
    pub highlight_pre_tag: String,
    pub highlight_post_tag: String,
}

impl Default for FullTextSearchParams {
    fn default() -> Self {
        Self {
            query: String::new(),
            fields: vec!["content".to_string(), "title".to_string()],
            filter: None,
            limit: 10,
            offset: 0,
            boost: HashMap::new(),
            fuzzy: true,
            phrase: true,
            highlight: true,
            highlight_pre_tag: "<em>".to_string(),
            highlight_post_tag: "</em>".to_string(),
        }
    }
}

/// High-performance full-text search engine using Tantivy
pub struct TantivyFullTextIndex {
    config: TantivyFullTextConfig,
    index: Arc<Index>,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    fields: HashMap<String, Field>,
    query_parser: Arc<RwLock<QueryParser>>,
}

impl TantivyFullTextIndex {
    /// Create a new Tantivy full-text index with the given configuration
    pub fn new(config: TantivyFullTextConfig) -> Result<Self, TantivyFullTextError> {
        let index_path = Path::new(&config.index_path);
        
        // Create index with directory
        let directory = tantivy::directory::MmapDirectory::open(index_path)
            .map_err(|e| TantivyFullTextError::IndexCreationFailed(e.to_string()))?;
        
        // Build schema based on configuration
        let mut schema_builder = Schema::builder();
        let mut fields = HashMap::new();
        
        for field_config in &config.fields {
            let field = match &field_config.field_type {
                FieldType::Text(text_config) => {
                    let mut text_options = TextOptions::default();
                    
                    if text_config.index {
                        let mut indexing = TextFieldIndexing::default();
                        indexing.set_tokenizer(&text_config.analyzer);
                        indexing.set_record(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                        text_options = text_options.set_indexing_options(indexing);
                    }
                    
                    if text_config.stored {
                        text_options = text_options.set_stored();
                    }
                    
                    if text_config.fast {
                        text_options = text_options.set_fast();
                    }
                    
                    schema_builder.add_text_field(&field_config.name, text_options)
                },
                FieldType::String => {
                    schema_builder.add_text_field(&field_config.name, STRING | STORED)
                },
                FieldType::Numeric(num_config) => {
                    let mut options = NumericOptions::default();
                    if num_config.indexed {
                        options = options.set_indexed();
                    }
                    if num_config.stored {
                        options = options.set_stored();
                    }
                    if num_config.fast {
                        options = options.set_fast();
                    }
                    schema_builder.add_u64_field(&field_config.name, options)
                },
                FieldType::Date => {
                    schema_builder.add_date_field(&field_config.name, STRING | STORED)
                },
                FieldType::Boolean => {
                    schema_builder.add_bool_field(&field_config.name, STRING | STORED)
                },
            };
            
            fields.insert(field_config.name.clone(), field);
        }
        
        let schema = schema_builder.build();
        
        // Create or open index
        let index = Index::open(directory)
            .map_err(|e| TantivyFullTextError::IndexCreationFailed(e.to_string()))?;
        
        // Configure index settings
        index.settings()
            .set_memory_ratio(0.5)
            .set_max_memory_usage(config.max_memory_usage)
            .set_num_threads(config.num_threads);
        
        // Create index writer
        let writer = index.writer(config.max_memory_usage)
            .map_err(|e| TantivyFullTextError::IndexCreationFailed(e.to_string()))?;
        
        // Create query parser
        let mut query_parser_builder = QueryParser::builder();
        
        // Add default search fields
        for field_name in &config.fields {
            if let Some(&field) = fields.get(&field_name.name) {
                query_parser_builder = query_parser_builder.add_field(field);
            }
        }
        
        // Configure query parser
        query_parser_builder = query_parser_builder
            .set_fieldboost("title".to_string(), 2.0)
            .set_fieldboost("content".to_string(), 1.0)
            .set_phrase_boost_weight(if config.enable_phrase { 1.0 } else { 0.0 })
            .set_fuzzy_terms_per_position(2)
            .set_fuzzy_max_distance(2);
        
        let query_parser = query_parser_builder.build();
        
        Ok(Self {
            config,
            index: Arc::new(index),
            writer: Arc::new(RwLock::new(writer)),
            schema,
            fields,
            query_parser: Arc::new(RwLock::new(query_parser)),
        })
    }
    
    /// Index a single document
    pub fn add_document(&self, doc: &FullTextDocument) -> Result<(), TantivyFullTextError> {
        let mut tantivy_doc = TantivyDocument::new();
        
        // Add ID field
        if let Some(&field) = self.fields.get("id") {
            tantivy_doc.add_text(field, &doc.id);
        }
        
        // Add title field
        if let (Some(title), Some(&field)) = (&doc.title, self.fields.get("title")) {
            tantivy_doc.add_text(field, title);
        }
        
        // Add content field
        if let (Some(content), Some(&field)) = (&doc.content, self.fields.get("content")) {
            tantivy_doc.add_text(field, content);
        }
        
        // Add metadata fields
        for (key, value) in &doc.metadata {
            if let Some(&field) = self.fields.get(key) {
                match value {
                    serde_json::Value::String(s) => {
                        tantivy_doc.add_text(field, s);
                    },
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            tantivy_doc.add_u64(field, i as u64);
                        }
                    },
                    serde_json::Value::Bool(b) => {
                        tantivy_doc.add_bool(field, *b);
                    },
                    _ => {
                        if let Ok(s) = serde_json::to_string(value) {
                            tantivy_doc.add_text(field, &s);
                        }
                    },
                }
            }
        }
        
        let mut writer = self.writer.write()
            .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
        
        writer.add_document(tantivy_doc)
            .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
        
        Ok(())
    }
    
    /// Index multiple documents in batch
    pub fn add_documents_batch<'a, I>(&self, docs: I) -> Result<usize, TantivyFullTextError>
    where
        I: IntoIterator<Item = &'a FullTextDocument>,
    {
        let mut writer = self.writer.write()
            .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
        
        let mut count = 0;
        for doc in docs {
            let mut tantivy_doc = TantivyDocument::new();
            
            if let Some(&field) = self.fields.get("id") {
                tantivy_doc.add_text(field, &doc.id);
            }
            
            if let (Some(title), Some(&field)) = (&doc.title, self.fields.get("title")) {
                tantivy_doc.add_text(field, title);
            }
            
            if let (Some(content), Some(&field)) = (&doc.content, self.fields.get("content")) {
                tantivy_doc.add_text(field, content);
            }
            
            for (key, value) in &doc.metadata {
                if let Some(&field) = self.fields.get(key) {
                    match value {
                        serde_json::Value::String(s) => {
                            tantivy_doc.add_text(field, s);
                        },
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                tantivy_doc.add_u64(field, i as u64);
                            }
                        },
                        serde_json::Value::Bool(b) => {
                            tantivy_doc.add_bool(field, *b);
                        },
                        _ => {},
                    }
                }
            }
            
            writer.add_document(tantivy_doc)
                .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
            
            count += 1;
        }
        
        Ok(count)
    }
    
    /// Execute a full-text search query
    pub fn search(&self, params: &FullTextSearchParams) -> Result<Vec<FullTextSearchResult>, TantivyFullTextError> {
        let reader = self.index.reader()
            .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
        
        let searcher = reader.searcher();
        let query_parser = self.query_parser.read()
            .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
        
        // Parse query
        let query = query_parser.parse_query(&params.query)
            .map_err(|e| TantivyFullTextError::InvalidQuery(e.to_string()))?;
        
        // Create collector
        let mut top_docs_collector = TopDocs::with_limit(params.limit)
            .order_by_score();
        
        // Execute search
        let top_docs = searcher.search(&query, &top_docs_collector)
            .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
        
        // Convert results
        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)
                .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
            
            // Extract document fields
            let doc = self.extract_document(&retrieved_doc)?;
            
            let result = FullTextSearchResult {
                document_id: doc.id,
                score,
                title: doc.title,
                snippet: None,
                highlights: Vec::new(),
                metadata: doc.metadata,
            };
            
            results.push(result);
        }
        
        Ok(results)
    }
    
    /// Extract document fields from TantivyDocument
    fn extract_document(&self, doc: &TantivyDocument) -> Result<FullTextDocument, TantivyFullTextError> {
        let mut result = FullTextDocument::default();
        let mut metadata = HashMap::new();
        
        for (field_name, &field) in &self.fields {
            let field_values = doc.get_all(field);
            
            if field_values.is_empty() {
                continue;
            }
            
            match field_name.as_str() {
                "id" => {
                    if let Some(value) = field_values[0].text() {
                        result.id = value.to_string();
                    }
                },
                "title" => {
                    if let Some(value) = field_values[0].text() {
                        result.title = Some(value.to_string());
                    }
                },
                "content" => {
                    if let Some(value) = field_values[0].text() {
                        result.content = Some(value.to_string());
                    }
                },
                _ => {
                    let value = field_values[0].clone();
                    let json_value = tantivy_value_to_json(&value);
                    metadata.insert(field_name.clone(), json_value);
                },
            }
        }
        
        result.metadata = metadata;
        Ok(result)
    }
    
    /// Delete a document by ID
    pub fn delete_document(&self, id: &str) -> Result<bool, TantivyFullTextError> {
        if let Some(&field) = self.fields.get("id") {
            let term = Term::from_field_text(field, id);
            let mut writer = self.writer.write()
                .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
            
            let deleted = writer.delete_term(term);
            return Ok(deleted > 0);
        }
        
        Ok(false)
    }
    
    /// Update a document
    pub fn update_document(&self, doc: &FullTextDocument) -> Result<bool, TantivyFullTextError> {
        let deleted = self.delete_document(&doc.id)?;
        
        if deleted {
            self.add_document(doc)?;
        }
        
        Ok(deleted)
    }
    
    /// Commit pending changes
    pub fn commit(&self) -> Result<(), TantivyFullTextError> {
        let mut writer = self.writer.write()
            .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
        
        writer.commit()
            .map_err(|e| TantivyFullTextError::IndexingFailed(e.to_string()))?;
        
        Ok(())
    }
    
    /// Get the number of indexed documents
    pub fn document_count(&self) -> Result<u64, TantivyFullTextError> {
        let reader = self.index.reader()
            .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
        
        let searcher = reader.searcher();
        Ok(searcher.num_docs())
    }
    
    /// Get index statistics
    pub fn statistics(&self) -> Result<serde_json::Value, TantivyFullTextError> {
        let reader = self.index.reader()
            .map_err(|e| TantivyFullTextError::SearchFailed(e.to_string()))?;
        
        let searcher = reader.searcher();
        let num_docs = searcher.num_docs();
        
        let stats = serde_json::json!({
            "type": "tantivy",
            "document_count": num_docs,
            "index_path": self.config.index_path,
            "fields": self.config.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>(),
            "default_search_field": self.config.default_search_field,
            "enable_fuzzy": self.config.enable_fuzzy,
            "enable_phrase": self.config.enable_phrase,
        });
        
        Ok(stats)
    }
    
    /// Get available fields
    pub fn get_fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }
    
    /// Create a new index with default configuration
    pub fn create_new(path: &str) -> Result<Self, TantivyFullTextError> {
        let config = TantivyFullTextConfig {
            index_path: path.to_string(),
            ..Default::default()
        };
        
        Self::new(config)
    }
}

/// Convert Tantivy value to JSON
fn tantivy_value_to_json(value: &tantivy::schema::Value) -> serde_json::Value {
    match value {
        tantivy::schema::Value::Str(s) => serde_json::Value::String(s.clone()),
        tantivy::schema::Value::U64(n) => serde_json::Value::Number((*n).into()),
        tantivy::schema::Value::I64(n) => serde_json::Value::Number((*n).into()),
        tantivy::schema::Value::F64(n) => serde_json::Value::Number(serde_json::Number::from_f64(*n).unwrap_or_default()),
        tantivy::schema::Value::Bool(b) => serde_json::Value::Bool(*b),
        tantivy::schema::Value::Date(d) => serde_json::Value::String(d.to_string()),
        tantivy::schema::Value::Bytes(_) => serde_json::Value::Null,
        tantivy::schema::Value::JsonObject(_) => serde_json::Value::Null,
        tantivy::schema::Value::Facet(_) => serde_json::Value::Null,
    }
}

/// Manager for multiple full-text indexes
pub struct FullTextIndexManager {
    indexes: Arc<RwLock<HashMap<String, Arc<TantivyFullTextIndex>>>>,
}

impl FullTextIndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Create or open an index
    pub fn get_or_create(&self, name: &str, config: &TantivyFullTextConfig) -> Result<Arc<TantivyFullTextIndex>, TantivyFullTextError> {
        let mut indexes = self.indexes.write()
            .map_err(|e| TantivyFullTextError::IndexCreationFailed(e.to_string()))?;
        
        if let Some(index) = indexes.get(name) {
            return Ok(index.clone());
        }
        
        let index = Arc::new(TantivyFullTextIndex::new(config.clone())?);
        indexes.insert(name.to_string(), index.clone());
        
        Ok(index)
    }
    
    /// Get an existing index
    pub fn get(&self, name: &str) -> Option<Arc<TantivyFullTextIndex>> {
        let indexes = self.indexes.read().ok()?;
        indexes.get(name).cloned()
    }
    
    /// Remove an index
    pub fn remove(&self, name: &str) -> bool {
        let mut indexes = self.indexes.write().ok()?;
        indexes.remove(name).is_some()
    }
    
    /// List all indexes
    pub fn list(&self) -> Vec<String> {
        let indexes = self.indexes.read().ok()?;
        indexes.keys().cloned().collect()
    }
}

impl Default for FullTextIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    
    #[test]
    fn test_tantivy_index_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = TantivyFullTextConfig {
            index_path: temp_dir.path().join("test_index").to_str().unwrap().to_string(),
            ..Default::default()
        };
        
        let index = TantivyFullTextIndex::new(config).unwrap();
        assert_eq!(index.document_count().unwrap(), 0);
    }
    
    #[test]
    fn test_document_indexing() {
        let temp_dir = TempDir::new().unwrap();
        let config = TantivyFullTextConfig {
            index_path: temp_dir.path().join("test_index").to_str().unwrap().to_string(),
            ..Default::default()
        };
        
        let index = TantivyFullTextIndex::new(config).unwrap();
        
        // Add test documents
        let docs = vec![
            FullTextDocument {
                id: "1".to_string(),
                title: Some("Hello World".to_string()),
                content: Some("This is a test document".to_string()),
                metadata: HashMap::new(),
                boost: 1.0,
            },
            FullTextDocument {
                id: "2".to_string(),
                title: Some("Tantivy Search".to_string()),
                content: Some("Full-text search with Tantivy is fast".to_string()),
                metadata: HashMap::new(),
                boost: 1.0,
            },
        ];
        
        index.add_documents_batch(&docs).unwrap();
        index.commit().unwrap();
        
        assert_eq!(index.document_count().unwrap(), 2);
    }
    
    #[test]
    fn test_search() {
        let temp_dir = TempDir::new().unwrap();
        let config = TantivyFullTextConfig {
            index_path: temp_dir.path().join("test_index").to_str().unwrap().to_string(),
            ..Default::default()
        };
        
        let index = TantivyFullTextIndex::new(config).unwrap();
        
        let docs = vec![
            FullTextDocument {
                id: "1".to_string(),
                title: Some("Rust Programming".to_string()),
                content: Some("Learn Rust programming language".to_string()),
                metadata: HashMap::new(),
                boost: 1.0,
            },
            FullTextDocument {
                id: "2".to_string(),
                title: Some("Python Programming".to_string()),
                content: Some("Learn Python programming language".to_string()),
                metadata: HashMap::new(),
                boost: 1.0,
            },
        ];
        
        index.add_documents_batch(&docs).unwrap();
        index.commit().unwrap();
        
        let params = FullTextSearchParams {
            query: "Rust".to_string(),
            limit: 10,
            ..Default::default()
        };
        
        let results = index.search(&params).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].document_id, "1");
    }
    
    #[test]
    fn test_delete_document() {
        let temp_dir = TempDir::new().unwrap();
        let config = TantivyFullTextConfig {
            index_path: temp_dir.path().join("test_index").to_str().unwrap().to_string(),
            ..Default::default()
        };
        
        let index = TantivyFullTextIndex::new(config).unwrap();
        
        let doc = FullTextDocument {
            id: "delete_test".to_string(),
            title: Some("Test".to_string()),
            content: Some("Content to delete".to_string()),
            metadata: HashMap::new(),
            boost: 1.0,
        };
        
        index.add_document(&doc).unwrap();
        index.commit().unwrap();
        assert_eq!(index.document_count().unwrap(), 1);
        
        let deleted = index.delete_document("delete_test").unwrap();
        assert!(deleted);
    }
}
