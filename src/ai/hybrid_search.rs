use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use std::cmp::Ordering;
use tokio::sync::{RwLock, Mutex};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use dashmap::DashMap;
useordered_float::OrderedFloat;
use tantivy::collector::{TopDocs, FilterCollector};
use tantivy::query::{Query, TermQuery, BooleanQuery, Occur, RangeQuery};
use tantivy::schema::{Field, IndexRecordOption, TextFieldIndexing, StoredValue};
use tantivy::{Index, Searcher};
use crate::ai::embedding::EmbeddingManager;

#[derive(Error, Debug)]
pub enum HybridSearchError {
    #[error("Index error: {0}")]
    IndexError(String),
    
    #[error("Query error: {0}")]
    QueryError(String),
    
    #[error("Fusion error: {0}")]
    FusionError(String),
    
    #[error("Reranking error: {0}")]
    RerankingError(String),
    
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Normalization error: {0}")]
    NormalizationError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchConfig {
    pub vector_search: VectorSearchConfig,
    pub text_search: TextSearchConfig,
    pub fusion: FusionConfig,
    pub reranking: RerankingConfig,
    pub hybrid_weight: f32,
    pub min_score: f32,
    pub max_results: u32,
    pub timeout_ms: u64,
    pub enable_fuzzy: bool,
    pub enable_synonyms: bool,
    pub enable_expansion: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchConfig {
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub ef_construction: u32,
    pub m: u32,
    pub max_elements: u64,
    pub num_subquantizers: u32,
    pub nprobe: u32,
    pub nlist: u32,
    pub use_gpu: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VectorIndexType {
    HNSW,
    IVF,
    PQ,
    SCANN,
    BruteForce,
    DiskANN,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
    Manhattan,
    Angular,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchConfig {
    pub analyzer: TextAnalyzer,
    pub indexing_options: IndexingOptions,
    pub field_weights: HashMap<String, f32>,
    pub phrase_slop: u32,
    pub fuzzy_max_edits: u32,
    pub fuzzy_prefix_length: u32,
    pub enable_phonetic: bool,
    pub enable_stemming: bool,
    pub enable_stop_words: bool,
    pub min_word_length: u32,
    pub max_word_length: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TextAnalyzer {
    Standard,
    Chinese,
    English,
    Whitespace,
    Token,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingOptions {
    pub index_full_text: bool,
    pub index_terms: bool,
    pub index_positions: bool,
    pub store_source: bool,
    pub field_boost: f32,
    pub norm_type: NormType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NormType {
    Euclidean,
    Max,
    NumberOfUniqueTerms,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusionConfig {
    pub method: FusionMethod,
    pub normalization: NormalizationMethod,
    pub score_weights: Vector<f32>,
    pub rank_fusion_k: f32,
    pub reciprocal_rank_k: f32,
    pub combine_algorithm: CombineAlgorithm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FusionMethod {
    RRF,
    RRF_Fusion,
    CombSUM,
    CombMNZ,
    BordaFuse,
    Condorcet,
    MarkovChain,
    WeightedAverage,
    ReciprocalRankFusion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NormalizationMethod {
    MinMax,
    ZScore,
    Percentile,
    Linear,
    Log,
    Sigmoid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CombineAlgorithm {
    Sum,
    Product,
    Max,
    Min,
    Average,
    Median,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankingConfig {
    pub enabled: bool,
    pub method: RerankingMethod,
    const N: u32,
    top_k: u32,
    diversity_penalty: f32,
    mmr_lambda: f32,
    similarity_threshold: f32,
    cross_encoder_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RerankingMethod {
    CrossEncoder,
    MonoT5,
    BERT,
    TwoTower,
    MMR,
    DPP,
    SLIDE,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridQuery {
    pub query_id: String,
    pub query_text: String,
    pub query_vector: Option<Vec<f32>>,
    pub filters: Vec<QueryFilter>,
    pub fields: Vec<String>,
    pub limit: u32,
    pub offset: u32,
    pub include_scores: bool,
    pub include_explanations: bool,
    pub user_context: Option<HashMap<String, f32>>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub field: String,
    pub filter_type: FilterType,
    pub value: FilterValue,
    pub boost: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterType {
    Term,
    Terms,
    Range,
    Exists,
    Prefix,
    Wildcard,
    Regex,
    Geo,
    IDS,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    String(String),
    Strings(Vec<String>),
    Integer(i64),
    Integers(Vec<i64>),
    Float(f64),
    Floats(Vec<f64>),
    Range(RangeValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeValue {
    pub lt: Option<f64>,
    pub lte: Option<f64>,
    pub gt: Option<f64>,
    pub gte: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResult {
    pub query_id: String,
    pub documents: Vec<HybridDocument>,
    pub total_hits: u64,
    pub vector_hits: u32,
    pub text_hits: u32,
    pub fusion_time_ms: u64,
    pub reranking_time_ms: u64,
    pub total_time_ms: u64,
    pub explanations: Option<SearchExplanations>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridDocument {
    pub doc_id: String,
    pub score: f32,
    pub vector_score: Option<f32>,
    pub text_score: Option<f32>,
    pub hybrid_score: f32,
    pub reranked_score: Option<f32>,
    pub rank: u32,
    pub document: DocumentContent,
    pub highlights: Vec<Highlight>,
    pub explanations: Option<DocumentExplanation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentContent {
    pub id: String,
    pub title: Option<String>,
    pub content: Option<String>,
    pub fields: HashMap<String, String>,
    pub vector: Option<Vec<f32>>,
    pub metadata: DocumentMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub source: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub size_bytes: u64,
    pub language: String,
    pub author: Option<String>,
    pub tags: Vec<String>,
    pub categories: Vec<String>,
    pub custom_fields: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Highlight {
    pub field: String,
    pub fragments: Vec<String>,
    pub scores: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentExplanation {
    pub vector_contribution: f32,
    pub text_contribution: f32,
    pub filter_bonus: f32,
    pub boost_factors: HashMap<String, f32>,
    pub raw_scores: RawScores,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawScores {
    pub vector_raw: f32,
    pub text_raw: f32,
    pub combined_raw: f32,
    pub normalized: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchExplanations {
    pub query_rewrite: String,
    pub vector_query: String,
    pub text_query: String,
    pub fusion_method: String,
    pub fusion_weights: HashMap<String, f32>,
    pub normalization: String,
    pub documents_considered: u32,
    pub documents_returned: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchPipeline {
    pub pipeline_id: String,
    pub query: HybridQuery,
    pub vector_results: Vec<VectorSearchResult>,
    pub text_results: Vec<TextSearchResult>,
    pub fused_results: Vec<FusedResult>,
    pub reranked_results: Vec<RerankedResult>,
    pub metrics: PipelineMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub doc_id: String,
    pub score: f32,
    pub distance: f32,
    pub vector: Option<Vec<f32>>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchResult {
    pub doc_id: String,
    pub score: f32,
    pub term_frequency: HashMap<String, f32>,
    pub field_scores: HashMap<String, f32>,
    pub highlights: Vec<Highlight>,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusedResult {
    pub doc_id: String,
    pub fused_score: f32,
    pub vector_score: f32,
    pub text_score: f32,
    pub rank: u32,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankedResult {
    pub doc_id: String,
    pub rerank_score: f32,
    pub original_score: f32,
    pub rank: u32,
    pub diversity_score: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetrics {
    pub query_rewrite_ms: u64,
    pub vector_search_ms: u64,
    pub text_search_ms: u64,
    pub fusion_ms: u64,
    pub reranking_ms: u64,
    pub total_ms: u64,
    pub memory_used_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub vector_index_size: u64,
    pub text_index_size: u64,
    pub total_documents: u64,
    pub last_updated: u64,
    pub fragmentation: f32,
    pub optimization_level: u32,
}

struct VectorIndex {
    index_type: VectorIndexType,
    metric: DistanceMetric,
    vectors: DashMap<String, Vec<f32>>,
    hnsw_index: Option<HNSWIndex>,
    ivf_index: Option<IVFIndex>,
}

struct HNSWIndex {
    m: u32,
    ef_construction: u32,
    nodes: DashMap<String, HNSWNode>,
    entry_point: Option<String>,
}

struct HNSWNode {
    id: String,
    vector: Vec<f32>,
    neighbors: BTreeMap<u32, Vec<String>>,
    level: u32,
}

struct IVFIndex {
    nlist: u32,
    nprobe: u32,
    clusters: DashMap<u32, Vec<String>>,
    cluster_vectors: DashMap<u32, Vec<f32>>,
}

struct TextIndex {
    index: Index,
    schema: tantivy::schema::Schema,
    fields: HashMap<String, Field>,
    searcher: Option<Searcher>,
}

struct SynonymMap {
    synonyms: HashMap<String, HashSet<String>>,
    bidirectional: bool,
}

struct QueryRewriteRule {
    pattern: String,
    replacement: String,
    priority: u32,
    enabled: bool,
}

pub struct HybridSearchManager {
    config: HybridSearchConfig,
    embedding_manager: Option<Arc<RwLock<EmbeddingManager>>>,
    vector_index: Arc<RwLock<VectorIndex>>,
    text_index: Arc<RwLock<TextIndex>>,
    synonyms: Arc<RwLock<SynonymMap>>,
    rewrite_rules: Arc<RwLock<Vec<QueryRewriteRule>>>,
    document_store: DashMap<String, DocumentContent>,
    search_pipelines: DashMap<String, SearchPipeline>,
    stats: Arc<RwLock<IndexStats>>,
    query_cache: DashMap<String, CachedQuery>,
    config_mutex: Mutex<()>,
}

struct CachedQuery {
    query: HybridQuery,
    result: HybridSearchResult,
    created_at: u64,
    ttl_seconds: u64,
    hit_count: u32,
}

impl Default for HybridSearchConfig {
    fn default() -> Self {
        Self {
            vector_search: VectorSearchConfig {
                index_type: VectorIndexType::HNSW,
                metric: DistanceMetric::Cosine,
                ef_construction: 200,
                m: 16,
                max_elements: 1000000,
                num_subquantizers: 0,
                nprobe: 20,
                nlist: 1024,
                use_gpu: false,
                batch_size: 64,
            },
            text_search: TextSearchConfig {
                analyzer: TextAnalyzer::Standard,
                indexing_options: IndexingOptions {
                    index_full_text: true,
                    index_terms: true,
                    index_positions: true,
                    store_source: false,
                    field_boost: 1.0,
                    norm_type: NormType::NumberOfUniqueTerms,
                },
                field_weights: HashMap::new(),
                phrase_slop: 0,
                fuzzy_max_edits: 2,
                fuzzy_prefix_length: 2,
                enable_phonetic: false,
                enable_stemming: true,
                enable_stop_words: true,
                min_word_length: 2,
                max_word_length: 50,
            },
            fusion: FusionConfig {
                method: FusionMethod::RRF,
                normalization: NormalizationMethod::MinMax,
                score_weights: vec![0.5, 0.5],
                rank_fusion_k: 60.0,
                reciprocal_rank_k: 100.0,
                combine_algorithm: CombineAlgorithm::Average,
            },
            reranking: RerankingConfig {
                enabled: true,
                method: RerankingMethod::CrossEncoder,
                const N: 100,
                top_k: 10,
                diversity_penalty: 0.1,
                mmr_lambda: 0.5,
                similarity_threshold: 0.9,
                cross_encoder_model: None,
            },
            hybrid_weight: 0.5,
            min_score: 0.01,
            max_results: 100,
            timeout_ms: 10000,
            enable_fuzzy: true,
            enable_synonyms: true,
            enable_expansion: true,
        }
    }
}

impl Default for VectorSearchConfig {
    fn default() -> Self {
        Self {
            index_type: VectorIndexType::HNSW,
            metric: DistanceMetric::Cosine,
            ef_construction: 200,
            m: 16,
            max_elements: 1000000,
            num_subquantizers: 0,
            nprobe: 20,
            nlist: 1024,
            use_gpu: false,
            batch_size: 64,
        }
    }
}

impl Default for TextSearchConfig {
    fn default() -> Self {
        Self {
            analyzer: TextAnalyzer::Standard,
            indexing_options: IndexingOptions::default(),
            field_weights: HashMap::new(),
            phrase_slop: 0,
            fuzzy_max_edits: 2,
            fuzzy_prefix_length: 2,
            enable_phonetic: false,
            enable_stemming: true,
            enable_stop_words: true,
            min_word_length: 2,
            max_word_length: 50,
        }
    }
}

impl Default for IndexingOptions {
    fn default() -> Self {
        Self {
            index_full_text: true,
            index_terms: true,
            index_positions: true,
            store_source: false,
            field_boost: 1.0,
            norm_type: NormType::NumberOfUniqueTerms,
        }
    }
}

impl Default for FusionConfig {
    fn default() -> Self {
        Self {
            method: FusionMethod::RRF,
            normalization: NormalizationMethod::MinMax,
            score_weights: vec![0.5, 0.5],
            rank_fusion_k: 60.0,
            reciprocal_rank_k: 100.0,
            combine_algorithm: CombineAlgorithm::Average,
        }
    }
}

impl Default for RerankingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: RerankingMethod::CrossEncoder,
            const N: 100,
            top_k: 10,
            diversity_penalty: 0.1,
            mmr_lambda: 0.5,
            similarity_threshold: 0.9,
            cross_encoder_model: None,
        }
    }
}

impl Default for RangeValue {
    fn default() -> Self {
        Self {
            lt: None,
            lte: None,
            gt: None,
            gte: None,
        }
    }
}

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self {
            query_rewrite_ms: 0,
            vector_search_ms: 0,
            text_search_ms: 0,
            fusion_ms: 0,
            reranking_ms: 0,
            total_ms: 0,
            memory_used_bytes: 0,
        }
    }
}

impl HybridSearchManager {
    pub async fn new(
        config: Option<HybridSearchConfig>,
        embedding_manager: Option<Arc<RwLock<EmbeddingManager>>>,
    ) -> Result<Self, HybridSearchError> {
        let config = config.unwrap_or_default();
        
        let vector_index = VectorIndex {
            index_type: config.vector_search.index_type.clone(),
            metric: config.vector_search.metric.clone(),
            vectors: DashMap::new(),
            hnsw_index: None,
            ivf_index: None,
        };
        
        let text_index = TextIndex {
            index: todo!(),
            schema: tantivy::schema::Schema::builder().build(),
            fields: HashMap::new(),
            searcher: None,
        };
        
        Ok(Self {
            config,
            embedding_manager,
            vector_index: Arc::new(RwLock::new(vector_index)),
            text_index: Arc::new(RwLock::new(text_index)),
            synonyms: Arc::new(RwLock::new(SynonymMap {
                synonyms: HashMap::new(),
                bidirectional: true,
            })),
            rewrite_rules: Arc::new(RwLock::new(Vec::new())),
            document_store: DashMap::new(),
            search_pipelines: DashMap::new(),
            stats: Arc::new(RwLock::new(IndexStats {
                vector_index_size: 0,
                text_index_size: 0,
                total_documents: 0,
                last_updated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                fragmentation: 0.0,
                optimization_level: 0,
            })),
            query_cache: DashMap::new(),
            config_mutex: Mutex::new(()),
        })
    }

    pub async fn index_document(&self, document: DocumentContent) -> Result<(), HybridSearchError> {
        self.document_store.insert(document.id.clone(), document.clone());
        
        {
            let mut vector_idx = self.vector_index.write().await;
            vector_idx.vectors.insert(document.id.clone(), 
                document.vector.clone().unwrap_or_default());
        }
        
        info!("Document indexed: {}", document.id);
        Ok(())
    }

    pub async fn index_documents(&self, documents: Vec<DocumentContent>) -> Result<usize, HybridSearchError> {
        let mut indexed = 0;
        
        {
            let mut vector_idx = self.vector_index.write().await;
            for doc in &documents {
                vector_idx.vectors.insert(doc.id.clone(),
                    doc.vector.clone().unwrap_or_default());
                indexed += 1;
            }
        }
        
        info!("Batch indexed {} documents", indexed);
        Ok(indexed)
    }

    pub async fn delete_document(&self, doc_id: &str) -> Result<bool, HybridSearchError> {
        let removed = self.document_store.remove(doc_id).is_some();
        
        {
            let mut vector_idx = self.vector_index.write().await;
            vector_idx.vectors.remove(doc_id);
        }
        
        if removed {
            info!("Document deleted: {}", doc_id);
        }
        
        Ok(removed)
    }

    pub async fn search(&self, query: HybridQuery) -> Result<HybridSearchResult, HybridSearchError> {
        let start_time = std::time::Instant::now();
        let query_id = query.query_id.clone();
        
        if let Some(cached) = self.query_cache.get(&query_id) {
            if Self::is_cache_valid(&cached) {
                let mut cached_result = cached.value().clone();
                cached_result.query_id = query_id;
                return Ok(cached_result);
            }
        }
        
        let _ = self.config_mutex.lock().await;
        
        let rewritten_query = self.rewrite_query(&query).await?;
        
        let (vector_results, text_results) = tokio::join!(
            self.vector_search(&rewritten_query),
            self.text_search(&rewritten_query)
        );
        
        let fused_results = self.fuse_results(
            &vector_results?,
            &text_results?,
            &query,
        ).await?;
        
        let reranked_results = self.rerank_results(fused_results, &query).await?;
        
        let total_time = start_time.elapsed().as_millis() as u64;
        
        let result = HybridSearchResult {
            query_id,
            documents: reranked_results,
            total_hits: reranked_results.len() as u64,
            vector_hits: vector_results?.len() as u32,
            text_hits: text_results?.len() as u32,
            fusion_time_ms: 0,
            reranking_time_ms: 0,
            total_time_ms: total_time,
            explanations: None,
        };
        
        self.cache_query(&query, &result).await;
        
        Ok(result)
    }

    async fn rewrite_query(&self, query: &HybridQuery) -> Result<HybridQuery, HybridSearchError> {
        let mut rewritten = query.clone();
        
        if self.config.enable_synonyms {
            rewritten.query_text = self.expand_synonyms(&query.query_text).await;
        }
        
        if self.config.enable_expansion {
            rewritten.query_text = self.expand_query(&rewritten.query_text).await;
        }
        
        if self.config.enable_fuzzy {
            rewritten.query_text = self.add_fuzzy_terms(&rewritten.query_text).await;
        }
        
        Ok(rewritten)
    }

    async fn expand_synonyms(&self, text: &str) -> String {
        let synonyms = self.synonyms.read().await;
        let mut words: Vec<&str> = text.split_whitespace().collect();
        let mut expanded = Vec::new();
        
        for word in &words {
            expanded.push(word.to_string());
            if let Some(syn_set) = synonyms.synonyms.get(*word) {
                if !syn_set.is_empty() {
                    expanded.push(format!("({} {})", word, syn_set.iter().next().unwrap()));
                }
            }
        }
        
        expanded.join(" ")
    }

    async fn expand_query(&self, text: &str) -> String {
        text.to_string()
    }

    async fn add_fuzzy_terms(&self, text: &str) -> String {
        text.to_string()
    }

    async fn vector_search(&self, query: &HybridQuery) -> Result<Vec<VectorSearchResult>, HybridSearchError> {
        let vector_idx = self.vector_index.read().await;
        
        if let Some(ref query_vector) = query.query_vector {
            let results: Vec<VectorSearchResult> = vector_idx.vectors.iter()
                .take(self.config.max_results as usize)
                .map(|entry| {
                    let doc_id = entry.key().clone();
                    let doc_vector = entry.value().clone();
                    let distance = Self::calculate_distance(
                        query_vector,
                        &doc_vector,
                        &vector_idx.metric,
                    );
                    let score = Self::distance_to_score(distance);
                    
                    VectorSearchResult {
                        doc_id,
                        score,
                        distance,
                        vector: Some(doc_vector),
                        metadata: HashMap::new(),
                    }
                })
                .filter(|r| r.score >= self.config.min_score)
                .collect();
            
            return Ok(results);
        }
        
        Ok(Vec::new())
    }

    async fn text_search(&self, query: &HybridQuery) -> Result<Vec<TextSearchResult>, HybridSearchError> {
        let text_idx = self.text_index.read().await;
        
        let results: Vec<TextSearchResult> = self.document_store.iter()
            .filter(|doc| {
                if let Some(ref content) = doc.value().content {
                    Self::text_matches_query(content, &query.query_text)
                } else {
                    false
                }
            })
            .take(self.config.max_results as usize)
            .map(|doc| {
                let doc_id = doc.key().clone();
                let content = doc.value().content.clone().unwrap_or_default();
                let tf_idf_score = Self::calculate_tf_idf(&content, &query.query_text);
                
                TextSearchResult {
                    doc_id,
                    score: tf_idf_score,
                    term_frequency: HashMap::new(),
                    field_scores: HashMap::new(),
                    highlights: Vec::new(),
                    snippet: Some(Self::create_snippet(&content, &query.query_text)),
                }
            })
            .filter(|r| r.score >= self.config.min_score)
            .collect();
        
        Ok(results)
    }

    fn text_matches_query(content: &str, query: &str) -> bool {
        content.to_lowercase().contains(&query.to_lowercase())
    }

    fn calculate_tf_idf(content: &str, query: &str) -> f32 {
        let content_words: HashSet<&str> = content.split_whitespace().collect();
        let query_words: Vec<&str> = query.split_whitespace().collect();
        
        let mut score = 0.0f32;
        for word in query_words {
            if content_words.contains(&word) {
                let term_freq = content_matches_word(content, word) as u32;
                let idf = (content_words.len() as f32 / (1 + term_freq) as f32).ln();
                score += (1 + term_freq as f32) * idf;
            }
        }
        
        score
    }

    fn create_snippet(content: &str, query: &str) -> String {
        let lower_content = content.to_lowercase();
        let lower_query = query.to_lowercase();
        
        if let Some(pos) = lower_content.find(&lower_query) {
            let start = pos.saturating_sub(50);
            let end = (pos + query.len() + 50).min(content.len());
            let mut snippet = content[start..end].to_string();
            
            if start > 0 {
                snippet = "...".to_string() + &snippet;
            }
            if end < content.len() {
                snippet = snippet + "...";
            }
            
            snippet
        } else {
            content[..std::cmp::min(100, content.len())].to_string()
        }
    }

    async fn fuse_results(
        &self,
        vector_results: &[VectorSearchResult],
        text_results: &[TextSearchResult],
        query: &HybridQuery,
    ) -> Result<Vec<HybridDocument>, HybridSearchError> {
        let fusion_start = std::time::Instant::now();
        
        let all_doc_ids: HashSet<&str> = vector_results.iter()
            .map(|r| r.doc_id.as_str())
            .chain(text_results.iter().map(|r| r.doc_id.as_str()))
            .collect();
        
        let normalized_vector = if !vector_results.is_empty() {
            Some(Self::normalize_scores_vector(
                vector_results,
                &self.config.fusion.normalization,
            ))
        } else {
            None
        };
        
        let normalized_text = if !text_results.is_empty() {
            Some(Self::normalize_scores_text(
                text_results,
                &self.config.fusion.normalization,
            ))
        } else {
            None
        };
        
        let mut fused_docs: Vec<HybridDocument> = all_doc_ids.iter()
            .map(|doc_id| {
                let vector_score = normalized_vector.as_ref()
                    .and_then(|n| n.get(*doc_id).copied())
                    .unwrap_or(0.0);
                
                let text_score = normalized_text.as_ref()
                    .and_then(|n| n.get(*doc_id).copied())
                    .unwrap_or(0.0);
                
                let hybrid_score = self.combine_scores(vector_score, text_score);
                
                HybridDocument {
                    doc_id: doc_id.to_string(),
                    score: hybrid_score,
                    vector_score: Some(vector_score),
                    text_score: Some(text_score),
                    hybrid_score,
                    reranked_score: None,
                    rank: 0,
                    document: self.document_store.get(*doc_id)
                        .map(|d| d.clone())
                        .unwrap_or_default(),
                    highlights: Vec::new(),
                    explanations: None,
                }
            })
            .filter(|d| d.hybrid_score >= self.config.min_score)
            .collect();
        
        fused_docs.sort_by(|a, b| b.hybrid_score.partial_cmp(&a.hybrid_score).unwrap_or(Ordering::Equal));
        
        for (rank, doc) in fused_docs.iter_mut().enumerate() {
            doc.rank = (rank + 1) as u32;
        }
        
        let fusion_time = fusion_start.elapsed().as_millis() as u64;
        
        self.update_stats(|stats| {
            stats.fusion_time_ms = fusion_time;
        });
        
        Ok(fused_docs)
    }

    fn normalize_scores_vector(
        results: &[VectorSearchResult],
        normalization: &NormalizationMethod,
    ) -> HashMap<String, f32> {
        if results.is_empty() {
            return HashMap::new();
        }
        
        let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
        let min = scores.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = scores.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let range = max - min;
        
        results.iter()
            .map(|r| {
                let normalized = match normalization {
                    NormalizationMethod::MinMax => {
                        if range > 0.0 {
                            (r.score - min) / range
                        } else {
                            0.5
                        }
                    }
                    NormalizationMethod::ZScore => {
                        let mean = scores.iter().sum::<f32>() / scores.len() as f32;
                        let variance = scores.iter()
                            .map(|s| (s - mean).powi(2))
                            .sum::<f32>() / scores.len() as f32;
                        let std = variance.sqrt();
                        if std > 0.0 {
                            (r.score - mean) / std
                        } else {
                            0.0
                        }
                    }
                    _ => r.score,
                };
                (r.doc_id.clone(), normalized)
            })
            .collect()
    }

    fn normalize_scores_text(
        results: &[TextSearchResult],
        normalization: &NormalizationMethod,
    ) -> HashMap<String, f32> {
        if results.is_empty() {
            return HashMap::new();
        }
        
        let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
        let min = scores.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = scores.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let range = max - min;
        
        results.iter()
            .map(|r| {
                let normalized = match normalization {
                    NormalizationMethod::MinMax => {
                        if range > 0.0 {
                            (r.score - min) / range
                        } else {
                            0.5
                        }
                    }
                    _ => r.score,
                };
                (r.doc_id.clone(), normalized)
            })
            .collect()
    }

    fn combine_scores(&self, vector_score: f32, text_score: f32) -> f32 {
        let weights = &self.config.fusion.score_weights;
        
        match self.config.fusion.combine_algorithm {
            CombineAlgorithm::Sum => {
                weights[0] * vector_score + weights[1] * text_score
            }
            CombineAlgorithm::Product => {
                (vector_score.powf(weights[0]) * text_score.powf(weights[1])).powf(
                    1.0 / (weights[0] + weights[1])
                )
            }
            CombineAlgorithm::Average => {
                (vector_score + text_score) / 2.0
            }
            CombineAlgorithm::Max => {
                vector_score.max(text_score)
            }
            _ => {
                (weights[0] * vector_score + weights[1] * text_score) / weights.iter().sum::<f32>()
            }
        }
    }

    async fn rerank_results(
        &self,
        documents: Vec<HybridDocument>,
        query: &HybridQuery,
    ) -> Result<Vec<HybridDocument>, HybridSearchError> {
        if !self.config.reranking.enabled || documents.is_empty() {
            return Ok(documents);
        }
        
        let rerank_start = std::time::Instant::now();
        
        let mut reranked = documents;
        
        match self.config.reranking.method {
            RerankingMethod::MMR => {
                self.apply_mmr_reranking(&mut reranked, query).await;
            }
            RerankingMethod::DPP => {
                self.apply_dpp_reranking(&mut reranked).await;
            }
            _ => {}
        }
        
        let rerank_time = rerank_start.elapsed().as_millis() as u64;
        
        self.update_stats(|stats| {
            stats.reranking_ms = rerank_time;
        });
        
        Ok(reranked)
    }

    async fn apply_mmr_reranking(
        &self,
        documents: &mut Vec<HybridDocument>,
        query: &HybridQuery,
    ) {
        let lambda = self.config.reranking.mmr_lambda;
        let k = self.config.reranking.top_k;
        
        let selected: Vec<HybridDocument> = Vec::new();
        let candidates: Vec<HybridDocument> = documents.clone();
        
        while selected.len() < k as usize && !candidates.is_empty() {
            let mut best_idx = 0;
            let mut best_score = f32::MIN;
            
            for (idx, doc) in candidates.iter().enumerate() {
                let relevance = doc.hybrid_score;
                let mut max_similarity = 0.0f32;
                
                for selected_doc in &selected {
                    if let (Some(v1), Some(v2)) = (
                        doc.document.vector.as_ref(),
                        selected_doc.document.vector.as_ref(),
                    ) {
                        let similarity = Self::cosine_similarity(v1, v2);
                        max_similarity = max_similarity.max(similarity);
                    }
                }
                
                let mmr_score = lambda * relevance - (1.0 - lambda) * max_similarity;
                
                if mmr_score > best_score {
                    best_score = mmr_score;
                    best_idx = idx;
                }
            }
            
            selected.push(candidates.remove(best_idx));
        }
        
        for (idx, doc) in selected.iter_mut().enumerate() {
            doc.reranked_score = Some(best_score);
            doc.rank = (idx + 1) as u32;
        }
        
        *documents = selected;
    }

    async fn apply_dpp_reranking(&self, documents: &mut Vec<HybridDocument>) {
        let k = self.config.reranking.top_k as usize;
        
        documents.truncate(std::cmp::min(documents.len(), k * 2));
        
        let selected: Vec<HybridDocument> = documents.iter().take(k).cloned().collect();
        *documents = selected;
    }

    async fn cache_query(&self, query: &HybridQuery, result: &HybridSearchResult) {
        let cached = CachedQuery {
            query: query.clone(),
            result: result.clone(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            ttl_seconds: 300,
            hit_count: 0,
        };
        
        self.query_cache.insert(query.query_id.clone(), cached);
    }

    fn is_cache_valid(cached: &CachedQuery) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        now - cached.created_at < cached.ttl_seconds
    }

    async fn update_stats<F>(&self, f: F) where F: FnOnce(&mut IndexStats) {
        let mut stats = self.stats.write().await;
        f(&mut stats);
    }

    pub async fn add_synonym(&self, word: &str, synonyms: Vec<&str>) -> Result<(), HybridSearchError> {
        let mut syn_map = self.synonyms.write().await;
        
        for syn in &synonyms {
            syn_map.synonyms.entry(word.to_string())
                .or_insert_with(HashSet::new)
                .insert(syn.to_string());
            
            if syn_map.bidirectional {
                syn_map.synonyms.entry(syn.to_string())
                    .or_insert_with(HashSet::new)
                    .insert(word.to_string());
            }
        }
        
        info!("Added synonyms for '{}': {:?}", word, synonyms);
        Ok(())
    }

    pub async fn add_rewrite_rule(&self, rule: QueryRewriteRule) -> Result<(), HybridSearchError> {
        let mut rules = self.rewrite_rules.write().await;
        rules.push(rule);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Ok(())
    }

    pub async fn get_index_stats(&self) -> IndexStats {
        self.stats.read().await.clone()
    }

    fn calculate_distance(a: &[f32], b: &[f32], metric: &DistanceMetric) -> f32 {
        match metric {
            DistanceMetric::Cosine => {
                1.0 - Self::cosine_similarity(a, b)
            }
            DistanceMetric::Euclidean => {
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::DotProduct => {
                -(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>())
            }
            _ => 0.0,
        }
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        if norm_a > 0.0 && norm_b > 0.0 {
            dot / (norm_a * norm_b)
        } else {
            0.0
        }
    }

    fn distance_to_score(distance: f32) -> f32 {
        1.0 / (1.0 + distance)
    }
}

fn content_matches_word(content: &str, word: &str) -> bool {
    content.to_lowercase().contains(&word.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hybrid_search_manager_creation() {
        let manager = HybridSearchManager::new(None, None).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_document_indexing() {
        let manager = HybridSearchManager::new(None, None).await.unwrap();
        
        let doc = DocumentContent {
            id: "test_doc_1".to_string(),
            title: Some("Test Document".to_string()),
            content: Some("This is a test document for hybrid search".to_string()),
            fields: HashMap::new(),
            vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
            metadata: DocumentMetadata {
                source: "test".to_string(),
                created_at: 0,
                updated_at: 0,
                size_bytes: 100,
                language: "en".to_string(),
                author: None,
                tags: vec!["test".to_string()],
                categories: vec![],
                custom_fields: HashMap::new(),
            },
        };
        
        let result = manager.index_document(doc).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_document_deletion() {
        let manager = HybridSearchManager::new(None, None).await.unwrap();
        
        let result = manager.delete_document("nonexistent").await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_vector_distance_calculation() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        
        let distance = HybridSearchManager::calculate_distance(&a, &b, &DistanceMetric::Cosine);
        assert!((distance - 1.0).abs() < 1e-6);
        
        let similarity = HybridSearchManager::cosine_similarity(&a, &b);
        assert!((similarity - 0.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_synonym_addition() {
        let manager = HybridSearchManager::new(None, None).await.unwrap();
        
        let result = manager.add_synonym("car", vec!["automobile", "vehicle"]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_rule() {
        let manager = HybridSearchManager::new(None, None).await.unwrap();
        
        let rule = QueryRewriteRule {
            pattern: "bad".to_string(),
            replacement: "poor".to_string(),
            priority: 1,
            enabled: true,
        };
        
        let result = manager.add_rewrite_rule(rule).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_score_normalization() {
        let results = vec![
            VectorSearchResult {
                doc_id: "1".to_string(),
                score: 0.9,
                distance: 0.1,
                vector: None,
                metadata: HashMap::new(),
            },
            VectorSearchResult {
                doc_id: "2".to_string(),
                score: 0.5,
                distance: 0.5,
                vector: None,
                metadata: HashMap::new(),
            },
            VectorSearchResult {
                doc_id: "3".to_string(),
                score: 0.1,
                distance: 0.9,
                vector: None,
                metadata: HashMap::new(),
            },
        ];
        
        let normalized = HybridSearchManager::normalize_scores_vector(
            &results,
            &NormalizationMethod::MinMax,
        );
        
        assert_eq!(normalized.get("1").copied(), Some(1.0));
        assert_eq!(normalized.get("3").copied(), Some(0.0));
    }

    #[tokio::test]
    async fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![-1.0, 0.0, 0.0];
        
        let sim_aa = HybridSearchManager::cosine_similarity(&a, &a);
        assert!((sim_aa - 1.0).abs() < 1e-6);
        
        let sim_ac = HybridSearchManager::cosine_similarity(&a, &c);
        assert!((sim_ac - (-1.0)).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_index_stats() {
        let manager = HybridSearchManager::new(None, None).await.unwrap();
        
        let stats = manager.get_index_stats().await;
        assert_eq!(stats.total_documents, 0);
    }
}
