//! HNSW (Hierarchical Navigable Small World) index implementation for CortexDB.
//!
//! This module provides an implementation of the HNSW algorithm for efficient
//! approximate nearest neighbor search in high-dimensional spaces.

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::{Document, Embedding};
use crate::cortex_index::vector::{
    DistanceMetric, VectorIndex, VectorIndexConfig, VectorIndexBuilder, compute_distance,
};
use crate::index::vector::hnsw::{HnswIndex, HnswError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

/// HNSW index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswIndexConfig {
    /// Base vector index configuration
    pub base_config: VectorIndexConfig,
    /// Maximum number of connections per node
    pub max_connections: usize,
    /// Number of elements to search during construction
    pub ef_construction: usize,
    /// Number of elements to search during query
    pub ef_search: usize,
}

/// HNSW index implementation
#[derive(Debug)]
pub struct HnswVectorIndex {
    config: VectorIndexConfig,
    hnsw_index: Arc<RwLock<HnswIndex>>,
    document_map: Arc<RwLock<HashMap<String, Document>>>,
    id_mapping: Arc<RwLock<HashMap<String, usize>>>,
    reverse_id_mapping: Arc<RwLock<HashMap<usize, String>>>,
    next_id: Arc<RwLock<usize>>,
}

impl VectorIndex for HnswVectorIndex {
    fn config(&self) -> &VectorIndexConfig {
        &self.config
    }

    fn add(
        &mut self,
        document_id: &str,
        embedding: &Embedding,
        document: Option<Document>,
    ) -> Result<(), CortexError> {
        // Check if document already exists
        let id_mapping = self.id_mapping.read().unwrap();
        if id_mapping.contains_key(document_id) {
            return Err(CortexError::Index(IndexError::DocumentAlreadyExists(
                format!("Document with ID '{}' already exists", document_id),
            )));
        }
        drop(id_mapping);

        // Get next internal ID
        let mut next_id = self.next_id.write().unwrap();
        let internal_id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Add to mappings
        let mut id_mapping = self.id_mapping.write().unwrap();
        let mut reverse_id_mapping = self.reverse_id_mapping.write().unwrap();
        id_mapping.insert(document_id.to_string(), internal_id);
        reverse_id_mapping.insert(internal_id, document_id.to_string());
        drop(id_mapping);
        drop(reverse_id_mapping);

        // Add document if provided
        if let Some(doc) = document {
            let mut document_map = self.document_map.write().unwrap();
            document_map.insert(document_id.to_string(), doc);
            drop(document_map);
        }

        // Add to HNSW index
        let mut hnsw_index = self.hnsw_index.write().unwrap();
        hnsw_index
            .add(internal_id, embedding.values().to_vec())
            .map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to add vector to HNSW index: {}",
                    e
                )))
            })?;

        Ok(())
    }

    fn add_batch(
        &mut self,
        items: impl Iterator<Item = (String, Embedding, Option<Document>)>,
    ) -> Result<usize, CortexError> {
        let mut count = 0;
        for (document_id, embedding, document) in items {
            if self.add(&document_id, &embedding, document).is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn search(
        &self,
        query: &Embedding,
        limit: usize,
        include_documents: bool,
    ) -> Result<Vec<super::SearchResult>, CortexError> {
        // Search HNSW index
        let hnsw_index = self.hnsw_index.read().unwrap();
        let results = hnsw_index
            .search(query.values(), limit)
            .map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to search HNSW index: {}",
                    e
                )))
            })?;
        drop(hnsw_index);

        // Convert to SearchResult
        let mut search_results = Vec::new();
        let reverse_id_mapping = self.reverse_id_mapping.read().unwrap();
        let document_map = self.document_map.read().unwrap();

        for (internal_id, score) in results {
            if let Some(document_id) = reverse_id_mapping.get(&internal_id) {
                let document = if include_documents {
                    document_map.get(document_id).cloned()
                } else {
                    None
                };

                search_results.push(super::SearchResult {
                    document_id: document_id.to_string(),
                    score,
                    document,
                });
            }
        }

        Ok(search_results)
    }

    fn search_with_threshold(
        &self,
        query: &Embedding,
        limit: usize,
        threshold: f32,
        include_documents: bool,
    ) -> Result<Vec<super::SearchResult>, CortexError> {
        let results = self.search(query, limit, include_documents)?;
        let filtered_results: Vec<super::SearchResult> = results
            .into_iter()
            .filter(|result| result.score <= threshold)
            .collect();
        Ok(filtered_results)
    }

    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError> {
        // Check if document exists
        let id_mapping = self.id_mapping.read().unwrap();
        let internal_id = match id_mapping.get(document_id) {
            Some(id) => *id,
            None => return Ok(false),
        };
        drop(id_mapping);

        // Remove from HNSW index
        let mut hnsw_index = self.hnsw_index.write().unwrap();
        hnsw_index
            .remove(internal_id)
            .map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to remove vector from HNSW index: {}",
                    e
                )))
            })?;
        drop(hnsw_index);

        // Remove from mappings
        let mut id_mapping = self.id_mapping.write().unwrap();
        let mut reverse_id_mapping = self.reverse_id_mapping.write().unwrap();
        let mut document_map = self.document_map.write().unwrap();

        id_mapping.remove(document_id);
        reverse_id_mapping.remove(&internal_id);
        document_map.remove(document_id);

        Ok(true)
    }

    fn size(&self) -> usize {
        let hnsw_index = self.hnsw_index.read().unwrap();
        hnsw_index.size()
    }

    fn clear(&mut self) -> Result<(), CortexError> {
        // Recreate HNSW index with same configuration
        let new_index = HnswIndex::new(
            self.config.dimension,
            self.config.ef_construction.unwrap_or(100),
            self.config.ef_search.unwrap_or(10),
        );

        *self.hnsw_index.write().unwrap() = new_index;
        *self.document_map.write().unwrap() = HashMap::new();
        *self.id_mapping.write().unwrap() = HashMap::new();
        *self.reverse_id_mapping.write().unwrap() = HashMap::new();
        *self.next_id.write().unwrap() = 0;

        Ok(())
    }

    fn optimize(&mut self) -> Result<(), CortexError> {
        // HNSW index doesn't require explicit optimization
        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value, CortexError> {
        let size = self.size();
        let dimension = self.config.dimension;
        let max_connections = self.config.ef_construction.unwrap_or(100);
        let ef_construction = self.config.ef_construction.unwrap_or(100);
        let ef_search = self.config.ef_search.unwrap_or(10);

        let stats = serde_json::json!({
            "type": "hnsw",
            "size": size,
            "dimension": dimension,
            "max_connections": max_connections,
            "ef_construction": ef_construction,
            "ef_search": ef_search,
            "distance_metric": format!("{:?}", self.config.distance_metric),
        });

        Ok(stats)
    }
}

/// HNSW index builder
#[derive(Debug)]
pub struct HnswIndexBuilder {
    config: HnswIndexConfig,
}

impl HnswIndexBuilder {
    /// Create a new HNSW index builder
    pub fn new(config: HnswIndexConfig) -> Self {
        Self { config }
    }
}

impl VectorIndexBuilder for HnswIndexBuilder {
    fn build(&self) -> Result<Arc<dyn VectorIndex>, CortexError> {
        let hnsw_index = HnswIndex::new(
            self.config.base_config.dimension,
            self.config.max_connections,
            self.config.ef_construction,
        );

        let index = HnswVectorIndex {
            config: self.config.base_config.clone(),
            hnsw_index: Arc::new(RwLock::new(hnsw_index)),
            document_map: Arc::new(RwLock::new(HashMap::new())),
            id_mapping: Arc::new(RwLock::new(HashMap::new())),
            reverse_id_mapping: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(0)),
        };

        Ok(Arc::new(index))
    }

    fn index_type(&self) -> &str {
        "hnsw"
    }
}

/// Test utilities for HNSW index
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Float32Vector;

    #[test]
    fn test_hnsw_index_builder() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Cosine,
            dimension: 3,
            ef_construction: Some(100),
            ef_search: Some(10),
            num_layers: None,
            parameters: None,
        };

        let hnsw_config = HnswIndexConfig {
            base_config: config,
            max_connections: 16,
            ef_construction: 100,
            ef_search: 10,
        };

        let builder = HnswIndexBuilder::new(hnsw_config);
        let index = builder.build().unwrap();

        assert_eq!(index.config().dimension, 3);
        assert_eq!(index.config().distance_metric, DistanceMetric::Cosine);
    }

    #[test]
    fn test_hnsw_index_add_search() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Cosine,
            dimension: 3,
            ef_construction: Some(100),
            ef_search: Some(10),
            num_layers: None,
            parameters: None,
        };

        let hnsw_config = HnswIndexConfig {
            base_config: config,
            max_connections: 16,
            ef_construction: 100,
            ef_search: 10,
        };

        let builder = HnswIndexBuilder::new(hnsw_config);
        let mut index = builder.build().unwrap();
        let mut index = Arc::get_mut(&mut index).unwrap();

        // Add vectors
        let vec1 = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        let vec2 = Float32Vector::from(vec![0.0, 1.0, 0.0]);
        let vec3 = Float32Vector::from(vec![0.0, 0.0, 1.0]);

        index.add("doc1", &vec1, None).unwrap();
        index.add("doc2", &vec2, None).unwrap();
        index.add("doc3", &vec3, None).unwrap();

        assert_eq!(index.size(), 3);

        // Search
        let query = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        let results = index.search(&query, 2, false).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document_id, "doc1");
        assert!(results[0].score < results[1].score);
    }

    #[test]
    fn test_hnsw_index_remove() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Cosine,
            dimension: 3,
            ef_construction: Some(100),
            ef_search: Some(10),
            num_layers: None,
            parameters: None,
        };

        let hnsw_config = HnswIndexConfig {
            base_config: config,
            max_connections: 16,
            ef_construction: 100,
            ef_search: 10,
        };

        let builder = HnswIndexBuilder::new(hnsw_config);
        let mut index = builder.build().unwrap();
        let mut index = Arc::get_mut(&mut index).unwrap();

        // Add vectors
        let vec1 = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        let vec2 = Float32Vector::from(vec![0.0, 1.0, 0.0]);

        index.add("doc1", &vec1, None).unwrap();
        index.add("doc2", &vec2, None).unwrap();

        assert_eq!(index.size(), 2);

        // Remove vector
        let removed = index.remove("doc1").unwrap();
        assert!(removed);
        assert_eq!(index.size(), 1);

        // Search after removal
        let query = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        let results = index.search(&query, 2, false).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].document_id, "doc2");
    }
}
