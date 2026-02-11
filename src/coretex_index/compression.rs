//! Vector Index Compression Algorithms for coretexdb Enterprise
//!
//! This module implements Product Quantization (PQ) and Residual Vector Quantization (RVQ)
//! for compressing high-dimensional vectors while maintaining high recall accuracy.
//!
//! Product Quantization divides the vector space into subspaces and quantizes each
//! independently using k-means clustering. Residual Vector Quantization applies
//! quantization iteratively on residuals for higher compression ratios.

use ndarray::{Array2, ArrayView2, Axis};
use rand::distributions::{Distribution, WeightedIndex};
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;
use rayon::prelude::*;

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Training data insufficient: {0}")]
    InsufficientTrainingData(String),

    #[error("Dimension mismatch: {0}")]
    DimensionMismatch(String),

    #[error("Cluster error: {0}")]
    ClusterError(String),

    #[error("Compression error: {0}")]
    CompressionError(String),
}

/// Distance metric for compressed vectors
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressedDistance {
    /// L2 (Euclidean) distance
    L2,
    /// Inner product
    InnerProduct,
    /// Cosine distance
    Cosine,
}

/// Configuration for Product Quantization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizationConfig {
    /// Number of subspaces (must divide original dimension evenly)
    pub num_subspaces: usize,
    /// Number of centroids per subspace (typically 256)
    pub num_centroids: usize,
    /// Number of iterations for k-means training
    pub training_iterations: usize,
    /// Number of threads for parallel training
    pub num_threads: usize,
    /// Subspace dimension (computed from original dim / num_subspaces)
    pub subspace_dim: Option<usize>,
    /// Whether to use symmetric distance
    pub symmetric_distance: bool,
    /// Distance metric
    pub distance_metric: CompressedDistance,
}

impl Default for ProductQuantizationConfig {
    fn default() -> Self {
        Self {
            num_subspaces: 8,
            num_centroids: 256,
            training_iterations: 20,
            num_threads: rayon::current_num_threads(),
            subspace_dim: None,
            symmetric_distance: true,
            distance_metric: CompressedDistance::L2,
        }
    }
}

/// Configuration for Residual Vector Quantization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidualQuantizationConfig {
    /// Number of quantization levels
    pub num_levels: usize,
    /// Number of centroids per level
    pub num_centroids: usize,
    /// Number of subspaces per level (for product quantization at each level)
    pub num_subspaces: usize,
    /// Training iterations per level
    pub training_iterations: usize,
    /// Whether to normalize residuals
    pub normalize_residuals: bool,
    /// Number of bits per code (computed from num_centroids)
    pub bits_per_code: Option<usize>,
    /// Distance metric
    pub distance_metric: CompressedDistance,
}

impl Default for ResidualQuantizationConfig {
    fn default() -> Self {
        Self {
            num_levels: 4,
            num_centroids: 256,
            num_subspaces: 4,
            training_iterations: 15,
            normalize_residuals: true,
            bits_per_code: None,
            distance_metric: CompressedDistance::L2,
        }
    }
}

/// Codebook entry for PQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodebookEntry {
    /// Centroid vector
    pub centroid: Vec<f32>,
    /// Variance of the cluster
    pub variance: f32,
    /// Number of vectors in this cluster
    pub count: usize,
}

/// Codebook for a single subspace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubspaceCodebook {
    /// Subspace index
    pub subspace_index: usize,
    /// Centroids for this subspace (num_centroids x subspace_dim)
    pub centroids: Vec<Vec<f32>>,
    /// Precomputed distances for asymmetric distance computation
    pub distance_table: Option<Vec<Vec<f32>>>,
    /// Number of vectors assigned to each centroid
    pub centroid_counts: Vec<usize>,
}

impl SubspaceCodebook {
    /// Create distance lookup table for asymmetric distance computation
    pub fn build_distance_table(&mut self, query: &[f32]) -> Vec<f32> {
        self.centroids
            .iter()
            .map(|centroid| {
                Self::compute_distance(centroid, query, CompressedDistance::L2)
            })
            .collect()
    }

    fn compute_distance(a: &[f32], b: &[f32], _metric: CompressedDistance) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y) * (x - y))
            .sum()
    }
}

/// Product Quantization encoder/decoder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantization {
    /// Configuration
    config: ProductQuantizationConfig,
    /// Original vector dimension
    original_dim: usize,
    /// Subspace dimension
    subspace_dim: usize,
    /// Codebooks for each subspace
    codebooks: Vec<SubspaceCodebook>,
    /// Total number of codes
    num_codes: usize,
    /// Whether the model is trained
    trained: bool,
}

impl ProductQuantization {
    /// Create a new PQ instance
    pub fn new(config: ProductQuantizationConfig, dimension: usize) -> Result<Self, CompressionError> {
        let subspace_dim = config
            .subspace_dim
            .unwrap_or(dimension / config.num_subspaces);

        if dimension % config.num_subspaces != 0 {
            return Err(CompressionError::DimensionMismatch(format!(
                "Dimension {} must be divisible by num_subspaces {}",
                dimension, config.num_subspaces
            )));
        }

        let num_codes = config.num_centroids.pow(config.num_subspaces as u32);

        Ok(Self {
            config,
            original_dim: dimension,
            subspace_dim,
            codebooks: Vec::with_capacity(config.num_subspaces),
            num_codes,
            trained: false,
        })
    }

    /// Train the PQ model on training data
    pub fn train(&mut self, training_data: &[Vec<f32>]) -> Result<(), CompressionError> {
        if training_data.len() < self.config.num_centroids {
            return Err(CompressionError::InsufficientTrainingData(format!(
                "Need at least {} vectors for training, got {}",
                self.config.num_centroids,
                training_data.len()
            )));
        }

        info!(
            "Training PQ with {} vectors, {} subspaces, {} centroids",
            training_data.len(),
            self.config.num_subspaces,
            self.config.num_centroids
        );

        // Convert to array for efficient slicing
        let data_array = Array2::from_rows(training_data.as_slice());

        // Split into subspaces and train each codebook
        for subspace_idx in 0..self.config.num_subspaces {
            let start = subspace_idx * self.subspace_dim;
            let end = start + self.subspace_dim;

            let subspace_data: Vec<Vec<f32>> = data_array
                .columns()
                .iter()
                .skip(start)
                .take(self.subspace_dim)
                .map(|col| col.to_vec())
                .collect();

            // Transpose to get vectors as rows
            let subspace_vectors: Vec<Vec<f32>> = (0..self.original_dim)
                .map(|i| {
                    subspace_data
                        .iter()
                        .skip(i)
                        .step_by(self.original_dim)
                        .take(1)
                        .cloned()
                        .flatten()
                        .collect()
                })
                .collect();

            let codebook = self.train_subspace_codebook(&subspace_vectors, subspace_idx)?;
            self.codebooks.push(codebook);
        }

        self.trained = true;
        info!("PQ training completed");

        Ok(())
    }

    /// Train a single subspace codebook
    fn train_subspace_codebook(
        &self,
        vectors: &[Vec<f32>],
        subspace_idx: usize,
    ) -> Result<SubspaceCodebook, CompressionError> {
        let k = self.config.num_centroids;
        let n = vectors.len();
        let d = self.subspace_dim;

        // Initialize centroids using k-means++
        let mut rng = StdRng::from_entropy();
        let mut centroids = self.kmeans_plus_plus_init(vectors, k, &mut rng);

        // K-means iterations
        for _ in 0..self.config.training_iterations {
            // Assignment step
            let assignments: Vec<usize> = vectors
                .par_iter()
                .map(|vec| self.find_nearest_centroid(vec, &centroids))
                .collect();

            // Update step
            let mut new_centroids: Vec<Vec<f32>> = vec![vec![0.0; d]; k];
            let mut centroid_counts: Vec<usize> = vec![0; k];

            for (i, assignment) in assignments.iter().enumerate() {
                let vec = &vectors[*assignment];
                for (j, val) in vec.iter().enumerate() {
                    new_centroids[*assignment][j] += val;
                }
                centroid_counts[*assignment] += 1;
            }

            // Compute means and variance
            let mut total_variance = 0.0;
            for i in 0..k {
                if centroid_counts[i] > 0 {
                    for j in 0..d {
                        new_centroids[i][j] /= centroid_counts[i] as f32;
                    }
                } else {
                    // Reinitialize empty centroid
                    centroids[i] = vectors[rng.gen::<usize>() % n].clone();
                }

                // Compute variance
                let var: f32 = vectors
                    .iter()
                    .filter(|v| self.find_nearest_centroid(v, &centroids) == i)
                    .map(|v| {
                        self.compute_distance(&new_centroids[i], v, CompressedDistance::L2)
                    })
                    .sum();
                total_variance += var / n as f32;
            }

            centroids = new_centroids;

            info!(
                "PQ subspace {} iteration complete, variance: {:.4}",
                subspace_idx, total_variance
            );
        }

        Ok(SubspaceCodebook {
            subspace_index: subspace_idx,
            centroids,
            distance_table: None,
            centroid_counts: vec![0; k],
        })
    }

    /// K-means++ initialization
    fn kmeans_plus_plus_init(
        &self,
        vectors: &[Vec<f32>],
        k: usize,
        rng: &mut StdRng,
    ) -> Vec<Vec<f32>> {
        let n = vectors.len();
        let d = self.subspace_dim;

        // Choose first centroid uniformly at random
        let first_idx = rng.gen::<usize>() % n;
        let mut centroids = vec![vectors[first_idx].clone()];

        // Choose remaining centroids
        while centroids.len() < k {
            let mut distances: Vec<f32> = Vec::with_capacity(n);
            let mut sum_distances = 0.0;

            for vec in vectors {
                // Find distance to nearest centroid
                let min_dist = centroids
                    .iter()
                    .map(|c| self.compute_distance(c, vec, CompressedDistance::L2))
                    .fold(f32::MAX, f32::min);
                distances.push(min_dist);
                sum_distances += min_dist;
            }

            // Choose next centroid with probability proportional to distance squared
            let weights: Vec<f32> = distances.iter().map(|d| d * d / sum_distances).collect();
            let dist = WeightedIndex::new(&weights).unwrap();
            centroids.push(vectors[dist.sample(rng)].clone());
        }

        centroids
    }

    /// Find the nearest centroid to a vector
    fn find_nearest_centroid(&self, vec: &[f32], centroids: &[Vec<f32>]) -> usize {
        centroids
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                self.compute_distance(a, vec, CompressedDistance::L2)
                    .partial_cmp(&self.compute_distance(b, vec, CompressedDistance::L2))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap()
            .0
    }

    /// Compute distance between two vectors
    fn compute_distance(a: &[f32], b: &[f32], _metric: CompressedDistance) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y) * (x - y))
            .sum()
    }

    /// Encode a single vector to PQ codes
    pub fn encode(&self, vector: &[f32]) -> Result<PQCode, CompressionError> {
        if !self.trained {
            return Err(CompressionError::CompressionError(
                "Model not trained".to_string(),
            ));
        }

        if vector.len() != self.original_dim {
            return Err(CompressionError::DimensionMismatch(format!(
                "Expected dimension {}, got {}",
                self.original_dim,
                vector.len()
            )));
        }

        let mut codes = Vec::with_capacity(self.config.num_subspaces);

        for (subspace_idx, codebook) in self.codebooks.iter().enumerate() {
            let start = subspace_idx * self.subspace_dim;
            let end = start + self.subspace_dim;
            let subspace_vec = &vector[start..end];

            let code = codebook
                .centroids
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    self.compute_distance(a, subspace_vec, CompressedDistance::L2)
                        .partial_cmp(&self.compute_distance(b, subspace_vec, CompressedDistance::L2))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap()
                .0 as u32;

            codes.push(code);
        }

        Ok(PQCode {
            codes,
            num_subspaces: self.config.num_subspaces,
        })
    }

    /// Encode multiple vectors in batch
    pub fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<PQCode>, CompressionError> {
        vectors
            .iter()
            .map(|v| self.encode(v))
            .collect()
    }

    /// Decode a PQ code back to a vector
    pub fn decode(&self, code: &PQCode) -> Result<Vec<f32>, CompressionError> {
        if !self.trained {
            return Err(CompressionError::CompressionError(
                "Model not trained".to_string(),
            ));
        }

        if code.codes.len() != self.config.num_subspaces {
            return Err(CompressionError::DimensionMismatch(format!(
                "Expected {} codes, got {}",
                self.config.num_subspaces,
                code.codes.len()
            )));
        }

        let mut result = Vec::with_capacity(self.original_dim);

        for (subspace_idx, code) in code.codes.iter().enumerate() {
            let centroid = &self.codebooks[subspace_idx].centroids[*code as usize];
            result.extend_from_slice(centroid);
        }

        Ok(result)
    }

    /// Compute asymmetric distance between query and code
    pub fn asymmetric_distance(&self, query: &[f32], code: &PQCode) -> f32 {
        let mut total_distance = 0.0;

        for (subspace_idx, subspace_code) in code.codes.iter().enumerate() {
            let start = subspace_idx * self.subspace_dim;
            let end = start + self.subspace_dim;
            let query_subspace = &query[start..end];

            let centroid = &self.codebooks[subspace_idx].centroids[*subspace_code as usize];
            let dist = self.compute_distance(query_subspace, centroid, self.config.distance_metric);
            total_distance += dist;
        }

        total_distance.sqrt()
    }

    /// Compute symmetric distance between two codes
    pub fn symmetric_distance(&self, code1: &PQCode, code2: &PQCode) -> f32 {
        let mut total_distance = 0.0;

        for (subspace_idx, (c1, c2)) in code1
            .codes
            .iter()
            .zip(code2.codes.iter())
            .enumerate()
        {
            let centroid1 = &self.codebooks[subspace_idx].centroids[*c1 as usize];
            let centroid2 = &self.codebooks[subspace_idx].centroids[*c2 as usize];
            let dist = self.compute_distance(centroid1, centroid2, self.config.distance_metric);
            total_distance += dist;
        }

        total_distance.sqrt()
    }

    /// Search for nearest neighbors using PQ
    pub fn search(
        &self,
        query: &[f32],
        codes: &[PQCode],
        k: usize,
    ) -> Vec<(usize, f32)> {
        let distances: Vec<(usize, f32)> = codes
            .iter()
            .enumerate()
            .map(|(idx, code)| {
                let dist = if self.config.symmetric_distance {
                    // For symmetric distance, we need to decode first
                    // In practice, asymmetric is preferred for speed
                    self.asymmetric_distance(query, code)
                } else {
                    self.asymmetric_distance(query, code)
                };
                (idx, dist)
            })
            .collect();

        // Partial sort to get top k
        let mut distances = distances;
        distances.select_nth_unstable_by(k, |(_, a), (_, b)| {
            a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
        });

        distances.into_iter().take(k).collect()
    }

    /// Get the compression ratio
    pub fn compression_ratio(&self) -> f32 {
        let original_bits = self.original_dim * 32; // f32 = 32 bits
        let compressed_bits = self.config.num_subspaces as f32
            * self.config.num_centroids.ilog2() as f32; // code per subspace

        original_bits as f32 / compressed_bits
    }

    /// Get memory savings percentage
    pub fn memory_savings(&self) -> f32 {
        1.0 - (1.0 / self.compression_ratio())
    }

    /// Get configuration
    pub fn config(&self) -> &ProductQuantizationConfig {
        &self.config
    }

    /// Check if trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }

    /// Get number of codebooks
    pub fn num_codebooks(&self) -> usize {
        self.codebooks.len()
    }
}

/// PQ code representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PQCode {
    /// Codes for each subspace
    pub codes: Vec<u32>,
    /// Number of subspaces
    pub num_subspaces: usize,
}

impl PQCode {
    /// Create a new PQ code
    pub fn new(codes: Vec<u32>) -> Self {
        Self {
            codes,
            num_subspaces: codes.len(),
        }
    }

    /// Serialize to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.codes.len() * 4);
        for code in &self.codes {
            bytes.extend_from_slice(&code.to_le_bytes());
        }
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let num_codes = bytes.len() / 4;
        let mut codes = Vec::with_capacity(num_codes);
        for i in 0..num_codes {
            let code = u32::from_le_bytes([
                bytes[i * 4],
                bytes[i * 4 + 1],
                bytes[i * 4 + 2],
                bytes[i * 4 + 3],
            ]);
            codes.push(code);
        }
        Self {
            codes,
            num_subspaces: num_codes,
        }
    }
}

/// Residual Vector Quantization encoder/decoder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidualVectorQuantization {
    /// Configuration
    config: ResidualQuantizationConfig,
    /// Original dimension
    original_dim: usize,
    /// Codebooks for each level
    level_codebooks: Vec<ProductQuantization>,
    /// Quantization scales for each level
    scales: Vec<f32>,
    /// Whether trained
    trained: bool,
}

impl ResidualVectorQuantization {
    /// Create a new RVQ instance
    pub fn new(config: ResidualQuantizationConfig, dimension: usize) -> Result<Self, CompressionError> {
        let bits_per_code = config
            .bits_per_code
            .unwrap_or(config.num_centroids.ilog2() as usize);

        Ok(Self {
            config,
            original_dim: dimension,
            level_codebooks: Vec::new(),
            scales: Vec::new(),
            trained: false,
        })
    }

    /// Train the RVQ model
    pub fn train(&mut self, training_data: &[Vec<f32>]) -> Result<(), CompressionError> {
        let mut residuals: Vec<Vec<f32>> = training_data.to_vec();

        info!(
            "Training RVQ with {} levels, {} centroids per level",
            self.config.num_levels, self.config.num_centroids
        );

        for level in 0..self.config.num_levels {
            info!("Training level {}", level + 1);

            // Create PQ for this level
            let pq_config = ProductQuantizationConfig {
                num_subspaces: self.config.num_subspaces,
                num_centroids: self.config.num_centroids,
                training_iterations: self.config.training_iterations,
                num_threads: rayon::current_num_threads(),
                subspace_dim: Some(self.original_dim / self.config.num_subspaces),
                symmetric_distance: false,
                distance_metric: self.config.distance_metric,
            };

            let mut pq = ProductQuantization::new(pq_config, self.original_dim)?;
            pq.train(&residuals)?;

            // Compute scale factor for this level
            let scale = self.compute_scale(&residuals, &pq)?;
            self.scales.push(scale);

            // Update residuals
            let codes: Vec<PQCode> = pq.encode_batch(&residuals)?;
            residuals = residuals
                .into_iter()
                .zip(codes)
                .map(|(residual, code)| {
                    let reconstructed = pq.decode(&code).unwrap();
                    residual
                        .into_iter()
                        .zip(reconstructed)
                        .map(|(r, rec)| (r - rec) * scale)
                        .collect()
                })
                .collect();

            self.level_codebooks.push(pq);
        }

        self.trained = true;
        info!("RVQ training completed");

        Ok(())
    }

    /// Compute scale factor for quantization
    fn compute_scale(
        &self,
        residuals: &[Vec<f32>],
        pq: &ProductQuantization,
    ) -> Result<f32, CompressionError> {
        let codes: Vec<PQCode> = pq.encode_batch(residuals)?;

        let mut total_error = 0.0;
        for (residual, code) in residuals.iter().zip(codes.iter()) {
            let reconstructed = pq.decode(code)?;
            let error: f32 = residual
                .iter()
                .zip(reconstructed.iter())
                .map(|(r, rec)| (r - rec).powi(2))
                .sum();
            total_error += error.sqrt();
        }

        let avg_error = total_error / residuals.len() as f32;
        if avg_error > 0.0 {
            Ok(1.0 / avg_error)
        } else {
            Ok(1.0)
        }
    }

    /// Encode a vector using RVQ
    pub fn encode(&self, vector: &[f32]) -> Result<RVQCode, CompressionError> {
        if !self.trained {
            return Err(CompressionError::CompressionError(
                "Model not trained".to_string(),
            ));
        }

        let mut residual = vector.to_vec();
        let mut codes = Vec::new();

        for (level, pq) in self.level_codebooks.iter().enumerate() {
            let code = pq.encode(&residual)?;
            let reconstructed = pq.decode(&code)?;

            if self.config.normalize_residuals {
                let scale = self.scales[level];
                for i in 0..residual.len() {
                    residual[i] = (residual[i] - reconstructed[i]) * scale;
                }
            } else {
                for i in 0..residual.len() {
                    residual[i] -= reconstructed[i];
                }
            }

            codes.push(code);
        }

        Ok(RVQCode {
            level_codes: codes,
            num_levels: self.config.num_levels,
        })
    }

    /// Decode an RVQ code
    pub fn decode(&self, code: &RVQCode) -> Result<Vec<f32>, CompressionError> {
        if !self.trained {
            return Err(CompressionError::CompressionError(
                "Model not trained".to_string(),
            ));
        }

        let mut result = vec![0.0; self.original_dim];

        for (level, pq) in self.level_codebooks.iter().enumerate() {
            let decoded = pq.decode(&code.level_codes[level])?;

            if self.config.normalize_residuals {
                let scale = self.scales[level];
                for i in 0..result.len() {
                    result[i] += decoded[i] * scale;
                }
            } else {
                for i in 0..result.len() {
                    result[i] += decoded[i];
                }
            }
        }

        Ok(result)
    }

    /// Compute distance using RVQ codes
    pub fn distance(&self, query: &[f32], code: &RVQCode) -> f32 {
        let mut query_copy = query.to_vec();
        let mut total_distance = 0.0;

        for (level, pq) in self.level_codebooks.iter().enumerate() {
            let distance = pq.asymmetric_distance(&query_copy, &code.level_codes[level]);
            total_distance += distance * distance;

            // Update query for next level
            let reconstructed = pq.decode(&code.level_codes[level]).unwrap();
            if self.config.normalize_residuals {
                let scale = self.scales[level];
                for i in 0..query_copy.len() {
                    query_copy[i] = (query_copy[i] - reconstructed[i]) * scale;
                }
            } else {
                for i in 0..query_copy.len() {
                    query_copy[i] -= reconstructed[i];
                }
            }
        }

        total_distance.sqrt()
    }

    /// Search for nearest neighbors
    pub fn search(&self, query: &[f32], codes: &[RVQCode], k: usize) -> Vec<(usize, f32)> {
        let distances: Vec<(usize, f32)> = codes
            .iter()
            .enumerate()
            .map(|(idx, code)| (idx, self.distance(query, code)))
            .collect();

        let mut distances = distances;
        distances.select_nth_unstable_by(k, |(_, a), (_, b)| {
            a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
        });

        distances.into_iter().take(k).collect()
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f32 {
        let base_pq = &self.level_codebooks[0];
        let base_ratio = base_pq.compression_ratio();
        base_ratio.powi(self.config.num_levels as i32)
    }

    /// Get memory savings
    pub fn memory_savings(&self) -> f32 {
        1.0 - (1.0 / self.compression_ratio())
    }

    /// Check if trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }
}

/// RVQ code representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RVQCode {
    /// Codes for each level
    pub level_codes: Vec<PQCode>,
    /// Number of levels
    pub num_levels: usize,
}

impl RVQCode {
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for code in &self.level_codes {
            bytes.extend_from_slice(&code.to_bytes());
        }
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8], num_levels: usize, num_subspaces: usize) -> Self {
        let code_size = num_subspaces * 4;
        let level_codes: Vec<PQCode> = bytes
            .chunks(code_size)
            .map(|chunk| PQCode::from_bytes(chunk))
            .collect();

        Self {
            level_codes,
            num_levels,
        }
    }
}

/// Compressed vector storage with PQ/RVQ
#[derive(Debug, Clone)]
pub struct CompressedVectorStore {
    /// Original dimension
    original_dim: usize,
    /// Compressed codes
    codes: Arc<RwLock<Vec<PQCode>>>,
    /// Product Quantization encoder
    pq: Arc<RwLock<Option<ProductQuantization>>>,
    /// Residual Vector Quantization (optional)
    rvq: Arc<RwLock<Option<ResidualVectorQuantization>>>,
    /// Use RVQ instead of PQ
    use_rvq: bool,
    /// Compression type
    compression_type: CompressionType,
    /// ID mapping
    id_map: Arc<RwLock<HashMap<String, usize>>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression
    None,
    /// Product Quantization
    PQ(ProductQuantizationConfig),
    /// Residual Vector Quantization
    RVQ(ResidualQuantizationConfig),
}

impl CompressedVectorStore {
    /// Create a new compressed store
    pub fn new(original_dim: usize, compression_type: CompressionType) -> Self {
        let use_rvq = matches!(compression_type, CompressionType::RVQ(_));

        Self {
            original_dim,
            codes: Arc::new(RwLock::new(Vec::new())),
            pq: Arc::new(RwLock::new(None)),
            rvq: Arc::new(RwLock::new(None)),
            use_rvq,
            compression_type,
            id_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Train compression model
    pub fn train(&mut self, training_data: &[Vec<f32>]) -> Result<(), CompressionError> {
        match self.compression_type {
            CompressionType::PQ(ref config) => {
                let mut pq = ProductQuantization::new(config.clone(), self.original_dim)?;
                pq.train(training_data)?;
                *self.pq.write().unwrap() = Some(pq);
            }
            CompressionType::RVQ(ref config) => {
                let mut rvq = ResidualVectorQuantization::new(config.clone(), self.original_dim)?;
                rvq.train(training_data)?;
                *self.rvq.write().unwrap() = Some(rvq);
            }
            CompressionType::None => {}
        }

        Ok(())
    }

    /// Add vectors to the store
    pub fn add_vectors(
        &mut self,
        ids: &[String],
        vectors: &[Vec<f32>],
    ) -> Result<(), CompressionError> {
        let mut codes = self.codes.write().unwrap();

        for (id, vector) in ids.iter().zip(vectors.iter()) {
            let code = if self.use_rvq {
                let rvq = self.rvq.read().unwrap();
                if let Some(ref rvq) = *rvq {
                    let code = rvq.encode(vector)?;
                    PQCode::new(code.level_codes.iter().flatten().cloned().collect())
                } else {
                    return Err(CompressionError::CompressionError(
                        "RVQ not trained".to_string(),
                    ));
                }
            } else {
                let pq = self.pq.read().unwrap();
                if let Some(ref pq) = *pq {
                    pq.encode(vector)?
                } else {
                    return Err(CompressionError::CompressionError(
                        "PQ not trained".to_string(),
                    ));
                }
            };

            let index = codes.len();
            codes.push(code);

            let mut id_map = self.id_map.write().unwrap();
            id_map.insert(id.clone(), index);
        }

        Ok(())
    }

    /// Search nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>, CompressionError> {
        let codes = self.codes.read().unwrap();
        let id_map = self.id_map.read().unwrap();

        if codes.is_empty() {
            return Ok(Vec::new());
        }

        let results = if self.use_rvq {
            let rvq = self.rvq.read().unwrap();
            if let Some(ref rvq) = *rvq {
                // Convert PQ codes back to RVQ codes
                let rvq_codes: Vec<RVQCode> = codes
                    .iter()
                    .map(|pq_code| {
                        let num_subspaces = self.original_dim / rvq.config.num_subspaces;
                        let level_codes: Vec<PQCode> = pq_code
                            .codes
                            .chunks(num_subspaces)
                            .map(|chunk| PQCode::new(chunk.to_vec()))
                            .collect();

                        RVQCode {
                            level_codes,
                            num_levels: rvq.config.num_levels,
                        }
                    })
                    .collect();

                rvq.search(query, &rvq_codes, k)
            } else {
                return Err(CompressionError::CompressionError(
                    "RVQ not trained".to_string(),
                ));
            }
        } else {
            let pq = self.pq.read().unwrap();
            if let Some(ref pq) = *pq {
                pq.search(query, &codes, k)
            } else {
                return Err(CompressionError::CompressionError(
                    "PQ not trained".to_string(),
                ));
            }
        };

        let results: Vec<(String, f32)> = results
            .into_iter()
            .filter_map(|(idx, dist)| {
                id_map
                    .iter()
                    .find(|(_, &i)| i == idx)
                    .map(|(id, _)| (id.clone(), dist))
            })
            .collect();

        Ok(results)
    }

    /// Get memory usage
    pub fn memory_usage(&self) -> usize {
        let codes = self.codes.read().unwrap();
        codes.len() * self.code_size_bytes()
    }

    /// Get code size in bytes
    fn code_size_bytes(&self) -> usize {
        let subspaces = if self.use_rvq {
            let rvq = self.rvq.read().unwrap();
            rvq.as_ref()
                .map(|rvq| rvq.config.num_subspaces * rvq.config.num_levels)
                .unwrap_or(0)
        } else {
            let pq = self.pq.read().unwrap();
            pq.as_ref().map(|pq| pq.config.num_subspaces).unwrap_or(0)
        };

        subspaces * 4 // u32 per subspace code
    }

    /// Get number of stored vectors
    pub fn len(&self) -> usize {
        self.codes.read().unwrap().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get compression info
    pub fn compression_info(&self) -> CompressionInfo {
        let info = match self.compression_type {
            CompressionType::PQ(ref config) => {
                let pq = self.pq.read().unwrap();
                if let Some(ref pq) = *pq {
                    Some(CompressionAlgorithmInfo {
                        algorithm: "Product Quantization".to_string(),
                        subspaces: config.num_subspaces,
                        centroids: config.num_centroids,
                        compression_ratio: pq.compression_ratio(),
                        memory_savings: pq.memory_savings(),
                    })
                } else {
                    None
                }
            }
            CompressionType::RVQ(ref config) => {
                let rvq = self.rvq.read().unwrap();
                if let Some(ref rvq) = *rvq {
                    Some(CompressionAlgorithmInfo {
                        algorithm: "Residual Vector Quantization".to_string(),
                        subspaces: config.num_subspaces * config.num_levels,
                        centroids: config.num_centroids,
                        compression_ratio: rvq.compression_ratio(),
                        memory_savings: rvq.memory_savings(),
                    })
                } else {
                    None
                }
            }
            CompressionType::None => None,
        };

        CompressionInfo {
            original_dim: self.original_dim,
            num_vectors: self.len(),
            memory_usage_bytes: self.memory_usage(),
            algorithm_info: info,
        }
    }
}

/// Information about compression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionInfo {
    /// Original vector dimension
    pub original_dim: usize,
    /// Number of stored vectors
    pub num_vectors: usize,
    /// Current memory usage
    pub memory_usage_bytes: usize,
    /// Algorithm information
    pub algorithm_info: Option<CompressionAlgorithmInfo>,
}

/// Information about compression algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionAlgorithmInfo {
    /// Algorithm name
    pub algorithm: String,
    /// Number of subspaces
    pub subspaces: usize,
    /// Number of centroids
    pub centroids: usize,
    /// Compression ratio
    pub compression_ratio: f32,
    /// Memory savings percentage
    pub memory_savings: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pq_creation() {
        let config = ProductQuantizationConfig {
            num_subspaces: 8,
            num_centroids: 256,
            ..Default::default()
        };

        let pq = ProductQuantization::new(config, 128).unwrap();
        assert_eq!(pq.original_dim, 128);
        assert_eq!(pq.subspace_dim, 16);
    }

    #[test]
    fn test_pq_dimension_mismatch() {
        let config = ProductQuantizationConfig {
            num_subspaces: 7,
            num_centroids: 256,
            ..Default::default()
        };

        let result = ProductQuantization::new(config, 128);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pq_train_and_encode() {
        // Generate training data
        let training_data: Vec<Vec<f32>> = (0..1000)
            .map(|_| (0..128).map(|_| rand::random::<f32>()).collect())
            .collect();

        let config = ProductQuantizationConfig {
            num_subspaces: 8,
            num_centroids: 256,
            training_iterations: 5,
            ..Default::default()
        };

        let mut pq = ProductQuantization::new(config, 128).unwrap();
        pq.train(&training_data).unwrap();

        // Encode a vector
        let test_vector: Vec<f32> = (0..128).map(|_| rand::random::<f32>()).collect();
        let code = pq.encode(&test_vector).unwrap();

        assert_eq!(code.codes.len(), 8);

        // Decode
        let decoded = pq.decode(&code).unwrap();
        assert_eq!(decoded.len(), 128);

        // Check compression ratio
        let ratio = pq.compression_ratio();
        assert!(ratio > 1.0);
        println!("Compression ratio: {:.2}x", ratio);
    }

    #[tokio::test]
    async fn test_pq_batch_encode() {
        let training_data: Vec<Vec<f32>> = (0..500)
            .map(|_| (0..64).map(|_| rand::random::<f32>()).collect())
            .collect();

        let config = ProductQuantizationConfig {
            num_subspaces: 4,
            num_centroids: 256,
            training_iterations: 3,
            ..Default::default()
        };

        let mut pq = ProductQuantization::new(config, 64).unwrap();
        pq.train(&training_data).unwrap();

        // Batch encode
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|_| (0..64).map(|_| rand::random::<f32>()).collect())
            .collect();

        let codes = pq.encode_batch(&vectors).unwrap();
        assert_eq!(codes.len(), 100);
    }

    #[test]
    fn test_pq_code_serialization() {
        let code = PQCode::new(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let bytes = code.to_bytes();
        let decoded = PQCode::from_bytes(&bytes);

        assert_eq!(code.codes, decoded.codes);
    }

    #[tokio::test]
    async fn test_compressed_vector_store() {
        let store = CompressedVectorStore::new(
            128,
            CompressionType::PQ(ProductQuantizationConfig {
                num_subspaces: 8,
                num_centroids: 256,
                training_iterations: 5,
                ..Default::default()
            }),
        );

        // Generate training data
        let training_data: Vec<Vec<f32>> = (0..1000)
            .map(|_| (0..128).map(|_| rand::random::<f32>()).collect())
            .collect();

        let mut store = store;
        store.train(&training_data).unwrap();

        // Add vectors
        let ids: Vec<String> = (0..100).map(|i| format!("vec_{}", i)).collect();
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|_| (0..128).map(|_| rand::random::<f32>()).collect())
            .collect();

        store.add_vectors(&ids, &vectors).unwrap();

        // Search
        let query: Vec<f32> = (0..128).map(|_| rand::random::<f32>()).collect();
        let results = store.search(&query, 10).unwrap();

        assert_eq!(results.len(), 10);

        // Check compression info
        let info = store.compression_info();
        println!("Compression info: {:?}", info.algorithm_info);
    }

    #[tokio::test]
    async fn test_rvq_train() {
        let training_data: Vec<Vec<f32>> = (0..2000)
            .map(|_| (0..128).map(|_| rand::random::<f32>()).collect())
            .collect();

        let config = ResidualQuantizationConfig {
            num_levels: 4,
            num_centroids: 256,
            num_subspaces: 4,
            ..Default::default()
        };

        let mut rvq = ResidualVectorQuantization::new(config, 128).unwrap();
        rvq.train(&training_data).unwrap();

        // Encode
        let test_vector: Vec<f32> = (0..128).map(|_| rand::random::<f32>()).collect();
        let code = rvq.encode(&test_vector).unwrap();

        assert_eq!(code.level_codes.len(), 4);

        // Decode
        let decoded = rvq.decode(&code).unwrap();
        assert_eq!(decoded.len(), 128);

        // Check compression ratio
        let ratio = rvq.compression_ratio();
        println!("RVQ compression ratio: {:.2}x", ratio);
        assert!(ratio > 8.0); // Should be much higher than PQ alone
    }
}

