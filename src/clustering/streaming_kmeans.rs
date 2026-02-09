use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};
use rayon::prelude::*;
use linalg::Vector;

#[derive(Debug, Error)]
pub enum KMeansError {
    #[error("Invalid number of clusters: {0}")]
    InvalidClusterCount(usize),
    #[error("Empty data set")]
    EmptyDataSet,
    #[error("Convergence failed after {0} iterations")]
    ConvergenceFailed(usize),
    #[error("Dimension mismatch: expected {0}, got {1}")]
    DimensionMismatch(usize, usize),
    #[error("Initialization failed")]
    InitializationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMeansConfig {
    pub k: usize,
    pub max_iterations: usize,
    pub tolerance: f64,
    pub min_clusters: usize,
    pub max_clusters: usize,
    pub decay_factor: f64,
    pub random_seed: Option<u64>,
    pub use_kmeans_plus_plus: bool,
    pub batch_size: usize,
    pub parallel_enabled: bool,
}

impl Default for KMeansConfig {
    fn default() -> Self {
        Self {
            k: 8,
            max_iterations: 100,
            tolerance: 1e-4,
            min_clusters: 2,
            max_clusters: 50,
            decay_factor: 0.0,
            random_seed: None,
            use_kmeans_plus_plus: true,
            batch_size: 1000,
            parallel_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMeansCluster {
    pub id: usize,
    pub centroid: Vec<f64>,
    pub size: u64,
    pub variance: f64,
    pub created_at: u64,
    pub last_updated: u64,
    pub label: Option<String>,
}

impl KMeansCluster {
    fn new(id: usize, centroid: Vec<f64>) -> Self {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        Self {
            id,
            centroid,
            size: 0,
            variance: 0.0,
            created_at: now,
            last_updated: now,
            label: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMeansStats {
    pub iterations: AtomicUsize,
    pub total_points_processed: AtomicU64,
    pub converged: AtomicUsize,
    pub centroid_updates: AtomicU64,
    pub total_time_ms: AtomicU64,
    pub avg_assignment_time_ns: AtomicU64,
    pub cluster_sizes: Vec<AtomicUsize>,
}

impl Default for KMeansStats {
    fn default() -> Self {
        Self {
            iterations: AtomicUsize::new(0),
            total_points_processed: AtomicU64::new(0),
            converged: AtomicUsize::new(0),
            centroid_updates: AtomicU64::new(0),
            total_time_ms: AtomicU64::new(0),
            avg_assignment_time_ns: AtomicU64::new(0),
            cluster_sizes: Vec::new(),
        }
    }
}

pub struct StreamingKMeans {
    config: KMeansConfig,
    clusters: Vec<KMeansCluster>,
    cluster_weights: Vec<f64>,
    dimension: usize,
    stats: Arc<KMeansStats>,
    rng: rand::rngs::StdRng,
}

impl StreamingKMeans {
    pub fn new(config: KMeansConfig, dimension: usize) -> Result<Self, KMeansError> {
        if config.k < config.min_clusters || config.k > config.max_clusters {
            return Err(KMeansError::InvalidClusterCount(config.k));
        }

        let rng = match config.random_seed {
            Some(seed) => rand::SeedableRng::seed_from_u64(seed),
            None => rand::SeedableRng::from_entropy(),
        };

        let stats = Arc::new(KMeansStats {
            cluster_sizes: (0..config.k).map(|_| AtomicUsize::new(0)).collect(),
            ..Default::default()
        });

        Ok(Self {
            config,
            clusters: Vec::new(),
            cluster_weights: Vec::new(),
            dimension,
            stats,
            rng,
        })
    }

    pub fn initialize_clusters(&mut self, data: &[Vec<f64>]) -> Result<(), KMeansError> {
        if data.is_empty() {
            return Err(KMeansError::EmptyDataSet);
        }

        if data[0].len() != self.dimension {
            return Err(KMeansError::DimensionMismatch(self.dimension, data[0].len()));
        }

        if self.config.use_kmeans_plus_plus {
            self.initialize_kmeans_plus_plus(data)?;
        } else {
            self.initialize_random(data)?;
        }

        self.cluster_weights = vec![1.0; self.config.k];
        debug!("Initialized {} clusters using k-means++", self.config.k);
        Ok(())
    }

    fn initialize_kmeans_plus_plus(&mut self, data: &[Vec<f64>]) -> Result<(), KMeansError> {
        let n = data.len();
        if n == 0 {
            return Err(KMeansError::EmptyDataSet);
        }

        let mut rng = rand::rngs::StdRng::from_entropy();
        let first_idx = rng.gen_range(0..n);
        self.clusters.push(KMeansCluster::new(0, data[first_idx].clone()));

        let mut distances: Vec<f64> = data.iter()
            .map(|point| self.squared_distance(point, &data[first_idx]))
            .collect();

        for k in 1..self.config.k {
            let sum: f64 = distances.iter().sum();
            if sum == 0.0 {
                self.initialize_random(data)?;
                return Ok(());
            }

            let mut r: f64 = rng.gen();
            r *= sum;
            
            let mut cumulative = 0.0;
            let mut selected_idx = 0;
            for (i, d) in distances.iter().enumerate() {
                cumulative += d;
                if cumulative >= r {
                    selected_idx = i;
                    break;
                }
            }

            self.clusters.push(KMeansCluster::new(k, data[selected_idx].clone()));

            for (i, point) in data.iter().enumerate() {
                let d = self.squared_distance(point, &data[selected_idx]);
                if d < distances[i] {
                    distances[i] = d;
                }
            }
        }

        Ok(())
    }

    fn initialize_random(&mut self, data: &[Vec<f64>]) -> Result<(), KMeansError> {
        let mut rng = rand::rngs::StdRng::from_entropy();
        let mut used_indices = HashSet::new();

        for i in 0..self.config.k {
            let mut idx;
            loop {
                idx = rng.gen_range(0..data.len());
                if !used_indices.contains(&idx) {
                    used_indices.insert(idx);
                    break;
                }
                if used_indices.len() >= data.len() {
                    return Err(KMeansError::InitializationFailed);
                }
            }
            self.clusters.push(KMeansCluster::new(i, data[idx].clone()));
        }

        Ok(())
    }

    pub fn fit(&mut self, data: &[Vec<f64>]) -> Result<(), KMeansError> {
        let start = Instant::now();
        
        if self.clusters.is_empty() {
            self.initialize_clusters(data)?;
        }

        let mut assignments = vec![0; data.len()];
        let mut convergence = false;
        let mut iteration = 0;

        while iteration < self.config.max_iterations && !convergence {
            let old_centroids: Vec<Vec<f64>> = self.clusters.iter()
                .map(|c| c.centroid.clone())
                .collect();

            if self.config.parallel_enabled {
                self.assign_points_parallel(data, &mut assignments)?;
            } else {
                self.assign_points(data, &mut assignments)?;
            }

            self.update_centroids(data, &assignments)?;

            convergence = self.check_convergence(&old_centroids);
            self.stats.iterations.fetch_add(1, Ordering::SeqCst);

            if iteration % 10 == 0 {
                debug!("Iteration {}: convergence = {}", iteration, convergence);
            }
            iteration += 1;
        }

        if convergence {
            self.stats.converged.fetch_add(1, Ordering::SeqCst);
        }

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.total_time_ms.store(elapsed, Ordering::SeqCst);
        debug!("K-Means converged in {} iterations ({} ms)", iteration, elapsed);
        Ok(())
    }

    pub fn fit_stream(&mut self, data: &[Vec<f64>]) -> Result<(), KMeansError> {
        let batch_size = self.config.batch_size;
        
        for batch in data.chunks(batch_size) {
            let assignments = self.assign_points_batch(batch)?;
            self.update_centroids_batch(batch, &assignments)?;
            self.stats.total_points_processed.fetch_add(batch.len() as u64, Ordering::SeqCst);
        }

        if self.config.decay_factor > 0.0 {
            self.apply_decay();
        }

        self.stats.centroid_updates.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn assign_points(&self, data: &[Vec<f64>], assignments: &mut [usize]) -> Result<(), KMeansError> {
        for (i, point) in data.iter().enumerate() {
            assignments[i] = self.find_nearest_cluster(point)?;
        }
        Ok(())
    }

    fn assign_points_parallel(&self, data: &[Vec<f64>], assignments: &mut [usize]) -> Result<(), KMeansError> {
        let clusters_ref = &self.clusters;
        data.par_iter().enumerate().for_each(|(i, point)| {
            let mut min_dist = f64::MAX;
            let mut min_idx = 0;
            
            for (j, cluster) in clusters_ref.iter().enumerate() {
                let dist = self.squared_distance(point, &cluster.centroid);
                if dist < min_dist {
                    min_dist = dist;
                    min_idx = j;
                }
            }
            assignments[i] = min_idx;
        });
        Ok(())
    }

    fn assign_points_batch(&self, batch: &[Vec<f64>]) -> Result<Vec<usize>, KMeansError> {
        let mut assignments = Vec::with_capacity(batch.len());
        for point in batch {
            assignments.push(self.find_nearest_cluster(point)?);
        }
        Ok(assignments)
    }

    fn find_nearest_cluster(&self, point: &[f64]) -> Result<usize, KMeansError> {
        let mut min_dist = f64::MAX;
        let mut min_idx = 0;
        
        for (j, cluster) in self.clusters.iter().enumerate() {
            let dist = self.squared_distance(point, &cluster.centroid);
            if dist < min_dist {
                min_dist = dist;
                min_idx = j;
            }
        }
        
        Ok(min_idx)
    }

    fn update_centroids(&mut self, data: &[Vec<f64>], assignments: &[usize]) -> Result<(), KMeansError> {
        let new_sums: Vec<Vec<f64>> = vec![vec![0.0; self.dimension]; self.config.k];
        let mut counts: Vec<usize> = vec![0; self.config.k];

        for (point, &assignment) in data.iter().zip(assignments.iter()) {
            for (dim, val) in point.iter().enumerate() {
                new_sums[assignment][dim] += val;
            }
            counts[assignment] += 1;
        }

        for (i, cluster) in self.clusters.iter_mut().enumerate() {
            if counts[i] > 0 {
                for dim in 0..self.dimension {
                    cluster.centroid[dim] = new_sums[i][dim] / counts[i] as f64;
                }
                cluster.size = counts[i] as u64;
                self.stats.cluster_sizes[i].store(counts[i], Ordering::SeqCst);
            }
            cluster.last_updated = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        }

        self.balance_clusters()?;
        Ok(())
    }

    fn update_centroids_batch(&mut self, batch: &[Vec<f64>], assignments: &[usize]) -> Result<(), KMeansError> {
        let learning_rate = 1.0 / (self.cluster_weights.iter().sum::<f64>() + batch.len() as f64);
        
        for (point, &assignment) in batch.iter().zip(assignments.iter()) {
            let cluster = &mut self.clusters[assignment];
            let weight = self.cluster_weights[assignment];
            
            for (dim, val) in point.iter().enumerate() {
                cluster.centroid[dim] = cluster.centroid[dim] + learning_rate * (val - cluster.centroid[dim]) * weight;
            }
            cluster.size += 1;
            self.cluster_weights[assignment] += learning_rate;
        }

        self.balance_clusters()?;
        Ok(())
    }

    fn balance_clusters(&mut self) -> Result<(), KMeansError> {
        if self.clusters.len() < self.config.min_clusters {
            return Ok(());
        }

        let empty_clusters: Vec<usize> = self.clusters.iter()
            .enumerate()
            .filter(|(_, c)| c.size == 0)
            .map(|(i, _)| i)
            .collect();

        for idx in empty_clusters {
            if self.clusters.len() > self.config.min_clusters {
                self.clusters.swap_remove(idx);
                self.cluster_weights.swap_remove(idx);
                continue;
            }

            let largest_cluster_idx = self.clusters.iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.size.cmp(&b.size))
                .map(|(i, _)| i)
                .unwrap();

            let mut rng = rand::rngs::StdRng::from_entropy();
            let dim = self.dimension;
            let new_centroid: Vec<f64> = (0..dim)
                .map(|_| rng.gen_range(0.0..1.0))
                .collect();

            self.clusters[idx].centroid = new_centroid;
            self.clusters[idx].size = 0;
        }

        if self.clusters.len() > self.config.max_clusters {
            self.merge_smallest_clusters()?;
        }

        Ok(())
    }

    fn merge_smallest_clusters(&mut self) -> Result<(), KMeansError> {
        while self.clusters.len() > self.config.max_clusters {
            let smallest: Vec<usize> = self.clusters.iter()
                .enumerate()
                .sorted_by(|(_, a), (_, b)| a.size.cmp(&b.size))
                .take(2)
                .map(|(i, _)| i)
                .collect();

            if smallest.len() < 2 {
                break;
            }

            let merged_centroid: Vec<f64> = self.clusters[smallest[0]].centroid.iter()
                .zip(self.clusters[smallest[1]].centroid.iter())
                .map(|(a, b)| (a + b) / 2.0)
                .collect();

            self.clusters[smallest[0]].centroid = merged_centroid;
            self.clusters[smallest[0]].size += self.clusters[smallest[1]].size;
            
            self.clusters.swap_remove(smallest[1]);
            self.cluster_weights.swap_remove(smallest[1]);
        }

        Ok(())
    }

    fn apply_decay(&mut self) {
        let decay = 1.0 - self.config.decay_factor;
        for weight in &mut self.cluster_weights {
            *weight *= decay;
        }
    }

    fn check_convergence(&self, old_centroids: &[Vec<f64>]) -> bool {
        for (old, new) in old_centroids.iter().zip(self.clusters.iter()) {
            let dist = self.squared_distance(old, &new.centroid);
            if dist > self.config.tolerance {
                return false;
            }
        }
        true
    }

    fn squared_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y) * (x - y))
            .sum()
    }

    pub fn predict(&self, point: &[f64]) -> Result<usize, KMeansError> {
        if point.len() != self.dimension {
            return Err(KMeansError::DimensionMismatch(self.dimension, point.len()));
        }
        self.find_nearest_cluster(point)
    }

    pub fn predict_batch(&self, points: &[Vec<f64>]) -> Result<Vec<usize>, KMeansError> {
        let mut predictions = Vec::with_capacity(points.len());
        for point in points {
            predictions.push(self.predict(point)?);
        }
        Ok(predictions)
    }

    pub fn get_clusters(&self) -> &[KMeansCluster] {
        &self.clusters
    }

    pub fn get_cluster(&self, id: usize) -> Option<&KMeansCluster> {
        self.clusters.get(id)
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("iterations".to_string(), self.stats.iterations.load(Ordering::SeqCst) as u64);
        stats.insert("total_points".to_string(), self.stats.total_points_processed.load(Ordering::SeqCst));
        stats.insert("converged".to_string(), self.stats.converged.load(Ordering::SeqCst) as u64);
        stats.insert("centroid_updates".to_string(), self.stats.centroid_updates.load(Ordering::SeqCst));
        stats.insert("total_time_ms".to_string(), self.stats.total_time_ms.load(Ordering::SeqCst));
        stats.insert("num_clusters".to_string(), self.clusters.len() as u64);
        
        for (i, cluster) in self.clusters.iter().enumerate() {
            stats.insert(format!("cluster_{}_size", i), cluster.size);
        }
        
        stats
    }

    pub fn silhouette_score(&self, data: &[Vec<f64>], assignments: &[usize]) -> Result<f64, KMeansError> {
        let mut total_score = 0.0;
        
        for (i, point) in data.iter().enumerate() {
            let cluster_id = assignments[i];
            
            let a_i = self.average_intra_cluster_distance(point, cluster_id, data, assignments)?;
            let b_i = self.minimum_inter_cluster_distance(point, cluster_id, data, assignments)?;
            
            total_score += (b_i - a_i) / a_i.max(b_i);
        }
        
        Ok(total_score / data.len() as f64)
    }

    fn average_intra_cluster_distance(&self, point: &[f64], cluster_id: usize, data: &[Vec<f64>], assignments: &[usize]) -> Result<f64, KMeansError> {
        let cluster_points: Vec<&[f64]> = data.iter()
            .zip(assignments.iter())
            .filter(|(_, &a)| a == cluster_id)
            .map(|(p, _)| p.as_slice())
            .collect();

        if cluster_points.is_empty() {
            return Ok(0.0);
        }

        let sum: f64 = cluster_points.iter()
            .map(|&p| self.squared_distance(point, p))
            .sum();

        Ok((sum / cluster_points.len() as f64).sqrt())
    }

    fn minimum_inter_cluster_distance(&self, point: &[f64], cluster_id: usize, data: &[Vec<f64>], assignments: &[usize]) -> Result<f64, KMeansError> {
        let mut min_dist = f64::MAX;
        
        for (i, cluster) in self.clusters.iter().enumerate() {
            if i != cluster_id {
                let dist = self.squared_distance(point, &cluster.centroid);
                if dist < min_dist {
                    min_dist = dist;
                }
            }
        }

        Ok(min_dist.sqrt())
    }
}

impl Drop for StreamingKMeans {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "StreamingKMeans stats - Points: {}, Clusters: {}, Iterations: {}",
            stats.total_points_processed.load(Ordering::SeqCst),
            self.clusters.len(),
            stats.iterations.load(Ordering::SeqCst)
        );
    }
}
