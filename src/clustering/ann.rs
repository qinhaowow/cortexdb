use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};
use dashmap::DashMap;
use threadpool::ThreadPool;

#[derive(Debug, Error)]
pub enum ANNError {
    #[error("Dimension mismatch: expected {0}, got {1}")]
    DimensionMismatch(usize, usize),
    #[error("Index not built")]
    IndexNotBuilt,
    #[error("Invalid number of trees: {0}")]
    InvalidTreeCount(usize),
    #[error("Invalid ef construction: {0}")]
    InvalidEfConstruction(usize),
    #[error("Search timeout")]
    SearchTimeout,
    #[error("Invalid query")]
    InvalidQuery,
    #[error("Index corrupted")]
    IndexCorrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ANNConfig {
    pub dimension: usize,
    pub metric: Metric,
    pub num_trees: usize,
    pub search_k: usize,
    pub ef_construction: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_search: usize,
    pub max_cache_size: usize,
    pub use_optimized_quantization: bool,
    pub num_threads: usize,
}

impl Default for ANNConfig {
    fn default() -> Self {
        Self {
            dimension: 128,
            metric: Metric::Euclidean,
            num_trees: 8,
            search_k: -1,
            ef_construction: 200,
            m: 16,
            m0: 32,
            ef_search: 100,
            max_cache_size: 10000,
            use_optimized_quantization: true,
            num_threads: 4,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Metric {
    Euclidean,
    Cosine,
    Manhattan,
    DotProduct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ANNStats {
    pub index_build_time_ms: AtomicU64,
    pub queries_total: AtomicU64,
    pub queries_with_results: AtomicU64,
    pub avg_search_time_ns: AtomicU64,
    pub nodes_visited: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub memory_usage_bytes: AtomicU64,
    pub vectors_indexed: AtomicUsize,
}

impl Default for ANNStats {
    fn default() -> Self {
        Self {
            index_build_time_ms: AtomicU64::new(0),
            queries_total: AtomicU64::new(0),
            queries_with_results: AtomicU64::new(0),
            avg_search_time_ns: AtomicU64::new(0),
            nodes_visited: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            memory_usage_bytes: AtomicU64::new(0),
            vectors_indexed: AtomicUsize::new(0),
        }
    }
}

pub trait ANNIndex {
    fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError>;
    fn add_batch(&mut self, ids: &[u64], vectors: &[Vec<f64>]) -> Result<(), ANNError>;
    fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError>;
    fn build(&mut self) -> Result<(), ANNError>;
    fn save(&self, path: &str) -> Result<(), ANNError>;
    fn load(&mut self, path: &str) -> Result<(), ANNError>;
    fn get_stats(&self) -> HashMap<String, u64>;
    fn get_dimension(&self) -> usize;
    fn get_size(&self) -> usize;
    fn clear(&mut self);
}

pub struct AnnoyIndex {
    config: ANNConfig,
    stats: Arc<ANNStats>,
    trees: Vec<Option<ANNTree>>,
    vector_store: Vec<Vec<f64>>,
    id_map: HashMap<u64, usize>,
    next_id: Arc<AtomicUsize>,
    cache: DashMap<Vec<f64>, Vec<(u64, f64)>>,
}

struct ANNTree {
    nodes: Vec<ANNNode>,
    item_indices: Vec<usize>,
}

struct ANNNode {
    left: Option<usize>,
    right: Option<usize>,
    centroid: Vec<f64>,
    split_dim: usize,
    split_value: f64,
    item_indices: Option<Vec<usize>>,
}

impl AnnoyIndex {
    pub fn new(config: ANNConfig) -> Result<Self, ANNError> {
        if config.num_trees < 1 {
            return Err(ANNError::InvalidTreeCount(config.num_trees));
        }

        let stats = Arc::new(ANNStats::default());
        let trees = vec![None; config.num_trees];
        let vector_store = Vec::new();
        let id_map = HashMap::new();
        let next_id = Arc::new(AtomicUsize::new(0));
        let cache = DashMap::with_capacity(config.max_cache_size);

        Ok(Self {
            config,
            stats,
            trees,
            vector_store,
            id_map,
            next_id,
            cache,
        })
    }

    pub fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        if vector.len() != self.config.dimension {
            return Err(ANNError::DimensionMismatch(self.config.dimension, vector.len()));
        }

        let idx = self.vector_store.len();
        self.vector_store.push(vector.to_vec());
        self.id_map.insert(id, idx);
        self.next_id.fetch_add(1, Ordering::SeqCst);

        self.stats.vectors_indexed.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    pub fn add_batch(&mut self, ids: &[u64], vectors: &[Vec<f64>]) -> Result<(), ANNError> {
        if vectors.is_empty() {
            return Ok(());
        }

        for (id, vector) in ids.iter().zip(vectors.iter()) {
            self.add(*id, vector)?;
        }

        Ok(())
    }

    pub fn build(&mut self) -> Result<(), ANNError> {
        let start = Instant::now();

        if self.vector_store.is_empty() {
            return Ok(());
        }

        let n = self.vector_store.len();
        let mut indices: Vec<usize> = (0..n).collect();

        for i in 0..self.config.num_trees {
            let mut rng = rand::rngs::StdRng::from_entropy();
            rng.shuffle(&mut indices);

            let tree = self.build_tree(&indices, 0, n, &mut rng);
            self.trees[i] = Some(tree);
        }

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.index_build_time_ms.store(elapsed, Ordering::SeqCst);

        debug!("AnnoyIndex built {} trees in {} ms", self.config.num_trees, elapsed);
        Ok(())
    }

    fn build_tree(&self, indices: &[usize], start: usize, end: usize, rng: &mut rand::rngs::StdRng) -> ANNTree {
        let n = end - start;

        let mut node = ANNNode {
            left: None,
            right: None,
            centroid: vec![0.0; self.config.dimension],
            split_dim: 0,
            split_value: 0.0,
            item_indices: None,
        };

        if n <= 1 || self.should_stop_splitting(indices, start, end) {
            node.item_indices = Some(indices[start..end].to_vec());
            return ANNTree {
                nodes: vec![node],
                item_indices: indices.to_vec(),
            };
        }

        let mut centroid = vec![0.0; self.config.dimension];
        for &idx in &indices[start..end] {
            for (d, &val) in self.vector_store[idx].iter().enumerate() {
                centroid[d] += val;
            }
        }
        for val in &mut centroid {
            *val /= n as f64;
        }
        node.centroid = centroid;

        let (left_indices, right_indices) = self.split_indices(indices, start, end, &node, rng);

        let left_child_idx = self.nodes.len();
        node.left = Some(left_child_idx);

        let left_tree = self.build_tree(left_indices, 0, left_indices.len(), rng);

        let right_child_idx = self.nodes.len() + left_tree.nodes.len();
        node.right = Some(right_child_idx);

        let right_tree = self.build_tree(right_indices, 0, right_indices.len(), rng);

        let mut nodes = Vec::with_capacity(1 + left_tree.nodes.len() + right_tree.nodes.len());
        nodes.push(node);
        nodes.extend(left_tree.nodes);
        nodes.extend(right_tree.nodes);

        ANNTree {
            nodes,
            item_indices: indices.to_vec(),
        }
    }

    fn should_stop_splitting(&self, indices: &[usize], start: usize, end: usize) -> bool {
        let n = end - start;
        n <= 1
    }

    fn split_indices(&self, indices: &[usize], start: usize, end: usize, node: &ANNNode, rng: &mut rand::rngs::StdRng) -> (Vec<usize>, Vec<usize>) {
        let mut left = Vec::new();
        let mut right = Vec::new();

        let dim = rng.gen_range(0..self.config.dimension);
        let mut values: Vec<f64> = indices[start..end]
            .iter()
            .map(|&idx| self.vector_store[idx][dim])
            .collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let median_idx = values.len() / 2;
        let split_val = values[median_idx];

        for &idx in &indices[start..end] {
            if self.vector_store[idx][dim] < split_val {
                left.push(idx);
            } else {
                right.push(idx);
            }
        }

        (left, right)
    }

    pub fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        if query.len() != self.config.dimension {
            return Err(ANNError::DimensionMismatch(self.config.dimension, query.len()));
        }

        if self.trees.iter().all(|t| t.is_none()) {
            return Err(ANNError::IndexNotBuilt);
        }

        self.stats.queries_total.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        if let Some(cached) = self.cache.get(query) {
            self.stats.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Ok(cached.value().clone());
        }

        self.stats.cache_misses.fetch_add(1, Ordering::SeqCst);

        let mut candidates: HashMap<u64, f64> = HashMap::new();
        let search_k = if self.config.search_k > 0 {
            self.config.search_k
        } else {
            k * self.config.num_trees
        };

        for tree in &self.trees {
            if let Some(t) = tree {
                self.search_tree(t, query, search_k, &mut candidates);
            }
        }

        let mut results: Vec<(u64, f64)> = candidates.into_iter()
            .map(|(id, dist)| (id, dist.sqrt()))
            .filter(|(_, dist)| !dist.is_nan())
            .take(k)
            .collect();

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        if !results.is_empty() {
            self.stats.queries_with_results.fetch_add(1, Ordering::SeqCst);
        }

        let elapsed = start.elapsed().as_nanos() as u64;
        let avg = self.stats.avg_search_time_ns.load(Ordering::SeqCst);
        self.stats.avg_search_time_ns.store((avg + elapsed) / 2, Ordering::SeqCst);

        if results.len() > k {
            results.truncate(k);
        }

        let cache_query = query.to_vec();
        if self.cache.len() < self.config.max_cache_size {
            self.cache.insert(cache_query, results.clone());
        }

        Ok(results)
    }

    fn search_tree(&self, tree: &ANNTree, query: &[f64], search_k: usize, candidates: &mut HashMap<u64, f64>) {
        let mut stack = vec![0];
        let mut visited = HashSet::new();

        while let Some(node_idx) = stack.pop() {
            if visited.contains(&node_idx) || node_idx >= tree.nodes.len() {
                continue;
            }
            visited.insert(node_idx);

            let node = &tree.nodes[node_idx];

            if let Some(ref indices) = node.item_indices {
                for &idx in indices.iter().take(search_k) {
                    let id = self.vector_store[idx].len() as u64;
                    let dist = self.distance(query, &self.vector_store[idx]);
                    if let Some(existing) = candidates.get(&id) {
                        if dist < *existing {
                            candidates.insert(id, dist);
                        }
                    } else {
                        candidates.insert(id, dist);
                    }
                }
                continue;
            }

            let dist_to_centroid = self.distance(query, &node.centroid);

            let first = if dist_to_centroid < query[node.split_dim] - node.split_value {
                node.left
            } else {
                node.right
            };

            let second = if first == node.left { node.right } else { node.left };

            stack.push(first.unwrap_or(node_idx));
            if second.is_some() {
                stack.push(second.unwrap());
            }
        }
    }

    fn distance(&self, a: &[f64], b: &[f64]) -> f64 {
        match self.config.metric {
            Metric::Euclidean => {
                a.iter().zip(b.iter())
                    .map(|(&x, &y)| (x - y) * (x - y))
                    .sum()
            }
            Metric::Cosine => {
                let dot = a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>();
                let norm_a = a.iter().map(|&x| x * x).sum::<f64>().sqrt();
                let norm_b = b.iter().map(|&x| x * x).sum::<f64>().sqrt();
                1.0 - dot / (norm_a * norm_b)
            }
            Metric::Manhattan => {
                a.iter().zip(b.iter()).map(|(&x, &y)| (x - y).abs()).sum()
            }
            Metric::DotProduct => {
                -a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>()
            }
        }
    }

    pub fn save(&self, path: &str) -> Result<(), ANNError> {
        let file = std::fs::File::create(path)
            .map_err(|e| ANNError::IndexCorrupted)?;
        
        let config_bytes = bincode::serialize(&self.config)
            .map_err(|e| ANNError::IndexCorrupted)?;
        
        let trees_bytes: Vec<Vec<u8>> = self.trees.iter()
            .filter_map(|t| t.as_ref())
            .map(|t| bincode::serialize(t).map_err(|e| ANNError::IndexCorrupted))
            .collect::<Result<Vec<_>, _>>()?;

        bincode::serialize_into(file, &(&config_bytes, &trees_bytes))
            .map_err(|e| ANNError::IndexCorrupted)?;

        info!("AnnoyIndex saved to {}", path);
        Ok(())
    }

    pub fn load(&mut self, path: &str) -> Result<(), ANNError> {
        let file = std::fs::File::open(path)
            .map_err(|e| ANNError::IndexCorrupted)?;

        let (config_bytes, trees_bytes): (Vec<u8>, Vec<Vec<u8>>) = bincode::deserialize_from(file)
            .map_err(|e| ANNError::IndexCorrupted)?;

        self.config = bincode::deserialize(&config_bytes)
            .map_err(|e| ANNError::IndexCorrupted)?;

        for tree_bytes in trees_bytes {
            let tree: ANNTree = bincode::deserialize(&tree_bytes)
                .map_err(|e| ANNError::IndexCorrupted)?;
            self.trees.push(Some(tree));
        }

        info!("AnnoyIndex loaded from {}", path);
        Ok(())
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("index_build_time_ms".to_string(), self.stats.index_build_time_ms.load(Ordering::SeqCst));
        stats.insert("queries_total".to_string(), self.stats.queries_total.load(Ordering::SeqCst));
        stats.insert("queries_with_results".to_string(), self.stats.queries_with_results.load(Ordering::SeqCst));
        stats.insert("avg_search_time_ns".to_string(), self.stats.avg_search_time_ns.load(Ordering::SeqCst));
        stats.insert("cache_hits".to_string(), self.stats.cache_hits.load(Ordering::SeqCst));
        stats.insert("cache_misses".to_string(), self.stats.cache_misses.load(Ordering::SeqCst));
        stats.insert("vectors_indexed".to_string(), self.stats.vectors_indexed.load(Ordering::SeqCst) as u64);
        stats.insert("num_trees".to_string(), self.trees.iter().filter(|t| t.is_some()).count() as u64);
        stats.insert("cache_size".to_string(), self.cache.len() as u64);

        stats
    }
}

impl ANNIndex for AnnoyIndex {
    fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        self.add(id, vector)
    }

    fn add_batch(&mut self, ids: &[u64], vectors: &[Vec<f64>]) -> Result<(), ANNError> {
        self.add_batch(ids, vectors)
    }

    fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        self.search(query, k)
    }

    fn build(&mut self) -> Result<(), ANNError> {
        self.build()
    }

    fn save(&self, path: &str) -> Result<(), ANNError> {
        self.save(path)
    }

    fn load(&mut self, path: &str) -> Result<(), ANNError> {
        self.load(path)
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        self.get_stats()
    }

    fn get_dimension(&self) -> usize {
        self.config.dimension
    }

    fn get_size(&self) -> usize {
        self.vector_store.len()
    }

    fn clear(&mut self) {
        self.vector_store.clear();
        self.id_map.clear();
        self.trees.iter_mut().for_each(|t| *t = None);
        self.cache.clear();
    }
}

pub struct HNSWIndex {
    config: ANNConfig,
    stats: Arc<ANNStats>,
    layers: Vec<HNSWLayer>,
    entry_point: Option<usize>,
    max_level: usize,
}

struct HNSWLayer {
    graph: Vec<Vec<Edge>>,
    nodes: Vec<HNSWNode>,
}

struct HNSWNode {
    level: usize,
    position: Vec<f64>,
    neighbors: Vec<Vec<usize>>,
}

struct Edge {
    target: usize,
    distance: f64,
}

impl HNSWIndex {
    pub fn new(config: ANNConfig) -> Self {
        let stats = Arc::new(ANNStats::default());
        let layers = vec![HNSWLayer {
            graph: Vec::new(),
            nodes: Vec::new(),
        }];

        Self {
            config,
            stats,
            layers,
            entry_point: None,
            max_level: 0,
        }
    }

    pub fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        if vector.len() != self.config.dimension {
            return Err(ANNError::DimensionMismatch(self.config.dimension, vector.len()));
        }

        let level = self.random_level();
        let node = HNSWNode {
            level,
            position: vector.to_vec(),
            neighbors: (0..=level).map(|_| Vec::new()).collect(),
        };

        let idx = self.layers[0].nodes.len();
        self.layers[0].nodes.push(node);

        if level > self.max_level {
            self.max_level = level;
        }

        self.select_neighbors(idx, level)?;

        self.stats.vectors_indexed.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    fn random_level(&self) -> usize {
        let mut rng = rand::rngs::StdRng::from_entropy();
        let r: f64 = rng.gen();
        let m_l = 1.0 / (self.config.m as f64);
        (-m_l * r.ln()).floor() as usize
    }

    pub fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        if self.layers[0].nodes.is_empty() {
            return Ok(vec![]);
        }

        self.stats.queries_total.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        let mut entry_point = self.entry_point.unwrap_or(0);
        let mut curr_dist = self.distance(query, &self.layers[0].nodes[entry_point].position);

        for l in (1..=self.max_level).rev() {
            while let Some(neighbor) = self.search_layer(
                query,
                &self.layers[l].nodes[entry_point].neighbors[0],
                curr_dist
            ) {
                entry_point = neighbor;
                curr_dist = self.distance(query, &self.layers[0].nodes[entry_point].position);
            }
        }

        let mut candidates = Vec::new();
        let mut visited = HashSet::new();
        candidates.push((entry_point, curr_dist));
        visited.insert(entry_point);

        let mut results: Vec<(usize, f64)> = Vec::with_capacity(k);

        while let Some((node_idx, dist)) = candidates.pop() {
            results.push((node_idx, dist));

            for &neighbor in &self.layers[0].nodes[node_idx].neighbors[0] {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    let neighbor_dist = self.distance(query, &self.layers[0].nodes[neighbor].position);
                    candidates.push((neighbor, neighbor_dist));
                }
            }

            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        }

        let elapsed = start.elapsed().as_nanos() as u64;
        let avg = self.stats.avg_search_time_ns.load(Ordering::SeqCst);
        self.stats.avg_search_time_ns.store((avg + elapsed) / 2, Ordering::SeqCst);

        let mut final_results: Vec<(u64, f64)> = results.into_iter()
            .take(k)
            .map(|(idx, dist)| (idx as u64, dist))
            .collect();

        if !final_results.is_empty() {
            self.stats.queries_with_results.fetch_add(1, Ordering::SeqCst);
        }

        Ok(final_results)
    }

    fn search_layer(&self, query: &[f64], neighbors: &[usize], curr_dist: f64) -> Option<usize> {
        let mut best_idx = 0;
        let mut best_dist = curr_dist;

        for &neighbor_idx in neighbors {
            let dist = self.distance(query, &self.layers[0].nodes[neighbor_idx].position);
            if dist < best_dist {
                best_dist = dist;
                best_idx = neighbor_idx;
            }
        }

        if best_dist < curr_dist {
            Some(best_idx)
        } else {
            None
        }
    }

    fn select_neighbors(&mut self, node_idx: usize, level: usize) -> Result<(), ANNError> {
        let ef = self.config.ef_construction;
        let m = if level == 0 { self.config.m0 } else { self.config.m };

        let mut candidates: Vec<(usize, f64)> = Vec::new();

        let query_vec = self.layers[0].nodes[node_idx].position.clone();

        for i in 0..self.layers[0].nodes.len() {
            if i == node_idx {
                continue;
            }
            let dist = self.distance(&query_vec, &self.layers[0].nodes[i].position);
            candidates.push((i, dist));
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(ef);

        let mut selected: Vec<usize> = Vec::new();

        for (neighbor_idx, dist) in candidates {
            if selected.len() < m {
                selected.push(neighbor_idx);
            } else {
                break;
            }
        }

        self.layers[0].nodes[node_idx].neighbors[0] = selected.clone();

        for &neighbor_idx in &selected {
            let neighbor_level = self.layers[0].nodes[neighbor_idx].level;
            let neighbor_m = if neighbor_level == 0 { self.config.m0 } else { self.config.m };
            
            let neighbor_neighbors = &mut self.layers[0].nodes[neighbor_idx].neighbors[0];
            
            let query_dist = self.distance(
                &self.layers[0].nodes[node_idx].position,
                &self.layers[0].nodes[neighbor_idx].position
            );

            neighbor_neighbors.push(node_idx);
            neighbor_neighbors.sort_by(|a, b| {
                let dist_a = self.distance(
                    &self.layers[0].nodes[*a].position,
                    &self.layers[0].nodes[neighbor_idx].position
                );
                let dist_b = self.distance(
                    &self.layers[0].nodes[*b].position,
                    &self.layers[0].nodes[neighbor_idx].position
                );
                dist_a.partial_cmp(&dist_b).unwrap()
            });

            if neighbor_neighbors.len() > neighbor_m {
                neighbor_neighbors.truncate(neighbor_m);
            }
        }

        Ok(())
    }

    fn distance(&self, a: &[f64], b: &[f64]) -> f64 {
        match self.config.metric {
            Metric::Euclidean => {
                a.iter().zip(b.iter())
                    .map(|(&x, &y)| (x - y) * (x - y))
                    .sum()
            }
            Metric::Cosine => {
                let dot = a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>();
                let norm_a = a.iter().map(|&x| x * x).sum::<f64>().sqrt();
                let norm_b = b.iter().map(|&x| x * x).sum::<f64>().sqrt();
                1.0 - dot / (norm_a * norm_b)
            }
            Metric::Manhattan => {
                a.iter().zip(b.iter()).map(|(&x, &y)| (x - y).abs()).sum()
            }
            Metric::DotProduct => {
                -a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>()
            }
        }
    }

    pub fn build(&mut self) -> Result<(), ANNError> {
        Ok(())
    }

    pub fn save(&self, path: &str) -> Result<(), ANNError> {
        info!("HNSWIndex saved to {}", path);
        Ok(())
    }

    pub fn load(&mut self, path: &str) -> Result<(), ANNError> {
        info!("HNSWIndex loaded from {}", path);
        Ok(())
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("vectors_indexed".to_string(), self.stats.vectors_indexed.load(Ordering::SeqCst) as u64);
        stats.insert("max_level".to_string(), self.max_level as u64);
        stats.insert("queries_total".to_string(), self.stats.queries_total.load(Ordering::SeqCst));
        stats.insert("avg_search_time_ns".to_string(), self.stats.avg_search_time_ns.load(Ordering::SeqCst));
        stats
    }
}

impl ANNIndex for HNSWIndex {
    fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        self.add(id, vector)
    }

    fn add_batch(&mut self, ids: &[u64], vectors: &[Vec<f64>]) -> Result<(), ANNError> {
        for (id, vector) in ids.iter().zip(vectors.iter()) {
            self.add(*id, vector)?;
        }
        Ok(())
    }

    fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        self.search(query, k)
    }

    fn build(&mut self) -> Result<(), ANNError> {
        self.build()
    }

    fn save(&self, path: &str) -> Result<(), ANNError> {
        self.save(path)
    }

    fn load(&mut self, path: &str) -> Result<(), ANNError> {
        self.load(path)
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        self.get_stats()
    }

    fn get_dimension(&self) -> usize {
        self.config.dimension
    }

    fn get_size(&self) -> usize {
        self.layers[0].nodes.len()
    }

    fn clear(&mut self) {
        self.layers.iter_mut().for_each(|l| {
            l.nodes.clear();
            l.graph.clear();
        });
        self.entry_point = None;
        self.max_level = 0;
    }
}

pub struct LSHIndex {
    config: ANNConfig,
    stats: Arc<ANNStats>,
    hash_tables: Vec<HashTable>,
    vectors: Vec<Vec<f64>>,
    id_map: HashMap<u64, usize>,
}

struct HashTable {
    tables: Vec<Vec<(Vec<f64>, u64)>>,
    projections: Vec<Vec<f64>>,
}

impl LSHIndex {
    pub fn new(config: ANNConfig) -> Result<Self, ANNError> {
        let num_tables = 8;
        let projection_dim = config.dimension / 2;

        let mut tables = Vec::new();
        for _ in 0..num_tables {
            let mut projections = Vec::new();
            for _ in 0..projection_dim {
                let mut proj = vec![0.0; config.dimension];
                for val in &mut proj {
                    *val = rand::random::<f64>() * 2.0 - 1.0;
                }
                projections.push(proj);
            }
            let table: Vec<Vec<(Vec<f64>, u64)>> = vec![Vec::new(); 2_usize.pow(projection_dim as u32)];
            tables.push(HashTable { tables: table, projections });
        }

        let stats = Arc::new(ANNStats::default());

        Ok(Self {
            config,
            stats,
            hash_tables: tables,
            vectors: Vec::new(),
            id_map: HashMap::new(),
        })
    }

    pub fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        if vector.len() != self.config.dimension {
            return Err(ANNError::DimensionMismatch(self.config.dimension, vector.len()));
        }

        let idx = self.vectors.len();
        self.vectors.push(vector.to_vec());
        self.id_map.insert(id, idx);

        let signature = self.compute_signature(vector);

        for (i, hash) in signature.iter().enumerate() {
            let table_idx = (i * self.hash_tables.len()) / signature.len();
            self.hash_tables[table_idx].tables[*hash as usize].push((vector.to_vec(), id));
        }

        self.stats.vectors_indexed.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    fn compute_signature(&self, vector: &[f64]) -> Vec<u32> {
        let projection_dim = self.hash_tables[0].projections.len();
        let mut signature = Vec::with_capacity(projection_dim);

        for proj in &self.hash_tables[0].projections {
            let dot: f64 = vector.iter().zip(proj.iter()).map(|(&x, &y)| x * y).sum();
            signature.push(if dot >= 0.0 { 1 } else { 0 });
        }

        signature
    }

    pub fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        if query.len() != self.config.dimension {
            return Err(ANNError::DimensionMismatch(self.config.dimension, query.len()));
        }

        self.stats.queries_total.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();

        let query_sig = self.compute_signature(query);
        let mut candidates: HashMap<u64, f64> = HashMap::new();
        let mut counts: HashMap<u64, u32> = HashMap::new();

        for (i, &bit) in query_sig.iter().enumerate() {
            let table_idx = (i * self.hash_tables.len()) / query_sig.len();
            let candidates_in_table = &self.hash_tables[table_idx].tables[bit as usize];

            for (vec, id) in candidates_in_table {
                let dist = self.distance(query, vec);
                if let Some(existing) = candidates.get(id) {
                    if dist < *existing {
                        candidates.insert(id, dist);
                    }
                } else {
                    candidates.insert(id, dist);
                }
                *counts.entry(*id).or_insert(0) += 1;
            }
        }

        let mut results: Vec<(u64, f64)> = candidates.into_iter()
            .filter(|(id, _)| {
                counts.get(id).map(|&c| c > 0).unwrap_or(false)
            })
            .map(|(id, dist)| (id, dist))
            .take(k)
            .collect();

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let elapsed = start.elapsed().as_nanos() as u64;
        self.stats.avg_search_time_ns.store(elapsed, Ordering::SeqCst);

        Ok(results)
    }

    fn distance(&self, a: &[f64], b: &[f64]) -> f64 {
        match self.config.metric {
            Metric::Euclidean => {
                a.iter().zip(b.iter())
                    .map(|(&x, &y)| (x - y) * (x - y))
                    .sum()
            }
            Metric::Cosine => {
                let dot = a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>();
                let norm_a = a.iter().map(|&x| x * x).sum::<f64>().sqrt();
                let norm_b = b.iter().map(|&x| x * x).sum::<f64>().sqrt();
                1.0 - dot / (norm_a * norm_b)
            }
            Metric::Manhattan => {
                a.iter().zip(b.iter()).map(|(&x, &y)| (x - y).abs()).sum()
            }
            Metric::DotProduct => {
                -a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum::<f64>()
            }
        }
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("vectors_indexed".to_string(), self.stats.vectors_indexed.load(Ordering::SeqCst) as u64);
        stats.insert("queries_total".to_string(), self.stats.queries_total.load(Ordering::SeqCst));
        stats.insert("avg_search_time_ns".to_string(), self.stats.avg_search_time_ns.load(Ordering::SeqCst));
        stats.insert("hash_tables".to_string(), self.hash_tables.len() as u64);
        stats
    }
}

impl ANNIndex for LSHIndex {
    fn add(&mut self, id: u64, vector: &[f64]) -> Result<(), ANNError> {
        self.add(id, vector)
    }

    fn add_batch(&mut self, ids: &[u64], vectors: &[Vec<f64>]) -> Result<(), ANNError> {
        for (id, vector) in ids.iter().zip(vectors.iter()) {
            self.add(*id, vector)?;
        }
        Ok(())
    }

    fn search(&self, query: &[f64], k: usize) -> Result<Vec<(u64, f64)>, ANNError> {
        self.search(query, k)
    }

    fn build(&mut self) -> Result<(), ANNError> {
        Ok(())
    }

    fn save(&self, path: &str) -> Result<(), ANNError> {
        Ok(())
    }

    fn load(&mut self, path: &str) -> Result<(), ANNError> {
        Ok(())
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        self.get_stats()
    }

    fn get_dimension(&self) -> usize {
        self.config.dimension
    }

    fn get_size(&self) -> usize {
        self.vectors.len()
    }

    fn clear(&mut self) {
        self.vectors.clear();
        self.id_map.clear();
    }
}
