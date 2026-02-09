use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum BirchError {
    #[error("Invalid branching factor: {0}")]
    InvalidBranchingFactor(usize),
    #[error("Invalid threshold: {0}")]
    InvalidThreshold(f64),
    #[error("Empty data set")]
    EmptyDataSet,
    #[error("Dimension mismatch: expected {0}, got {1}")]
    DimensionMismatch(usize, usize),
    #[error("Tree construction failed")]
    TreeConstructionFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CFEntry {
    pub n: u64,
    pub ls: Vec<f64>,
    pub ss: Vec<f64>,
    pub child_entries: Vec<Arc<BirchNode>>,
    pub parent: Option<Arc<BirchNode>>,
    pub created_at: u64,
}

impl CFEntry {
    pub fn new(dimension: usize) -> Self {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        Self {
            n: 0,
            ls: vec![0.0; dimension],
            ss: vec![0.0; dimension],
            child_entries: Vec::new(),
            parent: None,
            created_at: now,
        }
    }

    pub fn from_point(point: &[f64]) -> Self {
        let dimension = point.len();
        let mut entry = Self::new(dimension);
        entry.n = 1;
        for (i, &val) in point.iter().enumerate() {
            entry.ls[i] = val;
            entry.ss[i] = val * val;
        }
        entry
    }

    pub fn merge(&mut self, other: &CFEntry) {
        self.n += other.n;
        for i in 0..self.ls.len() {
            self.ls[i] += other.ls[i];
            self.ss[i] += other.ss[i];
        }
    }

    pub fn centroid(&self) -> Vec<f64> {
        if self.n == 0 {
            return vec![];
        }
        self.ls.iter().map(|&x| x / self.n as f64).collect()
    }

    pub fn variance(&self) -> f64 {
        if self.n <= 1 {
            return 0.0;
        }
        let centroid = self.centroid();
        self.ss.iter()
            .zip(centroid.iter())
            .map(|(&s, &c)| s - 2.0 * c * self.n as f64 + self.n as f64 * c * c)
            .sum::<f64>() / self.n as f64
    }

    pub fn radius(&self) -> f64 {
        if self.n == 0 {
            return 0.0;
        }
        let centroid = self.centroid();
        let mut sum_sq_dist = 0.0;
        for (i, &c) in centroid.iter().enumerate() {
            sum_sq_dist += self.ss[i] / self.n as f64 - c * c;
        }
        (sum_sq_dist / self.ls.len() as f64).sqrt()
    }

    pub fn diameter(&self) -> f64 {
        if self.n <= 1 {
            return 0.0;
        }
        2.0 * self.radius()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BirchNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub entries: Vec<CFEntry>,
    pub next_leaf: Option<Arc<BirchNode>>,
    pub prev_leaf: Option<Arc<BirchNode>>,
}

impl LeafNode {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode {
    pub entries: Vec<CFEntry>,
    pub children: Vec<Arc<BirchNode>>,
}

impl InternalNode {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            children: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BirchConfig {
    pub branching_factor: usize,
    pub max_nodes: usize,
    pub threshold: f64,
    pub compress: bool,
    pub memory_ratio: f64,
    pub initial_dummy_threshold: f64,
    pub use_sparse: bool,
    pub min_cluster_size: usize,
}

impl Default for BirchConfig {
    fn default() -> Self {
        Self {
            branching_factor: 50,
            max_nodes: 1000000,
            threshold: 0.5,
            compress: false,
            memory_ratio: 0.2,
            initial_dummy_threshold: 0.0,
            use_sparse: false,
            min_cluster_size: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BirchStats {
    pub nodes_created: AtomicU64,
    pub nodes_split: AtomicU64,
    pub entries_absorbed: AtomicU64,
    pub entries_merged: AtomicU64,
    pub leaves_visited: AtomicU64,
    pub cf_entries_created: AtomicU64,
    pub memory_usage_bytes: AtomicU64,
    pub total_time_ms: AtomicU64,
    pub avg_leaf_insertion_ns: AtomicU64,
}

impl Default for BirchStats {
    fn default() -> Self {
        Self {
            nodes_created: AtomicU64::new(0),
            nodes_split: AtomicU64::new(0),
            entries_absorbed: AtomicU64::new(0),
            entries_merged: AtomicU64::new(0),
            leaves_visited: AtomicU64::new(0),
            cf_entries_created: AtomicU64::new(0),
            memory_usage_bytes: AtomicU64::new(0),
            total_time_ms: AtomicU64::new(0),
            avg_leaf_insertion_ns: AtomicU64::new(0),
        }
    }
}

pub struct BIRCH {
    config: BirchConfig,
    dimension: usize,
    root: Arc<BirchNode>,
    leaves_head: Option<Arc<BirchNode>>,
    leaves_tail: Option<Arc<BirchNode>>,
    leaf_count: usize,
    total_entries: usize,
    stats: Arc<BirchStats>,
}

impl BIRCH {
    pub fn new(config: BirchConfig, dimension: usize) -> Result<Self, BirchError> {
        if config.branching_factor < 2 {
            return Err(BirchError::InvalidBranchingFactor(config.branching_factor));
        }
        if config.threshold < 0.0 {
            return Err(BirchError::InvalidThreshold(config.threshold));
        }

        let root = Arc::new(BirchNode::Leaf(LeafNode::new()));
        let stats = Arc::new(BirchStats::default());

        stats.nodes_created.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            config,
            dimension,
            root,
            leaves_head: None,
            leaves_tail: None,
            leaf_count: 0,
            total_entries: 0,
            stats,
        })
    }

    pub fn insert(&mut self, point: &[f64]) -> Result<(), BirchError> {
        let start = Instant::now();
        
        if point.len() != self.dimension {
            return Err(BirchError::DimensionMismatch(self.dimension, point.len()));
        }

        let entry = CFEntry::from_point(point);
        self.stats.cf_entries_created.fetch_add(1, Ordering::SeqCst);

        self.insert_entry(&self.root, &entry)?;

        self.total_entries += 1;
        
        let elapsed = start.elapsed().as_nanos() as u64;
        let avg = self.stats.avg_leaf_insertion_ns.load(Ordering::SeqCst);
        self.stats.avg_leaf_insertion_ns.store((avg + elapsed) / 2, Ordering::SeqCst);

        Ok(())
    }

    fn insert_entry(&mut self, node: &Arc<BirchNode>, entry: &CFEntry) -> Result<bool, BirchError> {
        match node.as_ref() {
            BirchNode::Leaf(leaf) => {
                self.insert_into_leaf(leaf, entry)
            }
            BirchNode::Internal(internal) => {
                self.insert_into_internal(internal, entry)
            }
        }
    }

    fn insert_into_leaf(&mut self, leaf: &LeafNode, entry: &CFEntry) -> Result<bool, BirchError> {
        let mut candidates: Vec<(usize, f64)> = leaf.entries.iter()
            .enumerate()
            .map(|(i, e)| (i, self.distance(entry, e)))
            .collect();

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        for (idx, dist) in candidates {
            if dist < self.config.threshold {
                let leaf_mut = leaf.entries.get(idx).unwrap();
                leaf_mut.merge(entry);
                self.stats.entries_absorbed.fetch_add(1, Ordering::SeqCst);
                return Ok(true);
            }
        }

        if leaf.entries.len() < self.config.branching_factor {
            let leaf_mut = leaf.entries.get(0).unwrap();
            leaf_mut.merge(entry);
            self.stats.entries_absorbed.fetch_add(1, Ordering::SeqCst);
            return Ok(true);
        }

        self.split_leaf()?;
        Ok(false)
    }

    fn insert_into_internal(&mut self, internal: &InternalNode, entry: &CFEntry) -> Result<bool, BirchError> {
        let mut min_dist = f64::MAX;
        let mut min_idx = 0;

        for (i, e) in internal.entries.iter().enumerate() {
            let dist = self.distance(entry, e);
            if dist < min_dist {
                min_dist = dist;
                min_idx = i;
            }
        }

        self.insert_entry(&internal.children[min_idx], entry)
    }

    fn split_leaf(&mut self) -> Result<(), BirchError> {
        self.stats.nodes_split.fetch_add(1, Ordering::SeqCst);

        let old_root = self.root.clone();
        
        let mut new_internal = InternalNode::new();
        new_internal.entries.push(CFEntry::from_point(&vec![0.0; self.dimension]));
        new_internal.children.push(old_root.clone());

        self.root = Arc::new(BirchNode::Internal(new_internal));

        self.redistribute_entries()
    }

    fn redistribute_entries(&mut self) -> Result<(), BirchError> {
        let mut entries_to_redistribute: Vec<CFEntry> = Vec::new();

        if let BirchNode::Internal(internal) = self.root.as_ref() {
            for child in &internal.children {
                if let BirchNode::Leaf(leaf) = child.as_ref() {
                    for entry in &leaf.entries {
                        entries_to_redistribute.push(entry.clone());
                    }
                }
            }
        }

        self.stats.entries_merged.fetch_add(entries_to_redistribute.len() as u64, Ordering::SeqCst);

        Ok(())
    }

    fn distance(&self, entry1: &CFEntry, entry2: &CFEntry) -> f64 {
        let centroid1 = entry1.centroid();
        let centroid2 = entry2.centroid();
        
        centroid1.iter()
            .zip(centroid2.iter())
            .map(|(&a, &b)| (a - b) * (a - b))
            .sum::<f64>()
            .sqrt()
    }

    pub fn insert_batch(&mut self, points: &[Vec<f64>]) -> Result<(), BirchError> {
        let start = Instant::now();
        
        for point in points {
            self.insert(point)?;
        }

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.total_time_ms.store(elapsed, Ordering::SeqCst);

        Ok(())
    }

    pub fn get_clusters(&self) -> Vec<CFEntry> {
        let mut clusters = Vec::new();
        
        self.collect_leaf_entries(&self.root, &mut clusters);
        
        clusters
    }

    fn collect_leaf_entries(&self, node: &Arc<BirchNode>, clusters: &mut Vec<CFEntry>) {
        match node.as_ref() {
            BirchNode::Leaf(leaf) => {
                for entry in &leaf.entries {
                    clusters.push(entry.clone());
                }
            }
            BirchNode::Internal(internal) => {
                for child in &internal.children {
                    self.collect_leaf_entries(child, clusters);
                }
            }
        }
    }

    pub fn get_cluster_labels(&self, data: &[Vec<f64>]) -> Result<Vec<usize>, BirchError> {
        let clusters = self.get_clusters();
        let mut labels = Vec::with_capacity(data.len());

        for point in data {
            let entry = CFEntry::from_point(point);
            let mut min_dist = f64::MAX;
            let mut min_idx = 0;

            for (i, cluster) in clusters.iter().enumerate() {
                let dist = self.distance(&entry, cluster);
                if dist < min_dist {
                    min_dist = dist;
                    min_idx = i;
                }
            }
            labels.push(min_idx);
        }

        Ok(labels)
    }

    pub fn finalize_clusters(&mut self) -> Vec<CFEntry> {
        let clusters = self.get_clusters();
        self.merge_similar_clusters(&clusters);
        self.get_clusters()
    }

    fn merge_similar_clusters(&self, clusters: &[CFEntry]) {
        let threshold = self.config.threshold;
        let mut merged = Vec::new();
        let mut processed = HashSet::new();

        for (i, cluster) in clusters.iter().enumerate() {
            if processed.contains(&i) {
                continue;
            }

            let mut similar_clusters: Vec<usize> = vec![i];
            processed.insert(i);

            for (j, other) in clusters.iter().enumerate() {
                if i != j && !processed.contains(&j) {
                    let dist = self.distance(cluster, other);
                    if dist < threshold {
                        similar_clusters.push(j);
                        processed.insert(j);
                    }
                }
            }

            if similar_clusters.len() > 1 {
                let mut merged_entry = CFEntry::new(self.dimension);
                for idx in &similar_clusters {
                    merged_entry.merge(&clusters[*idx]);
                }
                merged.push(merged_entry);
            }
        }
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("nodes_created".to_string(), self.stats.nodes_created.load(Ordering::SeqCst));
        stats.insert("nodes_split".to_string(), self.stats.nodes_split.load(Ordering::SeqCst));
        stats.insert("entries_absorbed".to_string(), self.stats.entries_absorbed.load(Ordering::SeqCst));
        stats.insert("entries_merged".to_string(), self.stats.entries_merged.load(Ordering::SeqCst));
        stats.insert("leaves_visited".to_string(), self.stats.leaves_visited.load(Ordering::SeqCst));
        stats.insert("cf_entries_created".to_string(), self.stats.cf_entries_created.load(Ordering::SeqCst));
        stats.insert("memory_usage_bytes".to_string(), self.stats.memory_usage_bytes.load(Ordering::SeqCst));
        stats.insert("total_time_ms".to_string(), self.stats.total_time_ms.load(Ordering::SeqCst));
        stats.insert("leaf_count".to_string(), self.leaf_count as u64);
        stats.insert("total_entries".to_string(), self.total_entries as u64);

        stats
    }

    pub fn memory_usage(&self) -> usize {
        self.total_entries * std::mem::size_of::<CFEntry>() +
        self.leaf_count * std::mem::size_of::<LeafNode>()
    }

    pub fn clear(&mut self) {
        let new_root = Arc::new(BirchNode::Leaf(LeafNode::new()));
        self.root = new_root;
        self.leaves_head = None;
        self.leaves_tail = None;
        self.leaf_count = 0;
        self.total_entries = 0;
    }
}

use std::collections::HashSet;

impl Drop for BIRCH {
    fn drop(&self) {
        let stats = &self.stats;
        info!(
            "BIRCH stats - Nodes: {}, Entries: {}, Splits: {}, Time: {}ms",
            stats.nodes_created.load(Ordering::SeqCst),
            self.total_entries,
            stats.nodes_split.load(Ordering::SeqCst),
            stats.total_time_ms.load(Ordering::SeqCst)
        );
    }
}
