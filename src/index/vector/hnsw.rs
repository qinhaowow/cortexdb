use std::sync::{Arc, RwLock};
use std::collections::{HashMap, VecDeque};

pub struct HnswIndex {
    layers: Arc<RwLock<Vec<Layer>>>,
    entry_point: usize,
    max_connections: usize,
    ef_construction: usize,
    ef_search: usize,
    dimension: usize,
    vectors: Arc<RwLock<HashMap<usize, Vec<f32>>>>,
}

struct Layer {
    nodes: HashMap<usize, Node>,
}

struct Node {
    id: usize,
    neighbors: Vec<usize>,
}

impl HnswIndex {
    pub fn new(dimension: usize, max_connections: usize, ef_construction: usize) -> Self {
        Self {
            layers: Arc::new(RwLock::new(Vec::new())),
            entry_point: 0,
            max_connections,
            ef_construction,
            ef_search: 10,
            dimension,
            vectors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add(&mut self, id: usize, vector: Vec<f32>) -> Result<(), HnswError> {
        if vector.len() != self.dimension {
            return Err(HnswError::InvalidDimension(vector.len(), self.dimension));
        }

        self.vectors.write().unwrap().insert(id, vector.clone());

        if self.layers.read().unwrap().is_empty() {
            self.initialize_index(id, vector);
            return Ok(());
        }

        self.insert(id, vector);
        Ok(())
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>, HnswError> {
        if query.len() != self.dimension {
            return Err(HnswError::InvalidDimension(query.len(), self.dimension));
        }

        if self.layers.read().unwrap().is_empty() {
            return Ok(Vec::new());
        }

        let mut result = self.search_knn(query, k);
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        Ok(result)
    }

    pub fn remove(&mut self, id: usize) -> Result<(), HnswError> {
        self.vectors.write().unwrap().remove(&id);
        
        let mut layers = self.layers.write().unwrap();
        for layer in layers.iter_mut() {
            layer.nodes.remove(&id);
            for node in layer.nodes.values_mut() {
                node.neighbors.retain(|&neighbor_id| neighbor_id != id);
            }
        }
        
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.vectors.read().unwrap().len()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    fn initialize_index(&mut self, id: usize, vector: Vec<f32>) {
        let layer = Layer {
            nodes: HashMap::from([(id, Node { id, neighbors: Vec::new() })]),
        };
        self.layers.write().unwrap().push(layer);
        self.entry_point = id;
    }

    fn insert(&mut self, id: usize, vector: Vec<f32>) {
        let level = self.random_level();
        let mut current = self.entry_point;

        let layers = self.layers.write().unwrap();
        for i in (0..layers.len()).rev() {
            current = self.search_layer(&vector, current, i, self.ef_construction);
        }

        for i in 0..=level {
            if i >= layers.len() {
                layers.push(Layer { nodes: HashMap::new() });
            }

            let neighbors = self.select_neighbors(&vector, current, i, self.ef_construction);
            let node = Node { id, neighbors };
            layers[i].nodes.insert(id, node);

            if i == layers.len() - 1 {
                self.entry_point = id;
            }
        }
    }

    fn search_knn(&self, query: &[f32], k: usize) -> Vec<(usize, f32)> {
        let layers = self.layers.read().unwrap();
        let mut current = self.entry_point;

        for i in (1..layers.len()).rev() {
            current = self.search_layer(query, current, i, self.ef_search);
        }

        let mut candidates = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(current);
        visited.insert(current);

        while let Some(node_id) = queue.pop_front() {
            let distance = self.cosine_distance(query, &self.vectors.read().unwrap()[&node_id]);
            candidates.push((node_id, distance));

            if let Some(node) = layers[0].nodes.get(&node_id) {
                for neighbor in &node.neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(*neighbor);
                        queue.push_back(*neighbor);
                    }
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.into_iter().take(k).collect()
    }

    fn search_layer(&self, query: &[f32], entry_point: usize, layer: usize, ef: usize) -> usize {
        let layers = self.layers.read().unwrap();
        let mut candidates = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(entry_point);
        visited.insert(entry_point);

        while let Some(node_id) = queue.pop_front() {
            let distance = self.cosine_distance(query, &self.vectors.read().unwrap()[&node_id]);
            candidates.push((node_id, distance));

            if let Some(node) = layers[layer].nodes.get(&node_id) {
                for neighbor in &node.neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(*neighbor);
                        queue.push_back(*neighbor);
                    }
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates[0].0
    }

    fn select_neighbors(&self, query: &[f32], entry_point: usize, layer: usize, ef: usize) -> Vec<usize> {
        let layers = self.layers.read().unwrap();
        let mut candidates = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(entry_point);
        visited.insert(entry_point);

        while let Some(node_id) = queue.pop_front() {
            let distance = self.cosine_distance(query, &self.vectors.read().unwrap()[&node_id]);
            candidates.push((node_id, distance));

            if let Some(node) = layers[layer].nodes.get(&node_id) {
                for neighbor in &node.neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(*neighbor);
                        queue.push_back(*neighbor);
                    }
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.into_iter()
            .take(self.max_connections)
            .map(|(id, _)| id)
            .collect()
    }

    fn random_level(&self) -> usize {
        let mut level = 0;
        while rand::random::<f32>() < 0.5 && level < 32 {
            level += 1;
        }
        level
    }

    fn cosine_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        let mut dot_product = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;

        for i in 0..a.len() {
            dot_product += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }

        1.0 - (dot_product / (norm_a.sqrt() * norm_b.sqrt()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    #[error("Invalid dimension: expected {1}, got {0}")]
    InvalidDimension(usize, usize),

    #[error("Node not found: {0}")]
    NodeNotFound(usize),

    #[error("Internal error: {0}")]
    InternalError(String),
}

use std::collections::HashSet;
