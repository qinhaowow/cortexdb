use std::sync::{Arc, RwLock};
use std::collections::{HashMap, BTreeMap};
use std::path::Path;

pub struct DiskAnnIndex {
    data: Arc<RwLock<HashMap<usize, Vec<f32>>>>,
    graph: Arc<RwLock<HashMap<usize, Vec<usize>>>>,
    dimension: usize,
    r: usize,
    l: usize,
    path: Option<String>,
}

impl DiskAnnIndex {
    pub fn new(dimension: usize, r: usize, l: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            graph: Arc::new(RwLock::new(HashMap::new())),
            dimension,
            r,
            l,
            path: None,
        }
    }

    pub fn add(&mut self, id: usize, vector: Vec<f32>) -> Result<(), DiskAnnError> {
        if vector.len() != self.dimension {
            return Err(DiskAnnError::InvalidDimension(vector.len(), self.dimension));
        }

        self.data.write().unwrap().insert(id, vector.clone());
        self.build_graph(id, vector);
        Ok(())
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>, DiskAnnError> {
        if query.len() != self.dimension {
            return Err(DiskAnnError::InvalidDimension(query.len(), self.dimension));
        }

        let mut candidates = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        if let Some((id, _)) = self.data.read().unwrap().iter().next() {
            queue.push_back(*id);
            visited.insert(*id);
        }

        while let Some(node_id) = queue.pop_front() {
            let distance = self.cosine_distance(query, &self.data.read().unwrap()[&node_id]);
            candidates.push((node_id, distance));

            if let Some(neighbors) = self.graph.read().unwrap().get(&node_id) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(*neighbor);
                        queue.push_back(*neighbor);
                    }
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        Ok(candidates.into_iter().take(k).collect())
    }

    pub fn remove(&mut self, id: usize) -> Result<(), DiskAnnError> {
        self.data.write().unwrap().remove(&id);
        self.graph.write().unwrap().remove(&id);
        
        for neighbors in self.graph.write().unwrap().values_mut() {
            neighbors.retain(|&neighbor_id| neighbor_id != id);
        }
        
        Ok(())
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), DiskAnnError> {
        let path = path.as_ref().to_str().ok_or(DiskAnnError::InvalidPath)?;
        self.path = Some(path.to_string());
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P, dimension: usize) -> Result<Self, DiskAnnError> {
        let path = path.as_ref().to_str().ok_or(DiskAnnError::InvalidPath)?;
        let index = Self::new(dimension, 32, 100);
        index.path = Some(path.to_string());
        Ok(index)
    }

    pub fn size(&self) -> usize {
        self.data.read().unwrap().len()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    fn build_graph(&mut self, id: usize, vector: Vec<f32>) {
        let mut neighbors = Vec::new();
        let data = self.data.read().unwrap();

        for (other_id, other_vector) in data.iter() {
            if *other_id == id {
                continue;
            }

            let distance = self.cosine_distance(&vector, other_vector);
            neighbors.push((*other_id, distance));
        }

        neighbors.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let selected = neighbors.into_iter()
            .take(self.r)
            .map(|(id, _)| id)
            .collect();

        self.graph.write().unwrap().insert(id, selected);
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
pub enum DiskAnnError {
    #[error("Invalid dimension: expected {1}, got {0}")]
    InvalidDimension(usize, usize),

    #[error("Invalid path")]
    InvalidPath,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),
}

use std::collections::{HashSet, VecDeque};
