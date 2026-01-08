use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub struct BruteForceIndex {
    vectors: Arc<RwLock<HashMap<usize, Vec<f32>>>>,
    dimension: usize,
}

impl BruteForceIndex {
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: Arc::new(RwLock::new(HashMap::new())),
            dimension,
        }
    }

    pub fn add(&mut self, id: usize, vector: Vec<f32>) -> Result<(), BruteForceError> {
        if vector.len() != self.dimension {
            return Err(BruteForceError::InvalidDimension(vector.len(), self.dimension));
        }

        self.vectors.write().unwrap().insert(id, vector);
        Ok(())
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(usize, f32)>, BruteForceError> {
        if query.len() != self.dimension {
            return Err(BruteForceError::InvalidDimension(query.len(), self.dimension));
        }

        let vectors = self.vectors.read().unwrap();
        let mut results = Vec::new();

        for (id, vector) in vectors.iter() {
            let distance = self.cosine_distance(query, vector);
            results.push((*id, distance));
        }

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        Ok(results.into_iter().take(k).collect())
    }

    pub fn remove(&mut self, id: usize) -> Result<(), BruteForceError> {
        self.vectors.write().unwrap().remove(&id);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.vectors.read().unwrap().len()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
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
pub enum BruteForceError {
    #[error("Invalid dimension: expected {1}, got {0}")]
    InvalidDimension(usize, usize),

    #[error("Internal error: {0}")]
    InternalError(String),
}
