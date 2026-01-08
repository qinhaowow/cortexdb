use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};

pub struct FullTextIndex {
    data: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    analyzer: String,
}

impl FullTextIndex {
    pub fn new(analyzer: String) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            analyzer,
        }
    }

    pub fn insert(&mut self, id: usize, text: &str) -> Result<(), FullTextIndexError> {
        let tokens = self.tokenize(text);
        let mut data = self.data.write().unwrap();

        for token in tokens {
            let ids = data.entry(token).or_insert_with(HashSet::new());
            ids.insert(id);
        }

        Ok(())
    }

    pub fn search(&self, query: &str) -> Result<HashSet<usize>, FullTextIndexError> {
        let tokens = self.tokenize(query);
        let data = self.data.read().unwrap();
        let mut results = HashSet::new();

        for token in tokens {
            if let Some(ids) = data.get(&token) {
                if results.is_empty() {
                    results.extend(ids);
                } else {
                    results = results.intersection(ids).cloned().collect();
                }
            }
        }

        Ok(results)
    }

    pub fn remove(&mut self, id: usize) -> Result<(), FullTextIndexError> {
        let mut data = self.data.write().unwrap();

        for (_, ids) in data.iter_mut() {
            ids.remove(&id);
        }

        data.retain(|_, ids| !ids.is_empty());
        Ok(())
    }

    pub fn remove_document(&mut self, id: usize, text: &str) -> Result<(), FullTextIndexError> {
        let tokens = self.tokenize(text);
        let mut data = self.data.write().unwrap();

        for token in tokens {
            if let Some(ids) = data.get_mut(&token) {
                ids.remove(&id);
                if ids.is_empty() {
                    data.remove(&token);
                }
            }
        }

        Ok(())
    }

    pub fn size(&self) -> usize {
        let data = self.data.read().unwrap();
        data.len()
    }

    pub fn document_count(&self) -> usize {
        let data = self.data.read().unwrap();
        let mut documents = HashSet::new();

        for ids in data.values() {
            documents.extend(ids);
        }

        documents.len()
    }

    pub fn analyzer(&self) -> &str {
        &self.analyzer
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FullTextIndexError {
    #[error("Invalid text: {0}")]
    InvalidText(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Default for FullTextIndex {
    fn default() -> Self {
        Self::new("default".to_string())
    }
}
