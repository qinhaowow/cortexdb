use std::sync::{Arc, RwLock};
use std::collections::{HashMap, VecDeque, HashSet};

pub struct GraphIndex {
    adjacency: Arc<RwLock<HashMap<usize, Vec<(usize, f32)>>>,
    edge_type: String,
}

impl GraphIndex {
    pub fn new(edge_type: String) -> Self {
        Self {
            adjacency: Arc::new(RwLock::new(HashMap::new())),
            edge_type,
        }
    }

    pub fn add_node(&mut self, id: usize) -> Result<(), GraphIndexError> {
        let mut adjacency = self.adjacency.write().unwrap();
        adjacency.entry(id).or_insert_with(Vec::new());
        Ok(())
    }

    pub fn add_edge(&mut self, from: usize, to: usize, weight: f32) -> Result<(), GraphIndexError> {
        let mut adjacency = self.adjacency.write().unwrap();
        
        adjacency.entry(from).or_insert_with(Vec::new()).push((to, weight));
        adjacency.entry(to).or_insert_with(Vec::new()).push((from, weight));
        
        Ok(())
    }

    pub fn remove_node(&mut self, id: usize) -> Result<(), GraphIndexError> {
        let mut adjacency = self.adjacency.write().unwrap();
        adjacency.remove(&id);
        
        for (_, edges) in adjacency.iter_mut() {
            edges.retain(|(to, _)| *to != id);
        }
        
        Ok(())
    }

    pub fn remove_edge(&mut self, from: usize, to: usize) -> Result<(), GraphIndexError> {
        let mut adjacency = self.adjacency.write().unwrap();
        
        if let Some(edges) = adjacency.get_mut(&from) {
            edges.retain(|(to_id, _)| *to_id != to);
        }
        
        if let Some(edges) = adjacency.get_mut(&to) {
            edges.retain(|(from_id, _)| *from_id != from);
        }
        
        Ok(())
    }

    pub fn get_neighbors(&self, id: usize) -> Result<Option<Vec<(usize, f32)>>, GraphIndexError> {
        let adjacency = self.adjacency.read().unwrap();
        Ok(adjacency.get(&id).cloned())
    }

    pub fn shortest_path(&self, from: usize, to: usize) -> Result<Option<(Vec<usize>, f32)>, GraphIndexError> {
        let adjacency = self.adjacency.read().unwrap();
        let mut distances = HashMap::new();
        let mut previous = HashMap::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        distances.insert(from, 0.0);
        queue.push_back(from);

        while let Some(current) = queue.pop_front() {
            if current == to {
                break;
            }

            if !visited.insert(current) {
                continue;
            }

            if let Some(neighbors) = adjacency.get(&current) {
                for (neighbor, weight) in neighbors {
                    let new_distance = distances.get(&current).unwrap_or(&f32::INFINITY) + weight;
                    let current_distance = distances.get(neighbor).unwrap_or(&f32::INFINITY);

                    if new_distance < *current_distance {
                        distances.insert(*neighbor, new_distance);
                        previous.insert(*neighbor, current);
                        queue.push_back(*neighbor);
                    }
                }
            }
        }

        if let Some(&distance) = distances.get(&to) {
            let mut path = Vec::new();
            let mut current = to;

            while let Some(&prev) = previous.get(&current) {
                path.push(current);
                current = prev;
            }

            path.push(from);
            path.reverse();

            Ok(Some((path, distance)))
        } else {
            Ok(None)
        }
    }

    pub fn breadth_first_search(&self, start: usize, max_depth: usize) -> Result<Vec<usize>, GraphIndexError> {
        let adjacency = self.adjacency.read().unwrap();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((current, depth)) = queue.pop_front() {
            if depth > max_depth {
                continue;
            }

            results.push(current);

            if let Some(neighbors) = adjacency.get(&current) {
                for (neighbor, _) in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(*neighbor);
                        queue.push_back((*neighbor, depth + 1));
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn node_count(&self) -> usize {
        let adjacency = self.adjacency.read().unwrap();
        adjacency.len()
    }

    pub fn edge_count(&self) -> usize {
        let adjacency = self.adjacency.read().unwrap();
        adjacency.values().map(|edges| edges.len()).sum() / 2
    }

    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GraphIndexError {
    #[error("Node not found: {0}")]
    NodeNotFound(usize),

    #[error("Edge not found")]
    EdgeNotFound,

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Default for GraphIndex {
    fn default() -> Self {
        Self::new("undirected".to_string())
    }
}
