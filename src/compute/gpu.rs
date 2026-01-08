use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use crate::compute::runtime::VectorOperation;

pub struct GpuOperations {
    devices: Vec<GpuDevice>,
    current_device: usize,
    resources: Arc<RwLock<GpuResources>>,
}

pub struct GpuDevice {
    id: usize,
    name: String,
    memory_size: usize,
    compute_capability: (u32, u32),
}

pub struct GpuResources {
    memory_usage: HashMap<usize, usize>,
    active_kernels: usize,
}

impl GpuResources {
    pub fn new() -> Self {
        Self {
            memory_usage: HashMap::new(),
            active_kernels: 0,
        }
    }

    pub fn allocate_memory(&mut self, device_id: usize, size: usize) -> Result<(), GpuError> {
        let current = self.memory_usage.get(&device_id).unwrap_or(&0);
        self.memory_usage.insert(device_id, current + size);
        Ok(())
    }

    pub fn free_memory(&mut self, device_id: usize, size: usize) {
        if let Some(current) = self.memory_usage.get_mut(&device_id) {
            if *current >= size {
                *current -= size;
            }
        }
    }

    pub fn memory_usage(&self, device_id: usize) -> usize {
        *self.memory_usage.get(&device_id).unwrap_or(&0)
    }

    pub fn active_kernels(&self) -> usize {
        self.active_kernels
    }

    pub fn increment_kernels(&mut self) {
        self.active_kernels += 1;
    }

    pub fn decrement_kernels(&mut self) {
        if self.active_kernels > 0 {
            self.active_kernels -= 1;
        }
    }
}

impl GpuDevice {
    pub fn new(id: usize, name: &str, memory_size: usize, compute_capability: (u32, u32)) -> Self {
        Self {
            id,
            name: name.to_string(),
            memory_size,
            compute_capability,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    pub fn compute_capability(&self) -> (u32, u32) {
        self.compute_capability
    }
}

impl GpuOperations {
    pub fn new() -> Result<Self, GpuError> {
        let devices = Self::detect_devices()?;
        if devices.is_empty() {
            return Err(GpuError::NoDevicesFound);
        }

        Ok(Self {
            devices,
            current_device: 0,
            resources: Arc::new(RwLock::new(GpuResources::new())),
        })
    }

    pub fn devices(&self) -> &[GpuDevice] {
        &self.devices
    }

    pub fn current_device(&self) -> &GpuDevice {
        &self.devices[self.current_device]
    }

    pub fn set_device(&mut self, device_id: usize) -> Result<(), GpuError> {
        if device_id >= self.devices.len() {
            return Err(GpuError::InvalidDeviceId(device_id));
        }
        self.current_device = device_id;
        Ok(())
    }

    pub fn execute_operation(
        &self,
        op: VectorOperation,
        a: &[f32],
        b: &[f32],
    ) -> Result<Vec<f32>, GpuError> {
        if a.len() != b.len() {
            return Err(GpuError::InvalidVectorLength);
        }

        match op {
            VectorOperation::Add => self.add(a, b),
            VectorOperation::Subtract => self.subtract(a, b),
            VectorOperation::Multiply => self.multiply(a, b),
            VectorOperation::Divide => self.divide(a, b),
            VectorOperation::DotProduct => {
                let result = self.dot_product(a, b)?;
                Ok(vec![result])
            },
            VectorOperation::CosineSimilarity => {
                let result = self.cosine_similarity(a, b)?;
                Ok(vec![result])
            },
            VectorOperation::EuclideanDistance => {
                let result = self.euclidean_distance(a, b)?;
                Ok(vec![result])
            },
        }
    }

    pub fn add(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, GpuError> {
        self.simulate_gpu_operation(a, b, |x, y| x + y)
    }

    pub fn subtract(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, GpuError> {
        self.simulate_gpu_operation(a, b, |x, y| x - y)
    }

    pub fn multiply(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, GpuError> {
        self.simulate_gpu_operation(a, b, |x, y| x * y)
    }

    pub fn divide(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, GpuError> {
        self.simulate_gpu_operation(a, b, |x, y| x / y)
    }

    pub fn dot_product(&self, a: &[f32], b: &[f32]) -> Result<f32, GpuError> {
        let result = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        Ok(result)
    }

    pub fn cosine_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32, GpuError> {
        let dot = self.dot_product(a, b)?;
        let norm_a = self.euclidean_norm(a)?;
        let norm_b = self.euclidean_norm(b)?;

        if norm_a == 0.0 || norm_b == 0.0 {
            return Ok(0.0);
        }

        Ok(dot / (norm_a * norm_b))
    }

    pub fn euclidean_distance(&self, a: &[f32], b: &[f32]) -> Result<f32, GpuError> {
        let mut sum = 0.0;
        for (x, y) in a.iter().zip(b.iter()) {
            let diff = x - y;
            sum += diff * diff;
        }
        Ok(sum.sqrt())
    }

    pub fn euclidean_norm(&self, a: &[f32]) -> Result<f32, GpuError> {
        let mut sum = 0.0;
        for &x in a {
            sum += x * x;
        }
        Ok(sum.sqrt())
    }

    pub fn resources(&self) -> Arc<RwLock<GpuResources>> {
        self.resources.clone()
    }

    fn simulate_gpu_operation<F>(&self, a: &[f32], b: &[f32], op: F) -> Result<Vec<f32>, GpuError>
    where
        F: Fn(f32, f32) -> f32,
    {
        let mut result = Vec::with_capacity(a.len());
        result.resize(a.len(), 0.0);

        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            result[i] = op(*x, *y);
        }

        Ok(result)
    }

    fn detect_devices() -> Result<Vec<GpuDevice>, GpuError> {
        Ok(vec![
            GpuDevice::new(0, "NVIDIA GeForce RTX 3090", 24 * 1024 * 1024 * 1024, (8, 6)),
            GpuDevice::new(1, "NVIDIA GeForce RTX 3080", 10 * 1024 * 1024 * 1024, (8, 6)),
        ])
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GpuError {
    #[error("No GPU devices found")]
    NoDevicesFound,

    #[error("Invalid device ID: {0}")]
    InvalidDeviceId(usize),

    #[error("Invalid vector length")]
    InvalidVectorLength,

    #[error("Out of GPU memory")]
    OutOfMemory,

    #[error("Kernel execution failed: {0}")]
    KernelExecutionFailed(String),

    #[error("CUDA error: {0}")]
    CudaError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_operations() {
        match GpuOperations::new() {
            Ok(gpu) => {
                println!("Found {} GPU devices", gpu.devices().len());
                for device in gpu.devices() {
                    println!(
                        "Device {}: {} ({} MB)",
                        device.id(),
                        device.name(),
                        device.memory_size() / (1024 * 1024)
                    );
                }
            },
            Err(e) => {
                println!("No GPU devices found: {}", e);
            }
        }
    }
}
