use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use crate::compute::{SimdOperations, GpuOperations, UdfManager};

pub struct ComputeRuntime {
    simd: Arc<SimdOperations>,
    gpu: Option<Arc<GpuOperations>>,
    udf_manager: Arc<UdfManager>,
    resources: Arc<RwLock<RuntimeResources>>,
}

pub struct RuntimeResources {
    memory_usage: usize,
    thread_count: usize,
    gpu_memory_usage: Option<usize>,
}

impl RuntimeResources {
    pub fn new() -> Self {
        Self {
            memory_usage: 0,
            thread_count: num_cpus::get(),
            gpu_memory_usage: None,
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    pub fn gpu_memory_usage(&self) -> Option<usize> {
        self.gpu_memory_usage
    }

    pub fn allocate_memory(&mut self, size: usize) -> Result<(), RuntimeError> {
        self.memory_usage += size;
        Ok(())
    }

    pub fn free_memory(&mut self, size: usize) {
        if self.memory_usage >= size {
            self.memory_usage -= size;
        }
    }
}

impl ComputeRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        let simd = Arc::new(SimdOperations::new()?);
        let gpu = match GpuOperations::new() {
            Ok(gpu) => Some(Arc::new(gpu)),
            Err(_) => None,
        };
        let udf_manager = Arc::new(UdfManager::new()?);
        let resources = Arc::new(RwLock::new(RuntimeResources::new()));

        Ok(Self {
            simd,
            gpu,
            udf_manager,
            resources,
        })
    }

    pub fn execute_vector_operation(
        &self,
        op: VectorOperation,
        a: &[f32],
        b: &[f32],
    ) -> Result<Vec<f32>, RuntimeError> {
        if let Some(gpu) = &self.gpu {
            if a.len() > 1024 {
                return gpu.execute_operation(op, a, b);
            }
        }
        self.simd.execute_operation(op, a, b)
    }

    pub fn execute_udf(&self, name: &str, args: &[Value]) -> Result<Value, RuntimeError> {
        self.udf_manager.execute(name, args)
    }

    pub fn register_udf(&self, name: &str, code: &str, language: &str) -> Result<(), RuntimeError> {
        self.udf_manager.register(name, code, language)
    }

    pub fn resources(&self) -> Arc<RwLock<RuntimeResources>> {
        self.resources.clone()
    }

    pub fn has_gpu(&self) -> bool {
        self.gpu.is_some()
    }

    pub fn simd_available(&self) -> bool {
        self.simd.is_available()
    }
}

#[derive(Debug, Clone)]
pub enum VectorOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<Value>),
    Vector(Vec<f32>),
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Operation failed: {0}")]
    OperationFailed(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Out of memory")]
    OutOfMemory,

    #[error("GPU not available")]
    GpuNotAvailable,

    #[error("UDF error: {0}")]
    UdfError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub struct TaskScheduler {
    tasks: Arc<RwLock<Vec<ScheduledTask>>>,
    workers: usize,
}

pub struct ScheduledTask {
    id: String,
    priority: u32,
    task: Box<dyn FnOnce() -> Result<(), RuntimeError> + Send + Sync>,
}

impl TaskScheduler {
    pub fn new(workers: usize) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(Vec::new())),
            workers,
        }
    }

    pub fn schedule<F>(&self, priority: u32, task: F) -> String
    where
        F: FnOnce() -> Result<(), RuntimeError> + Send + Sync + 'static,
    {
        let id = format!("task_{}", uuid::Uuid::new_v4());
        let scheduled_task = ScheduledTask {
            id: id.clone(),
            priority,
            task: Box::new(task),
        };
        self.tasks.write().unwrap().push(scheduled_task);
        id
    }

    pub fn execute(&self) -> Result<(), RuntimeError> {
        let mut tasks = self.tasks.write().unwrap();
        tasks.sort_by(|a, b| b.priority.cmp(&a.priority));

        let tasks_to_execute = tasks.drain(..).collect::<Vec<_>>();
        drop(tasks);

        for task in tasks_to_execute {
            (task.task)()?;
        }

        Ok(())
    }

    pub fn task_count(&self) -> usize {
        self.tasks.read().unwrap().len()
    }
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_runtime() {
        let runtime = ComputeRuntime::new().unwrap();
        assert!(runtime.simd_available());
    }

    #[test]
    fn test_task_scheduler() {
        let scheduler = TaskScheduler::default();
        let task_id = scheduler.schedule(1, || Ok(()));
        assert!(!task_id.is_empty());
        assert_eq!(scheduler.task_count(), 1);
    }
}
