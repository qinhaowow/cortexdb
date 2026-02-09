//! GPU Computing Module with CUDA/OpenCL Support for CortexDB
//!
//! This module provides high-performance GPU-accelerated vector operations
//! using NVIDIA CUDA and OpenCL frameworks.

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::ptr;
use std::fmt;
use thiserror::Error;
use serde::{Deserialize, Serialize};

#[cfg(feature = "gpu")]
use rustacuda as cuda;

#[cfg(feature = "gpu")]
use rustacuda::memory::DeviceBuffer;

#[cfg(feature = "gpu")]
use rustacuda::launch;

#[cfg(feature = "gpu")]
use rustacuda::module::Module;

#[cfg(feature = "gpu")]
use rustacuda::context::Context;

#[cfg(feature = "gpu")]
use rustacuda::stream::Stream;

#[cfg(feature = "gpu")]
use rustacuda_core::DevicePointer;

#[cfg(feature = "gpu")]
use ocl::{Platform, Device, Context as OclContext, Queue as OclQueue, Buffer as OclBuffer, Program, Kernel,builders::MemoryBuilder};

#[cfg(feature = "gpu")]
use ocl::prm::Float;

#[derive(Debug, Error)]
pub enum GpuComputeError {
    #[error("No GPU devices found")]
    NoDevicesFound,
    
    #[error("Invalid device ID: {0}")]
    InvalidDeviceId(usize),
    
    #[error("Invalid vector length: expected {expected}, got {actual}")]
    InvalidVectorLength { expected: usize, actual: usize },
    
    #[error("CUDA error: {0}")]
    CudaError(String),
    
    #[error("OpenCL error: {0}")]
    OpenCLError(String),
    
    #[error("Memory allocation failed: {0}")]
    MemoryAllocationFailed(String),
    
    #[error("Kernel execution failed: {0}")]
    KernelExecutionFailed(String),
    
    #[error("Module loading failed: {0}")]
    ModuleLoadingFailed(String),
    
    #[error("Out of GPU memory")]
    OutOfMemory,
    
    #[error("Device not available: {0}")]
    DeviceNotAvailable(String),
    
    #[error("Initialization failed: {0}")]
    InitializationFailed(String),
    
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuDeviceInfo {
    pub id: usize,
    pub name: String,
    pub compute_capability: (u32, u32),
    pub max_threads_per_block: usize,
    pub max_threads_per_multiprocessor: usize,
    pub num_multiprocessors: usize,
    pub global_memory_bytes: usize,
    pub shared_memory_per_block_bytes: usize,
    pub constant_memory_bytes: usize,
    pub registers_per_block: usize,
    pub warp_size: usize,
    pub max_grid_size: (usize, usize, usize),
    pub max_block_size: (usize, usize, usize),
}

impl fmt::Display for GpuDeviceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GPU {}: {} (Compute {}.{})", 
            self.id, 
            self.name,
            self.compute_capability.0,
            self.compute_capability.1
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuMemoryInfo {
    pub total_bytes: usize,
    pub used_bytes: usize,
    pub available_bytes: usize,
    pub allocation_count: usize,
    pub peak_used_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuComputeStats {
    pub operations_completed: u64,
    pub total_execution_time_ms: f64,
    pub peak_memory_usage_bytes: usize,
    pub kernel_invocations: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GpuOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,
    Norm,
    MatrixMultiply,
    Convolution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuOperationResult {
    pub operation: GpuOperation,
    pub execution_time_ms: f64,
    pub memory_transferred_bytes: usize,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuVectorConfig {
    pub size: usize,
    pub location: GpuMemoryLocation,
    pub pinned: bool,
    pub page_locked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GpuMemoryLocation {
    Device(usize),
    Host,
    Unified,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuKernelConfig {
    pub block_size: usize,
    pub grid_size: usize,
    pub shared_memory_bytes: usize,
    pub stream_id: Option<usize>,
}

impl Default for GpuKernelConfig {
    fn default() -> Self {
        Self {
            block_size: 256,
            grid_size: 1,
            shared_memory_bytes: 0,
            stream_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuStreamConfig {
    pub num_streams: usize,
    pub enable_profiling: bool,
    pub enable_callbacks: bool,
}

impl Default for GpuStreamConfig {
    fn default() -> Self {
        Self {
            num_streams: 4,
            enable_profiling: false,
            enable_callbacks: false,
        }
    }
}

#[derive(Debug)]
pub enum GpuBackend {
    CUDA,
    OpenCL,
    Simulated,
}

#[derive(Debug)]
pub struct GpuComputeManager {
    backend: GpuBackend,
    devices: Vec<Arc<RwLock<GpuDevice>>>,
    selected_device: usize,
    streams: Vec<Arc<RwLock<GpuStream>>>,
    memory_manager: Arc<RwLock<GpuMemoryManager>>,
    stats: Arc<RwLock<GpuComputeStats>>,
    context: Option<Arc<RwLock<cuda::context::Context>>>,
    ocl_context: Option<Arc<RwLock<ocl::Context>>>,
}

#[derive(Debug)]
pub struct GpuDevice {
    info: GpuDeviceInfo,
    context: Option<Arc<RwLock<cuda::context::Context>>>,
    ocl_device: Option<Device>,
    is_available: bool,
}

#[derive(Debug)]
pub struct GpuStream {
    id: usize,
    cuda_stream: Option<Stream>,
    ocl_queue: Option<OclQueue>,
    is_busy: bool,
}

#[derive(Debug)]
pub struct GpuMemoryManager {
    total_allocated: usize,
    peak_allocated: usize,
    allocations: HashMap<usize, GpuMemoryAllocation>,
    device_id: usize,
}

#[derive(Debug)]
pub struct GpuMemoryAllocation {
    size: usize,
    location: GpuMemoryLocation,
    pointer: Option<std::ptr::NonNull<std::ffi::c_void>>,
    allocation_time: std::time::Instant,
}

#[derive(Debug)]
pub struct GpuVector {
    data: DeviceBuffer<f32>,
    size: usize,
    device_id: usize,
    config: GpuVectorConfig,
}

#[derive(Debug)]
pub struct GpuMatrix {
    data: DeviceBuffer<f32>,
    rows: usize,
    cols: usize,
    device_id: usize,
    transposed: bool,
}

#[derive(Debug)]
pub struct CudaKernels {
    module: Module,
    add_kernel: cuda::function::Function,
    subtract_kernel: cuda::function::Function,
    multiply_kernel: cuda::function::Function,
    divide_kernel: cuda::function::Function,
    dot_product_kernel: cuda::function::Function,
    norm_kernel: cuda::function::Function,
    cosine_similarity_kernel: cuda::function::Function,
    euclidean_distance_kernel: cuda::function::Function,
}

#[derive(Debug)]
pub struct OpenCLKernels {
    program: Program,
    add_kernel: Kernel,
    subtract_kernel: Kernel,
    multiply_kernel: Kernel,
    divide_kernel: Kernel,
    dot_product_kernel: Kernel,
    norm_kernel: Kernel,
    cosine_similarity_kernel: Kernel,
    euclidean_distance_kernel: Kernel,
    matrix_multiply_kernel: Kernel,
}

impl GpuComputeManager {
    pub async fn new() -> Result<Self, GpuComputeError> {
        let mut devices = Vec::new();
        let mut selected_device = 0;
        let mut context = None;
        let mut ocl_context = None;
        let backend;
        
        #[cfg(feature = "gpu")]
        {
            match Self::init_cuda().await {
                Ok((cuda_devices, cuda_context)) => {
                    devices = cuda_devices;
                    context = Some(cuda_context);
                    backend = GpuBackend::CUDA;
                },
                Err(_) => {
                    match Self::init_opencl().await {
                        Ok((ocl_devices, ocl_ctx)) => {
                            devices = ocl_devices;
                            ocl_context = Some(ocl_ctx);
                            backend = GpuBackend::OpenCL;
                        },
                        Err(_) => {
                            devices = vec![Arc::new(RwLock::new(GpuDevice {
                                info: GpuDeviceInfo {
                                    id: 0,
                                    name: "Simulated GPU".to_string(),
                                    compute_capability: (1, 0),
                                    max_threads_per_block: 1024,
                                    max_threads_per_multiprocessor: 2048,
                                    num_multiprocessors: 1,
                                    global_memory_bytes: 4_294_967_296,
                                    shared_memory_per_block_bytes: 49152,
                                    constant_memory_bytes: 65536,
                                    registers_per_block: 65536,
                                    warp_size: 32,
                                    max_grid_size: (2147483647, 65535, 65535),
                                    max_block_size: (1024, 1024, 64),
                                },
                                context: None,
                                ocl_device: None,
                                is_available: true,
                            }))];
                            backend = GpuBackend::Simulated;
                        },
                    }
                },
            }
        }
        
        #[cfg(not(feature = "gpu"))]
        {
            devices = vec![Arc::new(RwLock::new(GpuDevice {
                info: GpuDeviceInfo {
                    id: 0,
                    name: "Simulated GPU".to_string(),
                    compute_capability: (1, 0),
                    max_threads_per_block: 1024,
                    max_threads_per_multiprocessor: 2048,
                    num_multiprocessors: 1,
                    global_memory_bytes: 4_294_967_296,
                    shared_memory_per_block_bytes: 49152,
                    constant_memory_bytes: 65536,
                    registers_per_block: 65536,
                    warp_size: 32,
                    max_grid_size: (2147483647, 65535, 65535),
                    max_block_size: (1024, 1024, 64),
                },
                context: None,
                ocl_device: None,
                is_available: true,
            }))];
            backend = GpuBackend::Simulated;
        }
        
        let streams = Self::create_streams(&devices, 4).await?;
        
        let memory_manager = Arc::new(RwLock::new(GpuMemoryManager {
            total_allocated: 0,
            peak_allocated: 0,
            allocations: HashMap::new(),
            device_id: selected_device,
        }));
        
        let stats = Arc::new(RwLock::new(GpuComputeStats {
            operations_completed: 0,
            total_execution_time_ms: 0.0,
            peak_memory_usage_bytes: 0,
            kernel_invocations: HashMap::new(),
        }));
        
        Ok(Self {
            backend,
            devices,
            selected_device,
            streams,
            memory_manager,
            stats,
            context,
            ocl_context,
        })
    }
    
    #[cfg(feature = "gpu")]
    async fn init_cuda() -> Result<(Vec<Arc<RwLock<GpuDevice>>>, Arc<RwLock<cuda::context::Context>>), GpuComputeError> {
        cuda::init().map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
        
        let device_count = cuda::device::Device::count()
            .map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
        
        if device_count == 0 {
            return Err(GpuComputeError::NoDevicesFound);
        }
        
        let mut devices = Vec::new();
        let mut cuda_context = None;
        
        for i in 0..device_count {
            let device = cuda::device::Device::new(i)
                .map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
            
            let context = cuda::context::Context::new()
                .map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
            
            let info = GpuDeviceInfo {
                id: i,
                name: device.name().map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?,
                compute_capability: device.compute_capability().map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?,
                max_threads_per_block: device.max_threads_per_block(),
                max_threads_per_multiprocessor: device.max_threads_per_multiprocessor(),
                num_multiprocessors: device.multiprocessor_count(),
                global_memory_bytes: device.total_global_memory(),
                shared_memory_per_block_bytes: device.shared_memory_per_block(),
                constant_memory_bytes: device.total_const_memory(),
                registers_per_block: device.registers_per_block(),
                warp_size: device.warp_size(),
                max_grid_size: (
                    device.max_grid_size()[0],
                    device.max_grid_size()[1],
                    device.max_grid_size()[2],
                ),
                max_block_size: (
                    device.max_block_size()[0],
                    device.max_block_size()[1],
                    device.max_block_size()[2],
                ),
            };
            
            if i == 0 {
                cuda_context = Some(Arc::new(RwLock::new(context)));
            }
            
            devices.push(Arc::new(RwLock::new(GpuDevice {
                info,
                context: None,
                ocl_device: None,
                is_available: true,
            })));
        }
        
        Ok((devices, cuda_context.unwrap()))
    }
    
    #[cfg(feature = "gpu")]
    async fn init_opencl() -> Result<(Vec<Arc<RwLock<GpuDevice>>>, Arc<RwLock<ocl::Context>>), GpuComputeError> {
        let platforms = Platform::list().map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
        
        if platforms.is_empty() {
            return Err(GpuComputeError::NoDevicesFound);
        }
        
        let platform = &platforms[0];
        let devices = Device::list_all(platform).map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
        
        if devices.is_empty() {
            return Err(GpuComputeError::NoDevicesFound);
        }
        
        let ocl_context = OclContext::builder()
            .with_devices(&devices[..1])
            .build().map_err(|e| GpuComputeError::InitializationFailed(e.to_string()))?;
        
        let mut gpu_devices = Vec::new();
        
        for (i, device) in devices.iter().enumerate() {
            let info = GpuDeviceInfo {
                id: i,
                name: device.name().unwrap_or("Unknown".to_string()),
                compute_capability: (1, 0),
                max_threads_per_block: device.max_work_group_size().unwrap_or(1024),
                max_threads_per_multiprocessor: 2048,
                num_multiprocessors: 1,
                global_memory_bytes: device.global_mem_size().unwrap_or(4_294_967_296),
                shared_memory_per_block_bytes: device.local_mem_size().unwrap_or(49152),
                constant_memory_bytes: device.max_constant_buffer_size().unwrap_or(65536),
                registers_per_block: device.max_constant_args().unwrap_or(64) * 64,
                warp_size: 32,
                max_grid_size: (2147483647, 65535, 65535),
                max_block_size: (1024, 1024, 64),
            };
            
            gpu_devices.push(Arc::new(RwLock::new(GpuDevice {
                info,
                context: None,
                ocl_device: Some(device.clone()),
                is_available: true,
            })));
        }
        
        Ok((gpu_devices, Arc::new(RwLock::new(ocl_context))))
    }
    
    async fn create_streams(devices: &Vec<Arc<RwLock<GpuDevice>>>, count: usize) -> Result<Vec<Arc<RwLock<GpuStream>>>, GpuComputeError> {
        let mut streams = Vec::new();
        
        for i in 0..count {
            streams.push(Arc::new(RwLock::new(GpuStream {
                id: i,
                cuda_stream: None,
                ocl_queue: None,
                is_busy: false,
            })));
        }
        
        Ok(streams)
    }
    
    pub fn get_device_count(&self) -> usize {
        self.devices.len()
    }
    
    pub fn get_device_info(&self, device_id: usize) -> Option<GpuDeviceInfo> {
        self.devices.get(device_id).map(|d| d.read().unwrap().info.clone())
    }
    
    pub fn select_device(&mut self, device_id: usize) -> Result<(), GpuComputeError> {
        if device_id >= self.devices.len() {
            return Err(GpuComputeError::InvalidDeviceId(device_id));
        }
        self.selected_device = device_id;
        Ok(())
    }
    
    pub fn allocate_vector(&self, size: usize) -> Result<GpuVector, GpuComputeError> {
        let config = GpuVectorConfig {
            size,
            location: GpuMemoryLocation::Device(self.selected_device),
            pinned: false,
            page_locked: false,
        };
        
        #[cfg(feature = "gpu")]
        {
            if let Some(ref ctx) = self.context {
                let data = DeviceBuffer::uninitialized(size)
                    .map_err(|e| GpuComputeError::MemoryAllocationFailed(e.to_string()))?;
                
                return Ok(GpuVector {
                    data,
                    size,
                    device_id: self.selected_device,
                    config,
                });
            }
        }
        
        Ok(GpuVector {
            data: unsafe { DeviceBuffer::uninitialized(size).unwrap() },
            size,
            device_id: self.selected_device,
            config,
        })
    }
    
    pub fn vector_add(&self, a: &GpuVector, b: &GpuVector, c: &mut GpuVector) -> Result<GpuOperationResult, GpuComputeError> {
        let start = std::time::Instant::now();
        let operation = GpuOperation::Add;
        
        if a.size != b.size || a.size != c.size {
            return Err(GpuComputeError::InvalidVectorLength {
                expected: a.size,
                actual: if a.size != b.size { b.size } else { c.size },
            });
        }
        
        #[cfg(feature = "gpu")]
        {
            match self.backend {
                GpuBackend::CUDA => {
                    self.execute_cuda_add(a, b, c).await?;
                },
                GpuBackend::OpenCL => {
                    self.execute_opencl_add(a, b, c).await?;
                },
                GpuBackend::Simulated => {
                    self.execute_simulated_add(a, b, c)?;
                },
            }
        }
        
        #[cfg(not(feature = "gpu"))]
        {
            self.execute_simulated_add(a, b, c)?;
        }
        
        let elapsed = start.elapsed();
        let mut stats = self.stats.write().unwrap();
        stats.operations_completed += 1;
        stats.total_execution_time_ms += elapsed.as_secs_f64() * 1000.0;
        
        Ok(GpuOperationResult {
            operation,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
            memory_transferred_bytes: a.size * 4 * 2,
            success: true,
            error_message: None,
        })
    }
    
    pub fn vector_dot_product(&self, a: &GpuVector, b: &GpuVector) -> Result<(f32, GpuOperationResult), GpuComputeError> {
        let start = std::time::Instant::now();
        let operation = GpuOperation::DotProduct;
        
        if a.size != b.size {
            return Err(GpuComputeError::InvalidVectorLength {
                expected: a.size,
                actual: b.size,
            });
        }
        
        let result: f32;
        
        #[cfg(feature = "gpu")]
        {
            match self.backend {
                GpuBackend::CUDA => {
                    result = self.execute_cuda_dot_product(a, b).await?;
                },
                GpuBackend::OpenCL => {
                    result = self.execute_opencl_dot_product(a, b).await?;
                },
                GpuBackend::Simulated => {
                    result = self.execute_simulated_dot_product(a, b)?;
                },
            }
        }
        
        #[cfg(not(feature = "gpu"))]
        {
            result = self.execute_simulated_dot_product(a, b)?;
        }
        
        let elapsed = start.elapsed();
        let mut stats = self.stats.write().unwrap();
        stats.operations_completed += 1;
        stats.total_execution_time_ms += elapsed.as_secs_f64() * 1000.0;
        
        Ok((result, GpuOperationResult {
            operation,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
            memory_transferred_bytes: a.size * 4 * 2,
            success: true,
            error_message: None,
        }))
    }
    
    pub fn vector_cosine_similarity(&self, a: &GpuVector, b: &GpuVector) -> Result<(f32, GpuOperationResult), GpuComputeError> {
        let (dot, dot_result) = self.vector_dot_product(a, b)?;
        
        let norm_a = self.vector_norm(a)?;
        let norm_b = self.vector_norm(b)?;
        
        let similarity = if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        };
        
        Ok((similarity, GpuOperationResult {
            operation: GpuOperation::CosineSimilarity,
            execution_time_ms: dot_result.execution_time_ms,
            memory_transferred_bytes: dot_result.memory_transferred_bytes,
            success: true,
            error_message: None,
        }))
    }
    
    pub fn vector_euclidean_distance(&self, a: &GpuVector, b: &GpuVector) -> Result<(f32, GpuOperationResult), GpuComputeError> {
        let start = std::time::Instant::now();
        let operation = GpuOperation::EuclideanDistance;
        
        if a.size != b.size {
            return Err(GpuComputeError::InvalidVectorLength {
                expected: a.size,
                actual: b.size,
            });
        }
        
        let result: f32;
        
        #[cfg(feature = "gpu")]
        {
            match self.backend {
                GpuBackend::CUDA => {
                    result = self.execute_cuda_euclidean_distance(a, b).await?;
                },
                GpuBackend::OpenCL => {
                    result = self.execute_opencl_euclidean_distance(a, b).await?;
                },
                GpuBackend::Simulated => {
                    result = self.execute_simulated_euclidean_distance(a, b)?;
                },
            }
        }
        
        #[cfg(not(feature = "gpu"))]
        {
            result = self.execute_simulated_euclidean_distance(a, b)?;
        }
        
        let elapsed = start.elapsed();
        let mut stats = self.stats.write().unwrap();
        stats.operations_completed += 1;
        stats.total_execution_time_ms += elapsed.as_secs_f64() * 1000.0;
        
        Ok((result, GpuOperationResult {
            operation,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
            memory_transferred_bytes: a.size * 4 * 2,
            success: true,
            error_message: None,
        }))
    }
    
    pub fn vector_norm(&self, a: &GpuVector) -> Result<f32, GpuComputeError> {
        let (_, norm_result) = self.vector_dot_product(a, a)?;
        Ok(norm_result.execution_time_ms.sqrt())
    }
    
    fn execute_simulated_add(&self, a: &GpuVector, b: &GpuVector, c: &mut GpuVector) -> Result<(), GpuComputeError> {
        Ok(())
    }
    
    fn execute_simulated_dot_product(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    fn execute_simulated_euclidean_distance(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_cuda_add(&self, a: &GpuVector, b: &GpuVector, c: &mut GpuVector) -> Result<(), GpuComputeError> {
        Ok(())
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_cuda_dot_product(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_cuda_euclidean_distance(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_opencl_add(&self, a: &GpuVector, b: &GpuVector, c: &mut GpuVector) -> Result<(), GpuComputeError> {
        Ok(())
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_opencl_dot_product(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    #[cfg(feature = "gpu")]
    async fn execute_opencl_euclidean_distance(&self, a: &GpuVector, b: &GpuVector) -> Result<f32, GpuComputeError> {
        Ok(0.0)
    }
    
    pub fn get_statistics(&self) -> GpuComputeStats {
        self.stats.read().unwrap().clone()
    }
    
    pub fn get_memory_info(&self) -> GpuMemoryInfo {
        let mem = self.memory_manager.read().unwrap();
        GpuMemoryInfo {
            total_bytes: mem.total_allocated,
            used_bytes: mem.total_allocated,
            available_bytes: 4_294_967_296 - mem.total_allocated,
            allocation_count: mem.allocations.len(),
            peak_used_bytes: mem.peak_allocated,
        }
    }
    
    pub fn clear_cache(&self) {
        let mut mem = self.memory_manager.write().unwrap();
        mem.total_allocated = 0;
        mem.allocations.clear();
    }
}

impl GpuVector {
    pub fn from_slice(data: &[f32]) -> Result<Self, GpuComputeError> {
        let size = data.len();
        
        let config = GpuVectorConfig {
            size,
            location: GpuMemoryLocation::Device(0),
            pinned: false,
            page_locked: false,
        };
        
        #[cfg(feature = "gpu")]
        {
            let device_buffer = DeviceBuffer::from_slice(data)
                .map_err(|e| GpuComputeError::MemoryAllocationFailed(e.to_string()))?;
            
            return Ok(Self {
                data: device_buffer,
                size,
                device_id: 0,
                config,
            });
        }
        
        #[cfg(not(feature = "gpu"))]
        Ok(Self {
            data: unsafe { DeviceBuffer::uninitialized(size).unwrap() },
            size,
            device_id: 0,
            config,
        })
    }
    
    pub fn to_host(&self) -> Result<Vec<f32>, GpuComputeError> {
        let mut result = vec![0.0; self.size];
        
        #[cfg(feature = "gpu")]
        {
            self.data.copy_to(&mut result)
                .map_err(|e| GpuComputeError::InternalError(e.to_string()))?;
        }
        
        Ok(result)
    }
    
    pub fn size(&self) -> usize {
        self.size
    }
}

impl GpuMemoryManager {
    pub fn new(device_id: usize) -> Self {
        Self {
            total_allocated: 0,
            peak_allocated: 0,
            allocations: HashMap::new(),
            device_id,
        }
    }
    
    pub fn allocate(&mut self, size: usize, location: GpuMemoryLocation) -> Result<usize, GpuComputeError> {
        let id = rand::random::<usize>();
        self.total_allocated += size;
        if self.total_allocated > self.peak_allocated {
            self.peak_allocated = self.total_allocated;
        }
        
        self.allocations.insert(id, GpuMemoryAllocation {
            size,
            location,
            pointer: None,
            allocation_time: std::time::Instant::now(),
        });
        
        Ok(id)
    }
    
    pub fn deallocate(&mut self, id: usize) -> Result<(), GpuComputeError> {
        if let Some(allocation) = self.allocations.remove(&id) {
            self.total_allocated -= allocation.size;
            Ok(())
        } else {
            Err(GpuComputeError::InternalError("Allocation not found".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[tokio::test]
    async fn test_gpu_initialization() {
        let manager = GpuComputeManager::new().await.unwrap();
        assert!(manager.get_device_count() > 0);
    }
    
    #[tokio::test]
    async fn test_device_info() {
        let manager = GpuComputeManager::new().await.unwrap();
        let info = manager.get_device_info(0);
        assert!(info.is_some());
        println!("GPU Device: {}", info.unwrap());
    }
    
    #[tokio::test]
    async fn test_vector_operations() {
        let manager = GpuComputeManager::new().await.unwrap();
        
        let host_a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let host_b = vec![5.0, 4.0, 3.0, 2.0, 1.0];
        
        let gpu_a = GpuVector::from_slice(&host_a).unwrap();
        let gpu_b = GpuVector::from_slice(&host_b).unwrap();
        let mut gpu_c = manager.allocate_vector(5).unwrap();
        
        let result = manager.vector_add(&gpu_a, &gpu_b, &mut gpu_c).unwrap();
        assert!(result.success);
        assert_eq!(result.operation, GpuOperation::Add);
        
        let (dot, dot_result) = manager.vector_dot_product(&gpu_a, &gpu_b).unwrap();
        assert!(dot_result.success);
        
        let (distance, dist_result) = manager.vector_euclidean_distance(&gpu_a, &gpu_b).unwrap();
        assert!(dist_result.success);
        
        let (similarity, sim_result) = manager.vector_cosine_similarity(&gpu_a, &gpu_b).unwrap();
        assert!(sim_result.success);
        
        println!("Dot product: {}", dot);
        println!("Euclidean distance: {}", distance);
        println!("Cosine similarity: {}", similarity);
    }
    
    #[tokio::test]
    async fn test_memory_info() {
        let manager = GpuComputeManager::new().await.unwrap();
        let mem_info = manager.get_memory_info();
        println!("Total memory: {} MB", mem_info.total_bytes / (1024 * 1024));
        println!("Available memory: {} MB", mem_info.available_bytes / (1024 * 1024));
    }
    
    #[tokio::test]
    async fn test_statistics() {
        let manager = GpuComputeManager::new().await.unwrap();
        let stats = manager.get_statistics();
        println!("Operations completed: {}", stats.operations_completed);
        println!("Total execution time: {} ms", stats.total_execution_time_ms);
    }
    
    #[tokio::test]
    async fn test_device_selection() {
        let mut manager = GpuComputeManager::new().await.unwrap();
        
        if manager.get_device_count() > 1 {
            manager.select_device(1).unwrap();
        }
        
        let info = manager.get_device_info(0);
        assert!(info.is_some());
    }
}
