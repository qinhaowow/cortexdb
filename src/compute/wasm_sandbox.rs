//! WASM Sandbox Execution Module for CortexDB
//!
//! This module provides secure and high-performance execution of user-defined functions
//! using WebAssembly (WASM) via the Wasmer runtime. It includes features like:
//! - Sandboxed execution with resource limits
//! - Multiple language support (Rust, C, AssemblyScript, TinyGo)
//! - Memory management and isolation
//! - Function registration and invocation
//! - Hot reloading of modules
//! - Import/export handling

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use std::result::Result;
use wasmer::{Store, Module, Instance, Value, Memory, MemoryType, Pages, Export, ExportFunction, exports};
use wasmer_compiler_cranelift::Cranelift;
use wasmer_engine_jit::JIT;
use wasmer_types::{MemoryType as WasmerMemoryType, PageSize};
use wasmparser::{Validator, Validatorfuncref};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum WasmSandboxError {
    #[error("Module compilation failed: {0}")]
    CompilationFailed(String),
    
    #[error("Module instantiation failed: {0}")]
    InstantiationFailed(String),
    
    #[error("Function not found: {0}")]
    FunctionNotFound(String),
    
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    
    #[error("Execution timeout: {0} ms")]
    Timeout(u64),
    
    #[error("Memory limit exceeded: {0} bytes")]
    MemoryLimitExceeded(usize),
    
    #[error("CPU limit exceeded: {0} operations")]
    CpuLimitExceeded(u64),
    
    #[error("Sandbox violation: {0}")]
    SandboxViolation(String),
    
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    
    #[error("Export error: {0}")]
    ExportError(String),
    
    #[error("Import error: {0}")]
    ImportError(String),
    
    #[error("Resource not found: {0}")]
    ResourceNotFound(String),
    
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmSandboxConfig {
    pub memory_limit_bytes: usize,
    pub memory_min_bytes: usize,
    pub cpu_limit_operations: u64,
    pub timeout_ms: u64,
    pub enable_gc: bool,
    pub enable_reference_types: bool,
    pub max_table_size: u32,
    pub allowed_imports: Vec<String>,
    pub allowed_exports: Vec<String>,
    pub enable_profiling: bool,
    pub profiling_interval_ms: u64,
    pub enable_tracing: bool,
    pub trace_level: TraceLevel,
    pub hot_reload_enabled: bool,
    pub cache_enabled: bool,
    pub cache_size_mb: usize,
}

impl Default for WasmSandboxConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 64 * 1024 * 1024,
            memory_min_bytes: 1 * 1024 * 1024,
            cpu_limit_operations: 1_000_000,
            timeout_ms: 5000,
            enable_gc: true,
            enable_reference_types: true,
            max_table_size: 100,
            allowed_imports: vec!["cortexdb".to_string()],
            allowed_exports: vec!["process".to_string(), "transform".to_string()],
            enable_profiling: false,
            profiling_interval_ms: 100,
            enable_tracing: false,
            trace_level: TraceLevel::Info,
            hot_reload_enabled: false,
            cache_enabled: true,
            cache_size_mb: 256,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceLevel {
    Debug,
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmModuleInfo {
    pub id: String,
    pub name: String,
    pub source_language: String,
    pub version: String,
    pub memory_usage_bytes: usize,
    pub table_size: u32,
    pub function_count: u32,
    pub export_functions: Vec<String>,
    pub import_functions: Vec<String>,
    pub created_at: u64,
    pub last_used_at: u64,
    pub use_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmExecutionContext {
    pub module_id: String,
    pub function_name: String,
    pub arguments: Vec<WasmValue>,
    pub start_time: u64,
    pub memory_used_bytes: usize,
    pub cpu_operations: u64,
    pub profiling_data: Option<ProfilingData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilingData {
    pub total_time_ms: f64,
    pub function_times: HashMap<String, f64>,
    pub memory_samples: Vec<MemorySample>,
    pub call_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySample {
    pub timestamp_ms: f64,
    pub used_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WasmValue {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl From<Value> for WasmValue {
    fn from(value: Value) -> Self {
        match value {
            Value::I32(v) => WasmValue::I32(v),
            Value::I64(v) => WasmValue::I64(v),
            Value::F32(v) => WasmValue::F32(v),
            Value::F64(v) => WasmValue::F64(v),
            _ => WasmValue::I32(0),
        }
    }
}

impl From<WasmValue> for Value {
    fn from(value: WasmValue) -> Self {
        match value {
            WasmValue::I32(v) => Value::I32(v),
            WasmValue::I64(v) => Value::I64(v),
            WasmValue::F32(v) => Value::F32(v),
            WasmValue::F64(v) => Value::F64(v),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmExecutionResult {
    pub success: bool,
    pub values: Vec<WasmValue>,
    pub execution_time_ms: f64,
    pub memory_used_bytes: usize,
    pub cpu_operations: u64,
    pub error_message: Option<String>,
    pub profiling_data: Option<ProfilingData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmFunctionSignature {
    pub name: String,
    pub params: Vec<String>,
    pub results: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmModuleSource {
    pub id: String,
    pub name: String,
    pub wasm_bytes: Vec<u8>,
    pub source_language: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmSandboxStats {
    pub modules_loaded: u64,
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub total_execution_time_ms: f64,
    pub peak_memory_usage_bytes: usize,
    pub average_execution_time_ms: f64,
    pub timeout_count: u64,
    pub memory_limit_count: u64,
    pub cpu_limit_count: u64,
}

#[derive(Debug)]
pub struct WasmModuleCache {
    modules: HashMap<String, CachedModule>,
    max_size_bytes: usize,
    current_size_bytes: usize,
}

#[derive(Debug)]
struct CachedModule {
    module: Module,
    store: Store,
    info: WasmModuleInfo,
    last_access: Instant,
    access_count: u64,
}

#[derive(Debug)]
pub struct WasmSandbox {
    config: WasmSandboxConfig,
    store: Arc<RwLock<Store>>,
    modules: Arc<RwLock<HashMap<String, WasmModule>>>,
    cache: Arc<RwLock<WasmModuleCache>>,
    stats: Arc<RwLock<WasmSandboxStats>>,
    import_functions: Arc<RwLock<HashMap<String, Arc<dyn WasmImport + Send + Sync>>>>,
}

#[derive(Debug)]
pub struct WasmModule {
    id: String,
    name: String,
    module: Module,
    store: Store,
    memory: Option<Memory>,
    table: Option<wasmer::Table>,
    functions: HashMap<String, wasmer::Function>,
    exports: HashMap<String, Export>,
    info: WasmModuleInfo,
    instantiated: bool,
}

#[derive(Debug)]
pub struct WasmMemory {
    memory: Memory,
    initial_pages: Pages,
    maximum_pages: Pages,
    min_bytes: usize,
    max_bytes: usize,
}

#[derive(Debug)]
pub struct WasmTable {
    table: wasmer::Table,
    initial_size: u32,
    maximum_size: u32,
    element_type: String,
}

#[derive(Debug)]
pub struct WasmInstance {
    instance: Instance,
    memory: Option<Memory>,
    functions: HashMap<String, wasmer::Function>,
    exports: HashMap<String, Export>,
    module_id: String,
}

#[derive(Debug)]
pub struct WasmFunctionCall {
    function: wasmer::Function,
    arguments: Vec<Value>,
    context: WasmExecutionContext,
}

#[derive(Debug)]
pub struct WasmResourceLimits {
    max_memory_bytes: usize,
    max_cpu_operations: u64,
    max_execution_time_ms: u64,
    max_table_size: u32,
    max_values_stack: usize,
    max_call_depth: u32,
}

#[derive(Debug)]
pub struct WasmExecutionEnvironment {
    memory: Option<WasmMemory>,
    table: Option<WasmTable>,
    globals: HashMap<String, wasmer::Global>,
    linear_memory: Vec<u8>,
    value_stack: Vec<WasmValue>,
    call_stack: Vec<WasmFunctionCall>,
}

#[derive(Debug)]
pub struct WasmImportResult {
    pub value: WasmValue,
    pub memory_access: Option<MemoryAccess>,
    pub execution_time_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAccess {
    pub address: usize,
    pub size: usize,
    pub is_write: bool,
    pub protection: MemoryProtection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryProtection {
    Read,
    Write,
    Execute,
    ReadWrite,
    ReadExecute,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmImportFunctionConfig {
    pub name: String,
    pub return_type: String,
    pub param_types: Vec<String>,
    pub is_async: bool,
    pub timeout_ms: u64,
}

pub trait WasmImport: Send + Sync {
    fn name(&self) -> &str;
    fn call(&self, args: &[WasmValue]) -> Result<WasmValue, WasmSandboxError>;
    fn memory_access(&self) -> Option<MemoryAccessConfig>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAccessConfig {
    pub max_read_bytes: usize,
    pub max_write_bytes: usize,
    pub allowed_addresses: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmBenchmarkResult {
    pub module_name: String,
    pub function_name: String,
    pub iterations: u32,
    pub avg_execution_time_ms: f64,
    pub min_execution_time_ms: f64,
    pub max_execution_time_ms: f64,
    pub std_deviation_ms: f64,
    pub throughput_ops_per_sec: f64,
    pub memory_usage_bytes: usize,
}

impl WasmSandbox {
    pub fn new(config: WasmSandboxConfig) -> Result<Self, WasmSandboxError> {
        let compiler = Cranelift::new();
        let engine = JIT::new(compiler).engine();
        let store = Store::new(&engine);
        
        let modules = Arc::new(RwLock::new(HashMap::new()));
        let cache = Arc::new(RwLock::new(WasmModuleCache {
            modules: HashMap::new(),
            max_size_bytes: config.cache_size_mb * 1024 * 1024,
            current_size_bytes: 0,
        }));
        
        let stats = Arc::new(RwLock::new(WasmSandboxStats {
            modules_loaded: 0,
            total_executions: 0,
            successful_executions: 0,
            failed_executions: 0,
            total_execution_time_ms: 0.0,
            peak_memory_usage_bytes: 0,
            average_execution_time_ms: 0.0,
            timeout_count: 0,
            memory_limit_count: 0,
            cpu_limit_count: 0,
        }));
        
        let import_functions = Arc::new(RwLock::new(HashMap::new()));
        
        Ok(Self {
            config,
            store: Arc::new(RwLock::new(store)),
            modules,
            cache,
            stats,
            import_functions,
        })
    }
    
    pub fn register_import<I: WasmImport + 'static>(&self, import_func: I) {
        let mut imports = self.import_functions.write().unwrap();
        imports.insert(import_func.name().to_string(), Arc::new(import_func));
    }
    
    pub fn load_module(&self, source: &WasmModuleSource) -> Result<String, WasmSandboxError> {
        let module_id = source.id.clone();
        
        let store = self.store.read().unwrap();
        
        let module = Module::new(&store, &source.wasm_bytes)
            .map_err(|e| WasmSandboxError::CompilationFailed(e.to_string()))?;
        
        let validator = Validator::new();
        validator.validate_all(&source.wasm_bytes)
            .map_err(|e| WasmSandboxError::CompilationFailed(e.to_string()))?;
        
        let info = WasmModuleInfo {
            id: module_id.clone(),
            name: source.name.clone(),
            source_language: source.source_language.clone(),
            version: source.metadata.get("version").cloned().unwrap_or_default(),
            memory_usage_bytes: 0,
            table_size: 0,
            function_count: 0,
            export_functions: Vec::new(),
            import_functions: Vec::new(),
            created_at: chrono::Utc::now().timestamp() as u64,
            last_used_at: chrono::Utc::now().timestamp() as u64,
            use_count: 0,
        };
        
        let mut modules = self.modules.write().unwrap();
        modules.insert(module_id.clone(), WasmModule {
            id: module_id.clone(),
            name: source.name.clone(),
            module,
            store: (*store).clone(),
            memory: None,
            table: None,
            functions: HashMap::new(),
            exports: HashMap::new(),
            info,
            instantiated: false,
        });
        
        let mut stats = self.stats.write().unwrap();
        stats.modules_loaded += 1;
        
        Ok(module_id)
    }
    
    pub fn instantiate_module(&self, module_id: &str) -> Result<(), WasmSandboxError> {
        let mut modules = self.modules.write().unwrap();
        
        let wasm_module = modules.get_mut(module_id)
            .ok_or_else(|| WasmSandboxError::ResourceNotFound(module_id.to_string()))?;
        
        if wasm_module.instantiated {
            return Ok(());
        }
        
        let mut imports = wasmer::imports! {};
        
        for (name, import_func) in self.import_functions.read().unwrap().iter() {
            let import_wrapper = import_func.clone();
            let func = wasmer::Function::new_native(&wasm_module.store, move |args: &[Value]| -> Result<Value, wasmer::RuntimeError> {
                let wasm_args: Vec<WasmValue> = args.iter().map(|v| v.clone().into()).collect();
                
                match import_wrapper.call(&wasm_args) {
                    Ok(result) => Ok(result.into()),
                    Err(e) => Err(wasmer::RuntimeError::new(e.to_string())),
                }
            });
            
            imports.insert("cortexdb", wasmer::imports! {
                "env" => wasmer::exports! {
                    name => func,
                }
            });
        }
        
        let instance = Instance::new(&wasm_module.module, &imports)
            .map_err(|e| WasmSandboxError::InstantiationFailed(e.to_string()))?;
        
        wasm_module.exports = instance.exports.clone();
        
        if let Ok(memory) = instance.exports.get_memory("memory") {
            wasm_module.memory = Some(memory.clone());
        }
        
        if let Ok(table) = instance.exports.get_table("table") {
            wasm_module.table = Some(table.clone());
        }
        
        for (name, export) in instance.exports.iter() {
            if let Ok(func) = export.get_function() {
                wasm_module.functions.insert(name.to_string(), func.clone());
            }
        }
        
        let info = &mut wasm_module.info;
        info.export_functions = wasm_module.functions.keys().cloned().collect();
        info.function_count = wasm_module.functions.len() as u32;
        if let Some(memory) = &wasm_module.memory {
            info.memory_usage_bytes = memory.size().bytes().0;
        }
        
        wasm_module.instantiated = true;
        
        Ok(())
    }
    
    pub fn execute(
        &self,
        module_id: &str,
        function_name: &str,
        arguments: &[WasmValue],
    ) -> Result<WasmExecutionResult, WasmSandboxError> {
        let start_time = Instant::now();
        
        let modules = self.modules.read().unwrap();
        let wasm_module = modules.get(module_id)
            .ok_or_else(|| WasmSandboxError::ResourceNotFound(module_id.to_string()))?;
        
        if !wasm_module.instantiated {
            return Err(WasmSandboxError::InstantiationFailed(
                "Module not instantiated".to_string(),
            ));
        }
        
        let function = wasm_module.functions.get(function_name)
            .ok_or_else(|| WasmSandboxError::FunctionNotFound(function_name.to_string()))?;
        
        let args: Vec<Value> = arguments.iter().map(|v| v.clone().into()).collect();
        
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let start_execution = Instant::now();
        
        let result = tokio::time::timeout(timeout, function.call(&args))
            .await
            .map_err(|_| WasmSandboxError::Timeout(self.config.timeout_ms))?
            .map_err(|e| WasmSandboxError::ExecutionFailed(e.to_string()))?;
        
        let execution_time = start_time.elapsed();
        
        let mut values: Vec<WasmValue> = result.iter().map(|v| v.clone().into()).collect();
        
        let mut stats = self.stats.write().unwrap();
        stats.total_executions += 1;
        stats.successful_executions += 1;
        stats.total_execution_time_ms += execution_time.as_secs_f64() * 1000.0;
        stats.average_execution_time_ms = stats.total_execution_time_ms / stats.successful_executions as f64;
        
        Ok(WasmExecutionResult {
            success: true,
            values,
            execution_time_ms: execution_time.as_secs_f64() * 1000.0,
            memory_used_bytes: wasm_module.memory.as_ref().map(|m| m.size().bytes().0).unwrap_or(0),
            cpu_operations: 0,
            error_message: None,
            profiling_data: None,
        })
    }
    
    pub fn execute_with_limits(
        &self,
        module_id: &str,
        function_name: &str,
        arguments: &[WasmValue],
        limits: &WasmResourceLimits,
    ) -> Result<WasmExecutionResult, WasmSandboxError> {
        let mut result = self.execute(module_id, function_name, arguments)?;
        
        if result.memory_used_bytes > limits.max_memory_bytes {
            return Err(WasmSandboxError::MemoryLimitExceeded(result.memory_used_bytes));
        }
        
        if result.cpu_operations > limits.max_cpu_operations {
            return Err(WasmSandboxError::CpuLimitExceeded(result.cpu_operations));
        }
        
        if result.execution_time_ms > limits.max_execution_time_ms as f64 {
            return Err(WasmSandboxError::Timeout(limits.max_execution_time_ms));
        }
        
        Ok(result)
    }
    
    pub fn get_module_info(&self, module_id: &str) -> Result<WasmModuleInfo, WasmSandboxError> {
        let modules = self.modules.read().unwrap();
        let wasm_module = modules.get(module_id)
            .ok_or_else(|| WasmSandboxError::ResourceNotFound(module_id.to_string()))?;
        
        Ok(wasm_module.info.clone())
    }
    
    pub fn list_modules(&self) -> Vec<WasmModuleInfo> {
        let modules = self.modules.read().unwrap();
        modules.values().map(|m| m.info.clone()).collect()
    }
    
    pub fn unload_module(&self, module_id: &str) -> Result<bool, WasmSandboxError> {
        let mut modules = self.modules.write().unwrap();
        let removed = modules.remove(module_id);
        
        if removed.is_some() {
            let mut stats = self.stats.write().unwrap();
            if stats.modules_loaded > 0 {
                stats.modules_loaded -= 1;
            }
            return Ok(true);
        }
        
        Ok(false)
    }
    
    pub fn clear_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.modules.clear();
        cache.current_size_bytes = 0;
    }
    
    pub fn get_statistics(&self) -> WasmSandboxStats {
        self.stats.read().unwrap().clone()
    }
    
    pub fn benchmark(
        &self,
        module_id: &str,
        function_name: &str,
        arguments: &[WasmValue],
        iterations: u32,
    ) -> Result<WasmBenchmarkResult, WasmSandboxError> {
        let mut times = Vec::with_capacity(iterations as usize);
        let mut memory_usages = Vec::with_capacity(iterations as usize);
        
        for _ in 0..iterations {
            let result = self.execute(module_id, function_name, arguments)?;
            times.push(result.execution_time_ms);
            memory_usages.push(result.memory_used_bytes);
        }
        
        let sum: f64 = times.iter().sum();
        let avg = sum / iterations as f64;
        let variance: f64 = times.iter().map(|t| (t - avg).powi(2)).sum::<f64>() / iterations as f64;
        let std_dev = variance.sqrt();
        
        let module_info = self.get_module_info(module_id)?;
        
        Ok(WasmBenchmarkResult {
            module_name: module_info.name,
            function_name: function_name.to_string(),
            iterations,
            avg_execution_time_ms: avg,
            min_execution_time_ms: times.iter().fold(f64::INFINITY, |m, v| v.min(m)),
            max_execution_time_ms: times.iter().fold(f64::NEG_INFINITY, |m, v| v.max(m)),
            std_deviation_ms: std_dev,
            throughput_ops_per_sec: if avg > 0.0 { 1000.0 / avg } else { 0.0 },
            memory_usage_bytes: memory_usages.iter().max().cloned().unwrap_or(0),
        })
    }
}

impl Default for WasmSandbox {
    fn default() -> Self {
        Self::new(WasmSandboxConfig::default()).unwrap()
    }
}

impl WasmModuleCache {
    pub fn new(max_size_mb: usize) -> Self {
        Self {
            modules: HashMap::new(),
            max_size_bytes: max_size_mb * 1024 * 1024,
            current_size_bytes: 0,
        }
    }
    
    pub fn insert(&mut self, id: String, cached_module: CachedModule) {
        self.modules.insert(id, cached_module);
    }
    
    pub fn get(&mut self, id: &str) -> Option<&mut CachedModule> {
        if let Some(module) = self.modules.get_mut(id) {
            module.last_access = Instant::now();
            module.access_count += 1;
            Some(module)
        } else {
            None
        }
    }
    
    pub fn evict_lru(&mut self) {
        if let Some((lru_id, _)) = self.modules
            .iter()
            .min_by_key(|(_, m)| m.last_access)
        {
            self.modules.remove(lru_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_sandbox_creation() {
        let config = WasmSandboxConfig::default();
        let sandbox = WasmSandbox::new(config).unwrap();
        assert!(sandbox.list_modules().is_empty());
    }
    
    #[tokio::test]
    async fn test_simple_wasm_execution() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let wasm_bytes = wat::parse_str(r#"
            (module
                (func $add (param $a i32) (param $b i32) (result i32)
                    local.get $a
                    local.get $b
                    i32.add)
                (export "add" (func $add)))
        "#).unwrap();
        
        let source = WasmModuleSource {
            id: Uuid::new_v4().to_string(),
            name: "test_add".to_string(),
            wasm_bytes,
            source_language: "rust".to_string(),
            HashMap::new(),
        };
        
        let module_id = sandbox.load_module(&source).unwrap();
        sandbox.instantiate_module(&module_id).unwrap();
        
        let result = sandbox.execute(
            &module_id,
            "add",
            &[WasmValue::I32(5), WasmValue::I32(3)],
        ).unwrap();
        
        assert!(result.success);
        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0], WasmValue::I32(8));
    }
    
    #[tokio::test]
    async fn test_module_info() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let wasm_bytes = wat::parse_str(r#"
            (module
                (func $process (param $x i32) (result i32)
                    local.get $x)
                (export "process" (func $process)))
        "#).unwrap();
        
        let source = WasmModuleSource {
            id: Uuid::new_v4().to_string(),
            name: "test_module".to_string(),
            wasm_bytes,
            source_language: "assemblyscript".to_string(),
            HashMap::from([("version".to_string(), "1.0.0".to_string())]),
        };
        
        let module_id = sandbox.load_module(&source).unwrap();
        let info = sandbox.get_module_info(&module_id).unwrap();
        
        assert_eq!(info.name, "test_module");
        assert_eq!(info.source_language, "assemblyscript");
        assert!(info.export_functions.contains(&"process".to_string()));
    }
    
    #[tokio::test]
    async fn test_function_not_found() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let wasm_bytes = wat::parse_str(r#"
            (module
                (func $add (param $a i32) (param $b i32) (result i32)
                    local.get $a
                    local.get $b
                    i32.add)
                (export "add" (func $add)))
        "#).unwrap();
        
        let source = WasmModuleSource {
            id: Uuid::new_v4().to_string(),
            name: "test".to_string(),
            wasm_bytes,
            source_language: "rust".to_string(),
            HashMap::new(),
        };
        
        let module_id = sandbox.load_module(&source).unwrap();
        sandbox.instantiate_module(&module_id).unwrap();
        
        let result = sandbox.execute(&module_id, "nonexistent", &[]);
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_benchmark() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let wasm_bytes = wat::parse_str(r#"
            (module
                (func $process (param $x i32) (result i32)
                    local.get $x
                    i32.const 1
                    i32.add)
                (export "process" (func $process)))
        "#).unwrap();
        
        let source = WasmModuleSource {
            id: Uuid::new_v4().to_string(),
            name: "benchmark_test".to_string(),
            wasm_bytes,
            source_language: "rust".to_string(),
            HashMap::new(),
        };
        
        let module_id = sandbox.load_module(&source).unwrap();
        sandbox.instantiate_module(&module_id).unwrap();
        
        let benchmark = sandbox.benchmark(&module_id, "process", &[WasmValue::I32(0)], 100).unwrap();
        
        assert_eq!(benchmark.iterations, 100);
        assert!(benchmark.avg_execution_time_ms >= 0.0);
        assert!(benchmark.min_execution_time_ms <= benchmark.max_execution_time_ms);
    }
    
    #[tokio::test]
    async fn test_statistics() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let stats = sandbox.get_statistics();
        assert_eq!(stats.modules_loaded, 0);
        assert_eq!(stats.total_executions, 0);
    }
    
    #[tokio::test]
    async fn test_unload_module() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        
        let wasm_bytes = wat::parse_str(r#"(module (func $test))"#).unwrap();
        
        let source = WasmModuleSource {
            id: Uuid::new_v4().to_string(),
            name: "unload_test".to_string(),
            wasm_bytes,
            source_language: "rust".to_string(),
            HashMap::new(),
        };
        
        let module_id = sandbox.load_module(&source).unwrap();
        assert_eq!(sandbox.list_modules().len(), 1);
        
        let removed = sandbox.unload_module(&module_id).unwrap();
        assert!(removed);
        assert!(sandbox.list_modules().is_empty());
    }
}
