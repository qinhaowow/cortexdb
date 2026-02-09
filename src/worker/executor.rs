use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use std::process::{Command, Child, Stdio};
use std::io::{self, Write};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use tokio::sync::{mpsc, broadcast, Mutex, Semaphore};
use tokio::time::{timeout, interval};
use tokio::process::Command as AsyncCommand;
use uuid::Uuid;
use dashmap::DashMap;
use fs2::FileExt;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Execution error: {0}")]
    ExecutionError(String),
    
    #[error("Timeout error: {0}")]
    TimeoutError(String),
    
    #[error("Resource error: {0}")]
    ResourceError(String),
    
    #[error("State error: {0}")]
    StateError(String),
    
    #[error("IO error: {0}")]
    IoError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub max_concurrent_tasks: usize,
    pub task_timeout: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub task_dir: String,
    pub env_vars: HashMap<String, String>,
    pub isolation: IsolationConfig,
    pub resource_limits: ResourceLimits,
    pub stdout_buffer_size: usize,
    pub stderr_buffer_size: usize,
    pub streaming: bool,
    pub auto_cleanup: bool,
    pub cleanup_delay: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsolationConfig {
    pub enabled: bool,
    pub use_namespaces: bool,
    pub use_cgroups: bool,
    pub container_runtime: Option<String>,
    pub memory_limit_mb: u64,
    pub cpu_limit: f32,
    pub network隔离: bool,
    pub disk_quota_bytes: u64,
    pub read_only_rootfs: bool,
    pub user_namespace: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub max_memory_bytes: u64,
    pub max_cpu_seconds: u64,
    pub max_file_descriptors: u32,
    pub max_processes: u32,
    pub max_file_size_bytes: u64,
    pub max_locked_memory_bytes: u64,
    pub max_stack_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub request_id: String,
    pub task_type: String,
    pub command: String,
    pub args: Vec<String>,
    pub env_vars: HashMap<String, String>,
    pub working_dir: Option<String>,
    pub input_data: Option<Vec<u8>>,
    pub timeout: Option<Duration>,
    pub priority: ExecutionPriority,
    pub resources: ResourceRequirements,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ExecutionPriority {
    Critical = 0,
    High = 1,
    Normal = 2,
    Low = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub cpu_cores: f32,
    pub memory_bytes: u64,
    pub gpu_required: bool,
    pub gpu_memory_bytes: u64,
    pub disk_io_ops: u64,
    pub network_bandwidth_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub request_id: String,
    pub task_id: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub output_data: Option<Vec<u8>>,
    pub execution_time_ms: u64,
    pub peak_memory_bytes: u64,
    pub peak_cpu_percent: f32,
    pub started_at: u64,
    pub completed_at: u64,
    pub error: Option<String>,
    pub retry_count: u32,
    pub metrics: ExecutionMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub syscalls: u64,
    pub context_switches: u64,
    pub page_faults: u64,
    pub ipc_count: u64,
    pub network_packets: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskState {
    pub task_id: String,
    pub request_id: String,
    pub state: TaskExecutionState,
    pub worker_id: String,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub attempt: u32,
    pub pid: Option<u32>,
    pub resource_usage: ResourceUsage,
    pub progress: Option<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TaskExecutionState {
    Pending,
    Queued,
    Starting,
    Running,
    OutputStreaming,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Retrying,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub cpu_percent: f32,
    pub thread_count: u32,
    pub file_descriptor_count: u32,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub network_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTemplate {
    pub template_id: String,
    pub name: String,
    pub description: String,
    pub command: String,
    pub args: Vec<String>,
    pub env_vars: HashMap<String, String>,
    pub default_resources: ResourceRequirements,
    pub timeout: Duration,
    pub retries: u32,
    pub tags: Vec<String>,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub plan_id: String,
    pub tasks: Vec<ExecutionTask>,
    pub dependencies: Vec<TaskDependency>,
    pub strategy: ExecutionStrategy,
    pub total_estimated_time_ms: u64,
    pub total_resources: ResourceRequirements,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub task_id: String,
    pub template_id: String,
    pub command: String,
    pub args: Vec<String>,
    pub env_vars: HashMap<String, String>,
    pub resources: ResourceRequirements,
    pub timeout: Duration,
    pub retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    pub from: String,
    pub to: String,
    pub dependency_type: DependencyType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DependencyType {
    FinishToStart,
    StartToStart,
    FinishToFinish,
    StartToFinish,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    Sequential,
    Parallel,
    Pipelined,
    Dynamic,
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorStats {
    pub total_tasks_submitted: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub total_tasks_retried: u64,
    pub current_running: usize,
    pub current_queued: usize,
    pub avg_execution_time_ms: f64,
    pub throughput_tasks_per_sec: f64,
    pub success_rate: f64,
    pub resource_utilization: ResourceUtilization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    pub avg_cpu_percent: f32,
    pub avg_memory_bytes: u64,
    pub peak_memory_bytes: u64,
    pub disk_io_ops_per_sec: f64,
    pub network_bytes_per_sec: u64,
}

struct ActiveTask {
    task_id: String,
    child: Child,
    start_time: Instant,
    stdin: Option<ChildStdin>,
    stdout_reader: Option<tokio::io::ReadHalf<tokio::process::ChildStdout>>,
    stderr_reader: Option<tokio::io::ReadHalf<tokio::process::ChildStderr>>,
    output_buffer: String,
    error_buffer: String,
}

pub struct TaskExecutor {
    config: ExecutorConfig,
    active_tasks: Arc<DashMap<String, ActiveTask>>,
    task_queue: Arc<Mutex<Vec<ExecutionRequest>>>,
    completed_tasks: Arc<DashMap<String, ExecutionResult>>,
    failed_tasks: Arc<DashMap<String, ExecutionResult>>,
    task_templates: Arc<DashMap<String, TaskTemplate>>,
    semaphore: Arc<Semaphore>,
    event_sender: Arc<Mutex<Option<mpsc::Sender<ExecutorEvent>>>>,
    shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
    stats: Arc<ExecutorStatsInternal>,
    resource_monitor: Arc<ResourceMonitor>,
}

struct ExecutorStatsInternal {
    total_submitted: std::sync::atomic::AtomicU64,
    total_completed: std::sync::atomic::AtomicU64,
    total_failed: std::sync::atomic::AtomicU64,
    total_retries: std::sync::atomic::AtomicU64,
    execution_times: Mutex<VecDeque<u64>>,
    last_throughput_calc: Mutex<Instant>,
    throughput: std::sync::atomic::AtomicU64,
}

struct ResourceMonitor {
    cpu_samples: Mutex<Vec<(Instant, f32)>>,
    memory_samples: Mutex<Vec<(Instant, u64)>>,
    disk_samples: Mutex<Vec<(Instant, u64)>>,
    network_samples: Mutex<Vec<(Instant, u64)>>,
}

#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    TaskStarted(String, String),
    TaskCompleted(String, ExecutionResult),
    TaskFailed(String, ExecutionResult),
    TaskOutput(String, String),
    TaskProgress(String, f32),
    ResourceWarning(String, ResourceUsage),
    Error(String),
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 10,
            task_timeout: Duration::from_secs(300),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
            task_dir: "/tmp/cortex_tasks".to_string(),
            env_vars: HashMap::new(),
            isolation: IsolationConfig::default(),
            resource_limits: ResourceLimits::default(),
            stdout_buffer_size: 64 * 1024,
            stderr_buffer_size: 16 * 1024,
            streaming: true,
            auto_cleanup: true,
            cleanup_delay: Duration::from_secs(300),
        }
    }
}

impl Default for IsolationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            use_namespaces: false,
            use_cgroups: false,
            container_runtime: None,
            memory_limit_mb: 512,
            cpu_limit: 1.0,
            network隔离: false,
            disk_quota_bytes: 1024 * 1024 * 1024,
            read_only_rootfs: false,
            user_namespace: false,
        }
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024,
            max_cpu_seconds: 3600,
            max_file_descriptors: 1024,
            max_processes: 256,
            max_file_size_bytes: 1024 * 1024 * 1024,
            max_locked_memory_bytes: 64 * 1024 * 1024,
            max_stack_bytes: 8 * 1024 * 1024,
        }
    }
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            cpu_cores: 1.0,
            memory_bytes: 256 * 1024 * 1024,
            gpu_required: false,
            gpu_memory_bytes: 0,
            disk_io_ops: 1000,
            network_bandwidth_bytes: 10 * 1024 * 1024,
        }
    }
}

impl Default for ExecutionMetrics {
    fn default() -> Self {
        Self {
            read_bytes: 0,
            written_bytes: 0,
            syscalls: 0,
            context_switches: 0,
            page_faults: 0,
            ipc_count: 0,
            network_packets: 0,
        }
    }
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            memory_bytes: 0,
            cpu_percent: 0.0,
            thread_count: 0,
            file_descriptor_count: 0,
            disk_read_bytes: 0,
            disk_write_bytes: 0,
            network_bytes: 0,
        }
    }
}

impl TaskExecutor {
    pub async fn new(config: Option<ExecutorConfig>) -> Result<Self, ExecutorError> {
        let config = config.unwrap_or_default();
        
        std::fs::create_dir_all(&config.task_dir)
            .map_err(|e| ExecutorError::IoError(e.to_string()))?;
        
        let active_tasks = Arc::new(DashMap::new());
        let task_queue = Arc::new(Mutex::new(Vec::new()));
        let completed_tasks = Arc::new(DashMap::new());
        let failed_tasks = Arc::new(DashMap::new());
        let task_templates = Arc::new(DashMap::new());
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));
        
        let (event_sender, _) = mpsc::channel(1000);
        
        let stats = Arc::new(ExecutorStatsInternal {
            total_submitted: std::sync::atomic::AtomicU64::new(0),
            total_completed: std::sync::atomic::AtomicU64::new(0),
            total_failed: std::sync::atomic::AtomicU64::new(0),
            total_retries: std::sync::atomic::AtomicU64::new(0),
            execution_times: Mutex::new(VecDeque::new()),
            last_throughput_calc: Mutex::new(Instant::now()),
            throughput: std::sync::atomic::AtomicU64::new(0),
        });
        
        let resource_monitor = Arc::new(ResourceMonitor {
            cpu_samples: Mutex::new(Vec::new()),
            memory_samples: Mutex::new(Vec::new()),
            disk_samples: Mutex::new(Vec::new()),
            network_samples: Mutex::new(Vec::new()),
        });
        
        Ok(Self {
            config,
            active_tasks,
            task_queue,
            completed_tasks,
            failed_tasks,
            task_templates,
            semaphore,
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            shutdown_signal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stats,
            resource_monitor,
        })
    }
    
    pub async fn start(&mut self) -> Result<(), ExecutorError> {
        info!("Task executor started with max_concurrent_tasks={}", self.config.max_concurrent_tasks);
        Ok(())
    }
    
    pub async fn stop(&mut self) {
        self.shutdown_signal.store(true, std::sync::atomic::Ordering::SeqCst);
        
        let task_ids: Vec<String> = self.active_tasks
            .iter()
            .map(|t| t.key().clone())
            .collect();
        
        for task_id in task_ids {
            self.cancel_task(&task_id).await.ok();
        }
        
        info!("Task executor stopped");
    }
    
    pub async fn submit(&self, request: ExecutionRequest) -> Result<String, ExecutorError> {
        let task_id = Uuid::new_v4().to_string();
        let request = ExecutionRequest {
            request_id: request.request_id,
            task_type: request.task_type,
            command: request.command,
            args: request.args,
            env_vars: request.env_vars,
            working_dir: request.working_dir,
            input_data: request.input_data,
            timeout: request.timeout.or(Some(self.config.task_timeout)),
            priority: request.priority,
            resources: request.resources,
            dependencies: request.dependencies,
        };
        
        self.stats.total_submitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let mut queue = self.task_queue.lock().await;
        queue.push(request);
        
        Ok(task_id)
    }
    
    pub async fn submit_template(&self, template_id: &str, params: HashMap<String, String>) 
        -> Result<String, ExecutorError> {
        let template = self.task_templates.get(template_id)
            .ok_or_else(|| ExecutorError::ExecutionError("Template not found".to_string()))?;
        
        let mut command = template.command.clone();
        let mut args = template.args.clone();
        
        for (key, value) in &params {
            command = command.replace(&format!("${{{{ {} }}}}", key), value);
            for arg in &mut args {
                *arg = arg.replace(&format!("${{{{ {} }}}}", key), value);
            }
        }
        
        let mut env_vars = template.env_vars.clone();
        env_vars.extend(params.iter()
            .map(|(k, v)| (format!("PARAM_{}", k.to_uppercase()), v.clone())));
        
        let request = ExecutionRequest {
            request_id: Uuid::new_v4().to_string(),
            task_type: template.name.clone(),
            command,
            args,
            env_vars,
            working_dir: None,
            input_data: None,
            timeout: Some(template.timeout),
            priority: ExecutionPriority::Normal,
            resources: template.default_resources.clone(),
            dependencies: Vec::new(),
        };
        
        self.submit(request).await
    }
    
    pub async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResult, ExecutorError> {
        let _permit = self.semaphore.acquire().await
            .map_err(|e| ExecutorError::ExecutionError(e.to_string()))?;
        
        let task_id = Uuid::new_v4().to_string();
        let start_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let timeout = request.timeout.unwrap_or(self.config.task_timeout);
        
        let env_vars: HashMap<String, String> = self.config.env_vars
            .iter()
            .chain(request.env_vars.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        let working_dir = request.working_dir
            .unwrap_or_else(|| format!("{}/{}", self.config.task_dir, task_id));
        
        std::fs::create_dir_all(&working_dir)
            .map_err(|e| ExecutorError::IoError(e.to_string()))?;
        
        let output = self.run_command(&request.command, &request.args, &env_vars, &working_dir, timeout).await;
        
        let end_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let execution_time_ms = (end_time - start_time) * 1000;
        
        let result = ExecutionResult {
            request_id: request.request_id,
            task_id: task_id.clone(),
            success: output.success,
            exit_code: output.exit_code,
            stdout: Some(output.stdout),
            stderr: Some(output.stderr),
            output_data: None,
            execution_time_ms,
            peak_memory_bytes: 0,
            peak_cpu_percent: 0.0,
            started_at: start_time,
            completed_at: end_time,
            error: output.error,
            retry_count: 0,
            metrics: ExecutionMetrics::default(),
        };
        
        if result.success {
            self.stats.total_completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.completed_tasks.insert(task_id.clone(), result.clone());
        } else {
            self.stats.total_failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.failed_tasks.insert(task_id.clone(), result.clone());
        }
        
        let mut exec_times = self.stats.execution_times.lock().unwrap();
        if exec_times.len() >= 1000 {
            exec_times.pop_front();
        }
        exec_times.push_back(execution_time_ms);
        
        if self.config.auto_cleanup {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(300)).await;
                let _ = std::fs::remove_dir_all(&working_dir);
            });
        }
        
        Ok(result)
    }
    
    async fn run_command(
        &self,
        command: &str,
        args: &[String],
        env_vars: &HashMap<String, String>,
        working_dir: &str,
        timeout: Duration,
    ) -> CommandOutput {
        let mut child = Command::new(command)
            .args(args)
            .envs(env_vars)
            .current_dir(working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
            .map_err(|e| CommandOutput {
                success: false,
                exit_code: None,
                stdout: String::new(),
                stderr: e.to_string(),
                error: Some(e.to_string()),
            })?;
        
        let mut output = CommandOutput {
            success: true,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            error: None,
        };
        
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        
        if let Some(mut input) = stdin {
            input.flush().ok();
        }
        
        let timeout_result = timeout(timeout, child.wait()).await;
        
        match timeout_result {
            Ok(wait_result) => {
                output.exit_code = Some(wait_result.code().unwrap_or(-1));
                output.success = wait_result.success();
            }
            Err(_) => {
                let _ = child.kill();
                output.success = false;
                output.exit_code = Some(-9);
                output.error = Some("Command timed out".to_string());
            }
        }
        
        output
    }
    
    pub async fn cancel_task(&self, task_id: &str) -> Result<bool, ExecutorError> {
        if let Some((_, task)) = self.active_tasks.remove(task_id) {
            let _ = task.child.kill();
            self.semaphore.add_permits(1);
            return Ok(true);
        }
        Ok(false)
    }
    
    pub async fn get_task_result(&self, task_id: &str) -> Option<ExecutionResult> {
        self.completed_tasks.get(task_id).map(|r| r.clone())
            .or_else(|| self.failed_tasks.get(task_id).map(|r| r.clone()))
    }
    
    pub async fn get_active_tasks(&self) -> Vec<TaskState> {
        self.active_tasks.iter()
            .map(|t| TaskState {
                task_id: t.key().clone(),
                request_id: String::new(),
                state: TaskExecutionState::Running,
                worker_id: String::new(),
                start_time: Some(t.start_time.elapsed().as_secs()),
                end_time: None,
                attempt: 1,
                pid: None,
                resource_usage: ResourceUsage::default(),
                progress: None,
            })
            .collect()
    }
    
    pub async fn register_template(&self, template: TaskTemplate) {
        self.task_templates.insert(template.template_id.clone(), template);
    }
    
    pub async fn get_stats(&self) -> ExecutorStats {
        let total_submitted = self.stats.total_submitted.load(std::sync::atomic::Ordering::SeqCst);
        let total_completed = self.stats.total_completed.load(std::sync::atomic::Ordering::SeqCst);
        let total_failed = self.stats.total_failed.load(std::sync::atomic::Ordering::SeqCst);
        
        let exec_times = self.stats.execution_times.lock().unwrap();
        let avg_time = if !exec_times.is_empty() {
            exec_times.iter().sum::<u64>() as f64 / exec_times.len() as f64
        } else {
            0.0
        };
        
        let success_rate = if total_submitted > 0 {
            total_completed as f64 / total_submitted as f64
        } else {
            0.0
        };
        
        ExecutorStats {
            total_tasks_submitted: total_submitted,
            total_tasks_completed: total_completed,
            total_tasks_failed: total_failed,
            total_tasks_retried: self.stats.total_retries.load(std::sync::atomic::Ordering::SeqCst),
            current_running: self.active_tasks.len(),
            current_queued: self.task_queue.lock().await.len(),
            avg_execution_time_ms: avg_time,
            throughput_tasks_per_sec: self.stats.throughput.load(std::sync::atomic::Ordering::SeqCst) as f64,
            success_rate,
            resource_utilization: ResourceUtilization {
                avg_cpu_percent: 0.0,
                avg_memory_bytes: 0,
                peak_memory_bytes: 0,
                disk_io_ops_per_sec: 0.0,
                network_bytes_per_sec: 0,
            },
        }
    }
    
    fn send_event(&self, event: ExecutorEvent) {
        if let Some(ref sender) = *self.event_sender.lock().unwrap() {
            let _ = sender.send(event);
        }
    }
}

struct CommandOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    error: Option<String>,
}

impl Default for TaskTemplate {
    fn default() -> Self {
        Self {
            template_id: String::new(),
            name: String::new(),
            description: String::new(),
            command: String::new(),
            args: Vec::new(),
            env_vars: HashMap::new(),
            default_resources: ResourceRequirements::default(),
            timeout: Duration::from_secs(300),
            retries: 3,
            tags: Vec::new(),
            version: 1,
        }
    }
}

impl Default for ExecutionPlan {
    fn default() -> Self {
        Self {
            plan_id: String::new(),
            tasks: Vec::new(),
            dependencies: Vec::new(),
            strategy: ExecutionStrategy::Sequential,
            total_estimated_time_ms: 0,
            total_resources: ResourceRequirements::default(),
        }
    }
}

impl Default for ExecutorStats {
    fn default() -> Self {
        Self {
            total_tasks_submitted: 0,
            total_tasks_completed: 0,
            total_tasks_failed: 0,
            total_tasks_retried: 0,
            current_running: 0,
            current_queued: 0,
            avg_execution_time_ms: 0.0,
            throughput_tasks_per_sec: 0.0,
            success_rate: 0.0,
            resource_utilization: ResourceUtilization::default(),
        }
    }
}

impl Default for ResourceUtilization {
    fn default() -> Self {
        Self {
            avg_cpu_percent: 0.0,
            avg_memory_bytes: 0,
            peak_memory_bytes: 0,
            disk_io_ops_per_sec: 0.0,
            network_bytes_per_sec: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_executor_creation() {
        let executor = TaskExecutor::new(None).await;
        assert!(executor.is_ok());
    }

    #[tokio::test]
    async fn test_executor_submit() {
        let executor = TaskExecutor::new(None).await.unwrap();
        let _ = executor.start().await;
        
        let request = ExecutionRequest {
            request_id: "test_1".to_string(),
            task_type: "test".to_string(),
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            env_vars: HashMap::new(),
            working_dir: None,
            input_data: None,
            timeout: Some(Duration::from_secs(10)),
            priority: ExecutionPriority::Normal,
            resources: ResourceRequirements::default(),
            dependencies: Vec::new(),
        };
        
        let task_id = executor.submit(request).await;
        assert!(task_id.is_ok());
        assert!(!task_id.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_executor_execution() {
        let executor = TaskExecutor::new(None).await.unwrap();
        let _ = executor.start().await;
        
        let request = ExecutionRequest {
            request_id: "test_2".to_string(),
            task_type: "shell".to_string(),
            command: "echo".to_string(),
            args: vec!["test_output".to_string()],
            env_vars: HashMap::new(),
            working_dir: None,
            input_data: None,
            timeout: Some(Duration::from_secs(10)),
            priority: ExecutionPriority::Normal,
            resources: ResourceRequirements::default(),
            dependencies: Vec::new(),
        };
        
        let result = executor.execute(request).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.success);
        assert!(result.stdout.contains("test_output"));
    }

    #[tokio::test]
    async fn test_executor_cancel() {
        let executor = TaskExecutor::new(None).await.unwrap();
        let _ = executor.start().await;
        
        let request = ExecutionRequest {
            request_id: "test_3".to_string(),
            task_type: "shell".to_string(),
            command: "sleep".to_string(),
            args: vec!["60".to_string()],
            env_vars: HashMap::new(),
            working_dir: None,
            input_data: None,
            timeout: Some(Duration::from_secs(30)),
            priority: ExecutionPriority::Normal,
            resources: ResourceRequirements::default(),
            dependencies: Vec::new(),
        };
        
        let task_id = executor.submit(request).await.unwrap();
        let cancelled = executor.cancel_task(&task_id).await;
        assert!(cancelled.is_ok());
        assert!(cancelled.unwrap());
    }

    #[tokio::test]
    async fn test_task_template() {
        let executor = TaskExecutor::new(None).await.unwrap();
        
        let template = TaskTemplate {
            template_id: "echo_template".to_string(),
            name: "Echo Template".to_string(),
            description: "Template for echo command".to_string(),
            command: "echo".to_string(),
            args: vec!["${{ message }}".to_string()],
            env_vars: HashMap::new(),
            default_resources: ResourceRequirements::default(),
            timeout: Duration::from_secs(10),
            retries: 1,
            tags: vec!["test".to_string()],
            version: 1,
        };
        
        executor.register_template(template).await;
        
        let mut params = HashMap::new();
        params.insert("message".to_string(), "Hello World".to_string());
        
        let task_id = executor.submit_template("echo_template", params).await;
        assert!(task_id.is_ok());
    }

    #[tokio::test]
    async fn test_executor_stats() {
        let executor = TaskExecutor::new(None).await.unwrap();
        
        let stats = executor.get_stats().await;
        assert_eq!(stats.total_tasks_submitted, 0);
        assert_eq!(stats.total_tasks_completed, 0);
    }

    #[tokio::test]
    async fn test_resource_requirements() {
        let requirements = ResourceRequirements {
            cpu_cores: 2.0,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            gpu_required: true,
            gpu_memory_bytes: 8 * 1024 * 1024 * 1024,
            disk_io_ops: 5000,
            network_bandwidth_bytes: 100 * 1024 * 1024,
        };
        
        assert!(requirements.gpu_required);
        assert_eq!(requirements.cpu_cores, 2.0);
    }

    #[tokio::test]
    async fn test_isolation_config() {
        let config = IsolationConfig {
            enabled: true,
            use_namespaces: true,
            use_cgroups: true,
            container_runtime: Some("docker".to_string()),
            memory_limit_mb: 1024,
            cpu_limit: 2.0,
            network隔离: true,
            disk_quota_bytes: 10 * 1024 * 1024 * 1024,
            read_only_rootfs: true,
            user_namespace: true,
        };
        
        assert!(config.enabled);
        assert!(config.use_namespaces);
        assert!(config.network隔离);
    }
}
