use std::collections::{HashMap, VecDeque, BinaryHeap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};
use std::cmp::{Ordering, PartialOrd};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use tokio::sync::{mpsc, oneshot, broadcast};
use tokio::time::{timeout, Interval};
use uuid::Uuid;
use dashmap::DashMap;
use rand::Rng;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Queue error: {0}")]
    QueueError(String),
    
    #[error("Task error: {0}")]
    TaskError(String),
    
    #[error("Execution error: {0}")]
    ExecutionError(String),
    
    #[error("Timeout error: {0}")]
    TimeoutError(String),
    
    #[error("Resource error: {0}")]
    ResourceError(String),
    
    #[error("State error: {0}")]
    StateError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub max_concurrent_tasks: usize,
    pub default_timeout: Duration,
    pub retry_count: u32,
    pub retry_delay: Duration,
    pub task_ttl: Duration,
    pub cleanup_interval: Duration,
    pub enable_prioritization: bool,
    pub enable_affinity: bool,
    pub max_queue_size: usize,
    pub fair_scheduling: bool,
    pub task_weights: HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub task_type: String,
    pub payload: Vec<u8>,
    pub metadata: TaskMetadata,
    pub schedule: TaskSchedule,
    pub priority: TaskPriority,
    pub state: TaskState,
    pub dependencies: Vec<String>,
    pub created_at: u64,
    pub scheduled_at: Option<u64>,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub resources: TaskResources,
    pub affinity: TaskAffinity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub correlation_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub source: String,
    pub user_id: Option<String>,
    pub tenant_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSchedule {
    pub trigger_type: TriggerType,
    pub cron_expression: Option<String>,
    pub interval: Option<Duration>,
    pub delay: Option<Duration>,
    pub at: Option<u64>,
    pub max_executions: u32,
    pub execution_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    Immediate,
    Scheduled,
    Cron,
    Interval,
    Event,
    Dependency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskPriority {
    Critical = 0,
    High = 1,
    Normal = 2,
    Low = 3,
    Background = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Queued,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Retrying,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResources {
    pub cpu_cores: f32,
    pub memory_bytes: u64,
    pub gpu_required: bool,
    pub gpu_memory_bytes: u64,
    pub storage_bytes: u64,
    pub network_bandwidth: u64,
    pub custom_resources: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAffinity {
    pub node_affinity: Vec<String>,
    pub node_anti_affinity: Vec<String>,
    pub pod_affinity: Option<PodAffinity>,
    pub preferred_nodes: Vec<String>,
    pub required_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodAffinity {
    pub label_selector: HashMap<String, String>,
    pub topology_key: String,
    pub namespaces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: String,
    pub success: bool,
    pub output: Option<Vec<u8>>,
    pub error: Option<String>,
    pub metrics: TaskMetrics,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub queue_time_ms: u64,
    pub execution_time_ms: u64,
    pub total_time_ms: u64,
    pub memory_peak_bytes: u64,
    pub cpu_time_ms: u64,
    pub io_bytes: u64,
    pub network_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStats {
    pub total_tasks_submitted: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub total_tasks_retried: u64,
    pub current_queue_size: usize,
    pub current_running: usize,
    pub avg_queue_time_ms: f64,
    pub avg_execution_time_ms: f64,
    pub throughput_tasks_per_sec: f64,
    pub utilization_percent: f64,
    pub queue_latency_percentiles: LatencyPercentiles,
    pub state_distribution: HashMap<TaskState, u64>,
    pub priority_distribution: HashMap<TaskPriority, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p75_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub hostname: String,
    pub capacity: WorkerCapacity,
    pub current_load: WorkerLoad,
    pub status: WorkerStatus,
    pub last_heartbeat: u64,
    pub labels: HashMap<String, String>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapacity {
    pub total_cpu_cores: u32,
    pub total_memory_bytes: u64,
    pub total_gpu_count: u32,
    pub total_gpu_memory_bytes: u64,
    pub max_concurrent_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLoad {
    pub current_cpu_percent: f32,
    pub current_memory_bytes: u64,
    pub current_tasks: usize,
    pub queued_tasks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WorkerStatus {
    Online,
    Offline,
    Busy,
    Draining,
    Maintenance,
}

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    task: Arc<Task>,
    scheduled_time: Instant,
    priority: TaskPriority,
    weight: u32,
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ScheduledTask {}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id == other.task.task_id
    }
}

struct TaskWrapper {
    task: Arc<Task>,
    inserted_at: Instant,
    attempts: u32,
}

impl Ord for TaskWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        let prio_cmp = other.task.priority.cmp(&self.task.priority);
        if prio_cmp != Ordering::Equal {
            return prio_cmp;
        }
        self.inserted_at.cmp(&other.inserted_at)
    }
}

impl PartialOrd for TaskWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for TaskWrapper {}

impl PartialEq for TaskWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id == other.task.task_id
    }
}

pub struct TaskQueue {
    queues: DashMap<TaskPriority, VecDeque<Arc<Task>>>,
    priority_heap: BinaryHeap<TaskWrapper>,
    fair_queues: DashMap<String, BinaryHeap<TaskWrapper>>,
    config: SchedulerConfig,
    total_size: Arc<std::sync::atomic::AtomicUsize>,
}

impl TaskQueue {
    pub fn new(config: SchedulerConfig) -> Self {
        let total_size = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        
        let mut queues = DashMap::new();
        for priority in [
            TaskPriority::Critical,
            TaskPriority::High,
            TaskPriority::Normal,
            TaskPriority::Low,
            TaskPriority::Background,
        ] {
            queues.insert(priority, VecDeque::new());
        }
        
        Self {
            queues,
            priority_heap: BinaryHeap::new(),
            fair_queues: DashMap::new(),
            config,
            total_size,
        }
    }
    
    pub fn push(&mut self, task: Arc<Task>) -> Result<(), SchedulerError> {
        let current_size = self.total_size.load(std::sync::atomic::Ordering::SeqCst);
        if current_size >= self.config.max_queue_size {
            return Err(SchedulerError::QueueError("Queue is full".to_string()));
        }
        
        if let Some(mut queue) = self.queues.get_mut(&task.priority) {
            queue.push_back(task.clone());
            self.total_size.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        
        self.priority_heap.push(TaskWrapper {
            task,
            inserted_at: Instant::now(),
            attempts: 0,
        });
        
        Ok(())
    }
    
    pub fn pop(&mut self) -> Option<Arc<Task>> {
        let mut attempts = 0;
        let max_attempts = 100;
        
        while attempts < max_attempts {
            if let Some(wrapper) = self.priority_heap.pop() {
                if let Some(mut queue) = self.queues.get_mut(&wrapper.task.priority) {
                    if let Some(task) = queue.pop_front() {
                        if task.task_id == wrapper.task.task_id {
                            self.total_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                            return Some(task);
                        }
                    }
                }
            }
            attempts += 1;
        }
        
        None
    }
    
    pub fn peek(&self) -> Option<&Arc<Task>> {
        self.priority_heap.peek().map(|w| &w.task)
    }
    
    pub fn len(&self) -> usize {
        self.total_size.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    pub fn capacity(&self) -> usize {
        self.config.max_queue_size
    }
    
    pub fn contains(&self, task_id: &str) -> bool {
        for queue in self.queues.values() {
            for task in queue.iter() {
                if task.task_id == task_id {
                    return true;
                }
            }
        }
        false
    }
    
    pub fn remove(&mut self, task_id: &str) -> Option<Arc<Task>> {
        for mut queue in self.queues.iter_mut() {
            if let Some(pos) = queue.value().iter().position(|t| t.task_id == task_id) {
                if let Some(task) = queue.value().remove(pos) {
                    self.total_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    
                    self.priority_heap = self.priority_heap.into_iter()
                        .filter(|w| w.task.task_id != task_id)
                        .collect();
                    
                    return Some(task);
                }
            }
        }
        None
    }
    
    pub fn clear(&mut self) {
        for mut queue in self.queues.iter_mut() {
            queue.value().clear();
        }
        self.priority_heap.clear();
        self.total_size.store(0, std::sync::atomic::Ordering::SeqCst);
    }
    
    pub fn get_queue_stats(&self) -> HashMap<TaskPriority, usize> {
        let mut stats = HashMap::new();
        for priority in [
            TaskPriority::Critical,
            TaskPriority::High,
            TaskPriority::Normal,
            TaskPriority::Low,
            TaskPriority::Background,
        ] {
            if let Some(queue) = self.queues.get(&priority) {
                stats.insert(priority, queue.len());
            }
        }
        stats
    }
}

pub struct Scheduler {
    config: SchedulerConfig,
    queue: Arc<Mutex<TaskQueue>>,
    workers: Arc<DashMap<String, WorkerInfo>>,
    task_store: Arc<DashMap<String, Arc<Task>>>,
    completed_tasks: Arc<DashMap<String, Arc<TaskResult>>>,
    pending_results: DashMap<String, oneshot::Sender<TaskResult>>,
    metrics: Arc<SchedulerMetrics>,
    event_sender: Arc<Mutex<Option<mpsc::Sender<SchedulerEvent>>>>,
    shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
    scheduler_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

struct SchedulerMetrics {
    total_submitted: std::sync::atomic::AtomicU64,
    total_completed: std::sync::atomic::AtomicU64,
    total_failed: std::sync::atomic::AtomicU64,
    total_retries: std::sync::atomic::AtomicU64,
    queue_times: Mutex<VecDeque<u64>>,
    execution_times: Mutex<VecDeque<u64>>,
    last_throughput_calc: Mutex<Instant>,
    throughput: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    TaskSubmitted(Arc<Task>),
    TaskQueued(Arc<Task>),
    TaskStarted(Arc<Task>, String),
    TaskCompleted(Arc<Task>, TaskResult),
    TaskFailed(Arc<Task>, String),
    TaskRetried(Arc<Task>, u32),
    TaskCancelled(Arc<Task>),
    WorkerJoined(WorkerInfo),
    WorkerLeft(String),
    QueueFull,
    Error(String),
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 100,
            default_timeout: Duration::from_secs(300),
            retry_count: 3,
            retry_delay: Duration::from_secs(5),
            task_ttl: Duration::from_secs(3600),
            cleanup_interval: Duration::from_secs(300),
            enable_prioritization: true,
            enable_affinity: false,
            max_queue_size: 10000,
            fair_scheduling: false,
            task_weights: HashMap::new(),
        }
    }
}

impl Default for TaskResources {
    fn default() -> Self {
        Self {
            cpu_cores: 1.0,
            memory_bytes: 1024 * 1024 * 100,
            gpu_required: false,
            gpu_memory_bytes: 0,
            storage_bytes: 1024 * 1024 * 100,
            network_bandwidth: 1024 * 1024,
            custom_resources: HashMap::new(),
        }
    }
}

impl Default for TaskAffinity {
    fn default() -> Self {
        Self {
            node_affinity: Vec::new(),
            node_anti_affinity: Vec::new(),
            pod_affinity: None,
            preferred_nodes: Vec::new(),
            required_labels: HashMap::new(),
        }
    }
}

impl Default for TaskMetadata {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            tags: Vec::new(),
            correlation_id: None,
            parent_task_id: None,
            source: String::new(),
            user_id: None,
            tenant_id: None,
            idempotency_key: None,
        }
    }
}

impl Default for TaskSchedule {
    fn default() -> Self {
        Self {
            trigger_type: TriggerType::Immediate,
            cron_expression: None,
            interval: None,
            delay: None,
            at: None,
            max_executions: 1,
            execution_count: 0,
        }
    }
}

impl Default for LatencyPercentiles {
    fn default() -> Self {
        Self {
            p50_ms: 0.0,
            p75_ms: 0.0,
            p90_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            p999_ms: 0.0,
        }
    }
}

impl Default for WorkerCapacity {
    fn default() -> Self {
        Self {
            total_cpu_cores: 4,
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
            total_gpu_count: 0,
            total_gpu_memory_bytes: 0,
            max_concurrent_tasks: 10,
        }
    }
}

impl Default for WorkerLoad {
    fn default() -> Self {
        Self {
            current_cpu_percent: 0.0,
            current_memory_bytes: 0,
            current_tasks: 0,
            queued_tasks: 0,
        }
    }
}

impl Task {
    pub fn new(task_type: &str, payload: Vec<u8>) -> Self {
        Self {
            task_id: Uuid::new_v4().to_string(),
            task_type: task_type.to_string(),
            payload,
            metadata: TaskMetadata::default(),
            schedule: TaskSchedule::default(),
            priority: TaskPriority::Normal,
            state: TaskState::Pending,
            dependencies: Vec::new(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            scheduled_at: None,
            started_at: None,
            completed_at: None,
            retry_count: 0,
            max_retries: 3,
            resources: TaskResources::default(),
            affinity: TaskAffinity::default(),
        }
    }
    
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }
    
    pub fn with_metadata(mut self, metadata: TaskMetadata) -> Self {
        self.metadata = metadata;
        self
    }
    
    pub fn with_schedule(mut self, schedule: TaskSchedule) -> Self {
        self.schedule = schedule;
        self
    }
    
    pub fn with_resources(mut self, resources: TaskResources) -> Self {
        self.resources = resources;
        self
    }
    
    pub fn with_affinity(mut self, affinity: TaskAffinity) -> Self {
        self.affinity = affinity;
        self
    }
    
    pub fn with_dependencies(mut self, deps: Vec<String>) -> Self {
        self.dependencies = deps;
        self
    }
    
    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }
}

impl Scheduler {
    pub async fn new(config: Option<SchedulerConfig>) -> Result<Self, SchedulerError> {
        let config = config.unwrap_or_default();
        
        let queue = Arc::new(Mutex::new(TaskQueue::new(config.clone())));
        let workers = Arc::new(DashMap::new());
        let task_store = Arc::new(DashMap::new());
        let completed_tasks = Arc::new(DashMap::new());
        let pending_results = DashMap::new();
        
        let metrics = Arc::new(SchedulerMetrics {
            total_submitted: std::sync::atomic::AtomicU64::new(0),
            total_completed: std::sync::atomic::AtomicU64::new(0),
            total_failed: std::sync::atomic::AtomicU64::new(0),
            total_retries: std::sync::atomic::AtomicU64::new(0),
            queue_times: Mutex::new(VecDeque::new()),
            execution_times: Mutex::new(VecDeque::new()),
            last_throughput_calc: Mutex::new(Instant::now()),
            throughput: std::sync::atomic::AtomicU64::new(0),
        });
        
        let (event_sender, _) = mpsc::channel(1000);
        
        Ok(Self {
            config,
            queue,
            workers,
            task_store,
            completed_tasks,
            pending_results,
            metrics,
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            shutdown_signal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            scheduler_task: Arc::new(Mutex::new(None)),
        })
    }
    
    pub async fn start(&mut self) -> Result<(), SchedulerError> {
        let shutdown = Arc::clone(&self.shutdown_signal);
        
        let task = tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });
        
        *self.scheduler_task.lock().unwrap() = Some(task);
        
        info!("Scheduler started with max_concurrent_tasks={}", self.config.max_concurrent_tasks);
        
        Ok(())
    }
    
    pub async fn stop(&mut self) {
        self.shutdown_signal.store(true, std::sync::atomic::Ordering::SeqCst);
        
        if let Some(task) = self.scheduler_task.lock().unwrap().take() {
            task.abort();
        }
        
        self.queue.lock().unwrap().clear();
        
        info!("Scheduler stopped");
    }
    
    pub async fn submit(&mut self, task: Task) -> Result<String, SchedulerError> {
        let task_id = task.task_id.clone();
        let task = Arc::new(task);
        
        self.task_store.insert(task_id.clone(), task.clone());
        
        self.metrics.total_submitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        self.send_event(SchedulerEvent::TaskSubmitted(task.clone()));
        
        let mut queue = self.queue.lock().unwrap();
        queue.push(task.clone())?;
        
        self.send_event(SchedulerEvent::TaskQueued(task.clone()));
        
        Ok(task_id)
    }
    
    pub async fn submit_batch(&mut self, tasks: Vec<Task>) -> Result<Vec<String>, SchedulerError> {
        let mut ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            ids.push(self.submit(task).await?);
        }
        Ok(ids)
    }
    
    pub async fn schedule(&mut self, task: Task, delay: Duration) -> Result<String, SchedulerError> {
        let task_id = task.task_id.clone();
        let task = Arc::new(task);
        
        self.task_store.insert(task_id.clone(), task.clone());
        
        let scheduled_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() + delay.as_secs() as u64;
        
        self.send_event(SchedulerEvent::TaskQueued(task.clone()));
        
        Ok(task_id)
    }
    
    pub async fn cancel(&mut self, task_id: &str) -> Result<bool, SchedulerError> {
        let mut queue = self.queue.lock().unwrap();
        
        if let Some(task) = queue.remove(task_id) {
            let mut task = (*task).clone();
            task.state = TaskState::Cancelled;
            self.task_store.insert(task_id.to_string(), Arc::new(task));
            
            self.send_event(SchedulerEvent::TaskCancelled(
                self.task_store.get(task_id).unwrap().clone()
            ));
            
            return Ok(true);
        }
        
        Ok(false)
    }
    
    pub async fn get_task(&self, task_id: &str) -> Option<Arc<Task>> {
        self.task_store.get(task_id).map(|t| t.clone())
    }
    
    pub async fn get_task_result(&self, task_id: &str) -> Option<Arc<TaskResult>> {
        self.completed_tasks.get(task_id).map(|r| r.clone())
    }
    
    pub async fn register_worker(&self, worker: WorkerInfo) {
        self.workers.insert(worker.worker_id.clone(), worker);
        self.send_event(SchedulerEvent::WorkerJoined(
            self.workers.get(&worker.worker_id).unwrap().clone()
        ));
    }
    
    pub async fn unregister_worker(&self, worker_id: &str) {
        self.workers.remove(worker_id);
        self.send_event(SchedulerEvent::WorkerLeft(worker_id.to_string()));
    }
    
    pub async fn get_workers(&self) -> Vec<WorkerInfo> {
        self.workers.iter().map(|w| w.clone()).collect()
    }
    
    pub async fn get_queue_size(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
    
    pub async fn get_stats(&self) -> SchedulerStats {
        let queue = self.queue.lock().unwrap();
        let queue_stats = queue.get_queue_stats();
        
        let total_submitted = self.metrics.total_submitted.load(std::sync::atomic::Ordering::SeqCst);
        let total_completed = self.metrics.total_completed.load(std::sync::atomic::Ordering::SeqCst);
        let total_failed = self.metrics.total_failed.load(std::sync::atomic::Ordering::SeqCst);
        
        let queue_times = self.metrics.queue_times.lock().unwrap();
        let execution_times = self.metrics.execution_times.lock().unwrap();
        
        let avg_queue_time = if !queue_times.is_empty() {
            queue_times.iter().sum::<u64>() as f64 / queue_times.len() as f64 / 1_000_000.0
        } else {
            0.0
        };
        
        let avg_execution_time = if !execution_times.is_empty() {
            execution_times.iter().sum::<u64>() as f64 / execution_times.len() as f64 / 1_000_000.0
        } else {
            0.0
        };
        
        let running = self.workers.iter()
            .map(|w| w.current_load.current_tasks)
            .sum();
        
        let utilization = if self.config.max_concurrent_tasks > 0 {
            (running as f64 / self.config.max_concurrent_tasks as f64) * 100.0
        } else {
            0.0
        };
        
        SchedulerStats {
            total_tasks_submitted: total_submitted,
            total_tasks_completed: total_completed,
            total_tasks_failed: total_failed,
            total_tasks_retried: self.metrics.total_retries.load(std::sync::atomic::Ordering::SeqCst),
            current_queue_size: queue.len(),
            current_running: running,
            avg_queue_time_ms: avg_queue_time,
            avg_execution_time_ms: avg_execution_time,
            throughput_tasks_per_sec: self.metrics.throughput.load(std::sync::atomic::Ordering::SeqCst) as f64,
            utilization_percent: utilization,
            queue_latency_percentiles: LatencyPercentiles::default(),
            state_distribution: HashMap::new(),
            priority_distribution: queue_stats,
        }
    }
    
    fn send_event(&self, event: SchedulerEvent) {
        if let Some(ref sender) = *self.event_sender.lock().unwrap() {
            let _ = sender.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = Scheduler::new(None).await;
        assert!(scheduler.is_ok());
    }

    #[tokio::test]
    async fn test_task_creation() {
        let task = Task::new("test_task", vec![1, 2, 3]);
        assert_eq!(task.task_type, "test_task");
        assert_eq!(task.payload, vec![1, 2, 3]);
        assert_eq!(task.priority, TaskPriority::Normal);
        assert_eq!(task.state, TaskState::Pending);
    }

    #[tokio::test]
    async fn test_task_with_priority() {
        let task = Task::new("test", vec![])
            .with_priority(TaskPriority::High);
        assert_eq!(task.priority, TaskPriority::High);
    }

    #[tokio::test]
    async fn test_task_queue_push_pop() {
        let config = SchedulerConfig::default();
        let mut queue = TaskQueue::new(config);
        
        let task1 = Arc::new(Task::new("task1", vec![1]));
        let task2 = Arc::new(Task::new("task2", vec![2]).with_priority(TaskPriority::Critical));
        let task3 = Arc::new(Task::new("task3", vec![3]).with_priority(TaskPriority::Low));
        
        queue.push(task1).unwrap();
        queue.push(task2).unwrap();
        queue.push(task3).unwrap();
        
        assert_eq!(queue.len(), 3);
        
        let popped = queue.pop();
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().priority, TaskPriority::Critical);
    }

    #[tokio::test]
    async fn test_scheduler_submit() {
        let mut scheduler = Scheduler::new(None).await.unwrap();
        let _ = scheduler.start().await;
        
        let task = Task::new("test_task", vec![1, 2, 3]);
        let id = scheduler.submit(task).await;
        assert!(id.is_ok());
        assert!(!id.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_scheduler_cancel() {
        let mut scheduler = Scheduler::new(None).await.unwrap();
        let _ = scheduler.start().await;
        
        let task = Task::new("test_task", vec![1, 2, 3]);
        let id = scheduler.submit(task).await.unwrap();
        
        let cancelled = scheduler.cancel(&id).await;
        assert!(cancelled.is_ok());
        assert!(cancelled.unwrap());
    }

    #[tokio::test]
    async fn test_worker_registration() {
        let scheduler = Scheduler::new(None).await.unwrap();
        
        let worker = WorkerInfo {
            worker_id: "worker1".to_string(),
            hostname: "localhost".to_string(),
            capacity: WorkerCapacity::default(),
            load: WorkerLoad::default(),
            status: WorkerStatus::Online,
            last_heartbeat: 0,
            labels: HashMap::new(),
            capabilities: vec!["cpu".to_string()],
        };
        
        scheduler.register_worker(worker).await;
        
        let workers = scheduler.get_workers().await;
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].worker_id, "worker1");
    }

    #[tokio::test]
    async fn test_scheduler_stats() {
        let scheduler = Scheduler::new(None).await.unwrap();
        
        let stats = scheduler.get_stats().await;
        assert_eq!(stats.total_tasks_submitted, 0);
        assert_eq!(stats.current_queue_size, 0);
    }

    #[tokio::test]
    async fn test_task_resources() {
        let resources = TaskResources {
            cpu_cores: 2.0,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            gpu_required: true,
            gpu_memory_bytes: 4 * 1024 * 1024 * 1024,
            storage_bytes: 10 * 1024 * 1024 * 1024,
            network_bandwidth: 100 * 1024 * 1024,
            custom_resources: HashMap::new(),
        };
        
        assert!(resources.gpu_required);
        assert_eq!(resources.cpu_cores, 2.0);
    }

    #[tokio::test]
    async fn test_task_affinity() {
        let affinity = TaskAffinity {
            node_affinity: vec!["gpu-node-1".to_string()],
            node_anti_affinity: vec!["old-node".to_string()],
            pod_affinity: None,
            preferred_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            required_labels: HashMap::from([
                ("zone".to_string(), "us-west".to_string()),
            ]),
        };
        
        assert!(affinity.node_affinity.contains(&"gpu-node-1".to_string()));
        assert_eq!(affinity.required_labels.get("zone"), Some(&"us-west".to_string()));
    }

    #[tokio::test]
    async fn test_task_schedule() {
        let schedule = TaskSchedule {
            trigger_type: TriggerType::Cron,
            cron_expression: Some("0 0 * * *".to_string()),
            interval: None,
            delay: None,
            at: None,
            max_executions: 24,
            execution_count: 0,
        };
        
        assert_eq!(schedule.trigger_type, TriggerType::Cron);
        assert!(schedule.cron_expression.is_some());
    }
}
