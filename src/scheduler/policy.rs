use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{debug, info, warn};
use dashmap::DashMap;

#[derive(Error, Debug)]
pub enum PolicyError {
    #[error("Policy error: {0}")]
    PolicyError(String),
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    
    #[error("Evaluation error: {0}")]
    EvaluationError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingPolicy {
    pub policy_type: PolicyType,
    pub config: PolicyConfig,
    pub weights: PolicyWeights,
    pub constraints: Vec<SchedulingConstraint>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyType {
    FIFO,
    LIFO,
    Priority,
    FairShare,
    ShortestJobFirst,
    LongestJobFirst,
    EarliestDeadlineFirst,
    LeastLoadedFirst,
    RoundRobin,
    WeightedRoundRobin,
    TokenBucket,
    Hierarchical,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub quantum: Duration,
    pub time_slice: Duration,
    pub max_age: Duration,
    pub fair_share_weight: HashMap<String, f64>,
    pub priority_boost: HashMap<String, f32>,
    pub aging_rate: f32,
    pub starvation_prevention: bool,
    pub max_starvation_time: Duration,
    pub adaptive_weighting: bool,
    pub weight_update_interval: Duration,
    pub history_window: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyWeights {
    pub fairness_weight: f64,
    pub latency_weight: f64,
    pub throughput_weight: f64,
    pub priority_weight: f64,
    pub resource_weight: f64,
    pub custom_weights: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingConstraint {
    pub constraint_type: ConstraintType,
    pub target: String,
    pub operator: ConstraintOperator,
    pub value: f64,
    pub action: ConstraintAction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConstraintType {
    CPUUsage,
    MemoryUsage,
    QueueLength,
    Latency,
    Throughput,
    ErrorRate,
    Custom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConstraintOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConstraintAction {
    Throttle,
    Pause,
    Redirect,
    Reject,
    Notify,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairShareConfig {
    pub shares: HashMap<String, u64>,
    pub total_shares: u64,
    pub burst_allowance: f64,
    pub weight_update_interval: Duration,
    pub history_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceWeight {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub gpu_weight: f64,
    pub storage_weight: f64,
    pub network_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityConfig {
    pub base_priority: u32,
    pub priority_range: u32,
    pub dynamic_boost: bool,
    pub boost_interval: Duration,
    pub max_boost: u32,
    pub decay_rate: f32,
    pub history_based: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBucketConfig {
    pub capacity: u64,
    pub refill_rate: u64,
    pub refill_interval: Duration,
    pub burst_capacity: u64,
    pub bucket_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchicalPolicyConfig {
    pub levels: Vec<PolicyLevel>,
    pub level_weights: Vec<f64>,
    pub level_transition: TransitionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyLevel {
    pub level: u32,
    pub policy: PolicyType,
    pub weight: f64,
    pub max_tasks: u32,
    pub min_quota: u64,
    pub burst_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionConfig {
    pub enabled: bool,
    pub threshold_up: f64,
    threshold_down: f64,
    pub hysteresis: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptivePolicyState {
    pub current_weights: PolicyWeights,
    pub performance_history: Vec<PerformanceSnapshot>,
    pub last_adjustment: Instant,
    pub adjustment_count: u32,
    pub learning_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub timestamp: u64,
    pub throughput: f64,
    pub latency_p50: f64,
    pub latency_p99: f64,
    pub fairness_index: f64,
    pub utilization: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub task_id: String,
    pub worker_id: Option<String>,
    pub priority: u32,
    pub scheduling_delay: Duration,
    pub decision_time: u64,
    pub reason: String,
    pub applied_policy: PolicyType,
    pub metadata: HashMap<String, String>,
}

pub trait SchedulingPolicyTrait: Send + Sync {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>>;
    fn calculate_priority(&self, task: &TaskInfo) -> u32;
    fn update_weights(&mut self, feedback: &SchedulingFeedback);
    fn get_policy_type(&self) -> PolicyType;
}

struct TaskInfo {
    task_id: String,
    priority: u32,
    created_at: u64,
    estimated_duration: Duration,
    resource_requirements: ResourceRequirements,
    tenant_id: String,
    past_executions: Vec<Duration>,
}

struct ResourceRequirements {
    cpu_cores: f32,
    memory_bytes: u64,
    gpu_required: bool,
}

struct SchedulingFeedback {
    task_id: String,
    actual_duration: Duration,
    success: bool,
    resource_usage: ResourceUsage,
}

struct ResourceUsage {
    cpu_used: f32,
    memory_used: u64,
}

pub struct FIFOPolicy {
    config: PolicyConfig,
}

pub struct PriorityPolicy {
    config: PriorityConfig,
    current_priorities: DashMap<String, u32>,
    last_boost: DashMap<String, Instant>,
}

pub struct FairSharePolicy {
    config: FairShareConfig,
    usage_history: DashMap<String, Vec<ResourceUsageSnapshot>>,
    current_shares: DashMap<String, u64>,
}

struct ResourceUsageSnapshot {
    timestamp: u64,
    cpu_used: f64,
    memory_used: u64,
    tasks_completed: u64,
}

pub struct ShortestJobFirstPolicy {
    avg_duration_estimator: DashMap<String, ExponentialMovingAverage>,
    history_window: usize,
}

struct ExponentialMovingAverage {
    alpha: f64,
    current: f64,
    count: u64,
}

pub struct TokenBucketPolicy {
    buckets: DashMap<String, TokenBucket>,
    config: TokenBucketConfig,
}

struct TokenBucket {
    tokens: u64,
    last_refill: Instant,
    capacity: u64,
    refill_rate: u64,
}

pub struct HierarchicalPolicy {
    levels: Vec<PolicyLevel>,
    level_buckets: Vec<DashMap<String, Vec<Arc<TaskInfo>>>>,
    current_level: usize,
}

pub struct AdaptivePolicy {
    base_policy: Box<dyn SchedulingPolicyTrait>,
    state: AdaptivePolicyState,
    config: PolicyConfig,
}

impl FIFOPolicy {
    pub fn new(config: PolicyConfig) -> Self {
        Self { config }
    }
}

impl SchedulingPolicyTrait for FIFOPolicy {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>> {
        available_tasks
            .iter()
            .min_by(|a, b| a.created_at.cmp(&b.created_at))
            .cloned()
    }
    
    fn calculate_priority(&self, _task: &TaskInfo) -> u32 {
        0
    }
    
    fn update_weights(&mut self, _feedback: &SchedulingFeedback) {}
    
    fn get_policy_type(&self) -> PolicyType {
        PolicyType::FIFO
    }
}

impl PriorityPolicy {
    pub fn new(config: PriorityConfig) -> Self {
        Self {
            config,
            current_priorities: DashMap::new(),
            last_boost: DashMap::new(),
        }
    }
}

impl SchedulingPolicyTrait for PriorityPolicy {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>> {
        available_tasks
            .iter()
            .max_by(|a, b| {
                let a_prio = self.calculate_priority(&a);
                let b_prio = self.calculate_priority(&b);
                a_prio.cmp(&b_prio)
            })
            .cloned()
    }
    
    fn calculate_priority(&self, task: &TaskInfo) -> u32 {
        let base = self.config.base_priority;
        let range = self.config.priority_range;
        
        let mut priority = base + (task.priority as u32 % range);
        
        if self.config.dynamic_boost {
            let last = self.last_boost.get(&task.task_id);
            if let Some(last_time) = last {
                let elapsed = last.elapsed().as_secs();
                if elapsed > self.config.boost_interval.as_secs() as u64 {
                    priority = (priority + self.config.max_boost).min(base + range);
                    self.last_boost.insert(task.task_id.clone(), Instant::now());
                }
            }
        }
        
        priority
    }
    
    fn update_weights(&mut self, _feedback: &SchedulingFeedback) {}
    
    fn get_policy_type(&self) -> PolicyType {
        PolicyType::Priority
    }
}

impl FairSharePolicy {
    pub fn new(config: FairShareConfig) -> Self {
        let mut shares = HashMap::new();
        for (tenant, share) in &config.shares {
            shares.insert(tenant.clone(), *share);
        }
        
        Self {
            config,
            usage_history: DashMap::new(),
            current_shares: DashMap::new(shares),
        }
    }
}

impl SchedulingPolicyTrait for FairSharePolicy {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>> {
        let tasks_by_tenant: HashMap<String, Vec<Arc<TaskInfo>>> = available_tasks
            .iter()
            .fold(HashMap::new(), |mut acc, task| {
                acc.entry(task.tenant_id.clone())
                    .or_insert_with(Vec::new)
                    .push(task.clone());
                acc
            });
        
        let effective_shares: HashMap<String, f64> = self.current_shares
            .iter()
            .map(|(tenant, share)| {
                let history = self.usage_history.get(tenant).map(|h| h.clone()).unwrap_or_default();
                let consumed = Self::calculate_consumed(history);
                let effective = Self::calculate_effective_share(*share, consumed, self.config.total_shares);
                (tenant.clone(), effective)
            })
            .collect();
        
        tasks_by_tenant
            .into_iter()
            .min_by(|(a_tenant, a_tasks), (b_tenant, b_tasks)| {
                let a_share = effective_shares.get(a_tenant).cloned().unwrap_or(0.0);
                let b_share = effective_shares.get(b_tenant).cloned().unwrap_or(0.0);
                let a_score = a_share / a_tasks.len() as f64;
                let b_score = b_share / b_tasks.len() as f64;
                a_score.partial_cmp(&b_score).unwrap_or(std::cmp::Ordering::Equal)
            })
            .and_then(|(_, tasks)| tasks.first().cloned())
    }
    
    fn calculate_priority(&self, task: &TaskInfo) -> u32 {
        let shares = self.current_shares.get(&task.tenant_id);
        shares.map(|s| *s as u32).unwrap_or(0)
    }
    
    fn update_weights(&mut self, feedback: &SchedulingFeedback) {
        let history = self.usage_history
            .entry(feedback.task_id.clone())
            .or_insert_with(Vec::new);
        
        history.push(ResourceUsageSnapshot {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            cpu_used: feedback.resource_usage.cpu_used as f64,
            memory_used: feedback.resource_usage.memory_used,
            tasks_completed: if feedback.success { 1 } else { 0 },
        });
        
        if history.len() > self.config.history_length {
            history.remove(0);
        }
        
        self.update_effective_shares();
    }
    
    fn get_policy_type(&self) -> PolicyType {
        PolicyType::FairShare
    }
}

impl FairSharePolicy {
    fn calculate_consumed(history: Vec<ResourceUsageSnapshot>) -> f64 {
        if history.is_empty() {
            return 0.0;
        }
        let avg_cpu = history.iter().map(|s| s.cpu_used).sum::<f64>() / history.len() as f64;
        avg_cpu
    }
    
    fn calculate_effective_share(allocated: u64, consumed: f64, total: u64) -> f64 {
        if total == 0 {
            return 0.0;
        }
        let allocated_share = allocated as f64 / total as f64;
        (allocated_share - consumed).max(0.0)
    }
    
    fn update_effective_shares(&mut self) {
        for tenant in self.config.shares.keys() {
            let history = self.usage_history.get(tenant).map(|h| h.clone()).unwrap_or_default();
            let consumed = Self::calculate_consumed(history);
            
            let allocated = self.config.shares.get(tenant).cloned().unwrap_or(0);
            let effective = Self::calculate_effective_share(allocated, consumed, self.config.total_shares);
            
            self.current_shares.insert(tenant.clone(), (effective * 100.0) as u64);
        }
    }
}

impl ShortestJobFirstPolicy {
    pub fn new(history_window: usize) -> Self {
        Self {
            avg_duration_estimator: DashMap::new(),
            history_window,
        }
    }
}

impl SchedulingPolicyTrait for ShortestJobFirstPolicy {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>> {
        available_tasks
            .iter()
            .min_by(|a, b| {
                let a_est = self.estimate_duration(&a);
                let b_est = self.estimate_duration(&b);
                a_est.partial_cmp(&b_est).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }
    
    fn calculate_priority(&self, task: &TaskInfo) -> u32 {
        let est = self.estimate_duration(task);
        (10000.0 / est.as_secs_f64().max(0.001)) as u32
    }
    
    fn update_weights(&mut self, feedback: &SchedulingFeedback) {
        let estimator = self.avg_duration_estimator
            .entry(feedback.task_id.clone())
            .or_insert_with(|| ExponentialMovingAverage {
                alpha: 0.3,
                current: feedback.actual_duration.as_secs_f64(),
                count: 1,
            });
        
        estimator.current = estimator.alpha * feedback.actual_duration.as_secs_f64() 
            + (1.0 - estimator.alpha) * estimator.current;
        estimator.count += 1;
    }
    
    fn get_policy_type(&self) -> PolicyType {
        PolicyType::ShortestJobFirst
    }
}

impl ShortestJobFirstPolicy {
    fn estimate_duration(&self, task: &TaskInfo) -> Duration {
        if let Some(estimator) = self.avg_duration_estimator.get(&task.task_id) {
            if estimator.count >= 3 {
                return Duration::from_secs_f64(estimator.current);
            }
        }
        
        let avg_past = if !task.past_executions.is_empty() {
            task.past_executions.iter().sum::<Duration>() / task.past_executions.len() as u32
        } else {
            Duration::from_secs(30)
        };
        
        avg_past
    }
}

impl TokenBucketPolicy {
    pub fn new(config: TokenBucketConfig) -> Self {
        Self {
            buckets: DashMap::new(),
            config,
        }
    }
    
    fn get_bucket(&self, tenant_id: &str) -> TokenBucket {
        self.buckets
            .entry(tenant_id.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: self.config.capacity,
                last_refill: Instant::now(),
                capacity: self.config.capacity,
                refill_rate: self.config.refill_rate,
            })
            .clone()
    }
}

impl SchedulingPolicyTrait for TokenBucketPolicy {
    fn select_task(&self, available_tasks: &[Arc<TaskInfo>]) -> Option<Arc<TaskInfo>> {
        available_tasks
            .iter()
            .filter(|task| {
                let bucket = self.get_bucket(&task.tenant_id);
                bucket.tokens > 0
            })
            .min_by(|a, b| {
                let a_bucket = self.get_bucket(&a.tenant_id);
                let b_bucket = self.get_bucket(&b.tenant_id);
                a_bucket.tokens.cmp(&b_bucket.tokens)
            })
            .map(|t| {
                let mut bucket = self.get_bucket(&t.tenant_id);
                bucket.tokens = bucket.tokens.saturating_sub(1);
                self.buckets.insert(t.tenant_id.clone(), bucket);
                t.clone()
            })
    }
    
    fn calculate_priority(&self, task: &TaskInfo) -> u32 {
        let bucket = self.get_bucket(&task.tenant_id);
        (bucket.tokens as f64 / bucket.capacity as f64 * 100.0) as u32
    }
    
    fn update_weights(&mut self, feedback: &SchedulingFeedback) {}
    
    fn get_policy_type(&self) -> PolicyType {
        PolicyType::TokenBucket
    }
}

impl SchedulingPolicy {
    pub fn new(policy_type: PolicyType) -> Self {
        Self {
            policy_type,
            config: PolicyConfig::default(),
            weights: PolicyWeights::default(),
            constraints: Vec::new(),
            enabled: true,
        }
    }
    
    pub fn with_config(mut self, config: PolicyConfig) -> Self {
        self.config = config;
        self
    }
    
    pub fn with_weights(mut self, weights: PolicyWeights) -> Self {
        self.weights = weights;
        self
    }
    
    pub fn with_constraints(mut self, constraints: Vec<SchedulingConstraint>) -> Self {
        self.constraints = constraints;
        self
    }
    
    pub fn create_policy(&self) -> Box<dyn SchedulingPolicyTrait> {
        match self.policy_type {
            PolicyType::FIFO => Box::new(FIFOPolicy::new(self.config.clone())),
            PolicyType::Priority => Box::new(PriorityPolicy::new(PriorityConfig::default())),
            PolicyType::FairShare => {
                let config = FairShareConfig {
                    shares: HashMap::new(),
                    total_shares: 100,
                    burst_allowance: 1.5,
                    weight_update_interval: Duration::from_secs(60),
                    history_length: 100,
                };
                Box::new(FairSharePolicy::new(config))
            }
            PolicyType::ShortestJobFirst => Box::new(ShortestJobFirstPolicy::new(self.config.history_window)),
            PolicyType::TokenBucket => Box::new(TokenBucketPolicy::new(TokenBucketConfig {
                capacity: 1000,
                refill_rate: 100,
                refill_interval: Duration::from_secs(1),
                burst_capacity: 2000,
                bucket_count: 10,
            })),
            _ => Box::new(FIFOPolicy::new(self.config.clone())),
        }
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            quantum: Duration::from_millis(100),
            time_slice: Duration::from_millis(100),
            max_age: Duration::from_secs(3600),
            fair_share_weight: HashMap::new(),
            priority_boost: HashMap::new(),
            aging_rate: 0.1,
            starvation_prevention: false,
            max_starvation_time: Duration::from_secs(300),
            adaptive_weighting: false,
            weight_update_interval: Duration::from_secs(60),
            history_window: 100,
        }
    }
}

impl Default for PolicyWeights {
    fn default() -> Self {
        Self {
            fairness_weight: 0.2,
            latency_weight: 0.3,
            throughput_weight: 0.2,
            priority_weight: 0.2,
            resource_weight: 0.1,
            custom_weights: HashMap::new(),
        }
    }
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            base_priority: 100,
            priority_range: 100,
            dynamic_boost: true,
            boost_interval: Duration::from_secs(60),
            max_boost: 20,
            decay_rate: 0.95,
            history_based: true,
        }
    }
}

impl Default for FairShareConfig {
    fn default() -> Self {
        Self {
            shares: HashMap::new(),
            total_shares: 100,
            burst_allowance: 1.5,
            weight_update_interval: Duration::from_secs(60),
            history_length: 100,
        }
    }
}

impl Default for TokenBucketConfig {
    fn default() -> Self {
        Self {
            capacity: 1000,
            refill_rate: 100,
            refill_interval: Duration::from_secs(1),
            burst_capacity: 2000,
            bucket_count: 10,
        }
    }
}

impl Default for HierarchicalPolicyConfig {
    fn default() -> Self {
        Self {
            levels: vec![
                PolicyLevel {
                    level: 0,
                    policy: PolicyType::Priority,
                    weight: 0.5,
                    max_tasks: 100,
                    min_quota: 50,
                    burst_multiplier: 2.0,
                },
                PolicyLevel {
                    level: 1,
                    policy: PolicyType::FairShare,
                    weight: 0.3,
                    max_tasks: 500,
                    min_quota: 30,
                    burst_multiplier: 1.5,
                },
                PolicyLevel {
                    level: 2,
                    policy: PolicyType::FIFO,
                    weight: 0.2,
                    max_tasks: 1000,
                    min_quota: 20,
                    burst_multiplier: 1.0,
                },
            ],
            level_weights: vec![0.5, 0.3, 0.2],
            level_transition: TransitionConfig {
                enabled: false,
                threshold_up: 0.8,
                threshold_down: 0.3,
                hysteresis: 0.1,
            },
        }
    }
}

impl Default for ResourceWeight {
    fn default() -> Self {
        Self {
            cpu_weight: 1.0,
            memory_weight: 1.0,
            gpu_weight: 2.0,
            storage_weight: 0.5,
            network_weight: 0.5,
        }
    }
}

impl Default for AdaptivePolicyState {
    fn default() -> Self {
        Self {
            current_weights: PolicyWeights::default(),
            performance_history: Vec::new(),
            last_adjustment: Instant::now(),
            adjustment_count: 0,
            learning_rate: 0.01,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fifo_policy() {
        let policy = FIFOPolicy::new(PolicyConfig::default());
        
        let tasks: Vec<Arc<TaskInfo>> = vec![
            Arc::new(TaskInfo {
                task_id: "1".to_string(),
                priority: 0,
                created_at: 100,
                estimated_duration: Duration::from_secs(10),
                resource_requirements: ResourceRequirements {
                    cpu_cores: 1.0,
                    memory_bytes: 1024,
                    gpu_required: false,
                },
                tenant_id: "tenant1".to_string(),
                past_executions: Vec::new(),
            }),
            Arc::new(TaskInfo {
                task_id: "2".to_string(),
                priority: 0,
                created_at: 50,
                estimated_duration: Duration::from_secs(5),
                resource_requirements: ResourceRequirements {
                    cpu_cores: 1.0,
                    memory_bytes: 1024,
                    gpu_required: false,
                },
                tenant_id: "tenant1".to_string(),
                past_executions: Vec::new(),
            }),
        ];
        
        let selected = policy.select_task(&tasks);
        assert!(selected.is_some());
        assert_eq!(selected.unwrap().task_id, "2");
    }

    #[tokio::test]
    async fn test_priority_policy() {
        let config = PriorityConfig {
            base_priority: 100,
            priority_range: 100,
            dynamic_boost: true,
            boost_interval: Duration::from_secs(60),
            max_boost: 20,
            decay_rate: 0.95,
            history_based: true,
        };
        let policy = PriorityPolicy::new(config);
        
        let high_priority_task = Arc::new(TaskInfo {
            task_id: "high".to_string(),
            priority: 80,
            created_at: 100,
            estimated_duration: Duration::from_secs(10),
            resource_requirements: ResourceRequirements {
                cpu_cores: 1.0,
                memory_bytes: 1024,
                gpu_required: false,
            },
            tenant_id: "tenant1".to_string(),
            past_executions: Vec::new(),
        });
        
        let low_priority_task = Arc::new(TaskInfo {
            task_id: "low".to_string(),
            priority: 20,
            created_at: 50,
            estimated_duration: Duration::from_secs(5),
            resource_requirements: ResourceRequirements {
                cpu_cores: 1.0,
                memory_bytes: 1024,
                gpu_required: false,
            },
            tenant_id: "tenant1".to_string(),
            past_executions: Vec::new(),
        });
        
        let tasks = vec![high_priority_task.clone(), low_priority_task.clone()];
        let selected = policy.select_task(&tasks);
        
        assert!(selected.is_some());
        assert_eq!(selected.unwrap().task_id, "high");
    }

    #[tokio::test]
    async fn test_scheduling_policy_creation() {
        let policy = SchedulingPolicy::new(PolicyType::FIFO);
        assert_eq!(policy.policy_type, PolicyType::FIFO);
        assert!(policy.enabled);
    }

    #[tokio::test]
    async fn test_scheduling_policy_with_config() {
        let config = PolicyConfig {
            quantum: Duration::from_millis(200),
            time_slice: Duration::from_millis(200),
            ..Default::default()
        };
        
        let policy = SchedulingPolicy::new(PolicyType::Priority)
            .with_config(config);
        
        assert_eq!(policy.policy_type, PolicyType::Priority);
        assert_eq!(policy.config.quantum, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_scheduling_policy_create_policy() {
        let fifo = SchedulingPolicy::new(PolicyType::FIFO);
        let _ = fifo.create_policy();
        
        let priority = SchedulingPolicy::new(PolicyType::Priority);
        let _ = priority.create_policy();
    }

    #[tokio::test]
    async fn test_shortest_job_first() {
        let policy = ShortestJobFirstPolicy::new(10);
        
        let long_task = Arc::new(TaskInfo {
            task_id: "long".to_string(),
            priority: 50,
            created_at: 100,
            estimated_duration: Duration::from_secs(100),
            resource_requirements: ResourceRequirements {
                cpu_cores: 1.0,
                memory_bytes: 1024,
                gpu_required: false,
            },
            tenant_id: "tenant1".to_string(),
            past_executions: vec![Duration::from_secs(95), Duration::from_secs(105)],
        });
        
        let short_task = Arc::new(TaskInfo {
            task_id: "short".to_string(),
            priority: 50,
            created_at: 50,
            estimated_duration: Duration::from_secs(10),
            resource_requirements: ResourceRequirements {
                cpu_cores: 1.0,
                memory_bytes: 1024,
                gpu_required: false,
            },
            tenant_id: "tenant1".to_string(),
            past_executions: vec![Duration::from_secs(9), Duration::from_secs(11)],
        });
        
        let tasks = vec![long_task.clone(), short_task.clone()];
        let selected = policy.select_task(&tasks);
        
        assert!(selected.is_some());
        assert_eq!(selected.unwrap().task_id, "short");
    }

    #[tokio::test]
    async fn test_token_bucket_policy() {
        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 1,
            refill_interval: Duration::from_secs(1),
            burst_capacity: 20,
            bucket_count: 10,
        };
        
        let policy = TokenBucketPolicy::new(config);
        
        let tasks: Vec<Arc<TaskInfo>> = (0..5).map(|i| {
            Arc::new(TaskInfo {
                task_id: format!("task_{}", i),
                priority: 50,
                created_at: 100,
                estimated_duration: Duration::from_secs(10),
                resource_requirements: ResourceRequirements {
                    cpu_cores: 1.0,
                    memory_bytes: 1024,
                    gpu_required: false,
                },
                tenant_id: "tenant1".to_string(),
                past_executions: Vec::new(),
            })
        }).collect();
        
        let selected = policy.select_task(&tasks);
        assert!(selected.is_some());
    }

    #[tokio::test]
    async fn test_fair_share_policy() {
        let config = FairShareConfig {
            shares: HashMap::from([
                ("tenant_a".to_string(), 60),
                ("tenant_b".to_string(), 40),
            ]),
            total_shares: 100,
            burst_allowance: 1.5,
            weight_update_interval: Duration::from_secs(60),
            history_length: 100,
        };
        
        let policy = FairSharePolicy::new(config);
        
        let tenant_a_tasks: Vec<Arc<TaskInfo>> = (0..10).map(|i| {
            Arc::new(TaskInfo {
                task_id: format!("tenant_a_task_{}", i),
                priority: 50,
                created_at: 100,
                estimated_duration: Duration::from_secs(10),
                resource_requirements: ResourceRequirements {
                    cpu_cores: 1.0,
                    memory_bytes: 1024,
                    gpu_required: false,
                },
                tenant_id: "tenant_a".to_string(),
                past_executions: Vec::new(),
            })
        }).collect();
        
        let tenant_b_tasks: Vec<Arc<TaskInfo>> = (0..2).map(|i| {
            Arc::new(TaskInfo {
                task_id: format!("tenant_b_task_{}", i),
                priority: 50,
                created_at: 100,
                estimated_duration: Duration::from_secs(10),
                resource_requirements: ResourceRequirements {
                    cpu_cores: 1.0,
                    memory_bytes: 1024,
                    gpu_required: false,
                },
                tenant_id: "tenant_b".to_string(),
                past_executions: Vec::new(),
            })
        }).collect();
        
        let all_tasks: Vec<Arc<TaskInfo>> = tenant_a_tasks.iter()
            .chain(tenant_b_tasks.iter())
            .cloned()
            .collect();
        
        let selected = policy.select_task(&all_tasks);
        assert!(selected.is_some());
    }

    #[tokio::test]
    async fn test_constraint_evaluation() {
        let constraint = SchedulingConstraint {
            constraint_type: ConstraintType::CPUUsage,
            target: "worker1".to_string(),
            operator: ConstraintOperator::GreaterThan,
            value: 90.0,
            action: ConstraintAction::Throttle,
        };
        
        assert_eq!(constraint.constraint_type, ConstraintType::CPUUsage);
        assert_eq!(constraint.operator, ConstraintOperator::GreaterThan);
        assert_eq!(constraint.action, ConstraintAction::Throttle);
    }

    #[tokio::test]
    async fn test_policy_decision() {
        let decision = PolicyDecision {
            task_id: "task_123".to_string(),
            worker_id: Some("worker_1".to_string()),
            priority: 100,
            scheduling_delay: Duration::from_millis(50),
            decision_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            reason: "Highest priority".to_string(),
            applied_policy: PolicyType::Priority,
            metadata: HashMap::new(),
        };
        
        assert_eq!(decision.task_id, "task_123");
        assert!(decision.worker_id.is_some());
        assert_eq!(decision.applied_policy, PolicyType::Priority);
    }
}
