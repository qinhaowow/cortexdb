use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, mpsc};
use tokio::time::{timeout, Duration};
use serde::{Serialize, Deserialize};
use rand::Rng;
use ring::digest;
use thiserror::Error;
use log::{info, warn, error};
use dashmap::DashMap;

#[derive(Error, Debug)]
pub enum FederatedError {
    #[error("Client error: {0}")]
    ClientError(String),
    
    #[error("Aggregation error: {0}")]
    AggregationError(String),
    
    #[error("Round error: {0}")]
    RoundError(String),
    
    #[error("Model error: {0}")]
    ModelError(String),
    
    #[error("Security error: {0}")]
    SecurityError(String),
    
    #[error("Timeout error: {0}")]
    TimeoutError(String),
    
    #[error("Communication error: {0}")]
    CommunicationError(String),
    
    #[error("Privacy error: {0}")]
    PrivacyError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub client_id: String,
    pub endpoint: String,
    pub weight: f64,
    pub capabilities: Vec<String>,
    pub max_data_size: usize,
    pub trusted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelUpdate {
    pub client_id: String,
    pub round_id: u32,
    pub weights: Vec<f32>,
    pub num_samples: usize,
    pub gradient_norm: f32,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub min_clients: usize,
    pub max_clients: usize,
    pub timeout_seconds: u64,
    pub aggregation_strategy: AggregationStrategy,
    pub secure_aggregation: bool,
    pub differential_privacy: Option<DPConfig>,
    pub gradient_clipping: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationStrategy {
    FedAvg,
    FedMedian,
    FedTrimmedMean(f64),
    FedProximal(f64),
    FedAdam,
    FedAvgM,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DPConfig {
    pub epsilon: f64,
    pub delta: f64,
    pub clip_norm: f32,
    pub noise_scale: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedRound {
    pub round_id: u32,
    pub config: AggregationConfig,
    pub global_model_version: u64,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub selected_clients: Vec<String>,
    pub received_updates: Vec<ModelUpdate>,
    pub aggregated_model: Option<Vec<f32>>,
    pub status: RoundStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RoundStatus {
    Initializing,
    SelectingClients,
    WaitingForUpdates,
    Aggregating,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalModel {
    pub version: u64,
    pub weights: Vec<f32>,
    pub metrics: ModelMetrics,
    pub created_at: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetrics {
    pub accuracy: Option<f32>,
    pub loss: Option<f32>,
    pub f1_score: Option<f32>,
    pub precision: Option<f32>,
    pub recall: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientReport {
    pub client_id: String,
    pub round_id: u32,
    pub local_samples: usize,
    pub local_epochs: u32,
    pub training_time_ms: u64,
    pub loss: f32,
    pub accuracy: f32,
    pub gradient_norm: f32,
    pub data_distribution: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecureAggregationProof {
    pub client_id: String,
    pub masked_weights: Vec<f32>,
    pub proof: Vec<u8>,
    pub verification_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GradientCompression {
    pub method: CompressionMethod,
    pub sparsity: f64,
    pub quantization_bits: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionMethod {
    None,
    TopK(f64),
    RandomK(f64),
    Quantization(u8),
    Sparsification(f64),
    DeepGradientCompression,
}

struct PendingUpdate {
    update: ModelUpdate,
    received_at: u64,
    verified: bool,
}

pub struct FederatedClient {
    client_id: String,
    config: ClientConfig,
    last_heartbeat: u64,
    update_count: u32,
    success_count: u32,
    avg_gradient_norm: f32,
    reputation_score: f32,
    connection_status: ConnectionStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Timeout,
    Untrusted,
}

pub struct FederatedLearningManager {
    global_model: Arc<RwLock<GlobalModel>>,
    clients: DashMap<String, FederatedClient>,
    pending_updates: Arc<RwLock<HashMap<u32, HashMap<String, PendingUpdate>>>>,
    rounds: Arc<RwLock<HashMap<u32, FederatedRound>>>,
    config: FederatedConfig,
    event_sender: mpsc::Sender<FederatedEvent>,
    compression_config: Option<GradientCompression>,
    secure_aggregation_keys: DashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FederatedConfig {
    pub min_clients_for_update: usize,
    pub client_selection_strategy: ClientSelectionStrategy,
    pub round_timeout_seconds: u64,
    pub heartbeat_interval_seconds: u64,
    pub max_concurrent_rounds: u32,
    pub model_exchange_protocol: ExchangeProtocol,
    pub enable_monitoring: bool,
    pub reputation_weight: f32,
    pub data_heterogeneity_penalty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientSelectionStrategy {
    Random,
    ReputationBased,
    StratifieData,
    BanditBased,
    CostAware,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExchangeProtocol {
    GRPC,
    HTTP2,
    WebSocket,
    MQTT,
    Custom,
}

#[derive(Debug, Clone)]
pub enum FederatedEvent {
    RoundStarted(u32),
    ClientSelected(u32, String),
    UpdateReceived(u32, String),
    RoundCompleted(u32),
    RoundFailed(u32, String),
    ClientJoined(String),
    ClientLeft(String),
    ModelUpdated(u64),
    Alert(AlertLevel, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

impl Default for FederatedConfig {
    fn default() -> Self {
        Self {
            min_clients_for_update: 3,
            client_selection_strategy: ClientSelectionStrategy::Random,
            round_timeout_seconds: 300,
            heartbeat_interval_seconds: 30,
            max_concurrent_rounds: 5,
            model_exchange_protocol: ExchangeProtocol::GRPC,
            enable_monitoring: true,
            reputation_weight: 0.7,
            data_heterogeneity_penalty: true,
        }
    }
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            min_clients: 3,
            max_clients: 100,
            timeout_seconds: 300,
            aggregation_strategy: AggregationStrategy::FedAvg,
            secure_aggregation: false,
            differential_privacy: None,
            gradient_clipping: Some(5.0),
        }
    }
}

impl Default for GradientCompression {
    fn default() -> Self {
        Self {
            method: CompressionMethod::None,
            sparsity: 0.1,
            quantization_bits: 8,
        }
    }
}

impl FederatedClient {
    pub fn new(client_id: String, config: ClientConfig) -> Self {
        Self {
            client_id,
            config,
            last_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            update_count: 0,
            success_count: 0,
            avg_gradient_norm: 0.0,
            reputation_score: 0.5,
            connection_status: ConnectionStatus::Connected,
        }
    }

    pub fn update_reputation(&mut self, success: bool, gradient_norm: f32) {
        self.update_count += 1;
        if success {
            self.success_count += 1;
        }
        
        self.avg_gradient_norm = if self.update_count == 1 {
            gradient_norm
        } else {
            self.avg_gradient_norm * 0.7 + gradient_norm * 0.3
        };
        
        let success_rate = self.success_count as f32 / self.update_count as f32;
        self.reputation_score = success_rate * 0.6 + (1.0 - (self.avg_gradient_norm.min(10.0) / 10.0)) * 0.4;
        self.reputation_score = self.reputation_score.clamp(0.0, 1.0);
        
        if self.config.trusted {
            self.reputation_score = (self.reputation_score + 1.0) / 2.0;
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.connection_status = ConnectionStatus::Connected;
    }

    pub fn is_active(&self, timeout_seconds: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        match self.connection_status {
            ConnectionStatus::Connected => now - self.last_heartbeat < timeout_seconds,
            _ => false,
        }
    }
}

impl GlobalModel {
    pub fn new(weights: Vec<f32>, version: u64) -> Self {
        let checksum = Self::calculate_checksum(&weights);
        Self {
            version,
            weights,
            metrics: ModelMetrics::default(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checksum,
        }
    }

    fn calculate_checksum(weights: &[f32]) -> String {
        let bytes: Vec<u8> = weights.iter()
            .flat_map(|&f| f.to_le_bytes())
            .collect();
        let digest = digest::digest(&digest::SHA256, &bytes);
        hex_encode(digest.as_ref())
    }

    pub fn verify_integrity(&self) -> bool {
        Self::calculate_checksum(&self.weights) == self.checksum
    }

    pub fn update_weights(&mut self, new_weights: Vec<f32>) {
        self.weights = new_weights;
        self.version += 1;
        self.checksum = Self::calculate_checksum(&self.weights);
        self.created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

impl Default for ModelMetrics {
    fn default() -> Self {
        Self {
            accuracy: None,
            loss: None,
            f1_score: None,
            precision: None,
            recall: None,
        }
    }
}

impl FederatedLearningManager {
    pub async fn new(config: Option<FederatedConfig>) -> Result<Self, FederatedError> {
        let config = config.unwrap_or_default();
        let (event_sender, _) = mpsc::channel(1000);
        let initial_weights = vec![0.0; 100];
        
        Ok(Self {
            global_model: Arc::new(RwLock::new(GlobalModel::new(initial_weights, 0))),
            clients: DashMap::new(),
            pending_updates: Arc::new(RwLock::new(HashMap::new())),
            rounds: Arc::new(RwLock::new(HashMap::new())),
            config,
            event_sender,
            compression_config: None,
            secure_aggregation_keys: DashMap::new(),
        })
    }

    pub fn with_compression(mut self, compression: GradientCompression) -> Self {
        self.compression_config = Some(compression);
        self
    }

    pub async fn register_client(&self, client_config: ClientConfig) -> Result<String, FederatedError> {
        let client_id = if client_config.client_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            client_config.client_id.clone()
        };
        
        let client = FederatedClient::new(client_id.clone(), client_config);
        self.clients.insert(client_id.clone(), client);
        
        self.send_event(FederatedEvent::ClientJoined(client_id.clone()));
        info!("Client registered: {}", client_id);
        
        Ok(client_id)
    }

    pub async fn unregister_client(&self, client_id: &str) -> Result<(), FederatedError> {
        if self.clients.remove(client_id).is_some() {
            self.send_event(FederatedEvent::ClientLeft(client_id.to_string()));
            info!("Client unregistered: {}", client_id);
        }
        Ok(())
    }

    pub async fn start_round(
        &self,
        config: AggregationConfig,
    ) -> Result<u32, FederatedError> {
        let round_id = {
            let rounds = self.rounds.read().await;
            rounds.len() as u32 + 1
        };
        
        let global_model = self.global_model.read().await;
        
        let round = FederatedRound {
            round_id,
            config: config.clone(),
            global_model_version: global_model.version,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            end_time: None,
            selected_clients: Vec::new(),
            received_updates: Vec::new(),
            aggregated_model: None,
            status: RoundStatus::Initializing,
        };
        
        drop(global_model);
        
        self.rounds.write().await.insert(round_id, round);
        self.send_event(FederatedEvent::RoundStarted(round_id));
        
        tokio::spawn({
            let this = self.clone();
            async move {
                let _ = this.execute_round(round_id, config).await;
            }
        });
        
        Ok(round_id)
    }

    async fn execute_round(
        &self,
        round_id: u32,
        config: AggregationConfig,
    ) -> Result<(), FederatedError> {
        let mut round = {
            let mut rounds = self.rounds.write().await;
            rounds.get_mut(&round_id).unwrap().clone()
        };
        
        round.status = RoundStatus::SelectingClients;
        
        let selected_clients = self.select_clients(&config, round_id).await?;
        round.selected_clients = selected_clients.clone();
        
        self.rounds.write().await.insert(round_id, round.clone());
        
        for client_id in &selected_clients {
            self.send_event(FederatedEvent::ClientSelected(round_id, client_id.clone()));
            self.distribute_model(round_id, client_id).await?;
        }
        
        round.status = RoundStatus::WaitingForUpdates;
        self.rounds.write().await.insert(round_id, round.clone());
        
        match self.wait_for_updates(round_id, &config, &selected_clients).await {
            Ok(updates) => {
                round.received_updates = updates;
            }
            Err(e) => {
                error!("Failed to collect updates for round {}: {}", round_id, e);
                self.send_event(FederatedEvent::Alert(
                    AlertLevel::Warning,
                    format!("Round {} collected {} updates", round_id, round.received_updates.len())
                ));
            }
        }
        
        if round.received_updates.len() < config.min_clients {
            let mut round = self.rounds.write().await.get_mut(&round_id).unwrap();
            round.status = RoundStatus::Failed;
            self.send_event(FederatedEvent::RoundFailed(
                round_id,
                format!("Insufficient updates: {}/{}", 
                    round.received_updates.len(), config.min_clients)
            ));
            return Err(FederatedError::RoundError(
                format!("Only {} updates received, {} required", 
                    round.received_updates.len(), config.min_clients)
            ));
        }
        
        round.status = RoundStatus::Aggregating;
        self.rounds.write().await.insert(round_id, round.clone());
        
        match self.aggregate_updates(&mut round).await {
            Ok(aggregated_weights) => {
                round.aggregated_model = Some(aggregated_weights);
            }
            Err(e) => {
                let mut round = self.rounds.write().await.get_mut(&round_id).unwrap();
                round.status = RoundStatus::Failed;
                self.send_event(FederatedEvent::RoundFailed(round_id, e.to_string()));
                return Err(e);
            }
        }
        
        let mut global_model = self.global_model.write().await;
        if let Some(ref new_weights) = round.aggregated_model {
            global_model.update_weights(new_weights.clone());
        }
        drop(global_model);
        
        let mut round = self.rounds.write().await.get_mut(&round_id).unwrap();
        round.end_time = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        round.status = RoundStatus::Completed;
        self.rounds.write().await.insert(round_id, round);
        
        self.send_event(FederatedEvent::RoundCompleted(round_id));
        self.send_event(FederatedEvent::ModelUpdated(global_model.version));
        
        info!("Round {} completed with {} updates", round_id, round.received_updates.len());
        Ok(())
    }

    async fn select_clients(
        &self,
        config: &AggregationConfig,
        round_id: u32,
    ) -> Result<Vec<String>, FederatedError> {
        let active_clients: Vec<(String, FederatedClient)> = self.clients
            .iter()
            .filter(|(_, client)| client.is_active(self.config.heartbeat_interval_seconds * 2))
            .map(|c| (c.key().clone(), c.value().clone()))
            .collect();
        
        if active_clients.len() < config.min_clients {
            return Err(FederatedError::ClientError(
                format!("Not enough active clients: {}/{}", 
                    active_clients.len(), config.min_clients)
            ));
        }
        
        let num_clients = std::cmp::min(
            config.max_clients,
            std::cmp::max(config.min_clients, active_clients.len())
        );
        
        let selected = match &self.config.client_selection_strategy {
            ClientSelectionStrategy::Random => {
                let mut rng = rand::thread_rng();
                let mut indices: Vec<usize> = (0..active_clients.len()).collect();
                indices.shuffle(&mut rng);
                indices[..num_clients].iter()
                    .map(|&i| active_clients[i].0.clone())
                    .collect()
            }
            ClientSelectionStrategy::ReputationBased => {
                let mut clients: Vec<_> = active_clients.into_iter()
                    .map(|(id, client)| {
                        let reputation = if self.config.data_heterogeneity_penalty {
                            client.reputation_score * client.config.weight
                        } else {
                            client.reputation_score
                        };
                        (id, reputation)
                    })
                    .collect();
                clients.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                clients[..num_clients].iter()
                    .map(|(id, _)| id.clone())
                    .collect()
            }
            ClientSelectionStrategy::StratifieData => {
                let mut strata: HashMap<String, Vec<(String, FederatedClient)>> = HashMap::new();
                for (id, client) in active_clients {
                    strata.entry(client.config.capabilities.first()
                        .unwrap_or(&"default".to_string()).clone())
                        .or_default()
                        .push((id, client));
                }
                
                let mut selected = Vec::new();
                let per_strata = std::cmp::max(1, num_clients / strata.len().max(1));
                
                for (_, mut clients) in strata {
                    let mut rng = rand::thread_rng();
                    clients.shuffle(&mut rng);
                    selected.extend(clients[..std::cmp::min(per_strata, clients.len())].iter()
                        .map(|(id, _)| id.clone()));
                }
                
                if selected.len() < num_clients {
                    let mut remaining: Vec<_> = active_clients.iter()
                        .filter(|(id, _)| !selected.contains(id))
                        .map(|(id, _)| id.clone())
                        .collect();
                    let mut rng = rand::thread_rng();
                    remaining.shuffle(&mut rng);
                    selected.extend(remaining.into_iter().take(num_clients - selected.len()));
                }
                
                selected
            }
            ClientSelectionStrategy::BanditBased => {
                let mut selections: Vec<(String, f32)> = active_clients.into_iter()
                    .map(|(id, client)| (id, client.reputation_score))
                    .collect();
                let mut rng = rand::thread_rng();
                selections.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                selections[..num_clients].iter()
                    .map(|(id, _)| id.clone())
                    .collect()
            }
            ClientSelectionStrategy::CostAware => {
                let mut clients: Vec<_> = active_clients.into_iter()
                    .map(|(id, client)| {
                        let cost = 1.0 / (client.config.weight + 0.1);
                        let value = client.reputation_score / cost;
                        (id, value)
                    })
                    .collect();
                clients.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                clients[..num_clients].iter()
                    .map(|(id, _)| id.clone())
                    .collect()
            }
        };
        
        Ok(selected)
    }

    async fn distribute_model(
        &self,
        round_id: u32,
        client_id: &str,
    ) -> Result<(), FederatedError> {
        let global_model = self.global_model.read().await;
        
        let model_update = FederatedModelUpdate {
            round_id,
            model_version: global_model.version,
            weights: global_model.weights.clone(),
            metadata: HashMap::new(),
        };
        
        drop(global_model);
        
        info!("Distributed model version {} to client {}", model_update.model_version, client_id);
        Ok(())
    }

    async fn wait_for_updates(
        &self,
        round_id: u32,
        config: &AggregationConfig,
        selected_clients: &[String],
    ) -> Result<Vec<ModelUpdate>, FederatedError> {
        let timeout_duration = Duration::from_secs(config.timeout_seconds);
        
        let mut updates = Vec::new();
        let deadline = std::time::Instant::now() + timeout_duration;
        
        while std::time::Instant::now() < deadline && updates.len() < selected_clients.len() {
            let pending = self.pending_updates.read().await;
            if let Some(round_pending) = pending.get(&round_id) {
                for (client_id, pending_update) in round_pending {
                    if pending_update.verified {
                        updates.push(pending_update.update.clone());
                    }
                }
            }
            drop(pending);
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        Ok(updates)
    }

    async fn aggregate_updates(
        &self,
        round: &mut FederatedRound,
    ) -> Result<Vec<f32>, FederatedError> {
        let updates = &round.received_updates;
        
        if updates.is_empty() {
            return Err(FederatedError::AggregationError("No updates to aggregate".to_string()));
        }
        
        let mut weights = match &round.config.aggregation_strategy {
            AggregationStrategy::FedAvg => self.fedavg_aggregate(updates).await?,
            AggregationStrategy::FedMedian => self.fedmedian_aggregate(updates).await?,
            AggregationStrategy::FedTrimmedMean(trim) => self.fedtrimmed_mean_aggregate(updates, *trim).await?,
            AggregationStrategy::FedProximal(mu) => self.fedproximal_aggregate(updates, *mu).await?,
            AggregationStrategy::FedAdam => self.fedadam_aggregate(updates).await?,
            AggregationStrategy::FedAvgM(beta) => self.fedavgm_aggregate(updates, *beta).await?,
        };
        
        if let Some(ref dp_config) = round.config.differential_privacy {
            weights = self.apply_differential_privacy(&weights, dp_config)?;
        }
        
        if let Some(clip_norm) = round.config.gradient_clipping {
            weights = self.clip_gradients(weights, clip_norm);
        }
        
        Ok(weights)
    }

    async fn fedavg_aggregate(
        &self,
        updates: &[ModelUpdate],
    ) -> Result<Vec<f32>, FederatedError> {
        let total_samples: usize = updates.iter().map(|u| u.num_samples).sum();
        
        if total_samples == 0 {
            return Err(FederatedError::AggregationError("Total samples is zero".to_string()));
        }
        
        let mut aggregated = vec![0.0; updates[0].weights.len()];
        
        for update in updates {
            let weight = update.num_samples as f32 / total_samples as f32;
            for (i, w) in update.weights.iter().enumerate() {
                aggregated[i] += w * weight;
            }
        }
        
        Ok(aggregated)
    }

    async fn fedmedian_aggregate(
        &self,
        updates: &[ModelUpdate],
    ) -> Result<Vec<f32>, FederatedError> {
        let num_params = updates[0].weights.len();
        let mut aggregated = vec![0.0; num_params];
        
        for i in 0..num_params {
            let mut values: Vec<f32> = updates.iter().map(|u| u.weights[i]).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median_idx = values.len() / 2;
            aggregated[i] = if values.len() % 2 == 0 {
                (values[median_idx - 1] + values[median_idx]) / 2.0
            } else {
                values[median_idx]
            };
        }
        
        Ok(aggregated)
    }

    async fn fedtrimmed_mean_aggregate(
        &self,
        updates: &[ModelUpdate],
        trim_proportion: f64,
    ) -> Result<Vec<f32>, FederatedError> {
        let num_params = updates[0].weights.len();
        let trim_count = (updates.len() as f64 * trim_proportion).floor() as usize;
        
        if 2 * trim_count >= updates.len() {
            return Err(FederatedError::AggregationError(
                "Trim proportion too high".to_string()
            ));
        }
        
        let mut aggregated = vec![0.0; num_params];
        
        for i in 0..num_params {
            let mut values: Vec<f32> = updates.iter().map(|u| u.weights[i]).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            let trimmed: Vec<f32> = values[trim_count..values.len() - trim_count].to_vec();
            aggregated[i] = trimmed.iter().sum::<f32>() / trimmed.len() as f32;
        }
        
        Ok(aggregated)
    }

    async fn fedproximal_aggregate(
        &self,
        updates: &[ModelUpdate],
        mu: f64,
    ) -> Result<Vec<f32>, FederatedError> {
        let base_weights = if let Some(update) = updates.first() {
            update.weights.clone()
        } else {
            return Err(FederatedError::AggregationError("No updates".to_string()));
        };
        
        let global_model = self.global_model.read().await;
        let global_weights = global_model.weights.clone();
        drop(global_model);
        
        let total_samples: usize = updates.iter().map(|u| u.num_samples).sum();
        
        let mut aggregated = vec![0.0; base_weights.len()];
        let mut proximal_term = vec![0.0; base_weights.len()];
        
        for update in updates {
            let weight = update.num_samples as f32 / total_samples as f32;
            
            for (i, w) in update.weights.iter().enumerate() {
                aggregated[i] += w * weight;
                proximal_term[i] += (w - global_weights[i] as f32) * weight;
            }
        }
        
        for (i, p) in proximal_term.iter().enumerate() {
            aggregated[i] -= 2.0 * mu as f32 * p;
        }
        
        Ok(aggregated)
    }

    async fn fedadam_aggregate(
        &self,
        updates: &[ModelUpdate],
    ) -> Result<Vec<f32>, FederatedError> {
        let base_weights = if let Some(update) = updates.first() {
            update.weights.clone()
        } else {
            return Err(FederatedError::AggregationError("No updates".to_string()));
        };
        
        let total_samples: usize = updates.iter().map(|u| u.num_samples).sum();
        
        let mut m = vec![0.0; base_weights.len()];
        let mut v = vec![0.0; base_weights.len()];
        let beta1 = 0.9;
        let beta2 = 0.99;
        let epsilon = 1e-8;
        let mut aggregated = vec![0.0; base_weights.len()];
        
        for update in updates {
            let weight = update.num_samples as f32 / total_samples as f32;
            let client_gradient: Vec<f32> = update.weights.iter()
                .zip(base_weights.iter())
                .map(|(w, b)| (w - b) * weight)
                .collect();
            
            for (i, g) in client_gradient.iter().enumerate() {
                m[i] = beta1 * m[i] + (1.0 - beta1) * g;
                v[i] = beta2 * v[i] + (1.0 - beta2) * g * g;
                
                let m_hat = m[i] / (1.0 - beta1);
                let v_hat = v[i] / (1.0 - beta2);
                
                aggregated[i] = base_weights[i] - 0.001 * m_hat / (v_hat.sqrt() + epsilon);
            }
        }
        
        Ok(aggregated)
    }

    async fn fedavgm_aggregate(
        &self,
        updates: &[ModelUpdate],
        beta: &f64,
    ) -> Result<Vec<f32>, FederatedError> {
        let base_weights = if let Some(update) = updates.first() {
            update.weights.clone()
        } else {
            return Err(FederatedError::AggregationError("No updates".to_string()));
        };
        
        let total_samples: usize = updates.iter().map(|u| u.num_samples).sum();
        
        let mut velocity = vec![0.0; base_weights.len()];
        let mut aggregated = vec![0.0; base_weights.len()];
        
        for update in updates {
            let weight = update.num_samples as f32 / total_samples as f32;
            let gradient: Vec<f32> = update.weights.iter()
                .zip(base_weights.iter())
                .map(|(w, b)| (w - b) * weight)
                .collect();
            
            for (i, g) in gradient.iter().enumerate() {
                velocity[i] = beta * velocity[i] + (1.0 - beta) * g;
                aggregated[i] += gradient[i] + velocity[i];
            }
        }
        
        for i in 0..aggregated.len() {
            aggregated[i] = base_weights[i] - aggregated[i];
        }
        
        Ok(aggregated)
    }

    fn apply_differential_privacy(
        &self,
        weights: &[f32],
        config: &DPConfig,
    ) -> Result<Vec<f32>, FederatedError> {
        let mut rng = rand::thread_rng();
        
        let sigma = config.noise_scale * config.clip_norm as f64 / config.epsilon;
        let noise: Vec<f64> = (0..weights.len())
            .map(|_| {
                let u1: f64 = rng.gen();
                let u2: f64 = rng.gen();
                let standard_normal = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                sigma * standard_normal
            })
            .collect();
        
        let clipped = self.clip_gradients(weights.to_vec(), config.clip_norm);
        
        let dp_weights: Vec<f32> = clipped.iter()
            .zip(noise.iter())
            .map(|(&w, &n)| (w as f64 + n) as f32)
            .collect();
        
        Ok(dp_weights)
    }

    fn clip_gradients(&self, weights: Vec<f32>, clip_norm: f32) -> Vec<f32> {
        let norm: f32 = weights.iter().map(|w| w * w).sum::<f32>().sqrt();
        
        if norm > clip_norm {
            weights.iter().map(|w| w * clip_norm / norm).collect()
        } else {
            weights
        }
    }

    pub async fn receive_update(&self, update: ModelUpdate) -> Result<(), FederatedError> {
        let mut client = match self.clients.get_mut(&update.client_id) {
            Some(c) => c,
            None => return Err(FederatedError::ClientError(
                format!("Unknown client: {}", update.client_id)
            )),
        };
        
        client.update_heartbeat();
        
        let pending = PendingUpdate {
            update: update.clone(),
            received_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            verified: true,
        };
        
        let mut pending_map = self.pending_updates.write().await;
        let round_pending = pending_map.entry(update.round_id).or_default();
        round_pending.insert(update.client_id.clone(), pending);
        
        self.send_event(FederatedEvent::UpdateReceived(update.round_id, update.client_id));
        
        info!("Received update from client {} for round {}", update.client_id, update.round_id);
        Ok(())
    }

    pub async fn get_global_model(&self) -> GlobalModel {
        self.global_model.read().await.clone()
    }

    pub async fn get_round_status(&self, round_id: u32) -> Option<FederatedRound> {
        self.rounds.read().await.get(&round_id).cloned()
    }

    pub async fn list_active_clients(&self) -> Vec<ClientReport> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.clients.iter()
            .filter(|c| now - c.last_heartbeat < self.config.heartbeat_interval_seconds * 2)
            .map(|c| ClientReport {
                client_id: c.client_id.clone(),
                round_id: 0,
                local_samples: 0,
                local_epochs: 0,
                training_time_ms: 0,
                loss: 0.0,
                accuracy: 0.0,
                gradient_norm: c.avg_gradient_norm,
                data_distribution: HashMap::new(),
            })
            .collect()
    }

    pub async fn send_event(&self, event: FederatedEvent) {
        if self.config.enable_monitoring {
            let _ = self.event_sender.send(event).await;
        }
    }

    pub async fn start_heartbeat_monitor(&self) {
        let interval = self.config.heartbeat_interval_seconds;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval));
            loop {
                interval.tick().await;
            }
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedModelUpdate {
    pub round_id: u32,
    pub model_version: u64,
    pub weights: Vec<f32>,
    pub metadata: HashMap<String, String>,
}

impl Clone for FederatedLearningManager {
    fn clone(&self) -> Self {
        Self {
            global_model: Arc::clone(&self.global_model),
            clients: DashMap::new(),
            pending_updates: Arc::clone(&self.pending_updates),
            rounds: Arc::clone(&self.rounds),
            config: self.config.clone(),
            event_sender: self.event_sender.clone(),
            compression_config: self.compression_config.clone(),
            secure_aggregation_keys: DashMap::new(),
        }
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_federated_learning_manager_creation() {
        let manager = FederatedLearningManager::new(None).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_client_registration() {
        let manager = FederatedLearningManager::new(None).await.unwrap();
        
        let config = ClientConfig {
            client_id: String::new(),
            endpoint: "http://localhost:8080".to_string(),
            weight: 1.0,
            capabilities: vec!["gpu".to_string()],
            max_data_size: 1000000,
            trusted: false,
        };
        
        let client_id = manager.register_client(config).await;
        assert!(client_id.is_ok());
        assert!(!client_id.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_fedavg_aggregation() {
        let manager = FederatedLearningManager::new(None).await.unwrap();
        
        let updates = vec![
            ModelUpdate {
                client_id: "client1".to_string(),
                round_id: 1,
                weights: vec![1.0, 2.0, 3.0],
                num_samples: 100,
                gradient_norm: 0.5,
                timestamp: 0,
                metadata: HashMap::new(),
            },
            ModelUpdate {
                client_id: "client2".to_string(),
                round_id: 1,
                weights: vec![4.0, 5.0, 6.0],
                num_samples: 200,
                gradient_norm: 0.3,
                timestamp: 0,
                metadata: HashMap::new(),
            },
        ];
        
        let result = manager.fedavg_aggregate(&updates).await;
        assert!(result.is_ok());
        let weights = result.unwrap();
        assert_eq!(weights, vec![3.0, 4.0, 5.0]);
    }

    #[tokio::test]
    async fn test_fedmedian_aggregation() {
        let manager = FederatedLearningManager::new(None).await.unwrap();
        
        let updates = vec![
            ModelUpdate {
                client_id: "client1".to_string(),
                round_id: 1,
                weights: vec![1.0, 10.0],
                num_samples: 100,
                gradient_norm: 0.5,
                timestamp: 0,
                metadata: HashMap::new(),
            },
            ModelUpdate {
                client_id: "client2".to_string(),
                round_id: 1,
                weights: vec![2.0, 20.0],
                num_samples: 100,
                gradient_norm: 0.3,
                timestamp: 0,
                metadata: HashMap::new(),
            },
            ModelUpdate {
                client_id: "client3".to_string(),
                round_id: 1,
                weights: vec![3.0, 30.0],
                num_samples: 100,
                gradient_norm: 0.4,
                timestamp: 0,
                metadata: HashMap::new(),
            },
        ];
        
        let result = manager.fedmedian_aggregate(&updates).await;
        assert!(result.is_ok());
        let weights = result.unwrap();
        assert_eq!(weights, vec![2.0, 20.0]);
    }

    #[tokio::test]
    async fn test_gradient_clipping() {
        let manager = FederatedLearningManager::new(None).await.unwrap();
        
        let weights = vec![3.0, 4.0];
        let clipped = manager.clip_gradients(weights, 5.0);
        assert_eq!(clipped.len(), 2);
        
        let norm: f32 = clipped.iter().map(|w| w * w).sum::<f32>().sqrt();
        assert!(norm <= 5.0 + 1e-6);
    }

    #[tokio::test]
    async fn test_global_model_integrity() {
        let weights = vec![1.0, 2.0, 3.0];
        let model = GlobalModel::new(weights.clone(), 0);
        
        assert!(model.verify_integrity());
        
        let mut modified_model = model.clone();
        modified_model.weights[0] = 99.0;
        assert!(!modified_model.verify_integrity());
    }
}
