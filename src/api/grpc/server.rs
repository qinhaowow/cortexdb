use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Server;

#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("Server start failed: {0}")]
    StartFailed(String),
    #[error("Server stop failed: {0}")]
    StopFailed(String),
    #[error("Service error: {0}")]
    ServiceError(String),
}

pub struct GrpcServer {
    address: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GrpcServer {
    pub async fn new(address: &str) -> Result<Self, GrpcError> {
        Ok(Self {
            address: address.to_string(),
            shutdown_tx: None,
        })
    }

    pub async fn start(&self) -> Result<(), GrpcError> {
        let addr: SocketAddr = self.address.parse().map_err(|e| {
            GrpcError::StartFailed(format!("Invalid address: {}", e))
        })?;
        
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        
        // Create service
        let cortexdb_service = CortexDbService::new();
        
        // Start server
        let server = Server::builder()
            .add_service(CortexDbServer::new(cortexdb_service))
            .serve_with_shutdown(addr, async {
                shutdown_rx.await.ok();
            });
        
        // Spawn server in background
        tokio::spawn(server);
        
        // Store shutdown sender
        let mut self_mut = unsafe { &mut *(self as *const _ as *mut Self) };
        self_mut.shutdown_tx = Some(shutdown_tx);
        
        println!("gRPC server started on {}", self.address);
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), GrpcError> {
        if let Some(shutdown_tx) = &self.shutdown_tx {
            shutdown_tx.send(()).map_err(|e| {
                GrpcError::StopFailed(format!("Failed to send shutdown signal: {}", e))
            })?;
            println!("gRPC server stopped");
        }
        Ok(())
    }
}

// Protobuf generated code (mock implementation)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct HealthCheckRequest {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct HealthCheckResponse {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: u32,
    pub metric: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CreateCollectionResponse {
    pub id: String,
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct InsertDocumentRequest {
    pub collection_name: String,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Document {
    pub id: Option<String>,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct InsertDocumentResponse {
    pub inserted_count: u32,
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SearchRequest {
    pub collection_name: String,
    pub query: Vec<f32>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub took_ms: f64,
}

#[tonic::async_trait]
pub trait CortexDbService: Send + Sync + 'static {
    async fn health_check(&self, request: HealthCheckRequest) -> Result<HealthCheckResponse, tonic::Status>;
    async fn create_collection(&self, request: CreateCollectionRequest) -> Result<CreateCollectionResponse, tonic::Status>;
    async fn insert_document(&self, request: InsertDocumentRequest) -> Result<InsertDocumentResponse, tonic::Status>;
    async fn search(&self, request: SearchRequest) -> Result<SearchResponse, tonic::Status>;
}

pub struct CortexDbServiceImpl {
    collections: Arc<RwLock<std::collections::HashMap<String, Collection>>>,
}

#[derive(Debug, Clone)]
pub struct Collection {
    id: String,
    name: String,
    dimension: u32,
    metric: String,
    documents: std::collections::HashMap<String, Document>,
}

impl CortexDbServiceImpl {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl CortexDbService for CortexDbServiceImpl {
    async fn health_check(&self, _request: HealthCheckRequest) -> Result<HealthCheckResponse, tonic::Status> {
        Ok(HealthCheckResponse {
            status: "healthy".to_string(),
        })
    }

    async fn create_collection(&self, request: CreateCollectionRequest) -> Result<CreateCollectionResponse, tonic::Status> {
        let id = format!("col_{}", uuid::Uuid::new_v4());
        let collection = Collection {
            id: id.clone(),
            name: request.name.clone(),
            dimension: request.dimension,
            metric: request.metric,
            documents: std::collections::HashMap::new(),
        };
        
        let mut collections = self.collections.write().await;
        collections.insert(request.name.clone(), collection);
        
        Ok(CreateCollectionResponse {
            id,
            name: request.name,
            status: "created".to_string(),
        })
    }

    async fn insert_document(&self, request: InsertDocumentRequest) -> Result<InsertDocumentResponse, tonic::Status> {
        let mut collections = self.collections.write().await;
        if let Some(collection) = collections.get_mut(&request.collection_name) {
            let mut inserted_count = 0;
            let mut ids = Vec::new();
            
            for doc in request.documents {
                let id = doc.id.unwrap_or_else(|| format!("doc_{}", uuid::Uuid::new_v4()));
                collection.documents.insert(id.clone(), doc);
                inserted_count += 1;
                ids.push(id);
            }
            
            Ok(InsertDocumentResponse {
                inserted_count: inserted_count as u32,
                ids,
            })
        } else {
            Err(tonic::Status::not_found("Collection not found"))
        }
    }

    async fn search(&self, request: SearchRequest) -> Result<SearchResponse, tonic::Status> {
        let collections = self.collections.read().await;
        if let Some(collection) = collections.get(&request.collection_name) {
            let mut results = Vec::new();
            
            for (id, doc) in collection.documents.iter().take(request.limit as usize) {
                results.push(SearchResult {
                    id: id.clone(),
                    score: rand::random::<f32>(),
                    metadata: doc.metadata.clone(),
                });
            }
            
            Ok(SearchResponse {
                results,
                took_ms: rand::random::<f64>() * 100.0,
            })
        } else {
            Err(tonic::Status::not_found("Collection not found"))
        }
    }
}

// gRPC server definition
#[derive(Debug)]
pub struct CortexDbServer<T: CortexDbService> {
    service: T,
}

impl<T: CortexDbService> CortexDbServer<T> {
    pub fn new(service: T) -> Self {
        Self { service }
    }
}

// Implement gRPC service methods
#[tonic::async_trait]
impl<T: CortexDbService> CortexDbService for CortexDbServer<T> {
    async fn health_check(&self, request: HealthCheckRequest) -> Result<HealthCheckResponse, tonic::Status> {
        self.service.health_check(request).await
    }

    async fn create_collection(&self, request: CreateCollectionRequest) -> Result<CreateCollectionResponse, tonic::Status> {
        self.service.create_collection(request).await
    }

    async fn insert_document(&self, request: InsertDocumentRequest) -> Result<InsertDocumentResponse, tonic::Status> {
        self.service.insert_document(request).await
    }

    async fn search(&self, request: SearchRequest) -> Result<SearchResponse, tonic::Status> {
        self.service.search(request).await
    }
}