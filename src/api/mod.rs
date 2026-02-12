pub mod rest;
pub mod grpc;
pub mod pg;

use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ApiManager {
    inner: Arc<RwLock<ApiManagerInner>>,
}

pub struct ApiManagerInner {
    rest_server: Option<rest::RestServer>,
    grpc_server: Option<grpc::GrpcServer>,
    pg_server: Option<pg::PgServer>,
}

impl ApiManagerInner {
    pub async fn new() -> Self {
        Self {
            rest_server: None,
            grpc_server: None,
            pg_server: None,
        }
    }
}

impl ApiManager {
    pub async fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ApiManagerInner::new().await)),
        }
    }

    pub async fn start_rest_server(&self, address: &str) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;
        if inner.rest_server.is_some() {
            return Err(ApiError::ServerAlreadyRunning);
        }
        let server = rest::RestServer::new(address).await?;
        server.start().await?;
        inner.rest_server = Some(server);
        Ok(())
    }

    pub async fn start_grpc_server(&self, address: &str) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;
        if inner.grpc_server.is_some() {
            return Err(ApiError::ServerAlreadyRunning);
        }
        let server = grpc::GrpcServer::new(address).await?;
        server.start().await?;
        inner.grpc_server = Some(server);
        Ok(())
    }

    pub async fn start_pg_server(&self, address: &str) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;
        if inner.pg_server.is_some() {
            return Err(ApiError::ServerAlreadyRunning);
        }
        let server = pg::PgServer::new(address).await?;
        server.start().await?;
        inner.pg_server = Some(server);
        Ok(())
    }

    pub async fn stop_all_servers(&self) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;
        
        if let Some(server) = inner.rest_server.take() {
            server.stop().await?;
        }
        
        if let Some(server) = inner.grpc_server.take() {
            server.stop().await?;
        }
        
        if let Some(server) = inner.pg_server.take() {
            server.stop().await?;
        }
        
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("REST server error: {0}")]
    RestError(#[from] rest::RestError),
    #[error("gRPC server error: {0}")]
    GrpcError(#[from] grpc::GrpcError),
    #[error("PostgreSQL server error: {0}")]
    PgError(#[from] pg::PgError),
    #[error("Server already running")]
    ServerAlreadyRunning,
}