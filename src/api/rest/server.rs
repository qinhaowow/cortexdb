use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;

use crate::api::rest::handlers::Handlers;
use crate::api::rest::middleware::auth;
use crate::api::rest::routes::Routes;

pub struct RestServer {
    address: String,
    handlers: Arc<RwLock<Handlers>>,
    routes: Routes,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("Server start failed: {0}")]
    StartFailed(String),
    #[error("Server stop failed: {0}")]
    StopFailed(String),
    #[error("Handler error: {0}")]
    HandlerError(String),
}

impl RestServer {
    pub async fn new(address: &str) -> Result<Self, RestError> {
        let handlers = Arc::new(RwLock::new(Handlers::new().await));
        let routes = Routes::new(handlers.clone());
        
        Ok(Self {
            address: address.to_string(),
            handlers,
            routes,
            shutdown_tx: None,
        })
    }

    pub async fn start(&self) -> Result<(), RestError> {
        let addr: SocketAddr = self.address.parse().map_err(|e| {
            RestError::StartFailed(format!("Invalid address: {}", e))
        })?;
        
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        
        // Build routes
        let api_routes = self.routes.build_routes();
        
        // Start server
        let server = warp::serve(api_routes)
            .bind_with_graceful_shutdown(addr, async {
                shutdown_rx.await.ok();
            });
        
        // Spawn server in background
        tokio::spawn(server);
        
        // Store shutdown sender
        let mut self_mut = unsafe { &mut *(self as *const _ as *mut Self) };
        self_mut.shutdown_tx = Some(shutdown_tx);
        
        println!("REST server started on {}", self.address);
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), RestError> {
        if let Some(shutdown_tx) = &self.shutdown_tx {
            shutdown_tx.send(()).map_err(|e| {
                RestError::StopFailed(format!("Failed to send shutdown signal: {}", e))
            })?;
            println!("REST server stopped");
        }
        Ok(())
    }

    pub async fn get_handlers(&self) -> Arc<RwLock<Handlers>> {
        self.handlers.clone()
    }
}