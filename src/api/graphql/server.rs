use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;

#[derive(Debug, thiserror::Error)]
pub enum GraphqlError {
    #[error("Server start failed: {0}")]
    StartFailed(String),
    #[error("Server stop failed: {0}")]
    StopFailed(String),
    #[error("Resolver error: {0}")]
    ResolverError(String),
}

pub struct GraphqlServer {
    address: String,
    schema: Arc<RwLock<GraphqlSchema>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

pub struct GraphqlSchema {
    collections: std::collections::HashMap<String, Collection>,
}

#[derive(Debug, Clone)]
pub struct Collection {
    id: String,
    name: String,
    dimension: usize,
    metric: String,
    documents: std::collections::HashMap<String, Document>,
}

#[derive(Debug, Clone)]
pub struct Document {
    id: String,
    vector: Vec<f32>,
    metadata: Option<serde_json::Value>,
}

impl GraphqlSchema {
    pub fn new() -> Self {
        Self {
            collections: std::collections::HashMap::new(),
        }
    }

    pub fn execute_query(&mut self, query: &str, variables: Option<serde_json::Value>) -> Result<serde_json::Value, GraphqlError> {
        // Mock GraphQL execution
        let response = serde_json::json!({
            "data": {
                "healthCheck": {
                    "status": "healthy",
                    "service": "cortexdb",
                    "version": "0.1.0"
                }
            }
        });
        
        Ok(response)
    }
}

impl GraphqlServer {
    pub async fn new(address: &str) -> Result<Self, GraphqlError> {
        let schema = Arc::new(RwLock::new(GraphqlSchema::new()));
        
        Ok(Self {
            address: address.to_string(),
            schema,
            shutdown_tx: None,
        })
    }

    pub async fn start(&self) -> Result<(), GraphqlError> {
        let addr: SocketAddr = self.address.parse().map_err(|e| {
            GraphqlError::StartFailed(format!("Invalid address: {}", e))
        })?;
        
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        
        // Build GraphQL endpoint
        let graphql_endpoint = self.build_graphql_endpoint();
        
        // Start server
        let server = warp::serve(graphql_endpoint)
            .bind_with_graceful_shutdown(addr, async {
                shutdown_rx.await.ok();
            });
        
        // Spawn server in background
        tokio::spawn(server);
        
        // Store shutdown sender
        let mut self_mut = unsafe { &mut *(self as *const _ as *mut Self) };
        self_mut.shutdown_tx = Some(shutdown_tx);
        
        println!("GraphQL server started on {}", self.address);
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), GraphqlError> {
        if let Some(shutdown_tx) = &self.shutdown_tx {
            shutdown_tx.send(()).map_err(|e| {
                GraphqlError::StopFailed(format!("Failed to send shutdown signal: {}", e))
            })?;
            println!("GraphQL server stopped");
        }
        Ok(())
    }

    fn build_graphql_endpoint(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let schema = self.schema.clone();
        
        // GraphQL endpoint
        let graphql = warp::path!("graphql")
            .and(warp::post())
            .and(warp::body::json())
            .and_then({
                let schema = schema.clone();
                move |body: serde_json::Value| {
                    let schema = schema.clone();
                    async move {
                        let query = body.get("query").and_then(|q| q.as_str()).unwrap_or("");
                        let variables = body.get("variables");
                        
                        let mut schema = schema.write().await;
                        match schema.execute_query(query, variables.cloned()) {
                            Ok(response) => Ok(warp::reply::json(&response)),
                            Err(e) => Err(warp::reject::custom(GraphqlError::ResolverError(e.to_string()))),
                        }
                    }
                }
            });
        
        // GraphQL playground
        let playground = warp::path!("playground")
            .and(warp::get())
            .map(|| {
                warp::reply::html(r#"
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <title>GraphQL Playground</title>
                        <link rel="stylesheet" href="https://unpkg.com/graphql-playground-react@1.7.28/build/static/css/index.css" />
                        <script src="https://unpkg.com/graphql-playground-react@1.7.28/build/static/js/middleware.js"></script>
                    </head>
                    <body style="margin: 0; overflow: hidden;">
                        <div id="root" style="height: 100vh; width: 100vw;"></div>
                        <script>
                            window.addEventListener('load', function() {
                                GraphQLPlayground.init(document.getElementById('root'), {
                                    endpoint: '/graphql',
                                });
                            });
                        </script>
                    </body>
                    </html>
                "#)
            });
        
        graphql.or(playground)
    }
}