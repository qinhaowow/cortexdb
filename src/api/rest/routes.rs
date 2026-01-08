use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;

use crate::api::rest::handlers::Handlers;

pub struct Routes {
    handlers: Arc<RwLock<Handlers>>,
}

impl Routes {
    pub fn new(handlers: Arc<RwLock<Handlers>>) -> Self {
        Self { handlers }
    }

    pub fn build_routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // Health check endpoint
        let health = warp::path!("health")
            .and(warp::get())
            .and_then({
                let handlers = self.handlers.clone();
                move || {
                    let handlers = handlers.clone();
                    async move {
                        let handlers = handlers.read().await;
                        handlers.health_check().await
                            .map_err(|e| warp::reject::custom(ApiError::HandlerError(e)))
                    }
                }
            });

        // Collections endpoints
        let collections = warp::path!("collections")
            .and(self.with_handlers());

        let get_collections = collections
            .and(warp::get())
            .and_then(|handlers| async move {
                handlers.get_collections().await
                    .map_err(|e| warp::reject::custom(ApiError::HandlerError(e)))
            });

        let create_collection = collections
            .and(warp::post())
            .and(warp::body::json())
            .and_then(|handlers, req| async move {
                let mut handlers = handlers.write().await;
                handlers.create_collection(req).await
                    .map_err(|e| warp::reject::custom(ApiError::HandlerError(e)))
            });

        // Documents endpoints
        let documents = warp::path!("documents")
            .and(self.with_handlers());

        let insert_document = documents
            .and(warp::post())
            .and(warp::body::json())
            .and_then(|handlers, req| async move {
                let mut handlers = handlers.write().await;
                handlers.insert_document(req).await
                    .map_err(|e| warp::reject::custom(ApiError::HandlerError(e)))
            });

        // Search endpoint
        let search = warp::path!("search")
            .and(warp::post())
            .and(warp::body::json())
            .and(self.with_handlers())
            .and_then(|req, handlers| async move {
                handlers.search(req).await
                    .map_err(|e| warp::reject::custom(ApiError::HandlerError(e)))
            });

        // Combine all routes
        health
            .or(get_collections)
            .or(create_collection)
            .or(insert_document)
            .or(search)
            .recover(Self::handle_rejection)
    }

    fn with_handlers(&self) -> impl Filter<Extract = (Arc<RwLock<Handlers>>,), Error = std::convert::Infallible> + Clone {
        let handlers = self.handlers.clone();
        warp::any().map(move || handlers.clone())
    }

    async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, std::convert::Infallible> {
        let code = match err.find::<ApiError>() {
            Some(ApiError::HandlerError(_)) => warp::http::StatusCode::BAD_REQUEST,
            _ => warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        };

        let message = match err.find::<ApiError>() {
            Some(e) => format!("{}", e),
            _ => "Internal server error".to_string(),
        };

        let json = warp::reply::json(&serde_json::json!({
            "error": message,
        }));

        Ok(warp::reply::with_status(json, code))
    }
}

#[derive(Debug)]
pub struct ApiError {
    message: String,
}

impl ApiError {
    pub fn HandlerError(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

impl warp::reject::Reject for ApiError {}