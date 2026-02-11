//! REST API implementation for coretexdb.
//!
//! This module provides the REST API implementation for coretexdb, including:
//! - HTTP server setup
//! - Route definitions
//! - Request handlers
//! - Middleware components
//! - API request/response models

pub mod server;
pub mod routes;
pub mod handlers;
pub mod middleware;

pub use server::*;
pub use routes::*;
pub use handlers::*;
pub use middleware::*;

