//! API module for coretexdb.
//!
//! This module provides API interfaces for coretexdb, including:
//! - REST API implementation
//! - Python bindings
//! - gRPC API (future plan)
//! - API request/response handling
//! - Authentication and authorization

pub mod rest;
pub mod python;

pub use rest::*;
pub use python::*;

