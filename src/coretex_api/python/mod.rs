//! Python bindings for coretexdb.
//!
//! This module provides Python bindings for coretexdb, including:
//! - FFI (Foreign Function Interface) bindings
//! - Python client implementation
//! - Type conversions between Rust and Python
//! - Python-specific utilities and helpers

pub mod client;
pub mod types;
pub mod utils;

pub use client::*;
pub use types::*;
pub use utils::*;

