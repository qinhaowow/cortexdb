//! Generated gRPC code

#[cfg(feature = "grpc")]
include!(concat!(env!("OUT_DIR"), "/coretexdb.rs"));

#[cfg(not(feature = "grpc"))]
pub mod coretexdb {}

