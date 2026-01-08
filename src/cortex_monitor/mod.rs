//! Monitoring tools for CortexDB cluster

#[cfg(feature = "distributed")]
pub mod metrics;
#[cfg(feature = "distributed")]
pub mod health;
#[cfg(feature = "distributed")]
pub mod dashboard;
#[cfg(feature = "distributed")]
pub mod logs;
#[cfg(feature = "distributed")]
pub mod alerts;

#[cfg(feature = "distributed")]
pub use metrics::MetricsCollector;
#[cfg(feature = "distributed")]
pub use health::HealthChecker;
#[cfg(feature = "distributed")]
pub use dashboard::MonitoringDashboard;
#[cfg(feature = "distributed")]
pub use logs::LogManager;
#[cfg(feature = "distributed")]
pub use alerts::AlertManager;

#[cfg(not(feature = "distributed"))]
pub struct MetricsCollector {}
#[cfg(not(feature = "distributed"))]
pub struct HealthChecker {}
#[cfg(not(feature = "distributed"))]
pub struct MonitoringDashboard {}
#[cfg(not(feature = "distributed"))]
pub struct LogManager {}
#[cfg(not(feature = "distributed"))]
pub struct AlertManager {}

impl MetricsCollector {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        metrics::MetricsCollector::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl HealthChecker {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        health::HealthChecker::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl MonitoringDashboard {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        dashboard::MonitoringDashboard::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl LogManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        logs::LogManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl AlertManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        alerts::AlertManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}
