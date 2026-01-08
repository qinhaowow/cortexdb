//! Logging system for CortexDB

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "distributed")]
use std::time::Duration;

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: LogLevel,
    pub component: String,
    pub message: String,
    pub node_id: String,
    pub correlation_id: Option<String>,
    pub metadata: serde_json::Value,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct LogManager {
    logs: Arc<RwLock<Vec<LogEntry>>>,
    max_logs: usize,
    log_retention: Duration,
}

#[cfg(feature = "distributed")]
impl LogManager {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(RwLock::new(Vec::new())),
            max_logs: 10000,
            log_retention: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        }
    }

    pub fn with_max_logs(mut self, max_logs: usize) -> Self {
        self.max_logs = max_logs;
        self
    }

    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.log_retention = retention;
        self
    }

    pub async fn log(&self, level: LogLevel, component: &str, message: &str, metadata: serde_json::Value) {
        let node_id = "self".to_string(); // In a real implementation, this would be the actual node ID
        
        let log_entry = LogEntry {
            timestamp: chrono::Utc::now().timestamp() as u64,
            level,
            component: component.to_string(),
            message: message.to_string(),
            node_id,
            correlation_id: None,
            metadata,
        };

        let mut logs = self.logs.write().await;
        logs.push(log_entry);
        
        // Truncate if over max logs
        if logs.len() > self.max_logs {
            logs.drain(0..logs.len() - self.max_logs);
        }
    }

    pub async fn get_logs(&self, level: Option<LogLevel>, component: Option<&str>, limit: usize) -> Vec<LogEntry> {
        let logs = self.logs.read().await;
        
        let filtered_logs: Vec<LogEntry> = logs
            .iter()
            .filter(|log| {
                if let Some(l) = &level {
                    if log.level != *l {
                        return false;
                    }
                }
                if let Some(c) = component {
                    if log.component != c {
                        return false;
                    }
                }
                true
            })
            .rev()
            .take(limit)
            .cloned()
            .collect();
        
        filtered_logs
    }

    pub async fn export_logs(&self, format: &str) -> Result<String, Box<dyn std::error::Error>> {
        let logs = self.logs.read().await;
        
        match format {
            "json" => {
                let json = serde_json::to_string(&logs)?;
                Ok(json)
            },
            "text" => {
                let mut text = String::new();
                for log in logs {
                    text.push_str(&format!(
                        "[{}] [{}] [{}] [{}] {}\n",
                        chrono::NaiveDateTime::from_timestamp_opt(log.timestamp as i64, 0).unwrap_or_default(),
                        log.level,
                        log.component,
                        log.node_id,
                        log.message
                    ));
                }
                Ok(text)
            },
            _ => Err("Unsupported format".into()),
        }
    }

    pub async fn start_log_cleanup(&self) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60 * 60)).await; // Run every hour
                cloned_self.cleanup_old_logs().await;
            }
        });
    }

    async fn cleanup_old_logs(&self) {
        let now = chrono::Utc::now().timestamp() as u64;
        let cutoff = now - self.log_retention.as_secs();
        
        let mut logs = self.logs.write().await;
        logs.retain(|log| log.timestamp > cutoff);
    }
}

#[cfg(feature = "distributed")]
impl Clone for LogManager {
    fn clone(&self) -> Self {
        Self {
            logs: self.logs.clone(),
            max_logs: self.max_logs,
            log_retention: self.log_retention,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_manager() {
        let log_manager = LogManager::new();
        
        // Log a message
        log_manager.log(
            LogLevel::Info,
            "test",
            "Test message",
            serde_json::json!({ "test": "value" })
        ).await;
        
        // Get logs
        let logs = log_manager.get_logs(None, None, 10).await;
        assert!(!logs.is_empty());
        assert_eq!(logs[0].message, "Test message");
    }
}
