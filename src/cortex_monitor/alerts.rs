//! Alerting system for CortexDB

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
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AlertStatus {
    Active,
    Resolved,
    Suppressed,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Alert {
    pub id: String,
    pub timestamp: u64,
    pub severity: AlertSeverity,
    pub status: AlertStatus,
    pub title: String,
    pub description: String,
    pub component: String,
    pub node_id: Option<String>,
    pub metadata: serde_json::Value,
    pub resolved_at: Option<u64>,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub severity: AlertSeverity,
    pub condition: String,
    pub threshold: f64,
    pub duration: Duration,
    pub enabled: bool,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Notification {
    pub id: String,
    pub timestamp: u64,
    pub alert_id: String,
    pub channel: String,
    pub status: String,
    pub message: String,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct AlertManager {
    alerts: Arc<RwLock<Vec<Alert>>>,
    rules: Arc<RwLock<Vec<AlertRule>>>,
    notifications: Arc<RwLock<Vec<Notification>>>,
    notification_channels: Arc<RwLock<Vec<String>>>,
}

#[cfg(feature = "distributed")]
impl AlertManager {
    pub fn new() -> Self {
        Self {
            alerts: Arc::new(RwLock::new(Vec::new())),
            rules: Arc::new(RwLock::new(Vec::new())),
            notifications: Arc::new(RwLock::new(Vec::new())),
            notification_channels: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_alert_rule(&self, rule: AlertRule) -> Result<(), Box<dyn std::error::Error>> {
        let mut rules = self.rules.write().await;
        rules.push(rule);
        Ok(())
    }

    pub async fn add_notification_channel(&self, channel: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut channels = self.notification_channels.write().await;
        if !channels.contains(&channel.to_string()) {
            channels.push(channel.to_string());
        }
        Ok(())
    }

    pub async fn trigger_alert(&self, severity: AlertSeverity, title: &str, description: &str, component: &str, node_id: Option<String>, metadata: serde_json::Value) -> Result<String, Box<dyn std::error::Error>> {
        let alert_id = uuid::Uuid::new_v4().to_string();
        
        let alert = Alert {
            id: alert_id.clone(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            severity,
            status: AlertStatus::Active,
            title: title.to_string(),
            description: description.to_string(),
            component: component.to_string(),
            node_id,
            metadata,
            resolved_at: None,
        };

        let mut alerts = self.alerts.write().await;
        alerts.push(alert);
        
        // Send notifications
        self.send_notifications(&alert_id, title, description, severity).await?;
        
        Ok(alert_id)
    }

    pub async fn resolve_alert(&self, alert_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.status = AlertStatus::Resolved;
            alert.resolved_at = Some(chrono::Utc::now().timestamp() as u64);
            
            // Send resolution notification
            self.send_resolution_notification(alert_id, &alert.title).await?;
        }
        Ok(())
    }

    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts
            .iter()
            .filter(|a| a.status == AlertStatus::Active)
            .cloned()
            .collect()
    }

    pub async fn get_alerts(&self, severity: Option<AlertSeverity>, status: Option<AlertStatus>, limit: usize) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        
        let filtered_alerts: Vec<Alert> = alerts
            .iter()
            .filter(|alert| {
                if let Some(s) = &severity {
                    if alert.severity != *s {
                        return false;
                    }
                }
                if let Some(st) = &status {
                    if alert.status != *st {
                        return false;
                    }
                }
                true
            })
            .rev()
            .take(limit)
            .cloned()
            .collect();
        
        filtered_alerts
    }

    async fn send_notifications(&self, alert_id: &str, title: &str, description: &str, severity: AlertSeverity) -> Result<(), Box<dyn std::error::Error>> {
        let channels = self.notification_channels.read().await;
        
        for channel in channels {
            let notification_id = uuid::Uuid::new_v4().to_string();
            let notification = Notification {
                id: notification_id,
                timestamp: chrono::Utc::now().timestamp() as u64,
                alert_id: alert_id.to_string(),
                channel: channel.clone(),
                status: "sent".to_string(),
                message: format!("{}: {} - {}", severity, title, description),
            };
            
            let mut notifications = self.notifications.write().await;
            notifications.push(notification);
            
            // In a real implementation, this would send the notification through the actual channel
            // For now, we'll just log it
            eprintln!("Sending notification to {}: {}", channel, notification.message);
        }
        
        Ok(())
    }

    async fn send_resolution_notification(&self, alert_id: &str, title: &str) -> Result<(), Box<dyn std::error::Error>> {
        let channels = self.notification_channels.read().await;
        
        for channel in channels {
            let notification_id = uuid::Uuid::new_v4().to_string();
            let notification = Notification {
                id: notification_id,
                timestamp: chrono::Utc::now().timestamp() as u64,
                alert_id: alert_id.to_string(),
                channel: channel.clone(),
                status: "sent".to_string(),
                message: format!("Resolved: {}", title),
            };
            
            let mut notifications = self.notifications.write().await;
            notifications.push(notification);
            
            // In a real implementation, this would send the notification through the actual channel
            // For now, we'll just log it
            eprintln!("Sending resolution notification to {}: {}", channel, notification.message);
        }
        
        Ok(())
    }

    pub async fn start_alert_monitoring(&self, metrics_collector: Arc<crate::cortex_monitor::metrics::MetricsCollector>) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await; // Check every minute
                cloned_self.check_alert_conditions(&metrics_collector).await;
            }
        });
    }

    async fn check_alert_conditions(&self, metrics_collector: &crate::cortex_monitor::metrics::MetricsCollector) {
        let rules = self.rules.read().await;
        let active_rules: Vec<AlertRule> = rules
            .iter()
            .filter(|r| r.enabled)
            .cloned()
            .collect();
        
        for rule in active_rules {
            // In a real implementation, this would check the actual metrics against the rule
            // For now, we'll just simulate a check
            if rand::random::<f64>() > 0.95 { // 5% chance to trigger an alert
                self.trigger_alert(
                    rule.severity,
                    &format!("{} threshold exceeded", rule.name),
                    &format!("{} has exceeded the threshold of {}", rule.name, rule.threshold),
                    "metrics",
                    None,
                    serde_json::json!({ "rule_id": rule.id, "threshold": rule.threshold })
                ).await.ok();
            }
        }
    }
}

#[cfg(feature = "distributed")]
impl Clone for AlertManager {
    fn clone(&self) -> Self {
        Self {
            alerts: self.alerts.clone(),
            rules: self.rules.clone(),
            notifications: self.notifications.clone(),
            notification_channels: self.notification_channels.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alert_manager() {
        let alert_manager = AlertManager::new();
        
        // Add a notification channel
        alert_manager.add_notification_channel("email").await.unwrap();
        
        // Trigger an alert
        let alert_id = alert_manager.trigger_alert(
            AlertSeverity::Warning,
            "Test Alert",
            "This is a test alert",
            "test",
            None,
            serde_json::json!({ "test": "value" })
        ).await.unwrap();
        
        // Get active alerts
        let active_alerts = alert_manager.get_active_alerts().await;
        assert!(!active_alerts.is_empty());
        
        // Resolve the alert
        alert_manager.resolve_alert(&alert_id).await.unwrap();
        
        // Check if alert is resolved
        let alerts = alert_manager.get_alerts(None, Some(AlertStatus::Resolved), 10).await;
        assert!(!alerts.is_empty());
    }
}
