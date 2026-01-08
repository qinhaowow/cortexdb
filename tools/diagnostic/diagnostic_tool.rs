use std::path::Path;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DiagnosticConfig {
    target: String,
    checks: Vec<String>,
    output: String,
    verbose: bool,
}

#[derive(Debug, Serialize)]
pub struct DiagnosticResult {
    check: String,
    status: String,
    message: String,
    details: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum DiagnosticError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Diagnostic failed: {0}")]
    DiagnosticFailed(String),
}

pub struct DiagnosticTool {
    config: DiagnosticConfig,
}

impl DiagnosticTool {
    pub fn new(config: DiagnosticConfig) -> Self {
        Self { config }
    }

    pub fn from_config_file<P: AsRef<Path>>(path: P) -> Result<Self, DiagnosticError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(Self::new(config))
    }

    pub async fn run(&self) -> Result<Vec<DiagnosticResult>, DiagnosticError> {
        println!("Starting diagnostics for {}", self.config.target);
        println!("Checks: {:?}", self.config.checks);
        println!("Output: {}", self.config.output);
        println!("Verbose: {}", self.config.verbose);

        let mut results = Vec::new();

        // Run checks
        for check in &self.config.checks {
            println!("Running check: {}", check);
            let result = self.run_check(check).await?;
            results.push(result);
        }

        // Save results
        if !self.config.output.is_empty() {
            self.save_results(&results).await?;
        }

        // Print summary
        self.print_summary(&results).await;

        Ok(results)
    }

    async fn run_check(&self, check: &str) -> Result<DiagnosticResult, DiagnosticError> {
        // Mock check implementation
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let result = match check {
            "connection" => DiagnosticResult {
                check: check.to_string(),
                status: "PASS".to_string(),
                message: "Connection successful".to_string(),
                details: Some(serde_json::json!({
                    "response_time_ms": 15.5,
                    "version": "0.1.0"
                })),
            },
            "collections" => DiagnosticResult {
                check: check.to_string(),
                status: "PASS".to_string(),
                message: "Collections healthy".to_string(),
                details: Some(serde_json::json!({
                    "count": 5,
                    "names": ["docs", "images", "audio", "video", "text"]
                })),
            },
            "indexes" => DiagnosticResult {
                check: check.to_string(),
                status: "PASS".to_string(),
                message: "Indexes healthy".to_string(),
                details: Some(serde_json::json!({
                    "count": 5,
                    "types": ["hnsw", "hnsw", "hnsw", "hnsw", "hnsw"]
                })),
            },
            "memory" => DiagnosticResult {
                check: check.to_string(),
                status: "WARNING".to_string(),
                message: "Memory usage high".to_string(),
                details: Some(serde_json::json!({
                    "used_mb": 2048,
                    "total_mb": 4096,
                    "percentage": 50.0
                })),
            },
            "disk" => DiagnosticResult {
                check: check.to_string(),
                status: "PASS".to_string(),
                message: "Disk space sufficient".to_string(),
                details: Some(serde_json::json!({
                    "used_gb": 10,
                    "total_gb": 100,
                    "percentage": 10.0
                })),
            },
            _ => DiagnosticResult {
                check: check.to_string(),
                status: "UNKNOWN".to_string(),
                message: "Unknown check".to_string(),
                details: None,
            },
        };

        Ok(result)
    }

    async fn save_results(&self, results: &[DiagnosticResult]) -> Result<(), DiagnosticError> {
        let file = File::create(&self.config.output)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, results)?;
        println!("Results saved to {}", self.config.output);
        Ok(())
    }

    async fn print_summary(&self, results: &[DiagnosticResult]) {
        println!("Diagnostic Summary:");
        for result in results {
            println!("  {}: {}", result.check, result.status);
            if self.config.verbose {
                println!("    Message: {}", result.message);
                if let Some(details) = &result.details {
                    println!("    Details: {}", serde_json::to_string_pretty(details).unwrap());
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: diagnostic_tool <config.json>");
        std::process::exit(1);
    }

    match DiagnosticTool::from_config_file(&args[1]) {
        Ok(tool) => {
            if let Err(e) = tool.run().await {
                eprintln!("Diagnostic failed: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Failed to initialize diagnostic tool: {}", e);
            std::process::exit(1);
        }
    }
}