use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::io::{Write, Read};
use crate::compute::runtime::Value;

pub struct UdfManager {
    functions: Arc<RwLock<HashMap<String, UdfFunction>>>,
    sandbox: Arc<UdfSandbox>,
}

pub struct UdfFunction {
    name: String,
    code: String,
    language: String,
    signature: String,
    sandboxed: bool,
}

pub struct UdfSandbox {
    enabled: bool,
    timeout_seconds: u32,
    memory_limit_mb: usize,
    cpu_limit: usize,
}

impl UdfSandbox {
    pub fn new() -> Self {
        Self {
            enabled: true,
            timeout_seconds: 10,
            memory_limit_mb: 1024,
            cpu_limit: 1,
        }
    }

    pub fn execute(&self, code: &str, language: &str, args: &[Value]) -> Result<String, UdfError> {
        if !self.enabled {
            return self.execute_unsandboxed(code, language, args);
        }

        match language {
            "python" => self.execute_python_sandboxed(code, args),
            "javascript" => self.execute_javascript_sandboxed(code, args),
            "rust" => self.execute_rust_sandboxed(code, args),
            _ => Err(UdfError::UnsupportedLanguage(language.to_string())),
        }
    }

    fn execute_unsandboxed(&self, code: &str, language: &str, args: &[Value]) -> Result<String, UdfError> {
        match language {
            "python" => self.execute_python(code, args),
            "javascript" => self.execute_javascript(code, args),
            "rust" => self.execute_rust(code, args),
            _ => Err(UdfError::UnsupportedLanguage(language.to_string())),
        }
    }

    fn execute_python(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        let args_json = serde_json::to_string(args).unwrap();
        let script = format!(
            r#"
import json
args = json.loads('{}')
{}
"#,
            args_json, code
        );

        let mut child = Command::new("python")
            .arg("-c")
            .arg(script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| UdfError::ExecutionFailed(e.to_string()))?;

        let output = child.wait_with_output()
            .map_err(|e| UdfError::ExecutionFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(UdfError::ExecutionFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim().to_string())
    }

    fn execute_javascript(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        let args_json = serde_json::to_string(args).unwrap();
        let script = format!(
            r#"
const args = JSON.parse('{}');
{}
"#,
            args_json, code
        );

        let mut child = Command::new("node")
            .arg("-e")
            .arg(script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| UdfError::ExecutionFailed(e.to_string()))?;

        let output = child.wait_with_output()
            .map_err(|e| UdfError::ExecutionFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(UdfError::ExecutionFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim().to_string())
    }

    fn execute_rust(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        Err(UdfError::UnsupportedLanguage("rust".to_string()))
    }

    fn execute_python_sandboxed(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        self.execute_python(code, args)
    }

    fn execute_javascript_sandboxed(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        self.execute_javascript(code, args)
    }

    fn execute_rust_sandboxed(&self, code: &str, args: &[Value]) -> Result<String, UdfError> {
        Err(UdfError::UnsupportedLanguage("rust".to_string()))
    }
}

impl UdfManager {
    pub fn new() -> Result<Self, UdfError> {
        Ok(Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
            sandbox: Arc::new(UdfSandbox::new()),
        })
    }

    pub fn register(&self, name: &str, code: &str, language: &str) -> Result<(), UdfError> {
        let function = UdfFunction {
            name: name.to_string(),
            code: code.to_string(),
            language: language.to_string(),
            signature: "".to_string(),
            sandboxed: true,
        };

        let mut functions = self.functions.write().unwrap();
        functions.insert(name.to_string(), function);
        Ok(())
    }

    pub fn execute(&self, name: &str, args: &[Value]) -> Result<Value, UdfError> {
        let functions = self.functions.read().unwrap();
        let function = functions.get(name)
            .ok_or_else(|| UdfError::FunctionNotFound(name.to_string()))?;

        let result = self.sandbox.execute(&function.code, &function.language, args)?;
        self.parse_result(&result)
    }

    pub fn unregister(&self, name: &str) -> Result<(), UdfError> {
        let mut functions = self.functions.write().unwrap();
        if functions.remove(name).is_none() {
            return Err(UdfError::FunctionNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn list_functions(&self) -> Vec<String> {
        let functions = self.functions.read().unwrap();
        functions.keys().cloned().collect()
    }

    pub fn function_exists(&self, name: &str) -> bool {
        let functions = self.functions.read().unwrap();
        functions.contains_key(name)
    }

    fn parse_result(&self, result: &str) -> Result<Value, UdfError> {
        if let Ok(s) = serde_json::from_str(result) {
            Ok(Value::String(s))
        } else if let Ok(n) = result.parse::<i64>() {
            Ok(Value::Integer(n))
        } else if let Ok(n) = result.parse::<f64>() {
            Ok(Value::Float(n))
        } else if result == "true" {
            Ok(Value::Boolean(true))
        } else if result == "false" {
            Ok(Value::Boolean(false))
        } else {
            Ok(Value::String(result.to_string()))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UdfError {
    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Unsupported language: {0}")]
    UnsupportedLanguage(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Sandbox error: {0}")]
    SandboxError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udf_manager() {
        let manager = UdfManager::new().unwrap();
        let code = "
result = sum(args)
print(result)
";
        manager.register("sum", code, "python").unwrap();
        let result = manager.execute("sum", &[Value::Integer(1), Value::Integer(2), Value::Integer(3)]).unwrap();
        assert_eq!(result, Value::Integer(6));
    }
}
