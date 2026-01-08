use std::ffi::{CStr, CString};
use std::mem;
use std::ptr;

pub struct JitCompiler {
    context: *mut JitContext,
}

impl JitCompiler {
    pub fn new() -> Result<Self, JitError> {
        let context = unsafe { jit_create_context() };
        if context.is_null() {
            return Err(JitError::CreateContextFailed);
        }
        Ok(Self { context })
    }

    pub fn compile(&self, code: &str) -> Result<JitFunction, JitError> {
        let code = CString::new(code).unwrap();
        let function = unsafe { jit_compile(self.context, code.as_ptr()) };
        if function.is_null() {
            return Err(JitError::CompileFailed);
        }
        Ok(JitFunction { function })
    }
}

impl Drop for JitCompiler {
    fn drop(&mut self) {
        unsafe { jit_destroy_context(self.context) };
    }
}

pub struct JitFunction {
    function: *mut JitFunctionPtr,
}

impl JitFunction {
    pub fn execute(&self, args: &[f64]) -> Result<f64, JitError> {
        let result = unsafe { jit_execute(self.function, args.as_ptr(), args.len() as u32) };
        Ok(result)
    }
}

impl Drop for JitFunction {
    fn drop(&mut self) {
        unsafe { jit_destroy_function(self.function) };
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JitError {
    #[error("Failed to create JIT context")]
    CreateContextFailed,

    #[error("Failed to compile code")]
    CompileFailed,

    #[error("Failed to execute function")]
    ExecuteFailed,

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[repr(C)]
struct JitContext;

#[repr(C)]
struct JitFunctionPtr;

extern "C" {
    fn jit_create_context() -> *mut JitContext;
    fn jit_destroy_context(context: *mut JitContext);
    fn jit_compile(context: *mut JitContext, code: *const i8) -> *mut JitFunctionPtr;
    fn jit_destroy_function(function: *mut JitFunctionPtr);
    fn jit_execute(function: *mut JitFunctionPtr, args: *const f64, argc: u32) -> f64;
}

pub struct ExpressionJit {
    compiler: JitCompiler,
}

impl ExpressionJit {
    pub fn new() -> Result<Self, JitError> {
        let compiler = JitCompiler::new()?;
        Ok(Self { compiler })
    }

    pub fn compile_expression(&self, expr: &str) -> Result<Box<dyn Fn(&[f64]) -> f64>, JitError> {
        let code = format!(
            "
            double evaluate(double* args) {{
                {}
            }}
            ",
            expr.replace("args[", "args[").replace("]", "]")
        );

        let function = self.compiler.compile(&code)?;
        Ok(Box::new(move |args| {
            function.execute(args).unwrap()
        }))
    }

    pub fn compile_vector_function(&self, expr: &str) -> Result<Box<dyn Fn(&[f32], &[f32]) -> f32>, JitError> {
        let code = format!(
            "
            float evaluate(float* a, float* b, int n) {{
                float result = 0.0;
                for (int i = 0; i < n; i++) {{
                    {}
                }}
                return result;
            }}
            ",
            expr.replace("a[i]", "a[i]").replace("b[i]", "b[i]")
        );

        let function = self.compiler.compile(&code)?;
        Ok(Box::new(move |a, b| {
            let mut args = vec![0.0; a.len() + b.len() + 1];
            for (i, &v) in a.iter().enumerate() {
                args[i] = v as f64;
            }
            for (i, &v) in b.iter().enumerate() {
                args[a.len() + i] = v as f64;
            }
            args[a.len() + b.len()] = a.len() as f64;
            function.execute(&args).unwrap() as f32
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_compile() {
        let compiler = JitCompiler::new().unwrap();
        let function = compiler.compile("double add(double a, double b) { return a + b; }").unwrap();
        let result = function.execute(&[1.0, 2.0]).unwrap();
        assert_eq!(result, 3.0);
    }
}
