use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("Parse error: {0}")]
    ParseError(String),
    
    #[error("Type error: {0}")]
    TypeError(String),
    
    #[error("Evaluation error: {0}")]
    EvaluationError(String),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Expression {
    Constant(f64),
    Variable(String),
    Add(Box<Expression>, Box<Expression>),
    Sub(Box<Expression>, Box<Expression>),
    Mul(Box<Expression>, Box<Expression>),
    Div(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),
    Abs(Box<Expression>),
    Sin(Box<Expression>),
    Cos(Box<Expression>),
    Exp(Box<Expression>),
    Log(Box<Expression>),
    Sqrt(Box<Expression>),
    Max(Box<Expression>, Box<Expression>),
    Min(Box<Expression>, Box<Expression>),
    If(Box<Expression>, Box<Expression>, Box<Expression>),
    Call(String, Vec<Expression>),
    FieldAccess(Box<Expression>, String),
    ArrayIndex(Box<Expression>, Box<Expression>),
    Ternary(Box<Expression>, Box<Expression>, Box<Expression>),
}

impl Expression {
    pub fn constant(value: f64) -> Self {
        Expression::Constant(value)
    }
    
    pub fn variable(name: &str) -> Self {
        Expression::Variable(name.to_string())
    }
    
    pub fn add(self, other: Expression) -> Self {
        Expression::Add(Box::new(self), Box::new(other))
    }
    
    pub fn sub(self, other: Expression) -> Self {
        Expression::Sub(Box::new(self), Box::new(other))
    }
    
    pub fn mul(self, other: Expression) -> Self {
        Expression::Mul(Box::new(self), Box::new(other))
    }
    
    pub fn div(self, other: Expression) -> Self {
        Expression::Div(Box::new(self), Box::new(other))
    }
    
    pub fn evaluate(&self, variables: &HashMap<String, f64>) -> Result<f64, ExpressionError> {
        match self {
            Expression::Constant(value) => Ok(*value),
            Expression::Variable(name) => {
                variables.get(name)
                    .cloned()
                    .ok_or_else(|| ExpressionError::EvaluationError(format!("Variable '{}' not found", name)))
            }
            Expression::Add(left, right) => {
                Ok(left.evaluate(variables)? + right.evaluate(variables)?)
            }
            Expression::Sub(left, right) => {
                Ok(left.evaluate(variables)? - right.evaluate(variables)?)
            }
            Expression::Mul(left, right) => {
                Ok(left.evaluate(variables)? * right.evaluate(variables)?)
            }
            Expression::Div(left, right) => {
                let right_val = right.evaluate(variables)?;
                if right_val == 0.0 {
                    Err(ExpressionError::EvaluationError("Division by zero".to_string()))
                } else {
                    Ok(left.evaluate(variables)? / right_val)
                }
            }
            Expression::Pow(base, exp) => {
                Ok(base.evaluate(variables)?.powf(exp.evaluate(variables)?))
            }
            Expression::Neg(expr) => {
                Ok(-expr.evaluate(variables)?)
            }
            Expression::Abs(expr) => {
                Ok(expr.evaluate(variables)?.abs())
            }
            Expression::Sin(expr) => {
                Ok(expr.evaluate(variables)?.sin())
            }
            Expression::Cos(expr) => {
                Ok(expr.evaluate(variables)?.cos())
            }
            Expression::Exp(expr) => {
                Ok(expr.evaluate(variables)?.exp())
            }
            Expression::Log(expr) => {
                let val = expr.evaluate(variables)?;
                if val <= 0.0 {
                    Err(ExpressionError::EvaluationError("Log of non-positive value".to_string()))
                } else {
                    Ok(val.ln())
                }
            }
            Expression::Sqrt(expr) => {
                let val = expr.evaluate(variables)?;
                if val < 0.0 {
                    Err(ExpressionError::EvaluationError("Square root of negative value".to_string()))
                } else {
                    Ok(val.sqrt())
                }
            }
            Expression::Max(left, right) => {
                Ok(left.evaluate(variables)?.max(right.evaluate(variables)?))
            }
            Expression::Min(left, right) => {
                Ok(left.evaluate(variables)?.min(right.evaluate(variables)?))
            }
            Expression::If(cond, then, else_) => {
                if cond.evaluate(variables)? > 0.0 {
                    then.evaluate(variables)
                } else {
                    else_.evaluate(variables)
                }
            }
            Expression::Call(name, args) => {
                let evaluated_args: Result<Vec<f64>, _> = args.iter()
                    .map(|a| a.evaluate(variables))
                    .collect();
                Self::evaluate_call(name, &evaluated_args?)
            }
            Expression::FieldAccess(expr, field) => {
                let base_val = expr.evaluate(variables)?;
                Err(ExpressionError::EvaluationError(format!("Field access not supported: {}", field)))
            }
            Expression::ArrayIndex(arr, index) => {
                let arr_val = arr.evaluate(variables)?;
                let idx_val = index.evaluate(variables)? as usize;
                Err(ExpressionError::EvaluationError(format!("Array index {} not supported", idx_val)))
            }
            Expression::Ternary(cond, then, else_) => {
                if cond.evaluate(variables)? > 0.0 {
                    then.evaluate(variables)
                } else {
                    else_.evaluate(variables)
                }
            }
        }
    }
    
    fn evaluate_call(name: &str, args: &[f64]) -> Result<f64, ExpressionError> {
        match name {
            "abs" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("abs expects 1 argument".to_string()));
                }
                Ok(args[0].abs())
            }
            "round" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("round expects 1 argument".to_string()));
                }
                Ok(args[0].round())
            }
            "floor" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("floor expects 1 argument".to_string()));
                }
                Ok(args[0].floor())
            }
            "ceil" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("ceil expects 1 argument".to_string()));
                }
                Ok(args[0].ceil())
            }
            "pow" => {
                if args.len() != 2 {
                    return Err(ExpressionError::EvaluationError("pow expects 2 arguments".to_string()));
                }
                Ok(args[0].powf(args[1]))
            }
            "sqrt" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("sqrt expects 1 argument".to_string()));
                }
                if args[0] < 0.0 {
                    return Err(ExpressionError::EvaluationError("sqrt of negative value".to_string()));
                }
                Ok(args[0].sqrt())
            }
            "exp" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("exp expects 1 argument".to_string()));
                }
                Ok(args[0].exp())
            }
            "ln" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("ln expects 1 argument".to_string()));
                }
                if args[0] <= 0.0 {
                    return Err(ExpressionError::EvaluationError("ln of non-positive value".to_string()));
                }
                Ok(args[0].ln())
            }
            "log10" => {
                if args.len() != 1 {
                    return Err(ExpressionError::EvaluationError("log10 expects 1 argument".to_string()));
                }
                if args[0] <= 0.0 {
                    return Err(ExpressionError::EvaluationError("log10 of non-positive value".to_string()));
                }
                Ok(args[0].log10())
            }
            "sin" => Ok(args.get(0).map(|x| x.sin()).unwrap_or(0.0)),
            "cos" => Ok(args.get(0).map(|x| x.cos()).unwrap_or(0.0)),
            "tan" => Ok(args.get(0).map(|x| x.tan()).unwrap_or(0.0)),
            "asin" => Ok(args.get(0).map(|x| x.asin()).unwrap_or(0.0)),
            "acos" => Ok(args.get(0).map(|x| x.acos()).unwrap_or(0.0)),
            "atan" => Ok(args.get(0).map(|x| x.atan()).unwrap_or(0.0)),
            "sinh" => Ok(args.get(0).map(|x| x.sinh()).unwrap_or(0.0)),
            "cosh" => Ok(args.get(0).map(|x| x.cosh()).unwrap_or(0.0)),
            "tanh" => Ok(args.get(0).map(|x| x.tanh()).unwrap_or(0.0)),
            "min" => {
                if args.len() < 2 {
                    return Err(ExpressionError::EvaluationError("min expects at least 2 arguments".to_string()));
                }
                Ok(args.iter().fold(std::f64::INFINITY, |a, &b| a.min(b)))
            }
            "max" => {
                if args.len() < 2 {
                    return Err(ExpressionError::EvaluationError("max expects at least 2 arguments".to_string()));
                }
                Ok(args.iter().fold(std::f64::NEG_INFINITY, |a, &b| a.max(b)))
            }
            "avg" | "mean" => {
                if args.is_empty() {
                    return Err(ExpressionError::EvaluationError("avg expects at least 1 argument".to_string()));
                }
                Ok(args.iter().sum::<f64>() / args.len() as f64)
            }
            "sum" => {
                Ok(args.iter().sum())
            }
            "clamp" => {
                if args.len() != 3 {
                    return Err(ExpressionError::EvaluationError("clamp expects 3 arguments".to_string()));
                }
                Ok(args[0].clamp(args[1], args[2]))
            }
            "lerp" => {
                if args.len() != 3 {
                    return Err(ExpressionError::EvaluationError("lerp expects 3 arguments".to_string()));
                }
                Ok(args[0] + (args[2] - args[0]) * args[1])
            }
            _ => Err(ExpressionError::EvaluationError(format!("Unknown function: {}", name))),
        }
    }
    
    pub fn to_string(&self) -> String {
        match self {
            Expression::Constant(v) => format!("{}", v),
            Expression::Variable(name) => name.clone(),
            Expression::Add(l, r) => format!("({} + {})", l.to_string(), r.to_string()),
            Expression::Sub(l, r) => format!("({} - {})", l.to_string(), r.to_string()),
            Expression::Mul(l, r) => format!("({} * {})", l.to_string(), r.to_string()),
            Expression::Div(l, r) => format!("({} / {})", l.to_string(), r.to_string()),
            Expression::Pow(b, e) => format!("{}^{}", b.to_string(), e.to_string()),
            Expression::Neg(e) => format!("-{}", e.to_string()),
            Expression::Abs(e) => format!("abs({})", e.to_string()),
            Expression::Sin(e) => format!("sin({})", e.to_string()),
            Expression::Cos(e) => format!("cos({})", e.to_string()),
            Expression::Exp(e) => format!("exp({})", e.to_string()),
            Expression::Log(e) => format!("log({})", e.to_string()),
            Expression::Sqrt(e) => format!("sqrt({})", e.to_string()),
            Expression::Max(l, r) => format!("max({}, {})", l.to_string(), r.to_string()),
            Expression::Min(l, r) => format!("min({}, {})", l.to_string(), r.to_string()),
            Expression::If(c, t, e) => format!("if {} then {} else {}", c.to_string(), t.to_string(), e.to_string()),
            Expression::Call(name, args) => {
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                format!("{}({})", name, args_str.join(", "))
            }
            Expression::FieldAccess(expr, field) => format!("{}.{}", expr.to_string(), field),
            Expression::ArrayIndex(arr, idx) => format!("{}[{}]", arr.to_string(), idx.to_string()),
            Expression::Ternary(c, t, e) => format!("{} ? {} : {}", c.to_string(), t.to_string(), e.to_string()),
        }
    }
    
    pub fn estimate_complexity(&self) -> usize {
        match self {
            Expression::Constant(_) | Expression::Variable(_) => 1,
            Expression::Add(l, r) | Expression::Sub(l, r) | Expression::Mul(l, r) | Expression::Div(l, r)
            | Expression::Pow(l, r) | Expression::Max(l, r) | Expression::Min(l, r) => {
                l.estimate_complexity() + r.estimate_complexity() + 1
            }
            Expression::Neg(e) | Expression::Abs(e) | Expression::Sin(e) | Expression::Cos(e)
            | Expression::Exp(e) | Expression::Log(e) | Expression::Sqrt(e) => {
                e.estimate_complexity() + 1
            }
            Expression::If(c, t, e) => {
                c.estimate_complexity() + t.estimate_complexity() + e.estimate_complexity() + 1
            }
            Expression::Call(_, args) => args.iter().map(|a| a.estimate_complexity()).sum::<usize>() + 1,
            Expression::FieldAccess(e, _) | Expression::ArrayIndex(e, _) => e.estimate_complexity() + 1,
            Expression::Ternary(c, t, e) => {
                c.estimate_complexity() + t.estimate_complexity() + e.estimate_complexity() + 1
            }
        }
    }
    
    pub fn is_constant(&self) -> bool {
        match self {
            Expression::Constant(_) => true,
            Expression::Variable(_) => false,
            Expression::Add(l, r) => l.is_constant() && r.is_constant(),
            Expression::Sub(l, r) => l.is_constant() && r.is_constant(),
            Expression::Mul(l, r) => l.is_constant() && r.is_constant(),
            Expression::Div(l, r) => l.is_constant() && r.is_constant(),
            Expression::Pow(l, r) => l.is_constant() && r.is_constant(),
            Expression::Neg(e) => e.is_constant(),
            Expression::Abs(e) => e.is_constant(),
            Expression::Sin(e) => e.is_constant(),
            Expression::Cos(e) => e.is_constant(),
            Expression::Exp(e) => e.is_constant(),
            Expression::Log(e) => e.is_constant(),
            Expression::Sqrt(e) => e.is_constant(),
            Expression::Max(l, r) => l.is_constant() && r.is_constant(),
            Expression::Min(l, r) => l.is_constant() && r.is_constant(),
            Expression::If(c, t, e) => c.is_constant() && t.is_constant() && e.is_constant(),
            Expression::Call(_, _) => false,
            Expression::FieldAccess(_, _) => false,
            Expression::ArrayIndex(_, _) => false,
            Expression::Ternary(c, t, e) => c.is_constant() && t.is_constant() && e.is_constant(),
        }
    }
    
    pub fn constant_fold(&self) -> Option<f64> {
        if self.is_constant() {
            self.evaluate(&HashMap::new()).ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_expression() {
        let expr = Expression::Constant(42.0);
        let result = expr.evaluate(&HashMap::new());
        assert_eq!(result.unwrap(), 42.0);
    }

    #[test]
    fn test_variable_expression() {
        let expr = Expression::variable("x");
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), 10.0);
        let result = expr.evaluate(&vars);
        assert_eq!(result.unwrap(), 10.0);
    }

    #[test]
    fn test_arithmetic_expressions() {
        let expr = Expression::add(
            Expression::constant(5.0),
            Expression::mul(
                Expression::variable("x"),
                Expression::constant(3.0)
            )
        );
        
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), 4.0);
        
        let result = expr.evaluate(&vars);
        assert_eq!(result.unwrap(), 17.0);
    }

    #[test]
    fn test_division_by_zero() {
        let expr = Expression::div(
            Expression::constant(10.0),
            Expression::constant(0.0)
        );
        
        let result = expr.evaluate(&HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_trigonometric_functions() {
        let expr = Expression::Sin(Expression::variable("x"));
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), std::f64::consts::PI / 2.0);
        
        let result = expr.evaluate(&vars);
        assert!((result.unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_call_expression() {
        let expr = Expression::Call("sqrt".to_string(), vec![Expression::Constant(16.0)]);
        let result = expr.evaluate(&HashMap::new());
        assert_eq!(result.unwrap(), 4.0);
    }

    #[test]
    fn test_conditional_expression() {
        let expr = Expression::If(
            Box::new(Expression::Variable("x".to_string())),
            Box::new(Expression::Constant(1.0)),
            Box::new(Expression::Constant(0.0))
        );
        
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), 1.0);
        assert_eq!(expr.evaluate(&vars).unwrap(), 1.0);
        
        vars.insert("x".to_string(), 0.0);
        assert_eq!(expr.evaluate(&vars).unwrap(), 0.0);
    }

    #[test]
    fn test_expression_to_string() {
        let expr = Expression::add(
            Expression::constant(5.0),
            Expression::variable("x")
        );
        
        let s = expr.to_string();
        assert!(s.contains("5"));
        assert!(s.contains("x"));
    }

    #[test]
    fn test_complexity_estimation() {
        let simple = Expression::constant(1.0);
        assert_eq!(simple.estimate_complexity(), 1);
        
        let complex = Expression::add(
            Expression::mul(
                Expression::variable("a"),
                Expression::variable("b")
            ),
            Expression::div(
                Expression::variable("c"),
                Expression::variable("d")
            )
        );
        assert!(complex.estimate_complexity() > 1);
    }

    #[test]
    fn test_is_constant() {
        assert!(Expression::constant(42.0).is_constant());
        assert!(!Expression::variable("x").is_constant());
        assert!(Expression::add(
            Expression::constant(1.0),
            Expression::constant(2.0)
        ).is_constant());
        assert!(!Expression::add(
            Expression::constant(1.0),
            Expression::variable("x")
        ).is_constant());
    }

    #[test]
    fn test_constant_folding() {
        let folded = Expression::add(
            Expression::constant(1.0),
            Expression::constant(2.0)
        );
        assert_eq!(folded.constant_fold(), Some(3.0));
        
        let not_folded = Expression::add(
            Expression::constant(1.0),
            Expression::variable("x")
        );
        assert_eq!(not_folded.constant_fold(), None);
    }
}
