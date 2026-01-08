use crate::query::{Row, ExecutionError};

pub trait Operator {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError>;
    fn name(&self) -> &str;
    fn children(&self) -> Vec<Box<dyn Operator>>;
}

pub struct TableScan {
    table: String,
    columns: Vec<String>,
    filter: Option<Expr>,
}

impl TableScan {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            columns: Vec::new(),
            filter: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl Operator for TableScan {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(Vec::new())
    }

    fn name(&self) -> &str {
        "TableScan"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct IndexScan {
    index: String,
    table: String,
    columns: Vec<String>,
    range: Option<(Expr, Expr)>,
}

impl IndexScan {
    pub fn new(index: &str, table: &str) -> Self {
        Self {
            index: index.to_string(),
            table: table.to_string(),
            columns: Vec::new(),
            range: None,
        }
    }

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_range(mut self, start: Expr, end: Expr) -> Self {
        self.range = Some((start, end));
        self
    }
}

impl Operator for IndexScan {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(Vec::new())
    }

    fn name(&self) -> &str {
        "IndexScan"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct Projection {
    expressions: Vec<Expr>,
}

impl Projection {
    pub fn new(expressions: Vec<Expr>) -> Self {
        Self { expressions }
    }
}

impl Operator for Projection {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(input)
    }

    fn name(&self) -> &str {
        "Projection"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct Filter {
    predicate: Expr,
}

impl Filter {
    pub fn new(predicate: Expr) -> Self {
        Self { predicate }
    }
}

impl Operator for Filter {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(input)
    }

    fn name(&self) -> &str {
        "Filter"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct Join {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    join_type: JoinType,
    condition: Expr,
}

impl Join {
    pub fn new(left: Box<dyn Operator>, right: Box<dyn Operator>, join_type: JoinType, condition: Expr) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
        }
    }
}

impl Operator for Join {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        let left_result = self.left.execute(input)?;
        let right_result = self.right.execute(Vec::new())?;
        Ok(Vec::new())
    }

    fn name(&self) -> &str {
        "Join"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        vec![self.left.clone(), self.right.clone()]
    }
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

pub struct Aggregation {
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
}

impl Aggregation {
    pub fn new(group_by: Vec<Expr>, aggregates: Vec<AggregateExpr>) -> Self {
        Self {
            group_by,
            aggregates,
        }
    }
}

impl Operator for Aggregation {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(Vec::new())
    }

    fn name(&self) -> &str {
        "Aggregation"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct AggregateExpr {
    function: String,
    expr: Expr,
    alias: Option<String>,
}

impl AggregateExpr {
    pub fn new(function: &str, expr: Expr, alias: Option<&str>) -> Self {
        Self {
            function: function.to_string(),
            expr,
            alias: alias.map(|s| s.to_string()),
        }
    }
}

pub struct Sort {
    order_by: Vec<OrderByExpr>,
}

impl Sort {
    pub fn new(order_by: Vec<OrderByExpr>) -> Self {
        Self { order_by }
    }
}

impl Operator for Sort {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(input)
    }

    fn name(&self) -> &str {
        "Sort"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

pub struct OrderByExpr {
    expr: Expr,
    direction: OrderDirection,
}

impl OrderByExpr {
    pub fn new(expr: Expr, direction: OrderDirection) -> Self {
        Self { expr, direction }
    }
}

pub enum OrderDirection {
    Ascending,
    Descending,
}

pub struct Limit {
    limit: usize,
    offset: usize,
}

impl Limit {
    pub fn new(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }
}

impl Operator for Limit {
    fn execute(&self, input: Vec<Row>) -> Result<Vec<Row>, ExecutionError> {
        Ok(input.into_iter().skip(self.offset).take(self.limit).collect())
    }

    fn name(&self) -> &str {
        "Limit"
    }

    fn children(&self) -> Vec<Box<dyn Operator>> {
        Vec::new()
    }
}

use crate::query::ast::Expr;
