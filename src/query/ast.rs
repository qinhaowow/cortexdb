use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
    Literal(Literal),
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    Subquery(Box<Query>),
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<Literal>),
    Vector(Vec<f32>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Like,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    String,
    Integer,
    Float,
    Boolean,
    Vector { dimension: usize },
    Array(Box<DataType>),
    Object,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub select: Select,
    pub from: Option<From>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderBy>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub items: Vec<SelectItem>,
    pub distinct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectItem {
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
    Star,
    TableStar {
        table: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum From {
    Table(String),
    Subquery {
        query: Box<Query>,
        alias: String,
    },
    Join {
        left: Box<From>,
        right: Box<From>,
        join_type: JoinType,
        on: Expr,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub expr: Expr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expr>,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub set: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    Query(Query),
    CreateTable(CreateTable),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        index_type: String,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
}
