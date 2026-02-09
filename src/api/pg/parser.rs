//! PostgreSQL Query Parser for CortexDB
//!
//! This module implements a SQL parser that supports both standard SQL syntax
//! and CortexDB-specific extensions for vector search operations.

use std::collections::HashMap;
use thiserror::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Error)]
pub enum PgParserError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Unsupported syntax: {0}")]
    UnsupportedSyntax(String),

    #[error("Unexpected token: {0}")]
    UnexpectedToken(String),

    #[error("Missing token: {0}")]
    MissingToken(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgToken {
    Identifier(String),
    String(String),
    Number(f64),
    Integer(i64),
    Symbol(String),
    Keyword(String),
    Comment(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgKeyword {
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Alter,
    Table,
    Index,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    On,
    And,
    Or,
    Not,
    In,
    Like,
    Ilike,
    Between,
    Is,
    Null,
    True,
    False,
    As,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Group,
    Having,
    Distinct,
    All,
    Union,
    Intersect,
    Except,
    Exists,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    Ann,
    Vector,
    Search,
    With,
    Cosine,
    Euclidean,
    InnerProduct,
    L2,
    Mips,
    LimitK,
    EfSearch,
    Distance,
    To,
    Collection,
    VectorColumn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgTokenizedQuery {
    pub original: String,
    pub tokens: Vec<PgToken>,
    pub normalized: String,
    pub is_vector_search: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgParsedQuery {
    pub query_type: PgQueryType,
    pub select_columns: Vec<PgSelectColumn>,
    pub from_table: Option<PgFromClause>,
    pub where_clause: Option<PgExpression>,
    pub join_clauses: Vec<PgJoin>,
    pub order_by: Vec<PgOrderBy>,
    pub group_by: Vec<PgExpression>,
    pub having: Option<PgExpression>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub ann_search: Option<PgAnnSearch>,
    pub vector_columns: HashMap<String, PgVectorColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgQueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    AnnSearch,
    Explain,
    Show,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgSelectColumn {
    pub name: String,
    pub alias: Option<String>,
    pub function: Option<PgFunction>,
    pub is_distinct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgFromClause {
    pub table_name: String,
    pub alias: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgExpression {
    Binary {
        left: Box<PgExpression>,
        operator: PgBinaryOperator,
        right: Box<PgExpression>,
    },
    Unary {
        operator: PgUnaryOperator,
        expr: Box<PgExpression>,
    },
    Function {
        name: String,
        arguments: Vec<PgExpression>,
    },
    Literal(PgLiteral),
    Column {
        name: String,
        table: Option<String>,
    },
    VectorLiteral(Vec<f32>),
    Parameter {
        index: usize,
        name: Option<String>,
    },
    Subquery(Box<PgParsedQuery>),
    Case {
        cases: Vec<(PgExpression, PgExpression)>,
        else_clause: Option<PgExpression>,
    },
    Exists {
        subquery: Box<PgParsedQuery>,
    },
    In {
        column: Box<PgExpression>,
        values: Vec<PgExpression>,
    },
    Between {
        column: Box<PgExpression>,
        lower: Box<PgExpression>,
        upper: Box<PgExpression>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgBinaryOperator {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessOrEquals,
    GreaterOrEquals,
    Like,
    Ilike,
    NotLike,
    NotIlike,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Power,
    Concatenate,
    AtTimeZone,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgUnaryOperator {
    Not,
    Negative,
    Positive,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgLiteral {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<PgLiteral>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgFunction {
    pub name: String,
    pub arguments: Vec<PgExpression>,
    pub is_aggregate: bool,
    pub is_window: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgJoin {
    pub join_type: PgJoinType,
    pub table: PgFromClause,
    pub condition: PgExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgJoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgOrderBy {
    pub expression: PgExpression,
    pub direction: PgOrderDirection,
    pub nulls_order: Option<PgNullsOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgOrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgNullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgAnnSearch {
    pub query_vector: PgExpression,
    pub column_name: String,
    pub collection_name: String,
    pub limit_k: Option<i64>,
    pub ef_search: Option<i64>,
    pub distance_metric: PgDistanceMetric,
    pub threshold: Option<f32>,
    pub filters: Option<Vec<PgExpression>>,
    pub output_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgDistanceMetric {
    Cosine,
    Euclidean,
    L2,
    InnerProduct,
    Mips,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgVectorColumn {
    pub name: String,
    pub dimension: usize,
    pub metric: PgDistanceMetric,
    pub index_type: String,
    pub is_indexed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgInsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<PgExpression>>,
    pub on_conflict: Option<PgOnConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgUpdateQuery {
    pub table_name: String,
    pub set_clauses: Vec<(String, PgExpression)>,
    pub where_clause: Option<PgExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgDeleteQuery {
    pub table_name: String,
    pub where_clause: Option<PgExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgOnConflict {
    pub target: PgConflictTarget,
    pub action: PgConflictAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgConflictTarget {
    Columns(Vec<String>),
    Constraint(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgConflictAction {
    DoNothing,
    Update(Vec<(String, PgExpression)>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgExplainResult {
    pub query: PgParsedQuery,
    pub plan: String,
    pub cost_estimate: Option<String>,
    pub execution_time_ms: Option<f64>,
}

#[derive(Debug)]
pub struct PgQueryParser {
    keywords: HashMap<String, PgKeyword>,
    functions: HashMap<String, i32>,
}

impl PgQueryParser {
    pub fn new() -> Self {
        let mut keywords = HashMap::new();
        keywords.insert("SELECT".to_string(), PgKeyword::Select);
        keywords.insert("FROM".to_string(), PgKeyword::From);
        keywords.insert("WHERE".to_string(), PgKeyword::Where);
        keywords.insert("INSERT".to_string(), PgKeyword::Insert);
        keywords.insert("INTO".to_string(), PgKeyword::Into);
        keywords.insert("VALUES".to_string(), PgKeyword::Values);
        keywords.insert("UPDATE".to_string(), PgKeyword::Update);
        keywords.insert("SET".to_string(), PgKeyword::Set);
        keywords.insert("DELETE".to_string(), PgKeyword::Delete);
        keywords.insert("CREATE".to_string(), PgKeyword::Create);
        keywords.insert("DROP".to_string(), PgKeyword::Drop);
        keywords.insert("ALTER".to_string(), PgKeyword::Alter);
        keywords.insert("TABLE".to_string(), PgKeyword::Table);
        keywords.insert("INDEX".to_string(), PgKeyword::Index);
        keywords.insert("JOIN".to_string(), PgKeyword::Join);
        keywords.insert("INNER".to_string(), PgKeyword::Inner);
        keywords.insert("LEFT".to_string(), PgKeyword::Left);
        keywords.insert("RIGHT".to_string(), PgKeyword::Right);
        keywords.insert("FULL".to_string(), PgKeyword::Full);
        keywords.insert("OUTER".to_string(), PgKeyword::Outer);
        keywords.insert("ON".to_string(), PgKeyword::On);
        keywords.insert("AND".to_string(), PgKeyword::And);
        keywords.insert("OR".to_string(), PgKeyword::Or);
        keywords.insert("NOT".to_string(), PgKeyword::Not);
        keywords.insert("IN".to_string(), PgKeyword::In);
        keywords.insert("LIKE".to_string(), PgKeyword::Like);
        keywords.insert("ILIKE".to_string(), PgKeyword::Ilike);
        keywords.insert("BETWEEN".to_string(), PgKeyword::Between);
        keywords.insert("IS".to_string(), PgKeyword::Is);
        keywords.insert("NULL".to_string(), PgKeyword::Null);
        keywords.insert("TRUE".to_string(), PgKeyword::True);
        keywords.insert("FALSE".to_string(), PgKeyword::False);
        keywords.insert("AS".to_string(), PgKeyword::As);
        keywords.insert("ORDER".to_string(), PgKeyword::Order);
        keywords.insert("BY".to_string(), PgKeyword::By);
        keywords.insert("ASC".to_string(), PgKeyword::Asc);
        keywords.insert("DESC".to_string(), PgKeyword::Desc);
        keywords.insert("LIMIT".to_string(), PgKeyword::Limit);
        keywords.insert("OFFSET".to_string(), PgKeyword::Offset);
        keywords.insert("GROUP".to_string(), PgKeyword::Group);
        keywords.insert("HAVING".to_string(), PgKeyword::Having);
        keywords.insert("DISTINCT".to_string(), PgKeyword::Distinct);
        keywords.insert("ALL".to_string(), PgKeyword::All);
        keywords.insert("UNION".to_string(), PgKeyword::Union);
        keywords.insert("INTERSECT".to_string(), PgKeyword::Intersect);
        keywords.insert("EXCEPT".to_string(), PgKeyword::Except);
        keywords.insert("EXISTS".to_string(), PgKeyword::Exists);
        keywords.insert("CASE".to_string(), PgKeyword::Case);
        keywords.insert("WHEN".to_string(), PgKeyword::When);
        keywords.insert("THEN".to_string(), PgKeyword::Then);
        keywords.insert("ELSE".to_string(), PgKeyword::Else);
        keywords.insert("END".to_string(), PgKeyword::End);
        keywords.insert("CAST".to_string(), PgKeyword::Cast);
        keywords.insert("ANN".to_string(), PgKeyword::Ann);
        keywords.insert("VECTOR".to_string(), PgKeyword::Vector);
        keywords.insert("SEARCH".to_string(), PgKeyword::Search);
        keywords.insert("WITH".to_string(), PgKeyword::With);
        keywords.insert("COSINE".to_string(), PgKeyword::Cosine);
        keywords.insert("EUCLIDEAN".to_string(), PgKeyword::Euclidean);
        keywords.insert("INNER_PRODUCT".to_string(), PgKeyword::InnerProduct);
        keywords.insert("L2".to_string(), PgKeyword::L2);
        keywords.insert("MIPS".to_string(), PgKeyword::Mips);
        keywords.insert("LIMIT_K".to_string(), PgKeyword::LimitK);
        keywords.insert("EF_SEARCH".to_string(), PgKeyword::EfSearch);
        keywords.insert("DISTANCE".to_string(), PgKeyword::Distance);
        keywords.insert("TO".to_string(), PgKeyword::To);
        keywords.insert("COLLECTION".to_string(), PgKeyword::Collection);

        let mut functions = HashMap::new();
        functions.insert("COUNT".to_string(), -1);
        functions.insert("SUM".to_string(), 1);
        functions.insert("AVG".to_string(), 1);
        functions.insert("MIN".to_string(), 1);
        functions.insert("MAX".to_string(), 1);
        functions.insert("ARRAY_AGG".to_string(), -1);
        functions.insert("STRING_AGG".to_string(), 2);
        functions.insert("JSON_AGG".to_string(), 1);
        functions.insert("NOW".to_string(), 0);
        functions.insert("CURRENT_TIMESTAMP".to_string(), 0);
        functions.insert("CURRENT_DATE".to_string(), 0);
        functions.insert("DATE_TRUNC".to_string(), 2);
        functions.insert("EXTRACT".to_string(), 2);
        functions.insert("UPPER".to_string(), 1);
        functions.insert("LOWER".to_string(), 1);
        functions.insert("LENGTH".to_string(), 1);
        functions.insert("TRIM".to_string(), -1);
        functions.insert("CONCAT".to_string(), -1);
        functions.insert("COALESCE".to_string(), -1);
        functions.insert("NULLIF".to_string(), 2);
        functions.insert("GREATEST".to_string(), -1);
        functions.insert("LEAST".to_string(), -1);
        functions.insert("ABS".to_string(), 1);
        functions.insert("CEIL".to_string(), 1);
        functions.insert("FLOOR".to_string(), 1);
        functions.insert("ROUND".to_string(), -1);
        functions.insert("SQRT".to_string(), 1);
        functions.insert("POWER".to_string(), 2);
        functions.insert("EXP".to_string(), 1);
        functions.insert("LN".to_string(), 1);
        functions.insert("LOG".to_string(), -1);
        functions.insert("SIN".to_string(), 1);
        functions.insert("COS".to_string(), 1);
        functions.insert("TAN".to_string(), 1);
        functions.insert("ASIN".to_string(), 1);
        functions.insert("ACOS".to_string(), 1);
        functions.insert("ATAN".to_string(), 1);
        functions.insert("VECTOR_DISTANCE".to_string(), 2);
        functions.insert("VECTOR_SIMILARITY".to_string(), 2);
        functions.insert("VECTOR_NORM".to_string(), 1);

        Self { keywords, functions }
    }

    pub fn tokenize(&self, query: &str) -> Vec<PgToken> {
        let mut tokens = Vec::new();
        let mut chars = query.chars().peekable();
        let mut current = String::new();

        while let Some(c) = chars.next() {
            if c.is_whitespace() {
                if !current.is_empty() {
                    tokens.push(self.classify_token(&current));
                    current.clear();
                }
                continue;
            }

            if c == '\'' {
                let mut string_val = String::new();
                loop {
                    if let Some(next) = chars.next() {
                        if next == '\'' {
                            if chars.peek() == Some(&'\'') {
                                string_val.push('\'');
                                chars.next();
                            } else {
                                break;
                            }
                        } else {
                            string_val.push(next);
                        }
                    } else {
                        break;
                    }
                }
                tokens.push(PgToken::String(string_val));
                continue;
            }

            if c.is_digit(10) || (c == '.' && current.is_empty()) {
                current.push(c);
                while let Some(&next) = chars.peek() {
                    if next.is_digit(10) || next == '.' || next == 'e' || next == 'E' {
                        current.push(chars.next().unwrap());
                    } else if next == '-' && (current.ends_with('e') || current.ends_with('E')) {
                        current.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                tokens.push(PgToken::Number(current.parse().unwrap_or(0.0)));
                current.clear();
                continue;
            }

            if c == '-' && chars.peek() == Some(&&'-') {
                chars.next();
                let mut comment = String::new();
                while let Some(&next) = chars.peek() {
                    if next == '\n' {
                        break;
                    }
                    comment.push(chars.next().unwrap());
                }
                tokens.push(PgToken::Comment(comment));
                continue;
            }

            if c == '/' && chars.peek() == Some(&&'*') {
                chars.next();
                let mut comment = String::new();
                loop {
                    if let Some(next) = chars.next() {
                        if next == '*' && chars.peek() == Some(&&'/') {
                            chars.next();
                            break;
                        }
                        comment.push(next);
                    } else {
                        break;
                    }
                }
                tokens.push(PgToken::Comment(comment));
                continue;
            }

            if c == ',' || c == ';' || c == '(' || c == ')' || c == '*' {
                if !current.is_empty() {
                    tokens.push(self.classify_token(&current));
                    current.clear();
                }
                tokens.push(PgToken::Symbol(c.to_string()));
                continue;
            }

            current.push(c);
        }

        if !current.is_empty() {
            tokens.push(self.classify_token(&current));
        }

        tokens
    }

    fn classify_token(&self, token: &str) -> PgToken {
        let upper = token.to_uppercase();
        if self.keywords.contains_key(&upper) {
            PgToken::Keyword(upper)
        } else if let Ok(num) = token.parse::<i64>() {
            PgToken::Integer(num)
        } else {
            PgToken::Identifier(token.to_string())
        }
    }

    pub fn parse(&self, query: &str) -> Result<PgParsedQuery, PgParserError> {
        let tokens = self.tokenize(query);

        if tokens.is_empty() {
            return Err(PgParserError::ParseError("Empty query".to_string()));
        }

        match &tokens[0] {
            PgToken::Keyword(k) => match k.as_str() {
                "SELECT" => self.parse_select(&tokens),
                "INSERT" => self.parse_insert(&tokens),
                "UPDATE" => self.parse_update(&tokens),
                "DELETE" => self.parse_delete(&tokens),
                "CREATE" => self.parse_create(&tokens),
                "DROP" => self.parse_drop(&tokens),
                "ANN" | "VECTOR" | "SEARCH" => self.parse_ann_search(&tokens),
                "EXPLAIN" => self.parse_explain(&tokens),
                _ => Err(PgParserError::UnsupportedSyntax(format!(
                    "Unsupported query type: {}",
                    k
                ))),
            },
            _ => Err(PgParserError::UnexpectedToken(format!(
                "Expected keyword but got: {:?}",
                tokens[0]
            ))),
        }
    }

    fn parse_select(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;
        let mut columns = Vec::new();

        while offset < tokens.len() && !matches!(tokens[offset], PgToken::Keyword(ref k) if k == "FROM") {
            columns.push(self.parse_select_column(tokens, &mut offset)?);
            if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                offset += 1;
            }
        }

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("FROM".to_string()));
        }

        offset += 1;

        let from_table = self.parse_from_clause(tokens, &mut offset)?;

        let mut joins = Vec::new();
        while offset < tokens.len() && matches!(tokens[offset], PgToken::Keyword(ref k) if ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"].contains(&k.as_str())) {
            joins.push(self.parse_join(tokens, &mut offset)?);
        }

        let mut where_clause = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("WHERE".to_string()) {
            offset += 1;
            where_clause = Some(self.parse_expression(tokens, &mut offset)?);
        }

        let mut order_by = Vec::new();
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("ORDER".to_string()) {
            offset += 1;
            if offset < tokens.len() && tokens[offset] == PgToken::Keyword("BY".to_string()) {
                offset += 1;
                while offset < tokens.len() {
                    order_by.push(self.parse_order_by(tokens, &mut offset)?);
                    if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                        offset += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        let mut group_by = Vec::new();
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("GROUP".to_string()) {
            offset += 1;
            if offset < tokens.len() && tokens[offset] == PgToken::Keyword("BY".to_string()) {
                offset += 1;
                while offset < tokens.len() {
                    group_by.push(self.parse_expression(tokens, &mut offset)?);
                    if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                        offset += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        let mut having = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("HAVING".to_string()) {
            offset += 1;
            having = Some(self.parse_expression(tokens, &mut offset)?);
        }

        let mut limit = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("LIMIT".to_string()) {
            offset += 1;
            if offset < tokens.len() {
                if let PgToken::Integer(n) = tokens[offset] {
                    limit = Some(*n);
                    offset += 1;
                }
            }
        }

        let mut offset_value = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("OFFSET".to_string()) {
            offset += 1;
            if offset < tokens.len() {
                if let PgToken::Integer(n) = tokens[offset] {
                    offset_value = Some(*n);
                    offset += 1;
                }
            }
        }

        Ok(PgParsedQuery {
            query_type: PgQueryType::Select,
            select_columns: columns,
            from_table: Some(from_table),
            where_clause,
            join_clauses: joins,
            order_by,
            group_by,
            having,
            limit,
            offset: offset_value,
            ann_search: None,
            vector_columns: HashMap::new(),
        })
    }

    fn parse_insert(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;
        if offset >= tokens.len() || tokens[offset] != PgToken::Keyword("INTO".to_string()) {
            return Err(PgParserError::MissingToken("INTO".to_string()));
        }
        offset += 1;

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("table name".to_string()));
        }

        let table_name = if let PgToken::Identifier(name) = &tokens[offset] {
            name.clone()
        } else {
            return Err(PgParserError::UnexpectedToken(format!(
                "Expected table name but got: {:?}",
                tokens[offset]
            )));
        };
        offset += 1;

        let mut columns = Vec::new();
        if offset < tokens.len() && tokens[offset] == PgToken::Symbol("(".to_string()) {
            offset += 1;
            while offset < tokens.len() {
                if let PgToken::Identifier(name) = &tokens[offset] {
                    columns.push(name.clone());
                    offset += 1;
                }
                if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                    offset += 1;
                } else {
                    break;
                }
            }
            if offset >= tokens.len() || tokens[offset] != PgToken::Symbol(")".to_string()) {
                return Err(PgParserError::MissingToken(")".to_string()));
            }
            offset += 1;
        }

        if offset >= tokens.len() || tokens[offset] != PgToken::Keyword("VALUES".to_string()) {
            return Err(PgParserError::MissingToken("VALUES".to_string()));
        }
        offset += 1;

        let mut values = Vec::new();
        while offset < tokens.len() {
            if tokens[offset] != PgToken::Symbol("(".to_string()) {
                break;
            }
            offset += 1;

            let mut row = Vec::new();
            while offset < tokens.len() && tokens[offset] != PgToken::Symbol(")".to_string()) {
                row.push(self.parse_expression(tokens, &mut offset)?);
                if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                    offset += 1;
                }
            }
            values.push(row);

            if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                offset += 1;
            }
        }

        Ok(PgParsedQuery {
            query_type: PgQueryType::Insert,
            select_columns: vec![],
            from_table: Some(PgFromClause {
                table_name,
                alias: None,
                schema: None,
            }),
            where_clause: None,
            join_clauses: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            ann_search: None,
            vector_columns: HashMap::new(),
        })
    }

    fn parse_update(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("table name".to_string()));
        }

        let table_name = if let PgToken::Identifier(name) = &tokens[offset] {
            name.clone()
        } else {
            return Err(PgParserError::UnexpectedToken(format!(
                "Expected table name but got: {:?}",
                tokens[offset]
            )));
        };
        offset += 1;

        if offset >= tokens.len() || tokens[offset] != PgToken::Keyword("SET".to_string()) {
            return Err(PgParserError::MissingToken("SET".to_string()));
        }
        offset += 1;

        let mut set_clauses = Vec::new();
        while offset < tokens.len() {
            if let PgToken::Identifier(col) = &tokens[offset] {
                offset += 1;
                if offset >= tokens.len() || tokens[offset] != PgToken::Symbol("=".to_string()) {
                    return Err(PgParserError::MissingToken("=".to_string()));
                }
                offset += 1;

                set_clauses.push((col.clone(), self.parse_expression(tokens, &mut offset)?));
            }

            if offset < tokens.len() && tokens[offset] == PgToken::Symbol(",".to_string()) {
                offset += 1;
            } else {
                break;
            }
        }

        let mut where_clause = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("WHERE".to_string()) {
            offset += 1;
            where_clause = Some(self.parse_expression(tokens, &mut offset)?);
        }

        Ok(PgParsedQuery {
            query_type: PgQueryType::Update,
            select_columns: vec![],
            from_table: Some(PgFromClause {
                table_name,
                alias: None,
                schema: None,
            }),
            where_clause,
            join_clauses: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            ann_search: None,
            vector_columns: HashMap::new(),
        })
    }

    fn parse_delete(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;

        if offset >= tokens.len() || tokens[offset] != PgToken::Keyword("FROM".to_string()) {
            return Err(PgParserError::MissingToken("FROM".to_string()));
        }
        offset += 1;

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("table name".to_string()));
        }

        let table_name = if let PgToken::Identifier(name) = &tokens[offset] {
            name.clone()
        } else {
            return Err(PgParserError::UnexpectedToken(format!(
                "Expected table name but got: {:?}",
                tokens[offset]
            )));
        };
        offset += 1;

        let mut where_clause = None;
        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("WHERE".to_string()) {
            offset += 1;
            where_clause = Some(self.parse_expression(tokens, &mut offset)?);
        }

        Ok(PgParsedQuery {
            query_type: PgQueryType::Delete,
            select_columns: vec![],
            from_table: Some(PgFromClause {
                table_name,
                alias: None,
                schema: None,
            }),
            where_clause,
            join_clauses: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            ann_search: None,
            vector_columns: HashMap::new(),
        })
    }

    fn parse_create(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("CREATE object type".to_string()));
        }

        match &tokens[offset] {
            PgToken::Keyword(k) if k == "TABLE" => {
                offset += 1;
                let table_name = if offset < tokens.len() && matches!(tokens[offset], PgToken::Identifier(_)) {
                    let name = tokens[offset].clone();
                    offset += 1;
                    name
                } else {
                    return Err(PgParserError::MissingToken("table name".to_string()));
                };

                Ok(PgParsedQuery {
                    query_type: PgQueryType::Create,
                    select_columns: vec![],
                    from_table: Some(PgFromClause {
                        table_name: match table_name {
                            PgToken::Identifier(n) => n,
                            _ => return Err(PgParserError::MissingToken("table name".to_string())),
                        },
                        alias: None,
                        schema: None,
                    }),
                    where_clause: None,
                    join_clauses: vec![],
                    order_by: vec![],
                    group_by: vec![],
                    having: None,
                    limit: None,
                    offset: None,
                    ann_search: None,
                    vector_columns: HashMap::new(),
                })
            }
            _ => Err(PgParserError::UnsupportedSyntax(
                "CREATE statement not fully implemented".to_string(),
            )),
        }
    }

    fn parse_drop(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;

        if offset >= tokens.len() {
            return Err(PgParserError::MissingToken("DROP object type".to_string()));
        }

        match &tokens[offset] {
            PgToken::Keyword(k) if k == "TABLE" || k == "INDEX" || k == "COLLECTION" => {
                offset += 1;
                let name = if offset < tokens.len() && matches!(tokens[offset], PgToken::Identifier(_)) {
                    let name = tokens[offset].clone();
                    offset += 1;
                    name
                } else {
                    return Err(PgParserError::MissingToken("object name".to_string()));
                };

                Ok(PgParsedQuery {
                    query_type: PgQueryType::Drop,
                    select_columns: vec![],
                    from_table: Some(PgFromClause {
                        table_name: match name {
                            PgToken::Identifier(n) => n,
                            _ => return Err(PgParserError::MissingToken("object name".to_string())),
                        },
                        alias: None,
                        schema: None,
                    }),
                    where_clause: None,
                    join_clauses: vec![],
                    order_by: vec![],
                    group_by: vec![],
                    having: None,
                    limit: None,
                    offset: None,
                    ann_search: None,
                    vector_columns: HashMap::new(),
                })
            }
            _ => Err(PgParserError::UnsupportedSyntax(
                "DROP statement not fully implemented".to_string(),
            )),
        }
    }

    fn parse_ann_search(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 0;

        let mut query_vector = None;
        let mut column_name = "embedding".to_string();
        let mut collection_name = String::new();
        let mut limit_k = Some(10);
        let mut ef_search = None;
        let mut distance_metric = PgDistanceMetric::Cosine;
        let mut threshold = None;
        let mut filters = None;
        let mut output_fields = vec!["*".to_string()];

        while offset < tokens.len() {
            match &tokens[offset] {
                PgToken::Keyword(k) => match k.as_str() {
                    "SEARCH" => {
                        offset += 1;
                        if offset < tokens.len() && tokens[offset] == PgToken::Keyword("WITH".to_string()) {
                            offset += 1;
                            if offset < tokens.len() && tokens[offset] == PgToken::Symbol("(".to_string()) {
                                offset += 1;
                                let mut params = Vec::new();
                                while offset < tokens.len() && tokens[offset] != PgToken::Symbol(")".to_string()) {
                                    params.push(tokens[offset].clone());
                                    offset += 1;
                                }
                                offset += 1;

                                let mut i = 0;
                                while i < params.len() {
                                    match params[i].clone() {
                                        PgToken::Keyword(k) => match k.as_str() {
                                            "QUERY_VECTOR" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    query_vector = Some(self.parse_vector_literal(&params, &mut i)?);
                                                }
                                            }
                                            "COLUMN" | "COL" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    if let PgToken::Identifier(name) = params[i].clone() {
                                                        column_name = name;
                                                    }
                                                    i += 1;
                                                }
                                            }
                                            "COLLECTION" | "TO" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    if let PgToken::Identifier(name) = params[i].clone() {
                                                        collection_name = name;
                                                    }
                                                    i += 1;
                                                }
                                            }
                                            "LIMIT_K" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    if let PgToken::Integer(n) = params[i] {
                                                        limit_k = Some(*n);
                                                    }
                                                    i += 1;
                                                }
                                            }
                                            "EF_SEARCH" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    if let PgToken::Integer(n) = params[i] {
                                                        ef_search = Some(*n);
                                                    }
                                                    i += 1;
                                                }
                                            }
                                            "METRIC" | "DISTANCE" => {
                                                i += 1;
                                                if i < params.len() && params[i] == PgToken::Symbol("=".to_string()) {
                                                    i += 1;
                                                    if let PgToken::Keyword(metric) = params[i].clone() {
                                                        distance_metric = match metric.as_str() {
                                                            "COSINE" => PgDistanceMetric::Cosine,
                                                            "EUCLIDEAN" | "L2" => PgDistanceMetric::L2,
                                                            "INNER_PRODUCT" | "MIPS" => PgDistanceMetric::InnerProduct,
                                                            _ => PgDistanceMetric::Cosine,
                                                        };
                                                    }
                                                    i += 1;
                                                }
                                            }
                                            _ => i += 1,
                                        }
                                        PgToken::Symbol(s) if s == "," => i += 1,
                                        _ => i += 1,
                                    }
                                }
                            }
                        }
                    }
                    "LIMIT" => {
                        offset += 1;
                        if let PgToken::Integer(n) = tokens.get(offset).cloned() {
                            limit_k = Some(*n);
                            offset += 1;
                        }
                    }
                    _ => offset += 1,
                }
                _ => offset += 1,
            }
        }

        let ann_search = Some(PgAnnSearch {
            query_vector: query_vector.unwrap_or(PgExpression::Literal(PgLiteral::Array(vec![]))),
            column_name,
            collection_name,
            limit_k,
            ef_search,
            distance_metric,
            threshold,
            filters,
            output_fields,
        });

        Ok(PgParsedQuery {
            query_type: PgQueryType::AnnSearch,
            select_columns: vec![PgSelectColumn {
                name: "*".to_string(),
                alias: None,
                function: None,
                is_distinct: false,
            }],
            from_table: Some(PgFromClause {
                table_name: collection_name.clone(),
                alias: None,
                schema: None,
            }),
            where_clause: None,
            join_clauses: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: limit_k,
            offset: None,
            ann_search,
            vector_columns: HashMap::new(),
        })
    }

    fn parse_explain(&self, tokens: &[PgToken]) -> Result<PgParsedQuery, PgParserError> {
        let mut offset = 1;
        let query = tokens[offset..].join(" ");
        let parsed = self.parse(&query)?;

        Ok(PgParsedQuery {
            query_type: PgQueryType::Explain,
            ..parsed
        })
    }

    fn parse_select_column(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgSelectColumn, PgParserError> {
        let name = if let PgToken::Identifier(name) = &tokens[*offset].clone() {
            name.clone()
        } else if let PgToken::Symbol("*".to_string()) = tokens[*offset].clone() {
            "*".to_string()
        } else {
            return Err(PgParserError::UnexpectedToken(format!(
                "Expected column name but got: {:?}",
                tokens[*offset]
            )));
        };
        *offset += 1;

        let alias = if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("AS".to_string()) {
            *offset += 1;
            if *offset < tokens.len() {
                let alias_name = if let PgToken::Identifier(name) = tokens[*offset].clone() {
                    name.clone()
                } else {
                    return Err(PgParserError::UnexpectedToken(format!(
                        "Expected alias name but got: {:?}",
                        tokens[*offset]
                    )));
                };
                *offset += 1;
                Some(alias_name)
            } else {
                None
            }
        } else {
            None
        };

        Ok(PgSelectColumn {
            name,
            alias,
            function: None,
            is_distinct: false,
        })
    }

    fn parse_from_clause(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgFromClause, PgParserError> {
        let table_name = if let PgToken::Identifier(name) = &tokens[*offset].clone() {
            name.clone()
        } else {
            return Err(PgParserError::UnexpectedToken(format!(
                "Expected table name but got: {:?}",
                tokens[*offset]
            )));
        };
        *offset += 1;

        let alias = if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("AS".to_string()) {
            *offset += 1;
            if *offset < tokens.len() {
                let alias_name = if let PgToken::Identifier(name) = tokens[*offset].clone() {
                    name.clone()
                } else {
                    return Err(PgParserError::UnexpectedToken(format!(
                        "Expected alias name but got: {:?}",
                        tokens[*offset]
                    )));
                };
                *offset += 1;
                Some(alias_name)
            } else {
                None
            }
        } else {
            None
        };

        Ok(PgFromClause {
            table_name,
            alias,
            schema: None,
        })
    }

    fn parse_join(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgJoin, PgParserError> {
        let join_type = match &tokens[*offset] {
            PgToken::Keyword(k) => match k.as_str() {
                "INNER" => {
                    *offset += 1;
                    PgJoinType::Inner
                }
                "LEFT" => {
                    *offset += 1;
                    PgJoinType::Left
                }
                "RIGHT" => {
                    *offset += 1;
                    PgJoinType::Right
                }
                "FULL" => {
                    *offset += 1;
                    PgJoinType::Full
                }
                "CROSS" => {
                    *offset += 1;
                    PgJoinType::Cross
                }
                _ => PgJoinType::Inner,
            },
            _ => PgJoinType::Inner,
        };

        if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("JOIN".to_string()) {
            *offset += 1;
        }

        let table = self.parse_from_clause(tokens, offset)?;

        if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("ON".to_string()) {
            *offset += 1;
        }

        let condition = self.parse_expression(tokens, offset)?;

        Ok(PgJoin {
            join_type,
            table,
            condition,
        })
    }

    fn parse_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        self.parse_or_expression(tokens, offset)
    }

    fn parse_or_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        let mut left = self.parse_and_expression(tokens, offset)?;

        while *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("OR".to_string()) {
            *offset += 1;
            let right = self.parse_and_expression(tokens, offset)?;
            left = PgExpression::Binary {
                left: Box::new(left),
                operator: PgBinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        let mut left = self.parse_not_expression(tokens, offset)?;

        while *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("AND".to_string()) {
            *offset += 1;
            let right = self.parse_not_expression(tokens, offset)?;
            left = PgExpression::Binary {
                left: Box::new(left),
                operator: PgBinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("NOT".to_string()) {
            *offset += 1;
            let expr = self.parse_comparison_expression(tokens, offset)?;
            return Ok(PgExpression::Unary {
                operator: PgUnaryOperator::Not,
                expr: Box::new(expr),
            });
        }

        self.parse_comparison_expression(tokens, offset)
    }

    fn parse_comparison_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        let left = self.parse_additive_expression(tokens, offset)?;

        if *offset >= tokens.len() {
            return Ok(left);
        }

        let operator = match &tokens[*offset] {
            PgToken::Symbol(s) => match s.as_str() {
                "=" => Some(PgBinaryOperator::Equals),
                "<>" | "!=" => Some(PgBinaryOperator::NotEquals),
                "<" => Some(PgBinaryOperator::LessThan),
                ">" => Some(PgBinaryOperator::GreaterThan),
                "<=" => Some(PgBinaryOperator::LessOrEquals),
                ">=" => Some(PgBinaryOperator::GreaterOrEquals),
                _ => None,
            },
            PgToken::Keyword(k) => match k.as_str() {
                "LIKE" => Some(PgBinaryOperator::Like),
                "ILIKE" => Some(PgBinaryOperator::Ilike),
                "BETWEEN" => {
                    let column = Box::new(left);
                    *offset += 1;
                    let lower = Box::new(self.parse_additive_expression(tokens, offset)?);
                    if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("AND".to_string()) {
                        *offset += 1;
                        let upper = Box::new(self.parse_additive_expression(tokens, offset)?);
                        return Ok(PgExpression::Between {
                            column,
                            lower,
                            upper,
                        });
                    } else {
                        return Err(PgParserError::MissingToken("AND".to_string()));
                    }
                }
                "IN" => {
                    *offset += 1;
                    if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol("(".to_string()) {
                        *offset += 1;
                        let mut values = Vec::new();
                        while *offset < tokens.len() && tokens[*offset] != PgToken::Symbol(")".to_string()) {
                            values.push(self.parse_additive_expression(tokens, offset)?);
                            if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol(",".to_string()) {
                                *offset += 1;
                            }
                        }
                        if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol(")".to_string()) {
                            *offset += 1;
                        }
                        return Ok(PgExpression::In {
                            column: Box::new(left),
                            values,
                        });
                    }
                    None
                }
                "IS" => {
                    *offset += 1;
                    let mut is_not = false;
                    if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("NOT".to_string()) {
                        is_not = true;
                        *offset += 1;
                    }
                    if *offset < tokens.len() && tokens[*offset] == PgToken::Keyword("NULL".to_string()) {
                        *offset += 1;
                        return Ok(PgExpression::Unary {
                            operator: if is_not {
                                PgUnaryOperator::IsNotNull
                            } else {
                                PgUnaryOperator::IsNull
                            },
                            expr: Box::new(left),
                        });
                    }
                    None
                }
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = operator {
            *offset += 1;
            let right = self.parse_additive_expression(tokens, offset)?;
            return Ok(PgExpression::Binary {
                left: Box::new(left),
                operator,
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_additive_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        let mut left = self.parse_multiplicative_expression(tokens, offset)?;

        while *offset < tokens.len() {
            match &tokens[*offset] {
                PgToken::Symbol(s) if s == "+" || s == "-" => {
                    let operator = if s == "+" {
                        PgBinaryOperator::Plus
                    } else {
                        PgBinaryOperator::Minus
                    };
                    *offset += 1;
                    let right = self.parse_multiplicative_expression(tokens, offset)?;
                    left = PgExpression::Binary {
                        left: Box::new(left),
                        operator,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }

        Ok(left)
    }

    fn parse_multiplicative_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        let mut left = self.parse_unary_expression(tokens, offset)?;

        while *offset < tokens.len() {
            match &tokens[*offset] {
                PgToken::Symbol(s) if s == "*" || s == "/" || s == "%" => {
                    let operator = match s.as_str() {
                        "*" => PgBinaryOperator::Multiply,
                        "/" => PgBinaryOperator::Divide,
                        "%" => PgBinaryOperator::Modulo,
                        _ => PgBinaryOperator::Multiply,
                    };
                    *offset += 1;
                    let right = self.parse_unary_expression(tokens, offset)?;
                    left = PgExpression::Binary {
                        left: Box::new(left),
                        operator,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }

        Ok(left)
    }

    fn parse_unary_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        if *offset >= tokens.len() {
            return Err(PgParserError::UnexpectedToken("End of input".to_string()));
        }

        match &tokens[*offset] {
            PgToken::Symbol(s) if s == "-" => {
                *offset += 1;
                let expr = self.parse_primary_expression(tokens, offset)?;
                Ok(PgExpression::Unary {
                    operator: PgUnaryOperator::Negative,
                    expr: Box::new(expr),
                })
            }
            PgToken::Symbol(s) if s == "+" => {
                *offset += 1;
                let expr = self.parse_primary_expression(tokens, offset)?;
                Ok(PgExpression::Unary {
                    operator: PgUnaryOperator::Positive,
                    expr: Box::new(expr),
                })
            }
            PgToken::Keyword(k) if k == "NOT" => {
                *offset += 1;
                let expr = self.parse_primary_expression(tokens, offset)?;
                Ok(PgExpression::Unary {
                    operator: PgUnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expression(tokens, offset),
        }
    }

    fn parse_primary_expression(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        if *offset >= tokens.len() {
            return Err(PgParserError::UnexpectedToken("End of input".to_string()));
        }

        match &tokens[*offset] {
            PgToken::Symbol(s) if s == "(" => {
                *offset += 1;
                let expr = self.parse_expression(tokens, offset)?;
                if *offset >= tokens.len() || tokens[*offset] != PgToken::Symbol(")".to_string()) {
                    return Err(PgParserError::MissingToken(")".to_string()));
                }
                *offset += 1;
                Ok(expr)
            }
            PgToken::Identifier(name) => {
                *offset += 1;

                if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol("(".to_string()) {
                    *offset += 1;
                    let mut args = Vec::new();
                    while *offset < tokens.len() && tokens[*offset] != PgToken::Symbol(")".to_string()) {
                        args.push(self.parse_expression(tokens, offset)?);
                        if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol(",".to_string()) {
                            *offset += 1;
                        }
                    }
                    if *offset >= tokens.len() || tokens[*offset] != PgToken::Symbol(")".to_string()) {
                        return Err(PgParserError::MissingToken(")".to_string()));
                    }
                    *offset += 1;

                    Ok(PgExpression::Function {
                        name: name.clone(),
                        arguments: args,
                    })
                } else {
                    Ok(PgExpression::Column {
                        name: name.clone(),
                        table: None,
                    })
                }
            }
            PgToken::String(s) => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::String(s.clone())))
            }
            PgToken::Integer(n) => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::Integer(*n)))
            }
            PgToken::Number(f) => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::Float(*f)))
            }
            PgToken::Keyword(k) if k == "NULL" => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::Null))
            }
            PgToken::Keyword(k) if k == "TRUE" => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::Boolean(true)))
            }
            PgToken::Keyword(k) if k == "FALSE" => {
                *offset += 1;
                Ok(PgExpression::Literal(PgLiteral::Boolean(false)))
            }
            _ => Err(PgParserError::UnexpectedToken(format!(
                "Unexpected token: {:?}",
                tokens[*offset]
            ))),
        }
    }

    fn parse_vector_literal(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgExpression, PgParserError> {
        if *offset >= tokens.len() {
            return Err(PgParserError::UnexpectedToken("End of input".to_string()));
        }

        match &tokens[*offset] {
            PgToken::Symbol(s) if s == "[" || s == "ARRAY" => {
                *offset += 1;
                let mut values = Vec::new();
                while *offset < tokens.len() && tokens[*offset] != PgToken::Symbol("]".to_string()) {
                    match &tokens[*offset] {
                        PgToken::Number(n) => {
                            values.push(*n as f32);
                            *offset += 1;
                        }
                        PgToken::Integer(n) => {
                            values.push(*n as f32);
                            *offset += 1;
                        }
                        PgToken::Symbol(s) if s == "," => {
                            *offset += 1;
                        }
                        _ => {
                            return Err(PgParserError::UnexpectedToken(format!(
                                "Expected vector value but got: {:?}",
                                tokens[*offset]
                            )));
                        }
                    }
                }
                if *offset < tokens.len() && tokens[*offset] == PgToken::Symbol("]".to_string()) {
                    *offset += 1;
                }
                Ok(PgExpression::VectorLiteral(values))
            }
            _ => Err(PgParserError::UnexpectedToken(format!(
                "Expected vector literal but got: {:?}",
                tokens[*offset]
            ))),
        }
    }

    fn parse_order_by(&self, tokens: &[PgToken], offset: &mut usize) -> Result<PgOrderBy, PgParserError> {
        let expr = self.parse_expression(tokens, offset)?;

        let direction = if *offset < tokens.len() {
            match &tokens[*offset] {
                PgToken::Keyword(k) if k == "ASC" => {
                    *offset += 1;
                    PgOrderDirection::Asc
                }
                PgToken::Keyword(k) if k == "DESC" => {
                    *offset += 1;
                    PgOrderDirection::Desc
                }
                _ => PgOrderDirection::Asc,
            }
        } else {
            PgOrderDirection::Asc
        };

        Ok(PgOrderBy {
            expression: expr,
            direction,
            nulls_order: None,
        })
    }
}

impl Default for PgQueryParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_initialization() {
        let parser = PgQueryParser::new();
        assert!(!parser.keywords.is_empty());
        assert!(!parser.functions.is_empty());
    }

    #[test]
    fn test_tokenize_simple_select() {
        let parser = PgQueryParser::new();
        let tokens = parser.tokenize("SELECT id, name FROM users WHERE age > 18");

        assert!(!tokens.is_empty());
        assert_eq!(tokens[0], PgToken::Keyword("SELECT".to_string()));
        assert_eq!(tokens[1], PgToken::Identifier("id".to_string()));
        assert_eq!(tokens[2], PgToken::Symbol(",".to_string()));
    }

    #[test]
    fn test_parse_simple_select() {
        let parser = PgQueryParser::new();
        let query = parser.parse("SELECT id, name FROM users").unwrap();

        assert_eq!(query.query_type, PgQueryType::Select);
        assert_eq!(query.select_columns.len(), 2);
        assert!(query.from_table.is_some());
        assert_eq!(query.from_table.unwrap().table_name, "users");
    }

    #[test]
    fn test_parse_ann_search() {
        let parser = PgQueryParser::new();
        let query = parser.parse("ANN SEARCH WITH (QUERY_VECTOR=[1.0,2.0,3.0], COLUMN=embedding, COLLECTION=documents, LIMIT_K=10)").unwrap();

        assert_eq!(query.query_type, PgQueryType::AnnSearch);
        assert!(query.ann_search.is_some());
        let ann = query.ann_search.unwrap();
        assert_eq!(ann.limit_k, Some(10));
        assert_eq!(ann.distance_metric, PgDistanceMetric::Cosine);
    }

    #[test]
    fn test_parse_vector_literal() {
        let parser = PgQueryParser::new();
        let tokens = parser.tokenize("[1.0, 2.0, 3.0, 4.0]");
        assert!(!tokens.is_empty());
    }
}
