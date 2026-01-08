use std::str::FromStr;
use crate::query::ast::{Statement, Query, Expr, Literal, BinaryOperator, DataType};

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(sql: &str) -> Self {
        let tokens = Self::tokenize(sql);
        Self { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        self.parse_statement()
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        let token = self.peek()?;
        match token {
            Token::Keyword(Keyword::Select) => self.parse_query(),
            Token::Keyword(Keyword::Create) => self.parse_create(),
            Token::Keyword(Keyword::Insert) => self.parse_insert(),
            Token::Keyword(Keyword::Update) => self.parse_update(),
            Token::Keyword(Keyword::Delete) => self.parse_delete(),
            Token::Keyword(Keyword::Drop) => self.parse_drop(),
            _ => Err(ParseError::UnexpectedToken(token)),
        }
    }

    fn parse_query(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Select))?;
        
        let distinct = self.eat(Token::Keyword(Keyword::Distinct)).is_some();
        let select_items = self.parse_select_items()?;
        
        let from = if self.peek()? == Token::Keyword(Keyword::From) {
            self.expect(Token::Keyword(Keyword::From))?;
            Some(self.parse_from()?)
        } else {
            None
        };
        
        let where_clause = if self.peek()? == Token::Keyword(Keyword::Where) {
            self.expect(Token::Keyword(Keyword::Where))?;
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        let query = Query {
            select: Select { items: select_items, distinct },
            from,
            where_clause,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        
        Ok(Statement::Query(query))
    }

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            let item = self.parse_select_item()?;
            items.push(item);
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        if self.peek()? == Token::Punctuation(Punctuation::Asterisk) {
            self.expect(Token::Punctuation(Punctuation::Asterisk))?;
            Ok(SelectItem::Star)
        } else {
            let expr = self.parse_expr()?;
            let alias = if self.peek()? == Token::Keyword(Keyword::As) {
                self.expect(Token::Keyword(Keyword::As))?;
                Some(self.expect_identifier()?)
            } else if matches!(self.peek()?, Token::Identifier(_)) {
                Some(self.expect_identifier()?)
            } else {
                None
            };
            Ok(SelectItem::Expr { expr, alias })
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_logical_expr()
    }

    fn parse_logical_expr(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_comparison_expr()?;
        while let Some(op) = self.eat_logical_operator() {
            let right = self.parse_comparison_expr()?;
            expr = Expr::BinaryOp {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_additive_expr()?;
        while let Some(op) = self.eat_comparison_operator() {
            let right = self.parse_additive_expr()?;
            expr = Expr::BinaryOp {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_multiplicative_expr()?;
        while let Some(op) = self.eat_additive_operator() {
            let right = self.parse_multiplicative_expr()?;
            expr = Expr::BinaryOp {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary_expr()?;
        while let Some(op) = self.eat_multiplicative_operator() {
            let right = self.parse_primary_expr()?;
            expr = Expr::BinaryOp {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek()? {
            Token::Identifier(_) => {
                let id = self.expect_identifier()?;
                Ok(Expr::Column(id))
            },
            Token::String(_) => {
                let s = self.expect_string()?;
                Ok(Expr::Literal(Literal::String(s)))
            },
            Token::Number(_) => {
                let n = self.expect_number()?;
                Ok(Expr::Literal(n))
            },
            Token::Keyword(Keyword::Null) => {
                self.expect(Token::Keyword(Keyword::Null))?;
                Ok(Expr::Literal(Literal::Null))
            },
            Token::Punctuation(Punctuation::LeftParen) => {
                self.expect(Token::Punctuation(Punctuation::LeftParen))?;
                let expr = self.parse_expr()?;
                self.expect(Token::Punctuation(Punctuation::RightParen))?;
                Ok(expr)
            },
            _ => Err(ParseError::UnexpectedToken(self.peek()?)),
        }
    }

    fn parse_from(&mut self) -> Result<From, ParseError> {
        let table = self.expect_identifier()?;
        Ok(From::Table(table))
    }

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Create))?;
        match self.peek()? {
            Token::Keyword(Keyword::Table) => self.parse_create_table(),
            Token::Keyword(Keyword::Index) => self.parse_create_index(),
            _ => Err(ParseError::UnexpectedToken(self.peek()?)),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Table))?;
        let if_not_exists = self.eat(Token::Keyword(Keyword::If)).is_some() && 
                           self.eat(Token::Keyword(Keyword::Not)).is_some() && 
                           self.eat(Token::Keyword(Keyword::Exists)).is_some();
        let name = self.expect_identifier()?;
        self.expect(Token::Punctuation(Punctuation::LeftParen))?;
        let mut columns = Vec::new();
        loop {
            let column = self.parse_column_definition()?;
            columns.push(column);
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        self.expect(Token::Punctuation(Punctuation::RightParen))?;
        Ok(Statement::CreateTable(CreateTable {
            name,
            columns,
            if_not_exists,
        }))
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefinition, ParseError> {
        let name = self.expect_identifier()?;
        let data_type = self.parse_data_type()?;
        let nullable = !self.eat(Token::Keyword(Keyword::Not)).is_some() || 
                      !self.eat(Token::Keyword(Keyword::Null)).is_some();
        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            default: None,
            primary_key: false,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        let name = self.expect_identifier()?;
        match name.as_str() {
            "STRING" => Ok(DataType::String),
            "INTEGER" => Ok(DataType::Integer),
            "FLOAT" => Ok(DataType::Float),
            "BOOLEAN" => Ok(DataType::Boolean),
            "VECTOR" => {
                self.expect(Token::Punctuation(Punctuation::LeftParen))?;
                let dim = self.expect_number()?;
                let dim = match dim {
                    Literal::Integer(n) => n as usize,
                    _ => return Err(ParseError::ExpectedInteger),
                };
                self.expect(Token::Punctuation(Punctuation::RightParen))?;
                Ok(DataType::Vector { dimension: dim })
            },
            _ => Err(ParseError::UnknownDataType(name)),
        }
    }

    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Insert))?;
        self.expect(Token::Keyword(Keyword::Into))?;
        let table = self.expect_identifier()?;
        self.expect(Token::Punctuation(Punctuation::LeftParen))?;
        let mut columns = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            columns.push(column);
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        self.expect(Token::Punctuation(Punctuation::RightParen))?;
        self.expect(Token::Keyword(Keyword::Values))?;
        let mut values = Vec::new();
        loop {
            self.expect(Token::Punctuation(Punctuation::LeftParen))?;
            let mut row = Vec::new();
            loop {
                let expr = self.parse_expr()?;
                row.push(expr);
                if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                    break;
                }
            }
            self.expect(Token::Punctuation(Punctuation::RightParen))?;
            values.push(row);
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        Ok(Statement::Insert(Insert {
            table,
            columns,
            values,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Update))?;
        let table = self.expect_identifier()?;
        self.expect(Token::Keyword(Keyword::Set))?;
        let mut set = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect(Token::Punctuation(Punctuation::Equal))?;
            let expr = self.parse_expr()?;
            set.push((column, expr));
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        let where_clause = if self.peek()? == Token::Keyword(Keyword::Where) {
            self.expect(Token::Keyword(Keyword::Where))?;
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update(Update {
            table,
            set,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Delete))?;
        self.expect(Token::Keyword(Keyword::From))?;
        let table = self.expect_identifier()?;
        let where_clause = if self.peek()? == Token::Keyword(Keyword::Where) {
            self.expect(Token::Keyword(Keyword::Where))?;
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete(Delete {
            table,
            where_clause,
        }))
    }

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Drop))?;
        match self.peek()? {
            Token::Keyword(Keyword::Table) => self.parse_drop_table(),
            Token::Keyword(Keyword::Index) => self.parse_drop_index(),
            _ => Err(ParseError::UnexpectedToken(self.peek()?)),
        }
    }

    fn parse_drop_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Table))?;
        let if_exists = self.eat(Token::Keyword(Keyword::If)).is_some() && 
                       self.eat(Token::Keyword(Keyword::Exists)).is_some();
        let name = self.expect_identifier()?;
        Ok(Statement::DropTable {
            name,
            if_exists,
        })
    }

    fn parse_drop_index(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Index))?;
        let if_exists = self.eat(Token::Keyword(Keyword::If)).is_some() && 
                       self.eat(Token::Keyword(Keyword::Exists)).is_some();
        let name = self.expect_identifier()?;
        Ok(Statement::DropIndex {
            name,
            if_exists,
        })
    }

    fn parse_create_index(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Keyword(Keyword::Index))?;
        let if_not_exists = self.eat(Token::Keyword(Keyword::If)).is_some() && 
                           self.eat(Token::Keyword(Keyword::Not)).is_some() && 
                           self.eat(Token::Keyword(Keyword::Exists)).is_some();
        let name = self.expect_identifier()?;
        self.expect(Token::Keyword(Keyword::On))?;
        let table = self.expect_identifier()?;
        self.expect(Token::Punctuation(Punctuation::LeftParen))?;
        let mut columns = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            columns.push(column);
            if !self.eat(Token::Punctuation(Punctuation::Comma)).is_some() {
                break;
            }
        }
        self.expect(Token::Punctuation(Punctuation::RightParen))?;
        Ok(Statement::CreateIndex {
            name,
            table,
            columns,
            index_type: "BTree".to_string(),
            if_not_exists,
        })
    }

    fn eat(&mut self, token: Token) -> Option<Token> {
        if self.pos < self.tokens.len() && self.tokens[self.pos] == token {
            let t = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(t)
        } else {
            None
        }
    }

    fn expect(&mut self, token: Token) -> Result<Token, ParseError> {
        if let Some(t) = self.eat(token.clone()) {
            Ok(t)
        } else {
            Err(ParseError::ExpectedToken(token))
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek()? {
            Token::Identifier(s) => {
                self.pos += 1;
                Ok(s)
            },
            _ => Err(ParseError::ExpectedIdentifier),
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.peek()? {
            Token::String(s) => {
                self.pos += 1;
                Ok(s)
            },
            _ => Err(ParseError::ExpectedString),
        }
    }

    fn expect_number(&mut self) -> Result<Literal, ParseError> {
        match self.peek()? {
            Token::Number(n) => {
                self.pos += 1;
                Ok(n)
            },
            _ => Err(ParseError::ExpectedNumber),
        }
    }

    fn peek(&self) -> Result<Token, ParseError> {
        if self.pos < self.tokens.len() {
            Ok(self.tokens[self.pos].clone())
        } else {
            Err(ParseError::EndOfInput)
        }
    }

    fn eat_logical_operator(&mut self) -> Option<BinaryOperator> {
        if self.eat(Token::Keyword(Keyword::And)).is_some() {
            Some(BinaryOperator::And)
        } else if self.eat(Token::Keyword(Keyword::Or)).is_some() {
            Some(BinaryOperator::Or)
        } else {
            None
        }
    }

    fn eat_comparison_operator(&mut self) -> Option<BinaryOperator> {
        if self.eat(Token::Punctuation(Punctuation::Equal)).is_some() {
            Some(BinaryOperator::Equal)
        } else if self.eat(Token::Punctuation(Punctuation::NotEqual)).is_some() {
            Some(BinaryOperator::NotEqual)
        } else if self.eat(Token::Punctuation(Punctuation::LessThan)).is_some() {
            Some(BinaryOperator::LessThan)
        } else if self.eat(Token::Punctuation(Punctuation::LessThanOrEqual)).is_some() {
            Some(BinaryOperator::LessThanOrEqual)
        } else if self.eat(Token::Punctuation(Punctuation::GreaterThan)).is_some() {
            Some(BinaryOperator::GreaterThan)
        } else if self.eat(Token::Punctuation(Punctuation::GreaterThanOrEqual)).is_some() {
            Some(BinaryOperator::GreaterThanOrEqual)
        } else {
            None
        }
    }

    fn eat_additive_operator(&mut self) -> Option<BinaryOperator> {
        if self.eat(Token::Punctuation(Punctuation::Plus)).is_some() {
            Some(BinaryOperator::Plus)
        } else if self.eat(Token::Punctuation(Punctuation::Minus)).is_some() {
            Some(BinaryOperator::Minus)
        } else {
            None
        }
    }

    fn eat_multiplicative_operator(&mut self) -> Option<BinaryOperator> {
        if self.eat(Token::Punctuation(Punctuation::Multiply)).is_some() {
            Some(BinaryOperator::Multiply)
        } else if self.eat(Token::Punctuation(Punctuation::Divide)).is_some() {
            Some(BinaryOperator::Divide)
        } else if self.eat(Token::Punctuation(Punctuation::Modulo)).is_some() {
            Some(BinaryOperator::Modulo)
        } else {
            None
        }
    }

    fn tokenize(sql: &str) -> Vec<Token> {
        // Simple tokenizer implementation
        let mut tokens = Vec::new();
        let mut chars = sql.chars().peekable();
        while let Some(&c) = chars.peek() {
            match c {
                ' ' | '\t' | '\n' | '\r' => {
                    chars.next();
                },
                '0'..='9' => {
                    let mut num = String::new();
                    let mut is_float = false;
                    while let Some(&c) = chars.peek() {
                        if c.is_ascii_digit() {
                            num.push(c);
                            chars.next();
                        } else if c == '.' && !is_float {
                            num.push(c);
                            is_float = true;
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    let literal = if is_float {
                        Literal::Float(num.parse().unwrap())
                    } else {
                        Literal::Integer(num.parse().unwrap())
                    };
                    tokens.push(Token::Number(literal));
                },
                '\'' | '"' => {
                    let quote = chars.next().unwrap();
                    let mut s = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == quote {
                            chars.next();
                            break;
                        } else if c == '\\' {
                            chars.next();
                            if let Some(&c) = chars.peek() {
                                s.push(c);
                                chars.next();
                            }
                        } else {
                            s.push(c);
                            chars.next();
                        }
                    }
                    tokens.push(Token::String(s));
                },
                '+' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Plus));
                },
                '-' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Minus));
                },
                '*' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Multiply));
                },
                '/' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Divide));
                },
                '%' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Modulo));
                },
                '=' => {
                    chars.next();
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        tokens.push(Token::Punctuation(Punctuation::Equal));
                    } else {
                        tokens.push(Token::Punctuation(Punctuation::Equal));
                    }
                },
                '!' => {
                    chars.next();
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        tokens.push(Token::Punctuation(Punctuation::NotEqual));
                    }
                },
                '<' => {
                    chars.next();
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        tokens.push(Token::Punctuation(Punctuation::LessThanOrEqual));
                    } else {
                        tokens.push(Token::Punctuation(Punctuation::LessThan));
                    }
                },
                '>' => {
                    chars.next();
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        tokens.push(Token::Punctuation(Punctuation::GreaterThanOrEqual));
                    } else {
                        tokens.push(Token::Punctuation(Punctuation::GreaterThan));
                    }
                },
                '(' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::LeftParen));
                },
                ')' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::RightParen));
                },
                ',' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Comma));
                },
                '.' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Dot));
                },
                ';' => {
                    chars.next();
                    tokens.push(Token::Punctuation(Punctuation::Semicolon));
                },
                _ => {
                    if c.is_ascii_alphabetic() || c == '_' {
                        let mut s = String::new();
                        while let Some(&c) = chars.peek() {
                            if c.is_ascii_alphanumeric() || c == '_' {
                                s.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if let Some(keyword) = Keyword::from_str(&s) {
                            tokens.push(Token::Keyword(keyword));
                        } else {
                            tokens.push(Token::Identifier(s));
                        }
                    } else {
                        chars.next();
                    }
                },
            }
        }
        tokens
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Keyword(Keyword),
    Identifier(String),
    String(String),
    Number(Literal),
    Punctuation(Punctuation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Group,
    By,
    Having,
    Order,
    Limit,
    Offset,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Index,
    Drop,
    Alter,
    As,
    On,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Distinct,
    Null,
    Not,
    And,
    Or,
    Like,
    In,
    Between,
    If,
    Exists,
    NotExists,
    Primary,
    Key,
    Default,
    Nullable,
    True,
    False,
}

impl FromStr for Keyword {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SELECT" => Ok(Keyword::Select),
            "FROM" => Ok(Keyword::From),
            "WHERE" => Ok(Keyword::Where),
            "GROUP" => Ok(Keyword::Group),
            "BY" => Ok(Keyword::By),
            "HAVING" => Ok(Keyword::Having),
            "ORDER" => Ok(Keyword::Order),
            "LIMIT" => Ok(Keyword::Limit),
            "OFFSET" => Ok(Keyword::Offset),
            "INSERT" => Ok(Keyword::Insert),
            "INTO" => Ok(Keyword::Into),
            "VALUES" => Ok(Keyword::Values),
            "UPDATE" => Ok(Keyword::Update),
            "SET" => Ok(Keyword::Set),
            "DELETE" => Ok(Keyword::Delete),
            "CREATE" => Ok(Keyword::Create),
            "TABLE" => Ok(Keyword::Table),
            "INDEX" => Ok(Keyword::Index),
            "DROP" => Ok(Keyword::Drop),
            "ALTER" => Ok(Keyword::Alter),
            "AS" => Ok(Keyword::As),
            "ON" => Ok(Keyword::On),
            "JOIN" => Ok(Keyword::Join),
            "INNER" => Ok(Keyword::Inner),
            "LEFT" => Ok(Keyword::Left),
            "RIGHT" => Ok(Keyword::Right),
            "FULL" => Ok(Keyword::Full),
            "CROSS" => Ok(Keyword::Cross),
            "DISTINCT" => Ok(Keyword::Distinct),
            "NULL" => Ok(Keyword::Null),
            "NOT" => Ok(Keyword::Not),
            "AND" => Ok(Keyword::And),
            "OR" => Ok(Keyword::Or),
            "LIKE" => Ok(Keyword::Like),
            "IN" => Ok(Keyword::In),
            "BETWEEN" => Ok(Keyword::Between),
            "IF" => Ok(Keyword::If),
            "EXISTS" => Ok(Keyword::Exists),
            "PRIMARY" => Ok(Keyword::Primary),
            "KEY" => Ok(Keyword::Key),
            "DEFAULT" => Ok(Keyword::Default),
            "TRUE" => Ok(Keyword::True),
            "FALSE" => Ok(Keyword::False),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Punctuation {
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
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Semicolon,
    Asterisk,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Unexpected token: {0:?}")]
    UnexpectedToken(Token),

    #[error("Expected token: {0:?}")]
    ExpectedToken(Token),

    #[error("Expected identifier")]
    ExpectedIdentifier,

    #[error("Expected string")]
    ExpectedString,

    #[error("Expected number")]
    ExpectedNumber,

    #[error("Expected integer")]
    ExpectedInteger,

    #[error("End of input")]
    EndOfInput,

    #[error("Unknown data type: {0}")]
    UnknownDataType(String),
}

use crate::query::ast::{Select, SelectItem, From};
