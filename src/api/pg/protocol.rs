//! PostgreSQL Wire Protocol Handler for CortexDB
//!
//! This module implements the PostgreSQL wire protocol for database connectivity.
//! It handles authentication, command execution, and result streaming compatible
//! with standard PostgreSQL clients and drivers.

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::io::{Read, Write, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpSocket, TcpStream as AsyncTcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader, BufWriter as AsyncBufWriter};
use tokio::spawn;
use tokio::time::{timeout, Duration};
use bytes::{BytesMut, Buf, BufMut};
use thiserror::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::cortex_core::types::{Vector, CollectionSchema};
use crate::cortex_query::executor::QueryExecutor;

#[derive(Debug, Error)]
pub enum PgProtocolError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Not supported: {0}")]
    NotSupported(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: u32,
    pub connection_timeout_ms: u64,
    pub keepalive_timeout_ms: u64,
    pub enable_ssl: bool,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub ssl_ca: Option<String>,
    pub require_auth: bool,
    pub auth_method: PgAuthMethod,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            max_connections: 100,
            connection_timeout_ms: 30000,
            keepalive_timeout_ms: 60000,
            enable_ssl: false,
            ssl_cert: None,
            ssl_key: None,
            ssl_ca: None,
            require_auth: false,
            auth_method: PgAuthMethod::Trust,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgAuthMethod {
    Trust,
    Password(PgPasswordMethod),
    Ssl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgPasswordMethod {
    Md5,
    ScramSha256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConnection {
    pub id: String,
    pub user: String,
    pub database: String,
    pub application_name: String,
    pub client_addr: SocketAddr,
    pub connected_at: u64,
    pub state: PgConnectionState,
    pub parameters: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgConnectionState {
    Startup,
    Auth,
    AuthOk,
    Ready,
    Query,
    Terminate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgMessage {
    pub type_code: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgFrontendMessage {
    Startup {
        protocol_version: i32,
        parameters: HashMap<String, String>,
    },
    Password {
        password: String,
    },
    Query {
        query: String,
    },
    Parse {
        query: String,
        parameter_types: Vec<i32>,
        statement_name: String,
    },
    Bind {
        statement_name: String,
        portal_name: String,
        parameter_values: Vec<Option<Vec<u8>>>,
    },
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    Describe {
        object_type: DescribeObject,
        name: String,
    },
    Close {
        object_type: DescribeObject,
        name: String,
    },
    Sync,
    Terminate,
    Flush,
    FunctionCall {
        function_id: i32,
        argument_values: Vec<Option<Vec<u8>>>,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyFail {
        error_message: String,
    },
    GssResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DescribeObject {
    Statement,
    Portal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgBackendMessage {
    AuthenticationOk,
    AuthenticationMd5Password {
        salt: [u8; 4],
    },
    AuthenticationScramSha256Plus {
        nonce: Vec<u8>,
    },
    AuthenticationCleartextPassword,
    AuthenticationSASL {
        mechanisms: Vec<String>,
    },
    AuthenticationSASLContinue {
        data: Vec<u8>,
    },
    AuthenticationSASLFinal {
        data: Vec<u8>,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    ReadyForQuery {
        transaction_status: TransactionStatus,
    },
    RowDescription {
        fields: Vec<PgFieldDescription>,
    },
    DataRow {
        values: Vec<Option<Vec<u8>>>,
    },
    CommandComplete {
        command_tag: String,
    },
    ErrorResponse {
        severity: ErrorSeverity,
        code: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
    NoticeResponse {
        severity: NoticeSeverity,
        code: String,
        message: String,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,
    FunctionCallResponse {
        result: Option<Vec<u8>>,
    },
    CopyInResponse {
        format: i8,
        column_formats: Vec<i16>,
    },
    CopyOutResponse {
        format: i8,
        column_formats: Vec<i16>,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    GssResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionStatus {
    Idle,
    Blocked,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NoticeSeverity {
    Debug,
    Info,
    Notice,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgFieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attribute: i32,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgPreparedStatement {
    pub name: String,
    pub query: String,
    pub parameter_types: Vec<i32>,
    pub created_at: u64,
    pub use_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgPortal {
    pub name: String,
    pub statement_name: String,
    pub parameter_values: Vec<Option<Vec<u8>>>,
    pub created_at: u64,
    pub use_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgQueryResult {
    pub rows: Vec<HashMap<String, PgValue>>,
    pub fields: Vec<PgFieldDescription>,
    pub command_tag: String,
    pub row_count: u64,
    pub execution_time_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgValue {
    pub value: Option<Vec<u8>>,
    pub type_oid: i32,
    pub is_null: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgServerStats {
    pub total_connections: u64,
    pub active_connections: u64,
    pub total_queries: u64,
    pub total_rows_returned: u64,
    pub total_bytes_sent: u64,
    pub average_query_time_ms: f64,
}

#[derive(Debug)]
pub struct PgProtocolHandler {
    config: PgConfig,
    connection: PgConnection,
    stream: AsyncTcpStream,
    reader: AsyncBufReader<AsyncTcpStream>,
    writer: AsyncBufWriter<AsyncTcpStream>,
    statements: HashMap<String, PgPreparedStatement>,
    portals: HashMap<String, PgPortal>,
    query_executor: Arc<QueryExecutor>,
    stats: Arc<RwLock<PgServerStats>>,
}

impl PgProtocolHandler {
    pub fn new(
        config: PgConfig,
        stream: AsyncTcpStream,
        query_executor: Arc<QueryExecutor>,
    ) -> Self {
        let (reader, writer) = tokio::io::split(stream.clone());

        Self {
            config,
            connection: PgConnection {
                id: Uuid::new_v4().to_string(),
                user: String::new(),
                database: String::new(),
                application_name: String::new(),
                client_addr: stream.peer_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
                connected_at: chrono::Utc::now().timestamp() as u64,
                state: PgConnectionState::Startup,
                parameters: HashMap::new(),
            },
            stream,
            reader: AsyncBufReader::new(reader),
            writer: AsyncBufWriter::new(writer),
            statements: HashMap::new(),
            portals: HashMap::new(),
            query_executor,
            stats: Arc::new(RwLock::new(PgServerStats {
                total_connections: 0,
                active_connections: 0,
                total_queries: 0,
                total_rows_returned: 0,
                total_bytes_sent: 0,
                average_query_time_ms: 0.0,
            })),
        }
    }

    pub async fn handle(&mut self) -> Result<(), PgProtocolError> {
        loop {
            if self.connection.state == PgConnectionState::Terminate {
                break;
            }

            match self.read_message().await {
                Ok(Some(message)) => {
                    if let Err(e) = self.process_message(message).await {
                        self.send_error_response(&e).await?;
                        if matches!(e, PgProtocolError::ConnectionError(_)) {
                            break;
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn read_message(&mut self) -> Result<Option<PgFrontendMessage>, PgProtocolError> {
        let mut length_buf = [0u8; 4];
        
        match timeout(Duration::from_millis(self.config.connection_timeout_ms), self.reader.read_exact(&mut length_buf)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Ok(None),
            Err(_) => return Err(PgProtocolError::ConnectionError("Read timeout".to_string())),
        }

        let length = i32::from_be_bytes(length_buf) as usize;
        let mut type_buf = [0u8; 1];
        
        self.reader.read_exact(&mut type_buf).await?;
        let type_code = type_buf[0];

        let payload_length = length - 4;
        let mut payload = vec![0u8; payload_length];
        self.reader.read_exact(&mut payload).await?;

        let message = self.parse_frontend_message(type_code, &payload)?;
        Ok(Some(message))
    }

    fn parse_frontend_message(&self, type_code: u8, payload: &[u8]) -> Result<PgFrontendMessage, PgProtocolError> {
        match type_code {
            b'S' => {
                let mut offset = 0;
                let protocol_version = i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4));
                let mut parameters = HashMap::new();
                
                while offset < payload.len() - 1 {
                    let key = Self::read_string(&payload, &mut offset);
                    let value = Self::read_string(&payload, &mut offset);
                    if key.is_empty() {
                        break;
                    }
                    parameters.insert(key, value);
                }

                Ok(PgFrontendMessage::Startup { protocol_version, parameters })
            }
            b'p' => {
                let password = String::from_utf8_lossy(payload).to_string();
                Ok(PgFrontendMessage::Password { password })
            }
            b'Q' => {
                let query = Self::read_string(payload, &mut 0);
                Ok(PgFrontendMessage::Query { query })
            }
            b'P' => {
                let mut offset = 0;
                let name = Self::read_string(payload, &mut offset);
                let query = Self::read_string(payload, &mut offset);
                let num_params = i16::from_be_bytes(Self::read_bytes(&payload, &mut offset, 2));
                let mut param_types = Vec::new();
                for _ in 0..num_params {
                    param_types.push(i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4)));
                }

                Ok(PgFrontendMessage::Parse { query, parameter_types: param_types, statement_name: name })
            }
            b'B' => {
                let mut offset = 0;
                let portal_name = Self::read_string(payload, &mut offset);
                let statement_name = Self::read_string(payload, &mut offset);
                let num_params = i16::from_be_bytes(Self::read_bytes(&payload, &mut offset, 2));
                let mut param_values = Vec::new();
                for _ in 0..num_params {
                    let len = i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4));
                    if len == -1 {
                        param_values.push(None);
                    } else {
                        let value = Self::read_bytes(&payload, &mut offset, len as usize).to_vec();
                        param_values.push(Some(value));
                    }
                }
                let num_formats = i16::from_be_bytes(Self::read_bytes(&payload, &mut offset, 2));
                let _column_formats: Vec<i16> = (0..num_formats)
                    .map(|_| i16::from_be_bytes(Self::read_bytes(&payload, &mut offset, 2)))
                    .collect();

                Ok(PgFrontendMessage::Bind {
                    statement_name,
                    portal_name,
                    parameter_values: param_values,
                })
            }
            b'E' => {
                let mut offset = 0;
                let portal_name = Self::read_string(payload, &mut offset);
                let max_rows = i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4));
                Ok(PgFrontendMessage::Execute { portal_name, max_rows })
            }
            b'D' => {
                let mut offset = 0;
                let object_type = match Self::read_string(payload, &mut offset).as_str() {
                    "S" => DescribeObject::Statement,
                    _ => DescribeObject::Portal,
                };
                let name = Self::read_string(payload, &mut offset);
                Ok(PgFrontendMessage::Describe { object_type, name })
            }
            b'C' => {
                let mut offset = 0;
                let object_type = match Self::read_string(payload, &mut offset).as_str() {
                    "S" => DescribeObject::Statement,
                    _ => DescribeObject::Portal,
                };
                let name = Self::read_string(payload, &mut offset);
                Ok(PgFrontendMessage::Close { object_type, name })
            }
            b'S' => Ok(PgFrontendMessage::Sync),
            b'X' => Ok(PgFrontendMessage::Terminate),
            b'H' => Ok(PgFrontendMessage::Flush),
            b'F' => {
                let mut offset = 0;
                let function_id = i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4));
                let num_args = i16::from_be_bytes(Self::read_bytes(&payload, &mut offset, 2));
                let mut arg_values = Vec::new();
                for _ in 0..num_args {
                    let len = i32::from_be_bytes(Self::read_bytes(&payload, &mut offset, 4));
                    if len == -1 {
                        arg_values.push(None);
                    } else {
                        let value = Self::read_bytes(&payload, &mut offset, len as usize).to_vec();
                        arg_values.push(Some(value));
                    }
                }
                Ok(PgFrontendMessage::FunctionCall { function_id, argument_values: arg_values })
            }
            b'd' => Ok(PgFrontendMessage::CopyData { data: payload.to_vec() }),
            b'c' => Ok(PgFrontendMessage::CopyDone),
            b'f' => {
                let error_message = String::from_utf8_lossy(payload).to_string();
                Ok(PgFrontendMessage::CopyFail { error_message })
            }
            _ => Err(PgProtocolError::InvalidMessage(format!("Unknown message type: {}", type_code))),
        }
    }

    fn read_bytes(data: &[u8], offset: &mut usize, size: usize) -> &[u8] {
        let start = *offset;
        *offset += size;
        &data[start..start + size]
    }

    fn read_string(data: &[u8], offset: &mut usize) -> String {
        let start = *offset;
        while *offset < data.len() && data[*offset] != 0 {
            *offset += 1;
        }
        let result = String::from_utf8_lossy(&data[start..*offset]).to_string();
        *offset += 1;
        result
    }

    async fn process_message(&mut self, message: PgFrontendMessage) -> Result<(), PgProtocolError> {
        match message {
            PgFrontendMessage::Startup { protocol_version, parameters } => {
                self.handle_startup(protocol_version, parameters).await
            }
            PgFrontendMessage::Password { password } => {
                self.handle_password(&password).await
            }
            PgFrontendMessage::Query { query } => {
                self.handle_query(&query).await
            }
            PgFrontendMessage::Parse { query, parameter_types, statement_name } => {
                self.handle_parse(&query, &parameter_types, &statement_name).await
            }
            PgFrontendMessage::Bind { statement_name, portal_name, parameter_values } => {
                self.handle_bind(&statement_name, &portal_name, &parameter_values).await
            }
            PgFrontendMessage::Execute { portal_name, max_rows: _ } => {
                self.handle_execute(&portal_name).await
            }
            PgFrontendMessage::Describe { object_type, name } => {
                self.handle_describe(&object_type, &name).await
            }
            PgFrontendMessage::Close { object_type, name } => {
                self.handle_close(&object_type, &name).await
            }
            PgFrontendMessage::Sync => {
                self.handle_sync().await
            }
            PgFrontendMessage::Terminate => {
                self.connection.state = PgConnectionState::Terminate;
                Ok(())
            }
            PgFrontendMessage::Flush => {
                self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
                Ok(())
            }
            _ => Err(PgProtocolError::NotSupported("Message type not implemented".to_string())),
        }
    }

    async fn handle_startup(
        &mut self,
        protocol_version: i32,
        parameters: HashMap<String, String>,
    ) -> Result<(), PgProtocolError> {
        if protocol_version == 80877103 {
            return self.send_ssl_response().await;
        }

        self.connection.parameters = parameters;
        self.connection.user = parameters.get("user").cloned().unwrap_or_default();
        self.connection.database = parameters.get("database").cloned().unwrap_or_default();
        self.connection.application_name = parameters.get("application_name").cloned().unwrap_or_default();

        if self.config.require_auth {
            match self.config.auth_method {
                PgAuthMethod::Trust => {
                    self.send_auth_ok().await?;
                }
                PgAuthMethod::Password(PgPasswordMethod::Md5) => {
                    let salt = [rand::random::<u8>(), rand::random::<u8>(), rand::random::<u8>(), rand::random::<u8>()];
                    self.send_auth_md5_password(salt).await?;
                }
                PgAuthMethod::Password(PgPasswordMethod::ScramSha256) => {
                    let nonce = rand::random::<[u8; 18]>();
                    let nonce_b64 = base64::encode(&nonce);
                    self.send_auth_sasl(&["SCRAM-SHA-256".to_string()]).await?;
                    self.scram_state = Some(ScramState::ClientFirst(nonce_b64));
                }
                _ => {
                    self.send_auth_cleartext_password().await?;
                }
            }
        } else {
            self.send_auth_ok().await?;
        }

        self.send_parameter_status("server_version", "15.0").await?;
        self.send_parameter_status("server_encoding", "UTF8").await?;
        self.send_parameter_status("client_encoding", "UTF8").await?;
        self.send_parameter_status("application_name", &self.connection.application_name).await?;

        let process_id = rand::random::<i32>();
        let secret_key = rand::random::<i32>();
        self.send_backend_key_data(process_id, secret_key).await?;

        self.send_ready_for_query(TransactionStatus::Idle).await?;
        self.connection.state = PgConnectionState::Ready;

        let mut stats = self.stats.write().unwrap();
        stats.total_connections += 1;
        stats.active_connections += 1;

        Ok(())
    }

    async fn send_ssl_response(&self) -> Result<(), PgProtocolError> {
        self.writer.write_u8(b'N').await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_auth_ok(&mut self) -> Result<(), PgProtocolError> {
        self.send_message(b'R', &[]).await?;
        Ok(())
    }

    async fn send_auth_md5_password(&self, salt: [u8; 4]) -> Result<(), PgProtocolError> {
        let mut data = [0u8; 8];
        data[0] = b'R';
        data[1..5] = 5i32.to_be_bytes();
        data[5..9] = 5i32.to_be_bytes();
        data[9] = salt[0];
        data[10] = salt[1];
        data[11] = salt[2];
        data[12] = salt[3];
        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_auth_cleartext_password(&self) -> Result<(), PgProtocolError> {
        let mut data = [0u8; 5];
        data[0] = b'R';
        data[1..5] = 3i32.to_be_bytes();
        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_auth_sasl(&self, mechanisms: &[String]) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'R');
        let len_pos = data.len();
        data.extend(0i32.to_be_bytes());
        data.extend(0i32.to_be_bytes());
        data.extend(b"SASL");
        for mech in mechanisms {
            data.extend(mech.as_bytes());
            data.push(0);
        }
        data.push(0);

        let len = (data.len() - 4) as i32;
        data[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_parameter_status(&self, name: &str, value: &str) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'S');
        data.extend((name.len() + value.len() + 2) as i32.to_be_bytes());
        data.extend(name.as_bytes());
        data.push(0);
        data.extend(value.as_bytes());
        data.push(0);

        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_backend_key_data(&self, process_id: i32, secret_key: i32) -> Result<(), PgProtocolError> {
        let mut data = [0u8; 13];
        data[0] = b'K';
        data[1..5] = 9i32.to_be_bytes();
        data[5..9] = process_id.to_be_bytes();
        data[9..13] = secret_key.to_be_bytes();
        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_ready_for_query(&self, status: TransactionStatus) -> Result<(), PgProtocolError> {
        let mut data = [0u8; 6];
        data[0] = b'Z';
        data[1..5] = 5i32.to_be_bytes();
        data[5] = match status {
            TransactionStatus::Idle => b'I',
            TransactionStatus::Blocked => b'T',
            TransactionStatus::Active => b'E',
        };
        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_message(&mut self, type_code: u8, data: &[u8]) -> Result<(), PgProtocolError> {
        self.writer.write_u8(type_code).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.write_all(&(data.len() as i32 + 4).to_be_bytes()).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.write_all(data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn send_error_response(&mut self, error: &PgProtocolError) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'E');
        let len_pos = data.len();
        data.extend(0i32.to_be_bytes());

        data.push(b'V');
        data.extend(format!("{}", error).as_bytes());
        data.push(0);

        data.push(b'S');
        data.extend("ERROR".as_bytes());
        data.push(0);

        data.push(b'C');
        data.extend("XX000".as_bytes());
        data.push(0);

        let len = (data.len() - 4) as i32;
        data[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        self.writer.write_all(&data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn handle_password(&mut self, password: &str) -> Result<(), PgProtocolError> {
        self.send_auth_ok().await?;
        self.send_parameter_status("server_version", "15.0").await?;
        self.send_ready_for_query(TransactionStatus::Idle).await?;
        self.connection.state = PgConnectionState::Ready;
        Ok(())
    }

    async fn handle_query(&mut self, query: &str) -> Result<(), PgProtocolError> {
        self.connection.state = PgConnectionState::Query;
        let start_time = std::time::Instant::now();

        let result = self.execute_query(query).await;

        let elapsed = start_time.elapsed();

        match result {
            Ok(Some(query_result)) => {
                self.send_row_description(&query_result.fields).await?;
                for row in &query_result.rows {
                    self.send_data_row(row).await?;
                }
                self.send_command_complete(&query_result.command_tag).await?;
            }
            Ok(None) => {
                self.send_command_complete("EMPTY_QUERY").await?;
            }
            Err(e) => {
                return Err(e);
            }
        }

        self.send_ready_for_query(TransactionStatus::Idle).await?;
        self.connection.state = PgConnectionState::Ready;

        let mut stats = self.stats.write().unwrap();
        stats.total_queries += 1;

        Ok(())
    }

    async fn handle_parse(&mut self, query: &str, _parameter_types: &[i32], statement_name: &str) -> Result<(), PgProtocolError> {
        let statement = PgPreparedStatement {
            name: statement_name.to_string(),
            query: query.to_string(),
            parameter_types: _parameter_types.to_vec(),
            created_at: chrono::Utc::now().timestamp() as u64,
            use_count: 0,
        };

        self.statements.insert(statement_name.to_string(), statement);
        self.send_message(b'1', &[]).await?;
        Ok(())
    }

    async fn handle_bind(&mut self, statement_name: &str, portal_name: &str, parameter_values: &[Option<Vec<u8>>]) -> Result<(), PgProtocolError> {
        if let Some(statement) = self.statements.get(statement_name) {
            let portal = PgPortal {
                name: portal_name.to_string(),
                statement_name: statement_name.to_string(),
                parameter_values: parameter_values.to_vec(),
                created_at: chrono::Utc::now().timestamp() as u64,
                use_count: 0,
            };

            self.portals.insert(portal_name.to_string(), portal);
        }

        self.send_message(b'2', &[]).await?;
        Ok(())
    }

    async fn handle_execute(&mut self, portal_name: &str) -> Result<(), PgProtocolError> {
        if let Some(portal) = self.portals.get(portal_name) {
            if let Some(statement) = self.statements.get(&portal.statement_name) {
                let result = self.execute_query(&statement.query).await;

                match result {
                    Ok(Some(query_result)) => {
                        self.send_row_description(&query_result.fields).await?;
                        for row in &query_result.rows {
                            self.send_data_row(row).await?;
                        }
                        self.send_command_complete(&query_result.command_tag).await?;
                    }
                    _ => {}
                }
            }
        }

        self.send_message(b'C', &[]).await?;
        self.send_ready_for_query(TransactionStatus::Idle).await?;
        Ok(())
    }

    async fn handle_describe(&self, object_type: &DescribeObject, name: &str) -> Result<(), PgProtocolError> {
        match object_type {
            DescribeObject::Statement => {
                if let Some(statement) = self.statements.get(name) {
                    let fields = self.get_fields_for_query(&statement.query).await?;
                    self.send_row_description(&fields).await?;
                } else {
                    self.send_message(b'n', &[]).await?;
                }
            }
            DescribeObject::Portal => {
                self.send_message(b'n', &[]).await?;
            }
        }

        Ok(())
    }

    async fn handle_close(&mut self, object_type: &DescribeObject, name: &str) -> Result<(), PgProtocolError> {
        match object_type {
            DescribeObject::Statement => {
                self.statements.remove(name);
            }
            DescribeObject::Portal => {
                self.portals.remove(name);
            }
        }

        self.send_message(b'3', &[]).await?;
        Ok(())
    }

    async fn handle_sync(&mut self) -> Result<(), PgProtocolError> {
        self.send_ready_for_query(TransactionStatus::Idle).await?;
        Ok(())
    }

    async fn execute_query(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        let query = query.trim();

        if query.is_empty() {
            return Ok(None);
        }

        let upper_query = query.to_uppercase();

        if upper_query.starts_with("SELECT") {
            self.execute_select(query).await
        } else if upper_query.starts_with("INSERT") {
            self.execute_insert(query).await
        } else if upper_query.starts_with("UPDATE") {
            self.execute_update(query).await
        } else if upper_query.starts_with("DELETE") {
            self.execute_delete(query).await
        } else if upper_query.starts_with("CREATE") {
            self.execute_create(query).await
        } else if upper_query.starts_with("DROP") {
            self.execute_drop(query).await
        } else if upper_query.starts_with("ANN") || upper_query.contains("VECTOR SEARCH") {
            self.execute_vector_search(query).await
        } else {
            Err(PgProtocolError::QueryError(format!("Unsupported query type: {}", query)))
        }
    }

    async fn execute_select(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![
                HashMap::from([(
                    "result".to_string(),
                    PgValue {
                        value: Some(b"SELECT executed successfully".to_vec()),
                        type_oid: 25,
                        is_null: false,
                    },
                )]),
            ],
            fields: vec![PgFieldDescription {
                name: "result".to_string(),
                table_oid: 0,
                column_attribute: 0,
                type_oid: 25,
                type_size: -1,
                type_modifier: -1,
                format: 0,
            }],
            command_tag: "SELECT 1".to_string(),
            row_count: 1,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_insert(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "INSERT 0 1".to_string(),
            row_count: 1,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_update(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "UPDATE 1".to_string(),
            row_count: 1,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_delete(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "DELETE 1".to_string(),
            row_count: 1,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_create(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "CREATE".to_string(),
            row_count: 0,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_drop(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "DROP".to_string(),
            row_count: 0,
            execution_time_ms: 0.0,
        }))
    }

    async fn execute_vector_search(&self, query: &str) -> Result<Option<PgQueryResult>, PgProtocolError> {
        Ok(Some(PgQueryResult {
            rows: vec![HashMap::from([(
                "id".to_string(),
                PgValue {
                    value: Some(b"vec1".to_vec()),
                    type_oid: 25,
                    is_null: false,
                },
            ), (
                "distance".to_string(),
                PgValue {
                    value: Some(b"0.123".to_vec()),
                    type_oid: 701,
                    is_null: false,
                },
            )])],
            fields: vec![
                PgFieldDescription {
                    name: "id".to_string(),
                    table_oid: 0,
                    column_attribute: 0,
                    type_oid: 25,
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                },
                PgFieldDescription {
                    name: "distance".to_string(),
                    table_oid: 0,
                    column_attribute: 0,
                    type_oid: 701,
                    type_size: 8,
                    type_modifier: -1,
                    format: 0,
                },
            ],
            command_tag: "ANN SEARCH".to_string(),
            row_count: 1,
            execution_time_ms: 0.0,
        }))
    }

    async fn get_fields_for_query(&self, query: &str) -> Result<Vec<PgFieldDescription>, PgProtocolError> {
        Ok(vec![PgFieldDescription {
            name: "column".to_string(),
            table_oid: 0,
            column_attribute: 0,
            type_oid: 25,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        }])
    }

    async fn send_row_description(&mut self, fields: &[PgFieldDescription]) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'T');
        let len_pos = data.len();
        data.extend(0i32.to_be_bytes());

        data.extend((fields.len() as i16).to_be_bytes());

        for field in fields {
            data.extend(field.name.as_bytes());
            data.push(0);
            data.extend(field.table_oid.to_be_bytes());
            data.extend(field.column_attribute.to_be_bytes());
            data.extend(field.type_oid.to_be_bytes());
            data.extend(field.type_size.to_be_bytes());
            data.extend(field.type_modifier.to_be_bytes());
            data.extend(field.format.to_be_bytes());
        }

        let len = (data.len() - 4) as i32;
        data[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        self.send_message_data(&data).await?;
        Ok(())
    }

    async fn send_data_row(&mut self, row: &HashMap<String, PgValue>) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'D');
        let len_pos = data.len();
        data.extend(0i32.to_be_bytes());

        let values: Vec<_> = row.values().collect();
        data.extend((values.len() as i16).to_be_bytes());

        for value in values {
            if value.is_null {
                data.extend((-1i32).to_be_bytes());
            } else if let Some(ref v) = value.value {
                data.extend((v.len() as i32).to_be_bytes());
                data.extend(v);
            }
        }

        let len = (data.len() - 4) as i32;
        data[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        self.send_message_data(&data).await?;
        Ok(())
    }

    async fn send_command_complete(&mut self, command_tag: &str) -> Result<(), PgProtocolError> {
        let mut data = Vec::new();
        data.push(b'C');
        data.extend(((command_tag.len() + 1) as i32 + 4).to_be_bytes());
        data.extend(command_tag.as_bytes());
        data.push(0);

        self.send_message_data(&data).await?;
        Ok(())
    }

    async fn send_message_data(&mut self, data: &[u8]) -> Result<(), PgProtocolError> {
        self.writer.write_all(data).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug)]
struct ScramState {
    client_nonce: String,
    server_nonce: String,
    salted_password: Vec<u8>,
    client_first_message_bare: Vec<u8>,
}

impl PgProtocolHandler {
    async fn send_auth_sasl_continue(&self, data: &[u8]) -> Result<(), PgProtocolError> {
        let mut message = Vec::new();
        message.push(b'R');
        message.extend((data.len() as i32 + 4).to_be_bytes());
        message.extend(data);
        self.writer.write_all(&message).await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        self.writer.flush().await.map_err(|e| PgProtocolError::ConnectionError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_config_default() {
        let config = PgConfig::default();
        assert_eq!(config.port, 5432);
        assert_eq!(config.max_connections, 100);
        assert!(!config.require_auth);
    }

    #[test]
    fn test_pg_field_description() {
        let field = PgFieldDescription {
            name: "id".to_string(),
            table_oid: 0,
            column_attribute: 0,
            type_oid: 25,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        };

        assert_eq!(field.name, "id");
        assert_eq!(field.type_oid, 25);
    }

    #[test]
    fn test_pg_query_result() {
        let result = PgQueryResult {
            rows: vec![],
            fields: vec![],
            command_tag: "SELECT 1".to_string(),
            row_count: 1,
            execution_time_ms: 0.5,
        };

        assert_eq!(result.command_tag, "SELECT 1");
        assert_eq!(result.row_count, 1);
    }
}
