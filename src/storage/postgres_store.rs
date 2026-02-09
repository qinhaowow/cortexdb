use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tokio_postgres::{Client, Config, Connection, NoTls, Row, Statement, Transaction};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum PostgresStoreError {
    #[error("Database connection failed: {0}")]
    ConnectionError(String),
    #[error("Query execution failed: {0}")]
    QueryError(String),
    #[error("Transaction error: {0}")]
    TransactionError(String),
    #[error("Schema error: {0}")]
    SchemaError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),
    #[error("Not found")]
    NotFound,
    #[error("Pool exhausted")]
    PoolExhausted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreTransaction<'a> {
    client: &'a Client,
    statements: Arc<RwLock<HashMap<String, Statement>>>,
}

impl<'a> StoreTransaction<'a> {
    pub async fn prepare(&self, name: &str, query: &str) -> Result<(), PostgresStoreError> {
        let mut stmts = self.statements.write().await;
        if !stmts.contains_key(name) {
            let stmt = self.client.prepare_typed(query, &[]).await
                .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
            stmts.insert(name.to_string(), stmt);
        }
        Ok(())
    }

    pub async fn execute(&self, name: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) 
        -> Result<u64, PostgresStoreError> {
        let stmts = self.statements.read().await;
        if let Some(stmt) = stmts.get(name) {
            self.client.execute(stmt, params).await
                .map_err(|e| PostgresStoreError::QueryError(e.to_string()))
        } else {
            Err(PostgresStoreError::QueryError(format!("Statement '{}' not prepared", name)))
        }
    }

    pub async fn query_one<T: for<'r> Deserialize<'r>>(&self, name: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)])
        -> Result<Option<T>, PostgresStoreError> {
        let stmts = self.statements.read().await;
        if let Some(stmt) = stmts.get(name) {
            let rows = self.client.query(stmt, params).await
                .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
            
            if let Some(row) = rows.first() {
                let value = deserialize_row::<T>(row)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            Err(PostgresStoreError::QueryError(format!("Statement '{}' not prepared", name)))
        }
    }

    pub async fn query<T: for<'r> Deserialize<'r>>(&self, name: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)])
        -> Result<Vec<T>, PostgresStoreError> {
        let stmts = self.statements.read().await;
        if let Some(stmt) = stmts.get(name) {
            let rows = self.client.query(stmt, params).await
                .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
            
            let mut results = Vec::with_capacity(rows.len());
            for row in rows {
                results.push(deserialize_row::<T>(&row)?);
            }
            Ok(results)
        } else {
            Err(PostgresStoreError::QueryError(format!("Statement '{}' not prepared", name)))
        }
    }
}

fn deserialize_row<'r, T: for<'de> Deserialize<'de>>(row: &'r Row) -> Result<T, PostgresStoreError> {
    let value: serde_json::Value = row.try_get(0)
        .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))?;
    
    serde_json::from_value(value)
        .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout: Duration,
    pub statement_timeout: Duration,
    pub pool_timeout: Duration,
    pub enable_ssl: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "cortexdb".to_string(),
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            max_connections: 20,
            min_connections: 5,
            connection_timeout: Duration::from_secs(30),
            statement_timeout: Duration::from_secs(30),
            pool_timeout: Duration::from_secs(10),
            enable_ssl: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreStats {
    pub total_connections: AtomicUsize,
    pub idle_connections: AtomicUsize,
    pub active_connections: AtomicUsize,
    pub queries_executed: AtomicU64,
    pub queries_failed: AtomicU64,
    pub avg_query_time_ms: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl Default for StoreStats {
    fn default() -> Self {
        Self {
            total_connections: AtomicUsize::new(0),
            idle_connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            queries_executed: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            avg_query_time_ms: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }
}

struct ConnectionPool {
    client: Client,
    in_use: Mutex<()>,
}

pub struct PostgresStore {
    pool: RwLock<Vec<ConnectionPool>>,
    config: StoreConfig,
    stats: Arc<StoreStats>,
    prepared_statements: RwLock<HashMap<String, Statement>>,
}

impl PostgresStore {
    pub async fn new(config: Option<StoreConfig>) -> Result<Self, PostgresStoreError> {
        let config = config.unwrap_or_default();
        let mut pool = RwLock::new(Vec::with_capacity(config.max_connections as usize));
        
        for _ in 0..config.min_connections {
            let client = create_connection(&config).await?;
            pool.write().await.push(ConnectionPool {
                client,
                in_use: Mutex::new(()),
            });
        }
        
        let stats = Arc::new(StoreStats::default());
        let prepared_statements = RwLock::new(HashMap::new());
        
        let store = Self {
            pool,
            config,
            stats,
            prepared_statements,
        };
        
        info!("PostgresStore initialized with {} min connections", store.config.min_connections);
        Ok(store)
    }

    async fn create_connection(&self) -> Result<Client, PostgresStoreError> {
        create_connection(&self.config).await
    }

    pub async fn acquire(&self) -> Result<PooledConnection<'_>, PostgresStoreError> {
        let pool = self.pool.read().await;
        
        for (i, conn) in pool.iter().enumerate() {
            let guard = conn.in_use.try_lock();
            if guard.is_some() {
                self.stats.active_connections.fetch_add(1, Ordering::SeqCst);
                self.stats.idle_connections.fetch_sub(1, Ordering::SeqCst);
                return Ok(PooledConnection {
                    store: self,
                    index: i,
                    _guard: guard.unwrap(),
                });
            }
        }
        
        if pool.len() < self.config.max_connections as usize {
            drop(pool);
            let mut pool = self.pool.write().await;
            
            if pool.len() < self.config.max_connections as usize {
                let client = self.create_connection().await?;
                pool.push(ConnectionPool {
                    client,
                    in_use: Mutex::new(()),
                });
                self.stats.total_connections.fetch_add(1, Ordering::SeqCst);
                
                let guard = pool.last().unwrap().in_use.lock().await;
                self.stats.active_connections.fetch_add(1, Ordering::SeqCst);
                return Ok(PooledConnection {
                    store: self,
                    index: pool.len() - 1,
                    _guard: guard,
                });
            }
        }
        
        Err(PostgresStoreError::PoolExhausted)
    }

    pub async fn with_transaction<F, T, R>(&self, f: F) -> Result<R, PostgresStoreError>
    where
        F: FnOnce(StoreTransaction<'_>) -> T,
        T: std::future::Future<Output = Result<R, PostgresStoreError>>,
    {
        let pooled = self.acquire().await?;
        let client = &pooled.client;
        
        let tx = client.transaction().await
            .map_err(|e| PostgresStoreError::TransactionError(e.to_string()))?;
        
        let tx_wrapper = StoreTransaction {
            client: &tx,
            statements: Arc::new(RwLock::new(HashMap::new())),
        };
        
        let result = f(tx_wrapper).await;
        
        match result {
            Ok(r) => {
                tx.commit().await
                    .map_err(|e| PostgresStoreError::TransactionError(e.to_string()))?;
                Ok(r)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    pub async fn execute(&self, query: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) 
        -> Result<u64, PostgresStoreError> {
        let start = Instant::now();
        let pooled = self.acquire().await?;
        let client = &pooled.client;
        
        let result = client.execute(query, params).await
            .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
        
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_query_stats(elapsed, true);
        
        let bytes_written = query.len() as u64;
        self.stats.bytes_written.fetch_add(bytes_written, Ordering::SeqCst);
        
        Ok(result)
    }

    pub async fn query_one<T: for<'r> Deserialize<'r>>(&self, query: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)])
        -> Result<Option<T>, PostgresStoreError> {
        let start = Instant::now();
        let pooled = self.acquire().await?;
        let client = &pooled.client;
        
        let rows = client.query(query, params).await
            .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
        
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_query_stats(elapsed, rows.is_empty());
        
        if let Some(row) = rows.first() {
            let bytes_read = format!("{:?}", row).len() as u64;
            self.stats.bytes_read.fetch_add(bytes_read, Ordering::SeqCst);
            
            let value: serde_json::Value = row.try_get(0)
                .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))?;
            
            serde_json::from_value(value)
                .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))
                .map(Some)
        } else {
            Ok(None)
        }
    }

    pub async fn query<T: for<'r> Deserialize<'r>>(&self, query: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)])
        -> Result<Vec<T>, PostgresStoreError> {
        let start = Instant::now();
        let pooled = self.acquire().await?;
        let client = &pooled.client;
        
        let rows = client.query(query, params).await
            .map_err(|e| PostgresStoreError::QueryError(e.to_string()))?;
        
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_query_stats(elapsed, false);
        
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes_read = format!("{:?}", row).len() as u64;
            self.stats.bytes_read.fetch_add(bytes_read, Ordering::SeqCst);
            
            let value: serde_json::Value = row.try_get(0)
                .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))?;
            
            results.push(serde_json::from_value(value)
                .map_err(|e| PostgresStoreError::DeserializationError(e.to_string()))?);
        }
        
        Ok(results)
    }

    fn update_query_stats(&self, elapsed_ms: u64, is_empty: bool) {
        self.stats.queries_executed.fetch_add(1, Ordering::SeqCst);
        
        let avg = self.stats.avg_query_time_ms.load(Ordering::SeqCst);
        let new_avg = if avg == 0 { elapsed_ms } else { (avg + elapsed_ms) / 2 };
        self.stats.avg_query_time_ms.store(new_avg, Ordering::SeqCst);
    }

    pub async fn initialize_schema(&self) -> Result<(), PostgresStoreError> {
        let schema = r#"
            CREATE TABLE IF NOT EXISTS cortex_entities (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entity_type VARCHAR(255) NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP WITH TIME ZONE,
                metadata JSONB DEFAULT '{}',
                version INTEGER DEFAULT 1
            );
            
            CREATE INDEX IF NOT EXISTS idx_cortex_entities_type ON cortex_entities(entity_type);
            CREATE INDEX IF NOT EXISTS idx_cortex_entities_created ON cortex_entities(created_at);
            CREATE INDEX IF NOT EXISTS idx_cortex_entities_expires ON cortex_entities(expires_at);
            CREATE INDEX IF NOT EXISTS idx_cortex_entities_data ON cortex_entities USING GIN(data);
            
            CREATE TABLE IF NOT EXISTS cortex_relationships (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_id UUID NOT NULL REFERENCES cortex_entities(id),
                target_id UUID NOT NULL REFERENCES cortex_entities(id),
                relationship_type VARCHAR(255) NOT NULL,
                properties JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB DEFAULT '{}'
            );
            
            CREATE INDEX IF NOT EXISTS idx_cortex_relationships_source ON cortex_relationships(source_id);
            CREATE INDEX IF NOT EXISTS idx_cortex_relationships_target ON cortex_relationships(target_id);
            CREATE INDEX IF NOT EXISTS idx_cortex_relationships_type ON cortex_relationships(relationship_type);
            
            CREATE TABLE IF NOT EXISTS cortex_indices (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL UNIQUE,
                entity_type VARCHAR(255) NOT NULL,
                field_paths JSONB NOT NULL,
                index_type VARCHAR(50) NOT NULL DEFAULT 'btree',
                options JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        "#;
        
        for statement in schema.split(';').filter(|s| !s.trim().is_empty()) {
            self.execute(statement.trim(), &[]).await?;
        }
        
        info!("PostgreSQL schema initialized successfully");
        Ok(())
    }

    pub async fn health_check(&self) -> Result<(), PostgresStoreError> {
        let pooled = self.acquire().await?;
        let client = &pooled.client;
        
        client.simple_query("SELECT 1").await
            .map_err(|e| PostgresStoreError::ConnectionError(e.to_string()))?;
        
        Ok(())
    }

    pub async fn get_stats(&self) -> HashMap<String, String> {
        let pool = self.pool.read().await;
        let mut stats = HashMap::new();
        
        stats.insert("total_connections".to_string(), pool.len().to_string());
        stats.insert("active_connections".to_string(), self.stats.active_connections.load(Ordering::SeqCst).to_string());
        stats.insert("queries_executed".to_string(), self.stats.queries_executed.load(Ordering::SeqCst).to_string());
        stats.insert("queries_failed".to_string(), self.stats.queries_failed.load(Ordering::SeqCst).to_string());
        stats.insert("avg_query_time_ms".to_string(), self.stats.avg_query_time_ms.load(Ordering::SeqCst).to_string());
        stats.insert("bytes_read".to_string(), self.stats.bytes_read.load(Ordering::SeqCst).to_string());
        stats.insert("bytes_written".to_string(), self.stats.bytes_written.load(Ordering::SeqCst).to_string());
        
        stats
    }
}

struct PooledConnection<'a> {
    store: &'a PostgresStore,
    index: usize,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a> Drop for PooledConnection<'a> {
    fn drop(&mut self) {
        self.store.stats.active_connections.fetch_sub(1, Ordering::SeqCst);
        self.store.stats.idle_connections.fetch_add(1, Ordering::SeqCst);
    }
}

impl<'a> std::ops::Deref for PooledConnection<'a> {
    type Target = Client;
    
    fn deref(&self) -> &Self::Target {
        &self.store.pool.read().await[self.index].client
    }
}

async fn create_connection(config: &StoreConfig) -> Result<Client, PostgresStoreError> {
    let mut pg_config = Config::new();
    pg_config.host(&config.host);
    pg_config.port(config.port);
    pg_config.user(&config.username, Some(&config.password));
    pg_config.dbname(&config.database);
    pg_config.connect_timeout(config.connection_timeout);
    pg_config.statement_timeout(config.statement_timeout);
    
    if config.enable_ssl {
        pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
    }
    
    let (client, connection) = pg_config.connect(NoTls).await
        .map_err(|e| PostgresStoreError::ConnectionError(e.to_string()))?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });
    
    info!("Created PostgreSQL connection to {}:{}/{}", config.host, config.port, config.database);
    Ok(client)
}

impl Drop for PostgresStore {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "PostgresStore stats - Queries: {}, Failed: {}, Avg time: {}ms",
            stats.queries_executed.load(Ordering::SeqCst),
            stats.queries_failed.load(Ordering::SeqCst),
            stats.avg_query_time_ms.load(Ordering::SeqCst)
        );
    }
}
