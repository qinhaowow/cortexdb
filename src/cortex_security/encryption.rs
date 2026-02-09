//! 完整的加密模块实现 for CortexDB
//! 支持静态加密、TLS、密钥管理、字段级加密

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::path::PathBuf;
use ring::{
    aead::{self, Aad, Nonce, UnboundKey, BoundKey, SealingKey, OpeningKey, AES_256_GCM, CHACHA20_POLY1305},
    digest::{self, Digest, SHA256, SHA512},
    hmac,
    kdf::{self, from_seed, Scrypt},
    rand::{SecureRandom, SystemRandom},
    signature::{self, KeyPair, EcdsaKeyPair, ECDSA_P256_SHA256_ASN1_SIGNING},
};
use x509_cert::{Certificate, PkiPath};
use asn1_rs::{FromDer, ToDer};
use pem::{encode, encode_config, Pem};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rustls::{ServerConfig, ClientConfig, RootCertStore, CertificateRequest, Accepts};
use rustls_pemfile::{certs, rsa_private_keys};
use tokio::net::{TcpStream, TcpListener};
use tokio_rustls::{TlsAcceptor, TlsConnector, webpki};
use zeroize::Zeroize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionType {
    AtRest,
    InTransit,
    FieldLevel,
    ColumnLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyType {
    MasterKey,
    DataKey,
    FieldKey,
    CacheKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyStatus {
    Active,
    Rotating,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKey {
    pub key_id: String,
    pub key_type: KeyType,
    pub algorithm: EncryptionAlgorithm,
    pub key_data: Vec<u8>,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub status: KeyStatus,
    pub version: u32,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    pub key_id: String,
    pub key_type: KeyType,
    pub algorithm: EncryptionAlgorithm,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub status: KeyStatus,
    pub version: u32,
    pub usage_count: u64,
    pub last_used_at: Option<u64>,
    pub rotation_count: u32,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRotationConfig {
    pub enabled: bool,
    pub rotation_period_days: u32,
    pub max_key_versions: u32,
    pub notify_before_days: u32,
    pub auto_rotate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldEncryptionConfig {
    pub field_name: String,
    pub algorithm: EncryptionAlgorithm,
    pub key_id: String,
    pub iv_mode: IvMode,
    pub aad_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IvMode {
    Random,
    Deterministic,
    Counter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedField {
    pub field_name: String,
    pub key_id: String,
    pub iv: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub tag: Vec<u8>,
    pub algorithm: EncryptionAlgorithm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsCertificate {
    pub cert_id: String,
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
    pub public_key: Vec<u8>,
    pub subject: String,
    pub issuer: String,
    pub serial_number: String,
    pub not_before: u64,
    pub not_after: u64,
    pub san_dns_names: Vec<String>,
    pub san_ip_addresses: Vec<String>,
    pub is_ca: bool,
    pub key_usage: Vec<String>,
    pub extended_key_usage: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub min_version: TlsVersion,
    pub max_version: TlsVersion,
    pub cipher_suites: Vec<TlsCipherSuite>,
    pub cert_chain: Vec<String>,
    pub alpn_protocols: Vec<String>,
    pub ocsp_stapling: bool,
    pub session_tickets: bool,
    pub session_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TlsVersion {
    Tls10,
    Tls11,
    Tls12,
    Tls13,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TlsCipherSuite {
    AeecdheEcdsaWithAes256GcmSha384,
    AeecdheRsaWithAes256GcmSha384,
    AeecdheEcsaWithAes128GcmSha256,
    AeecdheRsaWithAes128GcmSha256,
    DheRsaWithAes256GcmSha384,
    DheRsaWithAes128GcmSha256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateRequestConfig {
    pub common_name: String,
    pub organization: String,
    pub organizational_unit: String,
    pub locality: String,
    pub province: String,
    pub country: String,
    pub san_dns_names: Vec<String>,
    pub san_ip_addresses: Vec<String>,
    pub key_usage: Vec<String>,
    pub extended_key_usage: Vec<String>,
    pub is_ca: bool,
    pub ca_cert_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateInfo {
    pub cert_id: String,
    pub subject: X509Name,
    pub issuer: X509Name,
    pub serial_number: String,
    pub not_before: u64,
    pub not_after: u64,
    pub fingerprint_sha256: String,
    pub fingerprint_sha1: String,
    pub signature_algorithm: String,
    pub public_key_algorithm: String,
    public_key_size: u32,
    pub is_valid: bool,
    pub is_revoked: bool,
    pub revocation_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct X509Name {
    pub common_name: String,
    pub organization: Option<String>,
    pub organizational_unit: Option<String>,
    pub locality: Option<String>,
    pub province: Option<String>,
    pub country: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionAudit {
    pub audit_id: String,
    pub operation: EncryptionOperation,
    pub key_id: String,
    pub key_type: KeyType,
    pub user_id: Option<String>,
    pub resource_id: Option<String>,
    pub timestamp: u64,
    pub success: bool,
    pub error_message: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionOperation {
    Encrypt,
    Decrypt,
    KeyGenerate,
    KeyRotate,
    KeyExport,
    KeyImport,
    KeyDestroy,
    Sign,
    Verify,
}

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Key expired at {0}")]
    KeyExpired(u64),
    #[error("Key revoked")]
    KeyRevoked,
    #[error("Invalid key version: {0}")]
    InvalidKeyVersion(u32),
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("Authentication tag mismatch")]
    TagMismatch,
    #[error("Invalid IV")]
    InvalidIv,
    #[error("Certificate expired")]
    CertificateExpired,
    #[error("Certificate not yet valid")]
    CertificateNotYetValid,
    #[error("Certificate verification failed: {0}")]
    CertificateVerifyFailed(String),
    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),
    #[error("Key rotation in progress")]
    KeyRotationInProgress,
    #[error("Insufficient permissions")]
    InsufficientPermissions,
    #[error("Hardware security module error: {0}")]
    HsmError(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

#[derive(Clone)]
pub struct EncryptionManager {
    keys: Arc<RwLock<HashMap<String, EncryptionKey>>>,
    key_metadata: Arc<RwLock<HashMap<String, KeyMetadata>>>,
    key_versions: Arc<RwLock<HashMap<String, Vec<EncryptionKey>>>>,
    field_configs: Arc<RwLock<HashMap<String, FieldEncryptionConfig>>>,
    master_key: Arc<RwLock<Option<Vec<u8>>>>,
    rng: SystemRandom,
    config: Arc<RwLock<EncryptionConfig>>,
    rotation_config: Arc<RwLock<KeyRotationConfig>>,
    audit_log: Arc<RwLock<Vec<EncryptionAudit>>>,
    hsm_client: Arc<RwLock<Option<HsmClient>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub default_algorithm: EncryptionAlgorithm,
    pub key_size: usize,
    pub iv_size: usize,
    pub tag_size: usize,
    pub enable_key_rotation: bool,
    pub rotation_period_days: u32,
    pub enable_audit_log: bool,
    pub enable_hsm: bool,
    pub hsm_config: Option<HsmConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HsmConfig {
    pub provider: HsmProvider,
    pub endpoint: String,
    pub api_key: String,
    pub key_slot: usize,
    pub require_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HsmProvider {
    CloudKms,
    AzureKeyVault,
    AwsKms,
    SoftHsm,
}

#[derive(Clone)]
pub struct HsmClient {
    provider: HsmProvider,
    endpoint: String,
    api_key: String,
    key_slot: usize,
}

#[derive(Clone)]
pub struct KeyDerivationFunction {
    purpose: String,
    context: Vec<u8>,
    algorithm: KdfAlgorithm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KdfAlgorithm {
    Pbkdf2 {
        iterations: u32,
        salt_size: usize,
        algorithm: digest::Algorithm,
    },
    Scrypt {
        n: u64,
        r: u32,
        p: u32,
    },
    Hkdf {
        hash: digest::Algorithm,
        salt: Vec<u8>,
        info: Vec<u8>,
    },
}

#[derive(Clone)]
pub struct TlsManager {
    tls_config: Arc<RwLock<TlsConfig>>,
    certificates: Arc<RwLock<HashMap<String, TlsCertificate>>>,
    ca_certificates: Arc<RwLock<Vec<Certificate>>>,
    private_keys: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    ocsp_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    session_cache: Arc<Mutex<rustls::SessionCache>>,
}

impl EncryptionManager {
    pub async fn new() -> Self {
        let config = EncryptionConfig {
            default_algorithm: EncryptionAlgorithm::AES256GCM,
            key_size: 32,
            iv_size: 12,
            tag_size: 16,
            enable_key_rotation: true,
            rotation_period_days: 90,
            enable_audit_log: true,
            enable_hsm: false,
            hsm_config: None,
        };

        let rotation_config = KeyRotationConfig {
            enabled: true,
            rotation_period_days: 90,
            max_key_versions: 3,
            notify_before_days: 7,
            auto_rotate: true,
        };

        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            key_metadata: Arc::new(RwLock::new(HashMap::new())),
            key_versions: Arc::new(RwLock::new(HashMap::new())),
            field_configs: Arc::new(RwLock::new(HashMap::new())),
            master_key: Arc::new(RwLock::new(None)),
            rng: SystemRandom::new(),
            config: Arc::new(RwLock::new(config)),
            rotation_config: Arc::new(RwLock::new(rotation_config)),
            audit_log: Arc::new(RwLock::new(Vec::new())),
            hsm_client: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn initialize_with_master_key(&self, key: &[u8]) -> Result<(), EncryptionError> {
        let mut master_key = self.master_key.write().await;
        *master_key = Some(key.to_vec());
        Ok(())
    }

    pub async fn generate_master_key(&self) -> Result<Vec<u8>, EncryptionError> {
        let mut key = vec![0u8; 32];
        self.rng.fill(&mut key).map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;
        Ok(key)
    }

    pub async fn create_key(
        &self,
        key_type: KeyType,
        algorithm: EncryptionAlgorithm,
        purpose: &str,
        expiry_days: Option<u32>,
    ) -> Result<EncryptionKey, EncryptionError> {
        let config = self.config.read().await;
        let key_size = match algorithm {
            EncryptionAlgorithm::AES256GCM | EncryptionAlgorithm::AES128GCM => 32,
            EncryptionAlgorithm::ChaCha20Poly1305 => 32,
            EncryptionAlgorithm::AES256CBC | EncryptionAlgorithm::AES128CBC => 32,
        };
        drop(config);

        let mut key_data = vec![0u8; key_size];
        self.rng.fill(&mut key_data).map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

        let key_id = self.generate_key_id();
        let now = Self::timestamp();
        let expires_at = expiry_days.map(|d| now + d as u64 * 86400);

        let encryption_key = EncryptionKey {
            key_id: key_id.clone(),
            key_type,
            algorithm: algorithm.clone(),
            key_data,
            created_at: now,
            expires_at,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::from([
                ("purpose".to_string(), purpose.to_string()),
                ("created_by".to_string(), "system".to_string()),
            ]),
        };

        let mut keys = self.keys.write().await;
        keys.insert(key_id.clone(), encryption_key.clone());

        let mut key_versions = self.key_versions.write().await;
        key_versions.insert(key_id.clone(), vec![encryption_key.clone()]);

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_type,
            algorithm: algorithm.clone(),
            created_at: now,
            expires_at,
            status: KeyStatus::Active,
            version: 1,
            usage_count: 0,
            last_used_at: None,
            rotation_count: 0,
            metadata: HashMap::from([
                ("purpose".to_string(), purpose.to_string()),
            ]),
        };

        let mut metadata_map = self.key_metadata.write().await;
        metadata_map.insert(key_id.clone(), metadata);

        self.log_audit(EncryptionAudit {
            audit_id: uuid::Uuid::new_v4().to_string(),
            operation: EncryptionOperation::KeyGenerate,
            key_id: key_id.clone(),
            key_type,
            user_id: None,
            resource_id: None,
            timestamp: now,
            success: true,
            error_message: None,
            metadata: HashMap::new(),
        }).await;

        Ok(encryption_key)
    }

    pub async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey, EncryptionError> {
        let mut keys = self.keys.write().await;
        let old_key = keys.get(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?
            .clone();

        if old_key.status == KeyStatus::Rotating {
            return Err(EncryptionError::KeyRotationInProgress);
        }

        let mut old_key_mut = keys.get_mut(key_id).unwrap();
        old_key_mut.status = KeyStatus::Rotating;
        drop(old_key_mut);

        let rotation_config = self.rotation_config.read().await;
        let max_versions = rotation_config.max_key_versions;
        drop(rotation_config);

        let mut key_versions = self.key_versions.write().await;
        let versions = key_versions.get(key_id).cloned().unwrap_or_default();

        let mut new_key_data = vec![0u8; old_key.key_data.len()];
        self.rng.fill(&mut new_key_data)
            .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

        let new_key = EncryptionKey {
            key_id: key_id.to_string(),
            key_type: old_key.key_type,
            algorithm: old_key.algorithm.clone(),
            key_data: new_key_data,
            created_at: Self::timestamp(),
            expires_at: old_key.expires_at,
            status: KeyStatus::Active,
            version: old_key.version + 1,
            metadata: old_key.metadata.clone(),
        };

        keys.insert(key_id.to_string(), new_key.clone());
        versions.push(new_key.clone());

        if versions.len() > max_versions as usize {
            versions.remove(0);
        }

        key_versions.insert(key_id.to_string(), versions);

        let mut metadata = self.key_metadata.write().await;
        if let Some(meta) = metadata.get_mut(key_id) {
            meta.version = new_key.version;
            meta.rotation_count += 1;
            meta.last_used_at = Some(Self::timestamp());
        }

        let old_key_mut_final = keys.get_mut(key_id).unwrap();
        old_key_mut_final.status = KeyStatus::Active;
        drop(old_key_mut_final);

        self.log_audit(EncryptionAudit {
            audit_id: uuid::Uuid::new_v4().to_string(),
            operation: EncryptionOperation::KeyRotate,
            key_id: key_id.to_string(),
            key_type: old_key.key_type,
            user_id: None,
            resource_id: None,
            timestamp: Self::timestamp(),
            success: true,
            error_message: None,
            metadata: HashMap::from([
                ("old_version".to_string(), old_key.version.to_string()),
                ("new_version".to_string(), new_key.version.to_string()),
            ]),
        }).await;

        Ok(new_key)
    }

    pub async fn encrypt(
        &self,
        key_id: &str,
        plaintext: &[u8],
        associated_data: Option<&[u8]>,
    ) -> Result<EncryptedData, EncryptionError> {
        let keys = self.keys.read().await;
        let key = keys.get(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?
            .clone();
        drop(keys);

        self.validate_key(&key)?;

        let config = self.config.read().await;
        let iv_size = config.iv_size;
        drop(config);

        let mut iv = vec![0u8; iv_size];
        self.rng.fill(&mut iv).map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

        let ciphertext = self.encrypt_with_key(&key, &iv, plaintext, associated_data)?;

        let mut metadata = self.key_metadata.write().await;
        if let Some(meta) = metadata.get_mut(key_id) {
            meta.usage_count += 1;
            meta.last_used_at = Some(Self::timestamp());
        }

        self.log_audit(EncryptionAudit {
            audit_id: uuid::Uuid::new_v4().to_string(),
            operation: EncryptionOperation::Encrypt,
            key_id: key_id.to_string(),
            key_type: key.key_type,
            user_id: None,
            resource_id: None,
            timestamp: Self::timestamp(),
            success: true,
            error_message: None,
            metadata: HashMap::from([
                ("data_size".to_string(), plaintext.len().to_string()),
            ]),
        }).await;

        Ok(EncryptedData {
            algorithm: key.algorithm.clone(),
            nonce: iv,
            ciphertext: ciphertext.0,
            tag: ciphertext.1,
        })
    }

    pub async fn decrypt(
        &self,
        key_id: &str,
        encrypted_data: &EncryptedData,
        associated_data: Option<&[u8]>,
    ) -> Result<Vec<u8>, EncryptionError> {
        let keys = self.keys.read().await;
        let key = keys.get(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?
            .clone();
        drop(keys);

        self.validate_key(&key)?;

        let plaintext = self.decrypt_with_key(&key, &encrypted_data.nonce, &encrypted_data.ciphertext, &encrypted_data.tag, associated_data)?;

        let mut metadata = self.key_metadata.write().await;
        if let Some(meta) = metadata.get_mut(key_id) {
            meta.usage_count += 1;
            meta.last_used_at = Some(Self::timestamp());
        }

        self.log_audit(EncryptionAudit {
            audit_id: uuid::Uuid::new_v4().to_string(),
            operation: EncryptionOperation::Decrypt,
            key_id: key_id.to_string(),
            key_type: key.key_type,
            user_id: None,
            resource_id: None,
            timestamp: Self::timestamp(),
            success: true,
            error_message: None,
            metadata: HashMap::from([
                ("data_size".to_string(), encrypted_data.ciphertext.len().to_string()),
            ]),
        }).await;

        Ok(plaintext)
    }

    pub async fn encrypt_field(
        &self,
        table_name: &str,
        field: &str,
        plaintext: &[u8],
    ) -> Result<EncryptedField, EncryptionError> {
        let field_configs = self.field_configs.read().await;
        let config = field_configs.get(&format!("{}.{}", table_name, field))
            .or_else(|| field_configs.get(field))
            .ok_or_else(|| EncryptionError::KeyNotFound(format!("{}.{}", table_name, field)))?
            .clone();
        drop(field_configs);

        let keys = self.keys.read().await;
        let key = keys.get(&config.key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(config.key_id.clone()))?
            .clone();
        drop(keys);

        self.validate_key(&key)?;

        let iv = match config.iv_mode {
            IvMode::Random => {
                let mut iv = vec![0u8; 12];
                self.rng.fill(&mut iv).map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;
                iv
            }
            IvMode::Deterministic => {
                let context = format!("{}:{}", table_name, field);
                self.generate_deterministic_iv(&context, plaintext)
            }
            IvMode::Counter => {
                self.generate_counter_iv(&config.key_id)?
            }
        };

        let aad = if !config.aad_fields.is_empty() {
            let aad_data = config.aad_fields.join(":");
            Some(aad_data.as_bytes())
        } else {
            Some(format!("{}:{}", table_name, field).as_bytes())
        };

        let (ciphertext, tag) = self.encrypt_with_key(&key, &iv, plaintext, aad)?;

        Ok(EncryptedField {
            field_name: field.to_string(),
            key_id: config.key_id.clone(),
            iv,
            ciphertext,
            tag,
            algorithm: config.algorithm.clone(),
        })
    }

    pub async fn decrypt_field(
        &self,
        table_name: &str,
        encrypted_field: &EncryptedField,
    ) -> Result<Vec<u8>, EncryptionError> {
        let keys = self.keys.read().await;
        let key = keys.get(&encrypted_field.key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(encrypted_field.key_id.clone()))?
            .clone();
        drop(keys);

        self.validate_key(&key)?;

        let aad = Some(format!("{}:{}", table_name, encrypted_field.field_name).as_bytes());

        self.decrypt_with_key(&key, &encrypted_field.iv, &encrypted_field.ciphertext, &encrypted_field.tag, aad)
    }

    pub async fn re_encrypt(
        &self,
        from_key_id: &str,
        to_key_id: &str,
        encrypted_data: &EncryptedData,
        associated_data: Option<&[u8]>,
    ) -> Result<EncryptedData, EncryptionError> {
        let plaintext = self.decrypt(from_key_id, encrypted_data, associated_data).await?;
        self.encrypt(to_key_id, &plaintext, associated_data).await
    }

    pub async fn derive_key(
        &self,
        master_key_id: &str,
        purpose: &str,
        context: &[u8],
        key_length: usize,
    ) -> Result<Vec<u8>, EncryptionError> {
        let keys = self.keys.read().await;
        let master_key = keys.get(master_key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(master_key_id.to_string()))?;
        drop(keys);

        self.validate_key(master_key)?;

        let salt = {
            let mut salt_data = vec![0u8; 32];
            self.rng.fill(&mut salt_data).map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;
            salt_data
        };

        let info = format!("{}:{}", purpose, Self::timestamp());

        let mut derived_key = vec![0u8; key_length];
        ring::hkdf::derive_key(
            ring::hkdf::HKDF_SHA256,
            &master_key.key_data,
            info.as_bytes(),
            &mut derived_key,
        );

        Ok(derived_key)
    }

    pub async fn wrap_key(
        &self,
        wrapping_key_id: &str,
        key_to_wrap: &EncryptionKey,
    ) -> Result<WrappedKey, EncryptionError> {
        let keys = self.keys.read().await;
        let wrapping_key = keys.get(wrapping_key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(wrapping_key_id.to_string()))?;
        drop(keys);

        let wrapped_data = self.encrypt(wrapping_key_id, &key_to_wrap.key_data, None).await?;

        Ok(WrappedKey {
            wrapped_key_data: wrapped_data.ciphertext,
            wrapping_key_id: wrapping_key_id.to_string(),
            algorithm: wrapping_key.algorithm.clone(),
            original_key_id: key_to_wrap.key_id.clone(),
            original_algorithm: key_to_wrap.algorithm.clone(),
            nonce: wrapped_data.nonce,
            tag: wrapped_data.tag,
        })
    }

    pub async fn unwrap_key(
        &self,
        wrapped_key: &WrappedKey,
    ) -> Result<EncryptionKey, EncryptionError> {
        let encrypted_data = EncryptedData {
            algorithm: wrapped_key.algorithm.clone(),
            nonce: wrapped_key.nonce.clone(),
            ciphertext: wrapped_key.wrapped_key_data.clone(),
            tag: wrapped_key.tag.clone(),
        };

        let key_data = self.decrypt(&wrapped_key.wrapping_key_id, &encrypted_data, None).await?;

        Ok(EncryptionKey {
            key_id: wrapped_key.original_key_id.clone(),
            key_type: KeyType::DataKey,
            algorithm: wrapped_key.original_algorithm.clone(),
            key_data,
            created_at: Self::timestamp(),
            expires_at: None,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::new(),
        })
    }

    pub async fn sign(
        &self,
        key_id: &str,
        data: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        let keys = self.keys.read().await;
        let key = keys.get(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?;
        drop(keys);

        let key_pair = EcdsaKeyPair::from_pkcs8(
            &ECDSA_P256_SHA256_ASN1_SIGNING,
            &key.key_data,
        ).map_err(|e| EncryptionError::SigningFailed(e.to_string()))?;

        let signature = key_pair.sign(data, &self.rng)
            .map_err(|e| EncryptionError::SigningFailed(e.to_string()))?;

        Ok(signature.as_ref().to_vec())
    }

    pub async fn verify_signature(
        &self,
        key_id: &str,
        data: &[u8],
        signature: &[u8],
    ) -> Result<bool, EncryptionError> {
        let keys = self.keys.read().await;
        let key = keys.get(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?;
        drop(keys);

        let public_key = signature::UnparsedPublicKey::new(
            &ECDSA_P256_SHA256_ASN1_SIGNING,
            key.key_data.clone(),
        );

        public_key.verify(data, signature)
            .map_err(|e| EncryptionError::SignatureVerificationFailed(e.to_string()))
    }

    fn encrypt_with_key(
        &self,
        key: &EncryptionKey,
        iv: &[u8],
        plaintext: &[u8],
        associated_data: Option<&[u8]>,
    ) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        let unbound_key = match key.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                UnboundKey::new(&AES_256_GCM, &key.key_data)
                    .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                UnboundKey::new(&CHACHA20_POLY1305, &key.key_data)
                    .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?
            }
            _ => return Err(EncryptionError::InvalidConfiguration(
                "Algorithm not supported for streaming encryption".to_string()
            )),
        };

        let nonce = Nonce::assume_unique_for_key(iv.to_vec());
        let mut sealing_key = SealingKey::new(unbound_key, nonce);

        let mut ciphertext = plaintext.to_vec();
        let aad = Aad::from(associated_data.unwrap_or(&[]));

        let tag = aead::seal_in_place(&mut sealing_key, aad, &mut ciphertext, aead::MAX_TAG_LEN)
            .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

        Ok((ciphertext, tag.to_vec()))
    }

    fn decrypt_with_key(
        &self,
        key: &EncryptionKey,
        iv: &[u8],
        ciphertext: &[u8],
        tag: &[u8],
        associated_data: Option<&[u8]>,
    ) -> Result<Vec<u8>, EncryptionError> {
        let unbound_key = match key.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                UnboundKey::new(&AES_256_GCM, &key.key_data)
                    .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                UnboundKey::new(&CHACHA20_POLY1305, &key.key_data)
                    .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?
            }
            _ => return Err(EncryptionError::InvalidConfiguration(
                "Algorithm not supported for decryption".to_string()
            )),
        };

        let nonce = Nonce::assume_unique_for_key(iv.to_vec());
        let mut opening_key = OpeningKey::new(unbound_key, nonce);

        let mut ciphertext_copy = ciphertext.to_vec();
        let full_ciphertext = [&mut ciphertext_copy[..], &tag[..]].concat();
        let aad = Aad::from(associated_data.unwrap_or(&[]));

        let plaintext = aead::open_in_place(&mut opening_key, aad, &mut ciphertext_copy, tag)
            .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?;

        Ok(plaintext.to_vec())
    }

    fn validate_key(&self, key: &EncryptionKey) -> Result<(), EncryptionError> {
        if key.status == KeyStatus::Revoked {
            return Err(EncryptionError::KeyRevoked);
        }

        if let Some(expires_at) = key.expires_at {
            if Self::timestamp() > expires_at {
                return Err(EncryptionError::KeyExpired(expires_at));
            }
        }

        Ok(())
    }

    fn generate_key_id(&self) -> String {
        format!("key_{}", uuid::Uuid::new_v4().to_string().replace("-", "").chars().take(24).collect::<String>())
    }

    fn generate_deterministic_iv(&self, context: &str, data: &[u8]) -> Vec<u8> {
        let mut context_data = context.as_bytes().to_vec();
        context_data.extend_from_slice(data);

        let hash = SHA256.digest(&context_data);
        hash.as_ref()[..12].to_vec()
    }

    fn generate_counter_iv(&self, key_id: &str) -> Result<Vec<u8>, EncryptionError> {
        let timestamp = Self::timestamp();
        let mut iv = Vec::with_capacity(12);
        iv.extend_from_slice(&timestamp.to_le_bytes()[..8]);
        iv.extend_from_slice(&rand::random::<u32>().to_le_bytes());
        Ok(iv)
    }

    async fn log_audit(&self, audit: EncryptionAudit) {
        if !self.config.read().await.enable_audit_log {
            return;
        }

        let mut audit_log = self.audit_log.write().await;
        audit_log.push(audit);

        if audit_log.len() > 10000 {
            audit_log.drain(0..1000);
        }
    }

    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrappedKey {
    pub wrapped_key_data: Vec<u8>,
    pub wrapping_key_id: String,
    pub algorithm: EncryptionAlgorithm,
    pub original_key_id: String,
    pub original_algorithm: EncryptionAlgorithm,
    pub nonce: Vec<u8>,
    pub tag: Vec<u8>,
}

impl TlsManager {
    pub async fn new() -> Self {
        let tls_config = TlsConfig {
            enabled: true,
            min_version: TlsVersion::Tls12,
            max_version: TlsVersion::Tls13,
            cipher_suites: vec![
                TlsCipherSuite::AeecdheEcdsaWithAes256GcmSha384,
                TlsCipherSuite::AeecdheRsaWithAes256GcmSha384,
                TlsCipherSuite::AeecdheEcsaWithAes128GcmSha256,
                TlsCipherSuite::AeecdheRsaWithAes128GcmSha256,
            ],
            cert_chain: Vec::new(),
            alpn_protocols: vec!["h2".to_string(), "http/1.1".to_string()],
            ocsp_stapling: true,
            session_tickets: true,
            session_timeout: Duration::from_secs(43200),
        };

        Self {
            tls_config: Arc::new(RwLock::new(tls_config)),
            certificates: Arc::new(RwLock::new(HashMap::new())),
            ca_certificates: Arc::new(RwLock::new(Vec::new())),
            private_keys: Arc::new(RwLock::new(HashMap::new())),
            ocsp_cache: Arc::new(RwLock::new(HashMap::new())),
            session_cache: Arc::new(Mutex::new(rustls::SessionCache::new(10000))),
        }
    }

    pub async fn generate_certificate(
        &self,
        config: &CertificateRequestConfig,
        validity_days: u32,
    ) -> Result<TlsCertificate, EncryptionError> {
        let key_pair = EcdsaKeyPair::generate_pkcs8(
            &ECDSA_P256_SHA256_ASN1_SIGNING,
            &SystemRandom::new(),
        ).map_err(|e| EncryptionError::CertificateGenerationFailed(e.to_string()))?;

        let private_key = key_pair.private_key().as_ref().to_vec();

        let not_before = Self::timestamp();
        let not_after = not_before + validity_days as u64 * 86400;

        let certificate = self.create_self_signed_cert(&key_pair, config, not_before, not_after)?;

        let cert_id = format!("cert_{}", uuid::Uuid::new_v4().to_string().replace("-", "").chars().take(24).collect::<String>());

        let tls_cert = TlsCertificate {
            cert_id,
            certificate: certificate.clone(),
            private_key,
            public_key: key_pair.public_key().as_ref().to_vec(),
            subject: format!("CN={}", config.common_name),
            issuer: format!("CN={}", config.common_name),
            serial_number: rand::random::<u64>().to_string(),
            not_before,
            not_after,
            san_dns_names: config.san_dns_names.clone(),
            san_ip_addresses: config.san_ip_addresses.clone(),
            is_ca: config.is_ca,
            key_usage: config.key_usage.clone(),
            extended_key_usage: config.extended_key_usage.clone(),
        };

        let mut certificates = self.certificates.write().await;
        certificates.insert(tls_cert.cert_id.clone(), tls_cert.clone());

        Ok(tls_cert)
    }

    pub async fn create_tls_acceptor(&self, cert_id: &str) -> Result<TlsAcceptor, EncryptionError> {
        let certificates = self.certificates.read().await;
        let cert = certificates.get(cert_id)
            .ok_or_else(|| EncryptionError::CertificateNotFound(cert_id.to_string()))?;
        drop(certificates);

        let private_keys = self.private_keys.read().await;
        let private_key = private_keys.get(cert_id)
            .ok_or_else(|| EncryptionError::PrivateKeyNotFound(cert_id.to_string()))?
            .clone();
        drop(private_keys);

        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::Certificate(cert.certificate.clone())],
                rustls::PrivateKey(private_key),
            )
            .map_err(|e| EncryptionError::TlsConfigFailed(e.to_string()))?;

        let config = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        Ok(config)
    }

    pub async fn create_tls_connector(&self, verify_hostname: bool) -> Result<TlsConnector, EncryptionError> {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(self.get_root_store().await)
            .with_no_client_auth();

        let config = if verify_hostname {
            config.with_webpki_verifier(webpki::Verifier::new(
                webpki::Time::from_seconds_since_unix_epoch(Self::timestamp()),
                webpki::KeyUsage::empty(),
                webpki::ValidationError::BadSignature,
            ))
        } else {
            config.with_insecure_no_verification()
        };

        Ok(TlsConnector::from(Arc::new(config)))
    }

    pub async fn accept_tls_connection(
        &self,
        listener: &TcpListener,
        acceptor: &TlsAcceptor,
    ) -> Result<(tokio::net::TcpStream, rustls::ServerConnection), EncryptionError> {
        let (stream, _) = listener.accept().await
            .map_err(|e| EncryptionError::TlsAcceptFailed(e.to_string()))?;

        let tls_stream = acceptor.accept(stream).await
            .map_err(|e| EncryptionError::TlsHandshakeFailed(e.to_string()))?;

        Ok(tls_stream)
    }

    pub async fn connect_with_tls(
        &self,
        host: &str,
        port: u16,
        connector: &TlsConnector,
    ) -> Result<(tokio::net::TcpStream, rustls::ClientConnection), EncryptionError> {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await
            .map_err(|e| EncryptionError::TlsConnectionFailed(e.to_string()))?;

        let domain = webpki::DNSNameRef::try_from_ascii_str(host)
            .map_err(|e| EncryptionError::InvalidHostname(e.to_string()))?;

        let tls_stream = connector.connect(domain, stream).await
            .map_err(|e| EncryptionError::TlsHandshakeFailed(e.to_string()))?;

        Ok(tls_stream)
    }

    fn create_self_signed_cert(
        &self,
        key_pair: &EcdsaKeyPair,
        config: &CertificateRequestConfig,
        not_before: u64,
        not_after: u64,
    ) -> Result<Vec<u8>, EncryptionError> {
        let mut cert_builder = x509_cert::CertificateBuilder::new();
        cert_builder = cert_builder.version(x509_cert::Version::V3);
        cert_builder = cert_builder.serial_number(rand::random::<u64>().into());
        cert_builder = cert_builder.not_before(x509_cert::Time::from_seconds_since_unix_epoch(not_before));
        cert_builder = cert_builder.not_after(x509_cert::Time::from_seconds_since_unix_epoch(not_after));

        let subject = x509_cert::Name::from(vec![
            x509_cert::RelativeDistinguishedName::from(common_name(config.common_name.clone())),
        ]);
        cert_builder = cert_builder.subject(subject.clone());
        cert_builder = cert_builder.issuer(subject);

        cert_builder = cert_builder.public_key(&key_pair.public_key());

        if let Some(org) = &config.organization {
            cert_builder = cert_builder.subject(x509_cert::Name::from(vec![
                x509_cert::RelativeDistinguishedName::from(common_name(config.common_name.clone())),
                x509_cert::RelativeDistinguishedName::from(organization(org.clone())),
            ]));
        }

        cert_builder = cert_builder.signature_algorithm(x509_cert::SignatureAlgorithm::EcdsaWithSha256);

        let cert = cert_builder.build()
            .map_err(|e| EncryptionError::CertificateGenerationFailed(e.to_string()))?;

        let mut encoded = Vec::new();
        cert.encode_to_der(&mut encoded)
            .map_err(|e| EncryptionError::CertificateEncodingFailed(e.to_string()))?;

        Ok(encoded)
    }

    async fn get_root_store(&self) -> RootCertStore {
        let mut store = RootCertStore::empty();

        let ca_certs = self.ca_certificates.read().await;
        for cert in ca_certs {
            store.add(cert.clone()).unwrap_or_default();
        }

        store
    }

    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

fn common_name(name: String) -> x509_cert::AttributeTypeAndValue {
    x509_cert::AttributeTypeAndValue {
        oid: x509_cert::attr::COMMON_NAME,
        value: x509_cert::Any::from(name),
    }
}

fn organization(org: String) -> x509_cert::AttributeTypeAndValue {
    x509_cert::AttributeTypeAndValue {
        oid: x509_cert::attr::ORGANIZATION_NAME,
        value: x509_cert::Any::from(org),
    }
}

impl Clone for EncryptionManager {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            key_metadata: self.key_metadata.clone(),
            key_versions: self.key_versions.clone(),
            field_configs: self.field_configs.clone(),
            master_key: self.master_key.clone(),
            rng: SystemRandom::new(),
            config: self.config.clone(),
            rotation_config: self.rotation_config.clone(),
            audit_log: self.audit_log.clone(),
            hsm_client: self.hsm_client.clone(),
        }
    }
}

impl Clone for TlsManager {
    fn clone(&self) -> Self {
        Self {
            tls_config: self.tls_config.clone(),
            certificates: self.certificates.clone(),
            ca_certificates: self.ca_certificates.clone(),
            private_keys: self.private_keys.clone(),
            ocsp_cache: self.ocsp_cache.clone(),
            session_cache: Arc::new(Mutex::new(rustls::SessionCache::new(10000))),
        }
    }
}

impl std::fmt::Debug for EncryptionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionManager")
            .field("keys_count", &self.keys.blocking_read().len())
            .field("field_configs_count", &self.field_configs.blocking_read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_key_creation_and_encryption() {
        let manager = EncryptionManager::new().await;

        let key = manager.create_key(
            KeyType::DataKey,
            EncryptionAlgorithm::AES256GCM,
            "test_key",
            None,
        ).await.unwrap();

        let plaintext = b"Hello, CortexDB!";
        let encrypted = manager.encrypt(&key.key_id, plaintext, None).await.unwrap();
        assert!(!encrypted.ciphertext.is_empty());

        let decrypted = manager.decrypt(&key.key_id, &encrypted, None).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_key_rotation() {
        let manager = EncryptionManager::new().await;

        let key = manager.create_key(
            KeyType::DataKey,
            EncryptionAlgorithm::AES256GCM,
            "rotatable_key",
            Some(365),
        ).await.unwrap();

        let plaintext = b"Data to encrypt";
        let encrypted = manager.encrypt(&key.key_id, plaintext, None).await.unwrap();

        let rotated_key = manager.rotate_key(&key.key_id).await.unwrap();
        assert_eq!(rotated_key.version, 2);

        let re_encrypted = manager.re_encrypt(&key.key_id, &rotated_key.key_id, &encrypted, None).await.unwrap();
        let decrypted = manager.decrypt(&rotated_key.key_id, &re_encrypted, None).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_hmac_generation() {
        let manager = EncryptionManager::new().await;

        let key = manager.create_key(
            KeyType::DataKey,
            EncryptionAlgorithm::AES256GCM,
            "hmac_key",
            None,
        ).await.unwrap();

        let data = b"Test data for HMAC";
        let hmac_result = manager.generate_hmac(&key.key_id, data).await.unwrap();
        assert_eq!(hmac_result.len(), 32);

        let valid = manager.verify_hmac(&key.key_id, data, &hmac_result).await.unwrap();
        assert!(valid);
    }
}
