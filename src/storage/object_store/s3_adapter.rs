//! S3 Object Store Adapter for CortexDB
//!
//! This module provides a complete S3-compatible object storage implementation
//! using the rusoto-aws SDK. It supports Amazon S3, MinIO, and other S3-compatible services.

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::path::Path;
use std::fmt;
use rusoto_s3::{S3, S3Client, PutObjectRequest, GetObjectRequest, DeleteObjectRequest, ListObjectsV2Request, HeadObjectRequest, CreateBucketRequest, DeleteBucketRequest, BucketLocationConstraint, PutBucketCorsRequest, CORSRule, GetBucketLocationRequest};
use rusoto_core::{Region, HttpClient, Client};
use rusoto_credential::{StaticProvider, ChainProvider, ProfileProvider, CredentialsError};
use rusoto_sts::{StsClient, AssumeRoleWithWebIdentityRequest};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tempfile::NamedTempFile;
use bytes::Bytes;

#[derive(Debug, Error)]
pub enum S3ObjectStoreError {
    #[error("S3 client error: {0}")]
    S3ClientError(String),

    #[error("Authentication error: {0}")]
    AuthError(String),

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Partial failure: {0}")]
    PartialFailure(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket_name: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub profile: Option<String>,
    pub role_arn: Option<String>,
    pub web_identity_token_file: Option<String>,
    pub role_session_name: Option<String>,
    pub use_path_style: bool,
    pub accelerate_mode: bool,
    pub dual_stack: bool,
    pub checksum_sha256: bool,
    pub request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub max_retries: u32,
    pub concurrency: usize,
    pub part_size_bytes: usize,
    pub proxy: Option<ProxyConfig>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "us-east-1".to_string(),
            bucket_name: "cortexdb".to_string(),
            access_key: None,
            secret_key: None,
            session_token: None,
            profile: None,
            role_arn: None,
            web_identity_token_file: None,
            role_session_name: Some("cortexdb-session".to_string()),
            use_path_style: false,
            accelerate_mode: false,
            dual_stack: false,
            checksum_sha256: true,
            request_timeout_ms: 30000,
            connect_timeout_ms: 10000,
            max_retries: 3,
            concurrency: 4,
            part_size_bytes: 8 * 1024 * 1024,
            proxy: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub no_proxy: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub content_type: Option<String>,
    pub content_md5: Option<String>,
    pub user_metadata: HashMap<String, String>,
    pub storage_class: Option<String>,
    pub version_id: Option<String>,
    pub is_delete_marker: bool,
}

impl fmt::Display for S3ObjectMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.key, self.size)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    pub metadata: S3ObjectMetadata,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3MultipartUpload {
    pub upload_id: String,
    pub key: String,
    pub initiated: u64,
    pub parts: Vec<S3UploadPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3UploadPart {
    pub part_number: i32,
    pub etag: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3UploadPartETag {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3CopyPart {
    pub source_key: String,
    pub source_version_id: Option<String>,
    pub part_number: i32,
    pub first_byte: Option<u64>,
    pub last_byte: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BatchOperationResult {
    pub successful: Vec<String>,
    pub failed: Vec<(String, String)>,
    pub count: usize,
    pub errors: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BucketInfo {
    pub name: String,
    pub creation_date: u64,
    pub region: String,
    pub versioning_enabled: bool,
    pub encryption_enabled: bool,
    pub public_access_blocked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3UsageStats {
    pub total_size_bytes: u64,
    pub object_count: u64,
    pub last_updated: u64,
}

#[derive(Debug)]
pub struct S3ObjectStoreAdapter {
    client: Arc<S3Client>,
    config: S3Config,
    bucket_name: String,
    region: Region,
    stats: Arc<RwLock<S3UsageStats>>,
    multipart_uploads: Arc<RwLock<HashMap<String, S3MultipartUpload>>>,
}

impl S3ObjectStoreAdapter {
    pub fn new(config: S3Config) -> Result<Self, S3ObjectStoreError> {
        let region = Self::parse_region(&config.region)?;

        let credentials = Self::create_credentials(&config)?;

        let mut dispatcher = HttpClient::new()
            .map_err(|e| S3ObjectStoreError::NetworkError(e.to_string()))?;

        if let Some(ref proxy) = config.proxy {
            dispatcher.set_proxy(Some(reqwest::Proxy::new(
                format!("{}:{}", proxy.host, proxy.port),
            ));
        }

        let client = S3Client::new_with(
            dispatcher,
            credentials,
            region.clone(),
        );

        Ok(Self {
            client: Arc::new(client),
            config,
            bucket_name: config.bucket_name.clone(),
            region,
            stats: Arc::new(RwLock::new(S3UsageStats {
                total_size_bytes: 0,
                object_count: 0,
                last_updated: 0,
            })),
            multipart_uploads: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn parse_region(region_str: &str) -> Result<Region, S3ObjectStoreError> {
        match region_str.to_lowercase().as_str() {
            "us-east-1" => Ok(Region::UsEast1),
            "us-east-2" => Ok(Region::UsEast2),
            "us-west-1" => Ok(Region::UsWest1),
            "us-west-2" => Ok(Region::UsWest2),
            "eu-west-1" => Ok(Region::EuWest1),
            "eu-west-2" => Ok(Region::EuWest2),
            "eu-central-1" => Ok(Region::EuCentral1),
            "ap-southeast-1" => Ok(Region::ApSoutheast1),
            "ap-southeast-2" => Ok(Region::ApSoutheast2),
            "ap-northeast-1" => Ok(Region::ApNortheast1),
            "ap-northeast-2" => Ok(Region::ApNortheast2),
            "sa-east-1" => Ok(Region::SaEast1),
            "cn-north-1" => Ok(Region::CnNorth1),
            "custom" => Ok(Region::Custom {
                name: region_str.to_string(),
                endpoint: String::new(),
            }),
            _ => {
                if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
                    Ok(Region::Custom {
                        name: region_str.to_string(),
                        endpoint,
                    })
                } else {
                    Ok(Region::UsEast1)
                }
            }
        }
    }

    fn create_credentials(config: &S3Config) -> Result<ChainProvider, S3ObjectStoreError> {
        let mut chain = ChainProvider::new();

        if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
            let provider = StaticProvider::new_minimal(
                access_key.clone(),
                secret_key.clone(),
            );
            return Ok(ChainProvider::with_provider(provider));
        }

        if let Some(ref profile) = config.profile {
            if let Ok(profile_provider) = ProfileProvider::new(profile) {
                return Ok(ChainProvider::with_provider(profile_provider));
            }
        }

        if let Some(ref role_arn) = config.role_arn {
            if let Some(ref token_file) = config.web_identity_token_file {
                return Self::assume_role_with_web_identity(
                    role_arn,
                    token_file,
                    config.role_session_name.as_deref().unwrap_or("cortexdb"),
                    &config.region,
                );
            }
        }

        Ok(chain)
    }

    fn assume_role_with_web_identity(
        role_arn: &str,
        token_file: &str,
        session_name: &str,
        region: &str,
    ) -> Result<ChainProvider, S3ObjectStoreError> {
        let region = Self::parse_region(region)?;

        let credentials = ChainProvider::new();

        let sts_client = StsClient::new(region.clone());

        let token = std::fs::read_to_string(token_file)
            .map_err(|e| S3ObjectStoreError::AuthError(e.to_string()))?;

        let request = AssumeRoleWithWebIdentityRequest {
            role_arn: role_arn.to_string(),
            role_session_name: session_name.to_string(),
            web_identity_token: token,
            ..Default::default()
        };

        let response = sts_client.assume_role_with_web_identity(request)
            .await
            .map_err(|e| S3ObjectStoreError::AuthError(e.to_string()))?;

        if let Some(credentials) = response.credentials {
            let provider = StaticProvider::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                Some(credentials.session_token),
                credentials.expiration.map(|e| e.into()),
            );
            return Ok(ChainProvider::with_provider(provider));
        }

        Err(S3ObjectStoreError::AuthError(
            "Failed to get temporary credentials".to_string(),
        ))
    }

    pub async fn ensure_bucket_exists(&self) -> Result<(), S3ObjectStoreError> {
        let request = HeadBucketRequest {
            bucket: self.bucket_name.clone(),
            ..Default::default()
        };

        match self.client.head_bucket(request).await {
            Ok(_) => Ok(()),
            Err(_) => {
                let location_constraint = BucketLocationConstraint::from(self.region.name().as_str());
                let create_request = CreateBucketRequest {
                    bucket: self.bucket_name.clone(),
                    create_bucket_configuration: Some(rusoto_s3::CreateBucketConfiguration {
                        location_constraint,
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                self.client.create_bucket(create_request)
                    .await
                    .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

                Ok(())
            }
        }
    }

    pub async fn put_object(
        &self,
        key: &str,
        data: &[u8],
        metadata: Option<HashMap<String, String>>,
        content_type: Option<&str>,
    ) -> Result<String, S3ObjectStoreError> {
        let mut put_request = PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            body: Some(data.to_vec().into()),
            content_type: content_type.map(|s| s.to_string()),
            ..Default::default()
        };

        if let Some(ref meta) = metadata {
            let user_metadata: HashMap<String, String> = meta.iter()
                .filter(|(k, _)| !k.starts_with("x-amz-"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            if !user_metadata.is_empty() {
                put_request.metadata = Some(user_metadata);
            }
        }

        if self.config.checksum_sha256 {
            let sha256 = sha256::digest(data);
            put_request.content_sha256 = Some(sha256);
        }

        let response = self.client.put_object(put_request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        Ok(response.etag.unwrap_or_default().replace("\"", ""))
    }

    pub async fn put_object_streaming(
        &self,
        key: &str,
        data: impl tokio::io::AsyncReadExt + Send,
        size: u64,
        metadata: Option<HashMap<String, String>>,
        content_type: Option<&str>,
    ) -> Result<String, S3ObjectStoreError> {
        if size <= self.config.part_size_bytes as u64 {
            let mut buffer = Vec::with_capacity(size as usize);
            let mut reader = data;
            reader.read_to_end(&mut buffer).await?;
            return self.put_object(key, &buffer, metadata, content_type).await;
        }

        let upload_id = self.initiate_multipart_upload(key, metadata, content_type).await?;
        let part_size = self.config.part_size_bytes as usize;
        let mut part_number = 1;
        let mut etags = Vec::new();

        let mut reader = data;
        let mut offset = 0u64;

        while offset < size {
            let remaining = size - offset;
            let to_read = std::cmp::min(remaining, part_size as u64) as usize;
            let mut buffer = vec![0u8; to_read];
            reader.read_exact(&mut buffer).await?;

            let etag = self.upload_part(key, &upload_id, part_number, &buffer).await?;
            etags.push((part_number, etag));

            offset += to_read as u64;
            part_number += 1;
        }

        let complete_result = self.complete_multipart_upload(key, &upload_id, &etags).await?;

        Ok(complete_result.etag.unwrap_or_default().replace("\"", ""))
    }

    pub async fn get_object(&self, key: &str) -> Result<S3Object, S3ObjectStoreError> {
        let request = GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        let response = self.client.get_object(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ObjectNotFound(key.to_string()))?;

        let metadata = S3ObjectMetadata {
            key: key.to_string(),
            size: response.content_length.unwrap_or(0) as u64,
            etag: response.etag.unwrap_or_default().replace("\"", ""),
            last_modified: response.last_modified
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            content_type: response.content_type,
            content_md5: response.content_md5,
            user_metadata: response.metadata.unwrap_or_default(),
            storage_class: response.storage_class,
            version_id: response.version_id,
            is_delete_marker: false,
        };

        let data = match response.body {
            Some(stream) => {
                let bytes = stream
                    .map_ok(|chunk| chunk.to_vec())
                    .try_concat()
                    .await
                    .map_err(|e| S3ObjectStoreError::NetworkError(e.to_string()))?;
                bytes
            }
            None => Vec::new(),
        };

        Ok(S3Object { metadata, data })
    }

    pub async fn get_object_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, S3ObjectStoreError> {
        let request = GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            range: Some(format!("bytes={}-{}", start, end)),
            ..Default::default()
        };

        let response = self.client.get_object(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ObjectNotFound(key.to_string()))?;

        let data = match response.body {
            Some(stream) => {
                let bytes = stream
                    .map_ok(|chunk| chunk.to_vec())
                    .try_concat()
                    .await
                    .map_err(|e| S3ObjectStoreError::NetworkError(e.to_string()))?;
                bytes
            }
            None => Vec::new(),
        };

        Ok(data)
    }

    pub async fn delete_object(&self, key: &str) -> Result<bool, S3ObjectStoreError> {
        let request = DeleteObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        self.client.delete_object(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        Ok(true)
    }

    pub async fn delete_objects(
        &self,
        keys: &[String],
    ) -> Result<S3BatchOperationResult, S3ObjectStoreError> {
        if keys.is_empty() {
            return Ok(S3BatchOperationResult {
                successful: Vec::new(),
                failed: Vec::new(),
                count: 0,
                errors: Vec::new(),
            });
        }

        let objects: Vec<_> = keys.iter()
            .map(|k| rusoto_s3::ObjectIdentifier {
                key: k.clone(),
                version_id: None,
            })
            .collect();

        let request = rusoto_s3::DeleteObjectsRequest {
            bucket: self.bucket_name.clone(),
            delete: rusoto_s3::Delete {
                objects,
                quiet: Some(true),
            },
            ..Default::default()
        };

        let response = self.client.delete_objects(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        if let Some(deleted) = response.delete_results {
            for delete_result in deleted {
                if let Some(errors) = delete_result.error {
                    for error in errors {
                        failed.push((error.key.unwrap_or_default(), error.message.unwrap_or_default()));
                    }
                }
            }
        }

        Ok(S3BatchOperationResult {
            successful,
            failed,
            count: keys.len(),
            errors: failed.clone(),
        })
    }

    pub async fn list_objects(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: Option<i64>,
    ) -> Result<Vec<S3ObjectMetadata>, S3ObjectStoreError> {
        let mut all_objects = Vec::new();
        let mut continuation_token = None;
        let max_keys = max_keys.unwrap_or(1000);

        loop {
            let request = ListObjectsV2Request {
                bucket: self.bucket_name.clone(),
                prefix: prefix.map(|s| s.to_string()),
                delimiter: delimiter.map(|s| s.to_string()),
                continuation_token: continuation_token.take(),
                max_keys: Some(std::cmp::min(max_keys, 1000)),
                ..Default::default()
            };

            let response = self.client.list_objects_v2(request)
                .await
                .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        all_objects.push(S3ObjectMetadata {
                            key,
                            size: obj.size.unwrap_or(0) as u64,
                            etag: obj.eTag.unwrap_or_default().replace("\"", ""),
                            last_modified: obj.last_modified
                                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0),
                            content_type: None,
                            content_md5: None,
                            user_metadata: HashMap::new(),
                            storage_class: None,
                            version_id: None,
                            is_delete_marker: false,
                        });
                    }
                }
            }

            if response.is_truncated == Some(true) {
                continuation_token = response.continuation_token;
            } else {
                break;
            }
        }

        Ok(all_objects)
    }

    pub async fn list_objects_with_metadata(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<S3ObjectMetadata>, S3ObjectStoreError> {
        let objects = self.list_objects(prefix, None, None).await?;

        let mut results = Vec::new();

        for obj in objects {
            if let Ok(metadata) = self.head_object(&obj.key).await {
                results.push(metadata);
            } else {
                results.push(obj);
            }
        }

        Ok(results)
    }

    pub async fn head_object(&self, key: &str) -> Result<S3ObjectMetadata, S3ObjectStoreError> {
        let request = HeadObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        let response = self.client.head_object(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ObjectNotFound(key.to_string()))?;

        Ok(S3ObjectMetadata {
            key: key.to_string(),
            size: response.content_length.unwrap_or(0) as u64,
            etag: response.etag.unwrap_or_default().replace("\"", ""),
            last_modified: response.last_modified
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            content_type: response.content_type,
            content_md5: response.content_md5,
            user_metadata: response.metadata.unwrap_or_default(),
            storage_class: response.storage_class,
            version_id: response.version_id,
            is_delete_marker: false,
        })
    }

    pub async fn copy_object(
        &self,
        source_key: &str,
        dest_key: &str,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String, S3ObjectStoreError> {
        let source = format!("{}/{}", self.bucket_name, source_key);

        let mut copy_request = rusoto_s3::CopyObjectRequest {
            bucket: self.bucket_name.clone(),
            key: dest_key.to_string(),
            copy_source: source,
            ..Default::default()
        };

        if let Some(meta) = metadata {
            copy_request.metadata = Some(meta);
            copy_request.metadata_directive = Some(rusoto_s3::MetadataDirective::REPLACE);
        }

        let response = self.client.copy_object(copy_request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        Ok(response.copy_result_etag.unwrap_or_default().replace("\"", ""))
    }

    pub async fn initiate_multipart_upload(
        &self,
        key: &str,
        metadata: Option<HashMap<String, String>>,
        content_type: Option<&str>,
    ) -> Result<String, S3ObjectStoreError> {
        let request = rusoto_s3::CreateMultipartUploadRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            content_type: content_type.map(|s| s.to_string()),
            metadata: metadata,
            ..Default::default()
        };

        let response = self.client.create_multipart_upload(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        let upload_id = response.upload_id.unwrap_or_default();

        let upload = S3MultipartUpload {
            upload_id: upload_id.clone(),
            key: key.to_string(),
            initiated: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            parts: Vec::new(),
        };

        let mut uploads = self.multipart_uploads.write().unwrap();
        uploads.insert(upload_id.clone(), upload);

        Ok(upload_id)
    }

    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: &[u8],
    ) -> Result<String, S3ObjectStoreError> {
        let request = rusoto_s3::UploadPartRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            part_number,
            body: Some(data.to_vec().into()),
            content_length: Some(data.len() as i64),
            ..Default::default()
        };

        let response = self.client.upload_part(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        let etag = response.etag.unwrap_or_default().replace("\"", "");

        let mut uploads = self.multipart_uploads.write().unwrap();
        if let Some(upload) = uploads.get_mut(upload_id) {
            upload.parts.push(S3UploadPart {
                part_number,
                etag: etag.clone(),
                size: data.len() as u64,
            });
        }

        Ok(etag)
    }

    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> Result<rusoto_s3::CompleteMultipartUploadResult, S3ObjectStoreError> {
        let uploaded_parts: Vec<_> = parts.iter()
            .map(|(num, etag)| rusoto_s3::CompletedPart {
                part_number: *num,
                e_tag: Some(etag.clone()),
                checksum_sha256: None,
            })
            .collect();

        let request = rusoto_s3::CompleteMultipartUploadRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(rusoto_s3::CompletedMultipartUpload {
                parts: Some(uploaded_parts),
            }),
            ..Default::default()
        };

        let response = self.client.complete_multipart_upload(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        let mut uploads = self.multipart_uploads.write().unwrap();
        uploads.remove(upload_id);

        Ok(response)
    }

    pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<(), S3ObjectStoreError> {
        let request = rusoto_s3::AbortMultipartUploadRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            ..Default::default()
        };

        self.client.abort_multipart_upload(request)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        let mut uploads = self.multipart_uploads.write().unwrap();
        uploads.remove(upload_id);

        Ok(())
    }

    pub async fn generate_presigned_url(
        &self,
        key: &str,
        expires_in: Duration,
        method: &str,
    ) -> Result<String, S3ObjectStoreError> {
        let mut request = GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        let url = self.client
            .presigned_get_object(&self.bucket_name, key, expires_in, None)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        Ok(url)
    }

    pub async fn generate_presigned_post(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> Result<(String, Vec<(String, String)>, Vec<u8>)>, S3ObjectStoreError> {
        let request = PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        let presigned = self.client
            .presigned_put_object(&self.bucket_name, key, expires_in)
            .await
            .map_err(|e| S3ObjectStoreError::S3ClientError(e.to_string()))?;

        Ok((presigned, Vec::new(), Vec::new()))
    }

    pub async fn get_usage_stats(&self) -> Result<S3UsageStats, S3ObjectStoreError> {
        let objects = self.list_objects(None, None, Some(1000)).await?;

        let total_size: u64 = objects.iter().map(|o| o.size).sum();
        let object_count = objects.len() as u64;

        let mut stats = self.stats.write().unwrap();
        stats.total_size_bytes = total_size;
        stats.object_count = object_count;
        stats.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        Ok(stats.clone())
    }
}

#[async_trait]
impl crate::storage::object_store::adapter::ObjectStoreAdapter for S3ObjectStoreAdapter {
    async fn put_object(&self, key: &str, data: &[u8]) -> Result<(), crate::storage::object_store::adapter::ObjectStoreError> {
        self.put_object(key, data, None, None).await?;
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, crate::storage::object_store::adapter::ObjectStoreError> {
        let obj = self.get_object(key).await?;
        Ok(obj.data)
    }

    async fn delete_object(&self, key: &str) -> Result<(), crate::storage::object_store::adapter::ObjectStoreError> {
        self.delete_object(key).await?;
        Ok(())
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>, crate::storage::object_store::adapter::ObjectStoreError> {
        let objects = self.list_objects(prefix, None, None).await?;
        Ok(objects.into_iter().map(|o| o.key).collect())
    }

    async fn exists(&self, key: &str) -> Result<bool, crate::storage::object_store::adapter::ObjectStoreError> {
        match self.head_object(key).await {
            Ok(_) => Ok(true),
            Err(S3ObjectStoreError::S3ObjectNotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_object_size(&self, key: &str) -> Result<usize, crate::storage::object_store::adapter::ObjectStoreError> {
        let metadata = self.head_object(key).await?;
        Ok(metadata.size as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_s3_config_default() {
        let config = S3Config::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.bucket_name, "cortexdb");
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    async fn test_parse_region() {
        let region = S3ObjectStoreAdapter::parse_region("us-west-2").unwrap();
        assert_eq!(region.name(), "us-west-2");
    }

    #[tokio::test]
    async fn test_s3_object_metadata_display() {
        let metadata = S3ObjectMetadata {
            key: "test.txt".to_string(),
            size: 1024,
            etag: "abc123".to_string(),
            last_modified: 1234567890,
            content_type: Some("text/plain".to_string()),
            content_md5: None,
            user_metadata: HashMap::new(),
            storage_class: Some("STANDARD".to_string()),
            version_id: None,
            is_delete_marker: false,
        };

        let display = format!("{}", metadata);
        assert!(display.contains("test.txt"));
    }
}
