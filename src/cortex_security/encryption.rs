//! Encryption functionality for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use ring::{
    aead::{self, Aad, Nonce, UnboundKey, BoundKey, SealingKey, OpeningKey},
    digest::{self, Digest},
    rand::{SecureRandom, SystemRandom},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EncryptionAlgorithm {
    AES256GCM,
    ChaCha20Poly1305,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_size: usize,
    pub iv_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EncryptedData {
    pub algorithm: EncryptionAlgorithm,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub tag: Vec<u8>,
}

#[derive(Debug)]
pub struct EncryptionManager {
    config: EncryptionConfig,
    master_key: Arc<RwLock<Option<Vec<u8>>>>,
    rng: SystemRandom,
}

impl EncryptionManager {
    pub fn new() -> Self {
        Self {
            config: EncryptionConfig {
                algorithm: EncryptionAlgorithm::AES256GCM,
                key_size: 32,
                iv_size: 12,
            },
            master_key: Arc::new(RwLock::new(None)),
            rng: SystemRandom::new(),
        }
    }

    pub async fn set_master_key(&self, key: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut master_key = self.master_key.write().await;
        *master_key = Some(key.to_vec());
        Ok(())
    }

    pub async fn generate_master_key(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut key = vec![0; self.config.key_size];
        self.rng.fill(&mut key)?;
        
        let mut master_key = self.master_key.write().await;
        *master_key = Some(key.clone());
        
        Ok(key)
    }

    pub async fn encrypt(&self, data: &[u8]) -> Result<EncryptedData, Box<dyn std::error::Error>> {
        let master_key = self.master_key.read().await;
        let key = master_key.as_ref().ok_or("Master key not set")?;
        
        let mut nonce = vec![0; self.config.iv_size];
        self.rng.fill(&mut nonce)?;
        
        let unbound_key = match self.config.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                UnboundKey::new(&aead::AES_256_GCM, key)?
            },
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                UnboundKey::new(&aead::CHACHA20_POLY1305, key)?
            },
        };
        
        let sealing_key = SealingKey::new(unbound_key, Nonce::assume_unique_for_key(nonce.clone()));
        
        let mut ciphertext = data.to_vec();
        let aad = Aad::empty();
        
        let tag = aead::seal_in_place(&sealing_key, aad, &mut ciphertext, aead::MAX_TAG_LEN)?;
        
        Ok(EncryptedData {
            algorithm: self.config.algorithm.clone(),
            nonce,
            ciphertext,
            tag: tag.to_vec(),
        })
    }

    pub async fn decrypt(&self, encrypted_data: &EncryptedData) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let master_key = self.master_key.read().await;
        let key = master_key.as_ref().ok_or("Master key not set")?;
        
        let unbound_key = match encrypted_data.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                UnboundKey::new(&aead::AES_256_GCM, key)?
            },
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                UnboundKey::new(&aead::CHACHA20_POLY1305, key)?
            },
        };
        
        let opening_key = OpeningKey::new(unbound_key, Nonce::assume_unique_for_key(encrypted_data.nonce.clone()));
        
        let mut plaintext = encrypted_data.ciphertext.clone();
        let aad = Aad::empty();
        
        let decrypted = aead::open_in_place(&opening_key, aad, &mut plaintext, &encrypted_data.tag)?;
        
        Ok(decrypted.to_vec())
    }

    pub async fn hash_data(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut hasher = digest::Context::new(&digest::SHA256);
        hasher.update(data);
        Ok(hasher.finish().as_ref().to_vec())
    }

    pub async fn generate_random_bytes(&self, length: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut bytes = vec![0; length];
        self.rng.fill(&mut bytes)?;
        Ok(bytes)
    }
}

impl Clone for EncryptionManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            master_key: self.master_key.clone(),
            rng: SystemRandom::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_encryption_manager() {
        let encryption_manager = EncryptionManager::new();
        
        // Generate master key
        let key = encryption_manager.generate_master_key().await.unwrap();
        assert_eq!(key.len(), 32);
        
        // Encrypt data
        let plaintext = b"Hello, world!";
        let encrypted = encryption_manager.encrypt(plaintext).await.unwrap();
        assert!(!encrypted.ciphertext.is_empty());
        
        // Decrypt data
        let decrypted = encryption_manager.decrypt(&encrypted).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
