use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::Aes256Gcm;
use argon2::Argon2;
use mrhc_core::Result;
use tokio::io::AsyncReadExt;

use crate::errors;

/// Derives a new 32-byte key from the given passphrase using argon2.
/// Returns: A tuple containing the 16-byte generated salt and the 32-byte derived key.
pub fn derive_new_key(passphrase: &str) -> Result<([u8; 16], [u8; 32])> {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);

    let mut key = [0u8; 32];

    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), &salt, &mut key)
        .map_err(|_| errors::create_unknown("Error hashing password"))?;

    Ok((salt, key))
}

/// Derives a key from an already existing salt using argon2.
pub fn derive_key(passphrase: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let mut key = [0u8; 32];

    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|_| errors::create_unknown("Error hashing password"))?;

    Ok(key)
}

/// Encrypts the given data using AES 256 GCM with the provided 32 bytes key.
/// Returns a byte vector, the first 12 bytes of which contain the generated nonce,
/// followed by the encrypted data.
pub fn encrypt(data: Vec<u8>, key: &[u8; 32]) -> Result<Vec<u8>> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let cipher = Aes256Gcm::new(key.into());
    let mut ciphertext = cipher
        .encrypt(&nonce, data.as_ref())
        .map_err(|_| errors::create_unknown("Error encrypting data"))?;

    let mut result = nonce.to_vec();
    result.append(&mut ciphertext);

    Ok(result)
}

/// Decryptes the given data using AES 256 GCM with the provided 32 bytes key.
/// Returns a byte vector containing the decrypted data.
/// Expects the nonce to be the first 12 bytes.
pub async fn decrypt<R: AsyncReadExt + Unpin>(mut reader: R, key: &[u8; 32]) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    reader
        .read_exact(&mut nonce)
        .await
        .map_err(|_| errors::create_unknown("Error reading nonce"))?;

    let mut data = Vec::new();
    reader
        .read_to_end(&mut data)
        .await
        .map_err(|_| errors::create_unknown("Error reading data"))?;

    let cipher = Aes256Gcm::new(key.into());
    let decrypted = cipher
        .decrypt(&nonce.into(), data.as_ref())
        .map_err(|_| errors::create_unknown("Error decrypting data"))?;

    Ok(decrypted)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[tokio::test]
    async fn test_derive_new_key() {
        // Arrange
        let passphrase = "test-secret";

        // Act
        let (salt_1, key_1) = derive_new_key(passphrase).unwrap();
        let (salt_2, key_2) = derive_new_key(passphrase).unwrap();

        // Assert
        assert_ne!(salt_1, salt_2);
        assert_ne!(key_1, key_2);
    }

    #[tokio::test]
    async fn test_derive_key() {
        // Arrange
        let passphrase = "test-secret";
        let salt1: &[u8; 16] = &[
            0x3A, 0xF1, 0x7C, 0x92, 0x55, 0x08, 0xE4, 0xB7, 0x1D, 0x63, 0xA9, 0xC0, 0x4F, 0x82,
            0x36, 0xD5,
        ];
        let salt2: &[u8; 16] = &[
            0x9E, 0x42, 0x17, 0xC8, 0x3B, 0xA1, 0xF4, 0x6D, 0x58, 0x0C, 0xE9, 0x72, 0xAD, 0x33,
            0xB6, 0x81,
        ];

        // Act
        let key_1 = derive_key(passphrase, salt1).unwrap();
        let key_2 = derive_key(passphrase, salt2).unwrap();

        // Assert
        assert_ne!(key_1, key_2);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt() {
        // Arrange
        let plaintext = *b"Hello world!";
        let key: [u8; 32] = *b"some-secret-12345678912345678912";

        // Act
        let encrypted = encrypt(plaintext.to_vec(), &key).unwrap();
        let decrypted = decrypt(Cursor::new(encrypted), &key).await.unwrap();

        // Assert
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_invalid_key() {
        // Arrange
        let plaintext = *b"Hello world!";
        let key1: [u8; 32] = *b"some-secret-12345678912345678912";
        let key2: [u8; 32] = *b"some-incorrect-secret-2345678912";

        // Act
        let encrypted = encrypt(plaintext.to_vec(), &key1).unwrap();
        let result = decrypt(Cursor::new(encrypted), &key2).await;

        // Assert
        assert!(result.is_err());
    }
}
