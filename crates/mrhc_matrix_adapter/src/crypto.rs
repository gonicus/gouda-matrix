use std::path::Path;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::Aes256Gcm;
use argon2::Argon2;
use tokio::io::AsyncReadExt;

#[derive(thiserror::Error, Debug)]
pub enum CryptoError {
    #[error("hash error")]
    Hash,

    #[error("cipher error")]
    Cipher,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CryptoError>;

/// Decrypts the file located at the specified file path using the specified passphrase.
pub async fn decrypt_file(file: impl AsRef<Path>, passphrase: impl AsRef<str>) -> Result<Vec<u8>> {
    let mut reader = tokio::fs::File::open(file).await?;

    let mut salt = [0u8; 16];
    reader.read_exact(&mut salt).await?;

    let key = derive_key(passphrase.as_ref(), &salt)?;
    let decrypted = decrypt(reader, &key).await?;

    Ok(decrypted)
}

/// Encrypts the file located at the specified file path using the specified passphrase.
pub async fn encrypt_to_file(
    file: impl AsRef<Path>,
    passphrase: impl AsRef<str>,
    decrypted: Vec<u8>,
) -> Result<()> {
    let (salt, key) = derive_new_key(passphrase.as_ref())?;

    let mut encrypted = encrypt(decrypted, &key)?;

    let mut result = salt.to_vec();
    result.append(&mut encrypted);

    tokio::fs::write(file, result).await?;

    Ok(())
}

/// Derives a new 32-byte key from the given passphrase using argon2.
/// Returns: A tuple containing the 16-byte generated salt and the 32-byte derived key.
fn derive_new_key(passphrase: &str) -> Result<([u8; 16], [u8; 32])> {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);

    let mut key = [0u8; 32];

    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), &salt, &mut key)
        .map_err(|_| CryptoError::Hash)?;

    Ok((salt, key))
}

/// Derives a key from an already existing salt using argon2.
fn derive_key(passphrase: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let mut key = [0u8; 32];

    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|_| CryptoError::Hash)?;

    Ok(key)
}

/// Encrypts the given data using AES 256 GCM with the provided 32 bytes key.
/// Returns a byte vector, the first 12 bytes of which contain the generated nonce,
/// followed by the encrypted data.
fn encrypt(data: Vec<u8>, key: &[u8; 32]) -> Result<Vec<u8>> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let cipher = Aes256Gcm::new(key.into());

    let mut ciphertext = cipher
        .encrypt(&nonce, data.as_ref())
        .map_err(|_| CryptoError::Cipher)?;

    let mut result = nonce.to_vec();
    result.append(&mut ciphertext);

    Ok(result)
}

/// Decrypts the given data using AES 256 GCM with the provided 32 bytes key.
/// Returns a byte vector containing the decrypted data.
/// Expects the nonce to be the first 12 bytes.
async fn decrypt<R: AsyncReadExt + Unpin>(mut reader: R, key: &[u8; 32]) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    reader.read_exact(&mut nonce).await?;

    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;

    let cipher = Aes256Gcm::new(key.into());

    let decrypted = cipher
        .decrypt(&nonce.into(), data.as_ref())
        .map_err(|_| CryptoError::Cipher)?;

    Ok(decrypted)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Cursor;

    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_derive_new_key() {
        let passphrase = "test-secret";

        let (salt_1, key_1) = derive_new_key(passphrase).unwrap();
        let (salt_2, key_2) = derive_new_key(passphrase).unwrap();

        assert_ne!(salt_1, salt_2);
        assert_ne!(key_1, key_2);
    }

    #[tokio::test]
    async fn test_derive_new_key_output_sizes() {
        let (salt, key) = derive_new_key("passphrase").unwrap();

        assert_eq!(salt.len(), 16);
        assert_eq!(key.len(), 32);
    }

    #[tokio::test]
    async fn test_derive_new_key_uniqueness() {
        let passphrase = "unique-test";
        let results: Vec<_> = (0..10)
            .map(|_| derive_new_key(passphrase).unwrap())
            .collect();

        let salts: Vec<_> = results.iter().map(|(s, _)| *s).collect();
        let keys: Vec<_> = results.iter().map(|(_, k)| *k).collect();

        // All salts must be unique
        for i in 0..salts.len() {
            for j in (i + 1)..salts.len() {
                assert_ne!(
                    salts[i], salts[j],
                    "Salt collision at indices {} and {}",
                    i, j
                );
            }
        }

        // All keys must be unique
        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                assert_ne!(keys[i], keys[j], "Key collision at indices {} and {}", i, j);
            }
        }
    }

    #[tokio::test]
    async fn test_derive_key() {
        let passphrase = "test-secret";
        let salt1: &[u8; 16] = &[
            0x3A, 0xF1, 0x7C, 0x92, 0x55, 0x08, 0xE4, 0xB7, 0x1D, 0x63, 0xA9, 0xC0, 0x4F, 0x82,
            0x36, 0xD5,
        ];
        let salt2: &[u8; 16] = &[
            0x9E, 0x42, 0x17, 0xC8, 0x3B, 0xA1, 0xF4, 0x6D, 0x58, 0x0C, 0xE9, 0x72, 0xAD, 0x33,
            0xB6, 0x81,
        ];

        let key_1 = derive_key(passphrase, salt1).unwrap();
        let key_2 = derive_key(passphrase, salt2).unwrap();

        assert_ne!(key_1, key_2);
    }

    #[tokio::test]
    async fn test_derive_key_determinism() {
        let passphrase = "deterministic-test";
        let salt: [u8; 16] = [
            0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45,
            0x67, 0x89,
        ];

        let key_1 = derive_key(passphrase, &salt).unwrap();
        let key_2 = derive_key(passphrase, &salt).unwrap();
        let key_3 = derive_key(passphrase, &salt).unwrap();

        assert_eq!(key_1, key_2);
        assert_eq!(key_2, key_3);
    }

    #[tokio::test]
    async fn test_derive_key_different_passphrases() {
        let salt: [u8; 16] = [0x42; 16];
        let key_1 = derive_key("pass1", &salt).unwrap();
        let key_2 = derive_key("pass2", &salt).unwrap();

        assert_ne!(key_1, key_2);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt() {
        let plaintext = *b"Hello world!";
        let key: [u8; 32] = *b"some-secret-12345678912345678912";

        let encrypted = encrypt(plaintext.to_vec(), &key).unwrap();
        let decrypted = decrypt(Cursor::new(encrypted), &key).await.unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_invalid_key() {
        let plaintext = *b"Hello world!";
        let key1: [u8; 32] = *b"some-secret-12345678912345678912";
        let key2: [u8; 32] = *b"some-incorrect-secret-2345678912";

        let encrypted = encrypt(plaintext.to_vec(), &key1).unwrap();
        let result = decrypt(Cursor::new(encrypted), &key2).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_empty_data() {
        let key: [u8; 32] = *b"empty-data-test-key-123456789012";
        let plaintext: Vec<u8> = Vec::new();

        let encrypted = encrypt(plaintext.clone(), &key).unwrap();
        let decrypted = decrypt(Cursor::new(encrypted), &key).await.unwrap();

        assert_eq!(decrypted, plaintext);
        assert_eq!(decrypted.len(), 0);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_binary_data() {
        let key: [u8; 32] = *b"binary-data-test-key-12345678901";
        let plaintext: Vec<u8> = (0..=255).cycle().take(512).collect();

        let encrypted = encrypt(plaintext.clone(), &key).unwrap();
        let decrypted = decrypt(Cursor::new(encrypted), &key).await.unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_decrypt_wrong_nonce() {
        let key: [u8; 32] = *b"wrong-nonce-test-key-12345678901";
        let plaintext = b"secret message";

        let encrypted = encrypt(plaintext.to_vec(), &key).unwrap();
        let mut tampered = encrypted.clone();
        tampered[0] ^= 0xFF;

        let result = decrypt(Cursor::new(tampered), &key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decrypt_tampered_ciphertext() {
        let key: [u8; 32] = *b"tamper-test-key-1234567890123456";
        let plaintext = b"do not tamper with this";

        let mut encrypted = encrypt(plaintext.to_vec(), &key).unwrap();
        encrypted[42] ^= 0xFF;

        let result = decrypt(Cursor::new(encrypted), &key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decrypt_truncated_data() {
        let key: [u8; 32] = *b"truncated-test-key-1234567890123";
        let plaintext = b"truncated data test";

        let encrypted = encrypt(plaintext.to_vec(), &key).unwrap();
        let truncated = &encrypted[..6];

        let result = decrypt(Cursor::new(truncated), &key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_unicode() {
        let key: [u8; 32] = *b"unicode-test-key-123456789012345";
        let plaintext = "Hello, 世界! 🌍 Привет! 🎉".as_bytes().to_vec();

        let encrypted = encrypt(plaintext.clone(), &key).unwrap();
        let decrypted = decrypt(Cursor::new(encrypted), &key).await.unwrap();

        assert_eq!(decrypted, plaintext);
        assert_eq!(
            String::from_utf8(decrypted).unwrap(),
            "Hello, 世界! 🌍 Привет! 🎉"
        );
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_multiple_rounds() {
        let key: [u8; 32] = *b"multi-round-test-key-12345678901";
        let plaintext = b"round-trip test data";

        let mut current = plaintext.to_vec();
        for _ in 0..5 {
            let encrypted = encrypt(current.clone(), &key).unwrap();
            current = decrypt(Cursor::new(encrypted), &key).await.unwrap();
        }

        assert_eq!(current, plaintext);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_different_keys_same_data() {
        let key1: [u8; 32] = *b"key-one-for-encryption-123456789";
        let key2: [u8; 32] = *b"key-two-for-encryption-123456789";
        let plaintext = b"shared secret";

        let encrypted1 = encrypt(plaintext.to_vec(), &key1).unwrap();
        let encrypted2 = encrypt(plaintext.to_vec(), &key2).unwrap();

        let decrypted1 = decrypt(Cursor::new(encrypted1.clone()), &key1)
            .await
            .unwrap();
        let decrypted2 = decrypt(Cursor::new(encrypted2.clone()), &key2)
            .await
            .unwrap();

        assert_eq!(decrypted1, plaintext);
        assert_eq!(decrypted2, plaintext);

        assert!(decrypt(Cursor::new(encrypted1), &key2).await.is_err());
        assert!(decrypt(Cursor::new(encrypted2), &key1).await.is_err());
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_file_roundtrip() {
        let tmp_dir = TempDir::new("crypto-test").unwrap();
        let file_path = tmp_dir.path().join("encrypted.bin");
        let passphrase = "file-encryption-passphrase";
        let plaintext = b"file encryption test data";

        encrypt_to_file(&file_path, passphrase, plaintext.to_vec())
            .await
            .unwrap();

        assert!(file_path.exists());

        let decrypted = decrypt_file(&file_path, passphrase).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_file_different_passphrases() {
        let tmp_dir = TempDir::new("crypto-test").unwrap();
        let file_path = tmp_dir.path().join("encrypted.bin");
        let passphrase1 = "correct-passphrase";
        let passphrase2 = "wrong-passphrase";
        let plaintext = b"passphrase test";

        encrypt_to_file(&file_path, passphrase1, plaintext.to_vec())
            .await
            .unwrap();

        let decrypted = decrypt_file(&file_path, passphrase1).await.unwrap();
        assert_eq!(decrypted, plaintext);

        let result = decrypt_file(&file_path, passphrase2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decrypt_file_nonexistent() {
        let result = decrypt_file("/nonexistent/path/encrypted.bin", "any-passphrase").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_file_large_data() {
        let tmp_dir = TempDir::new("crypto-test").unwrap();
        let file_path = tmp_dir.path().join("large-encrypted.bin");
        let passphrase = "large-file-passphrase";
        let plaintext: Vec<u8> = (0..255).cycle().take(500000).collect();

        encrypt_to_file(&file_path, passphrase, plaintext.clone())
            .await
            .unwrap();

        let decrypted = decrypt_file(&file_path, passphrase).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_file_empty_data() {
        let tmp_dir = TempDir::new("crypto-test").unwrap();
        let file_path = tmp_dir.path().join("empty-encrypted.bin");
        let passphrase = "empty-file-passphrase";
        let plaintext: Vec<u8> = vec![];

        encrypt_to_file(&file_path, passphrase, plaintext.clone())
            .await
            .unwrap();

        let decrypted = decrypt_file(&file_path, passphrase).await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_file_format_salt_and_encrypted() {
        let tmp_dir = TempDir::new("crypto-test").unwrap();
        let file_path = tmp_dir.path().join("format-test.bin");
        let passphrase = "format-test";
        let plaintext = b"format";

        encrypt_to_file(&file_path, passphrase, plaintext.to_vec())
            .await
            .unwrap();

        let data = tokio::fs::read(&file_path).await.unwrap();

        assert!(
            data.len() >= 28,
            "File should be at least 28 bytes, got {}",
            data.len()
        );
    }
}
