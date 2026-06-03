use std::path::{Path, PathBuf};
use std::sync::Arc;

use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;
use tokio::sync::RwLock;

use crate::{crypto, debug_assert_or_log};

/// Save the cache every x seconds.
const SAVE_INTERVAL_SECONDS: u64 = 300;

const CACHED_INFO_FILE: &str = "info";
const CACHED_ROOMS_FILE: &str = "rooms";
const CACHED_USERS_FILE: &str = "users";

#[derive(thiserror::Error, Debug)]
pub enum ProtoCacheError {
    #[error("decode error")]
    DecodeError,

    #[error("unknown and unexpected error")]
    Unknown,

    #[error("prost decode error: {0}")]
    ProstDecodeError(#[from] prost::DecodeError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("crypto error: {0}")]
    Crypto(#[from] crypto::CryptoError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, ProtoCacheError>;

/// Unlike the `Cache`, the `ProtoCache` is a persistent cache that stores
/// proto-messages sent to the application. This speeds up application startup
/// because the old application state can be read from the file system and we don't have
/// to fetch everything again from the server.
/// This allows us to perform synchronization in the background and only send update
/// events to the application.
/// Since this cache is the central storage location for the application state, it
/// also stores the session sync token in addition to the cached data.
/// This ensures that sync always starts with the current state of the cache at startup.
#[derive(Clone)]
pub struct ProtoCache {
    inner: Arc<RwLock<ProtoCacheInner>>,
}

impl ProtoCache {
    /// Creates a new [`ProtoCache`] object.
    ///
    /// # Arguments
    ///
    /// * `cache_directory` - The absolute path to the persistent cache directory.
    /// * `cache_passphrase` - The passphrase used to encrypt or decrypt the cached data.
    pub async fn new(
        cache_directory: impl Into<PathBuf>,
        cache_passphrase: impl Into<String>,
    ) -> Self {
        let inner =
            ProtoCacheInner::from_directory(cache_directory.into(), cache_passphrase.into()).await;

        let obj = Self {
            inner: Arc::new(RwLock::new(inner)),
        };

        obj.clone().begin_save_interval();

        obj
    }

    fn begin_save_interval(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(SAVE_INTERVAL_SECONDS)).await;
                self.inner.read().await.save().await;
            }
        });
    }

    pub async fn sync_token(&self) -> Option<String> {
        self.inner.read().await.sync_token().clone()
    }

    pub async fn set_sync_token(&self, sync_token: String) {
        log::debug!("Updating sync token");
        *self.inner.write().await.sync_token_mut() = Some(sync_token);
    }

    pub async fn user_status(&self) -> Option<UserStatus> {
        self.inner.read().await.user_status().clone()
    }

    pub async fn set_user_status(&self, user_status: UserStatus) {
        log::debug!("Caching user status: {user_status:?}");
        *self.inner.write().await.user_status_mut() = Some(user_status)
    }

    /// Caches the specified response content.
    pub async fn cache_response_content(&self, content: &ResponseContent) {
        match content {
            ResponseContent::RoomListResponse(room_list) => {
                self.cache_rooms(room_list.room_list.clone()).await;
            }
            ResponseContent::RoomCreatedEvent(room) => {
                self.cache_room(room.clone()).await;
            }
            ResponseContent::RoomLeftEvent(event) => {
                self.remove_room(&event.room_id).await;
            }
            ResponseContent::RoomChangeEvent(event) => {
                self.update_room(event.clone()).await;
            }
            ResponseContent::UserResponse(user) => {
                self.cache_user(user.clone()).await;
            }
            ResponseContent::UserChangeEvent(event) => {
                self.update_user(event.clone()).await;
            }
            _ => (),
        }
    }

    /// Gets all cached rooms.
    /// None is returned if no rooms have been cached previously.
    pub async fn cached_rooms(&self) -> Option<Vec<Room>> {
        let reader = self.inner.read().await;
        let rooms = reader.room_list()?;
        Some(rooms.clone())
    }

    /// Gets a cached user.
    pub async fn cached_user(&self, user_id: impl AsRef<str>) -> Option<User> {
        self.inner.read().await.get_user(user_id.as_ref()).cloned()
    }

    /// Cache the specified room.
    async fn cache_room(&self, room: Room) {
        log::debug!("Caching room: {room:?}");
        self.inner.write().await.cache_room(room);
    }

    /// Update an already cached room.
    async fn update_room(&self, event: RoomChangeEvent) {
        log::debug!("Updating room: {event:?}");
        self.inner.write().await.update_room(event);
    }

    /// Caches many rooms.
    async fn cache_rooms(&self, rooms: Vec<Room>) {
        log::debug!("Caching room list: {rooms:?}");
        let mut writer = self.inner.write().await;
        for room in rooms {
            writer.cache_room(room);
        }
    }

    /// Removes a previously cached room.
    async fn remove_room(&self, room_id: impl AsRef<str>) {
        let room_id = room_id.as_ref();
        log::debug!("Removing cached room: {room_id:?}");
        self.inner.write().await.remove_room(room_id);
    }

    /// Caches a user.
    async fn cache_user(&self, user: User) {
        log::debug!("Caching user: {user:?}");
        self.inner.write().await.cache_user(user);
    }

    /// Updates an already cached user, if the user exists.
    async fn update_user(&self, event: UserChangeEvent) {
        log::debug!("Updating user: {event:?}");
        self.inner.write().await.update_user(event);
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct Info {
    /// The sync token of the cache's current state.
    pub(crate) sync_token: Option<String>,
    /// The currently set user status including presence state and status message.
    /// This is persisted on the storage so the status doesn't reset when
    /// the application restarts. The user status is none if the user did not set any status
    /// manually or the status was set by another session.
    pub(crate) user_status: Option<UserStatus>,
}

#[derive(Default)]
struct ProtoCacheInner {
    /// The passphrase used to encrypt the cache.
    passphrase: String,
    /// The absolute path to the info file.
    info_file: PathBuf,
    /// The absolute path to the file where the cached rooms are stored.
    rooms_file: PathBuf,
    /// The absolute path to the file where the cached users are stored.
    users_file: PathBuf,

    /// The persistent stored data.
    info: Info,
    /// The rooms that have been cached.
    cached_rooms: Option<Vec<Room>>,
    /// The users that have been cached.
    cached_users: Option<Vec<User>>,
}

impl ProtoCacheInner {
    /// Creates a new [`ProtoCacheInner`] object.
    ///
    /// # Arguments
    ///
    /// * `cache_directory` - The absolute path to the persistent cache directory.
    /// * `cache_passphrase` - The passphrase used to encrypt or decrypt the cached data.
    pub async fn from_directory(cache_directory: PathBuf, cache_passphrase: String) -> Self {
        Self::initialize_directory(&cache_directory).await;

        debug_assert_or_log!(
            cache_directory.is_absolute(),
            "Received a relative cache directory"
        );

        let mut obj = Self {
            passphrase: cache_passphrase,
            info_file: cache_directory.join(CACHED_INFO_FILE),
            rooms_file: cache_directory.join(CACHED_ROOMS_FILE),
            users_file: cache_directory.join(CACHED_USERS_FILE),

            info: Info::default(),
            cached_rooms: None,
            cached_users: None,
        };

        obj.read_from_file_system().await;

        obj
    }

    async fn initialize_directory(cache_directory: &Path) {
        if let Err(err) = tokio::fs::create_dir_all(cache_directory).await {
            log::error!("Error creating cache directory: {err}");
        }
    }

    async fn read_from_file_system(&mut self) {
        if let Err(err) = self.read_info().await {
            log::error!("Error reading cache info from the file system: {err}");
        }

        if let Err(err) = self.read_rooms().await {
            log::error!("Error reading cached rooms from the file system: {err}");
        }

        if let Err(err) = self.read_users().await {
            log::error!("Error reading cached users from the file system: {err}");
        }
    }

    /// Reads the cache information from the file system.
    async fn read_info(&mut self) -> Result<()> {
        log::info!("Reading cache info from: {:?}", &self.info_file);

        let decrypted = crypto::decrypt_file(&self.info_file, &self.passphrase).await?;
        let storage: Info = serde_json::from_slice(&decrypted)?;

        log::info!("Successfully read cache info");

        self.info = storage;

        Ok(())
    }

    /// Reads the rooms from the file system.
    async fn read_rooms(&mut self) -> Result<()> {
        log::info!("Reading cached rooms from: {:?}", &self.rooms_file);

        let encoded = crypto::decrypt_file(&self.rooms_file, &self.passphrase).await?;
        let decoded = decode_proto_messages::<Room>(&encoded)?;

        log::info!("Successfully read cached rooms");

        self.cached_rooms = Some(decoded);

        Ok(())
    }

    /// Reads the cached users from the file system.
    async fn read_users(&mut self) -> Result<()> {
        log::info!("Reading cached users from: {:?}", &self.users_file);

        let encoded = crypto::decrypt_file(&self.users_file, &self.passphrase).await?;
        let decoded = decode_proto_messages::<User>(&encoded)?;

        log::info!("Successfully read cached users");

        self.cached_users = Some(decoded);

        Ok(())
    }

    /// Saves the cache to the file system.
    pub async fn save(&self) {
        log::info!("Persisting cache");

        // NOTE: All data must be saved before the info.
        // This ensures that the state of the data always corresponds historically
        // to the same point in time or in the future of the stored sync token.
        // We would loose information when the stored data is behind the sync token.
        // This can happen, for example, if errors occur or the application closes
        // during saving.

        if let Err(err) = self.write_rooms().await {
            log::error!("Error persisting cached rooms: {err}");
            return;
        }

        if let Err(err) = self.write_users().await {
            log::error!("Error persisting cached users: {err}");
            return;
        }

        if let Err(err) = self.write_info().await {
            log::error!("Error persisting cache info: {err}");
        }
    }

    async fn write_rooms(&self) -> Result<()> {
        log::info!("Persisting cached rooms");

        let Some(rooms) = &self.cached_rooms else {
            log::info!("No rooms to cache, nothing to do");
            return Ok(());
        };

        let encoded = encode_proto_messages(rooms);
        crypto::encrypt_to_file(&self.rooms_file, &self.passphrase, encoded).await?;

        Ok(())
    }

    async fn write_users(&self) -> Result<()> {
        log::info!("Persisting cached users");

        let Some(users) = &self.cached_users else {
            log::info!("No users to cache, nothing to do");
            return Ok(());
        };

        let encoded = encode_proto_messages(users);
        crypto::encrypt_to_file(&self.users_file, &self.passphrase, encoded).await?;

        Ok(())
    }

    async fn write_info(&self) -> Result<()> {
        log::info!("Persisting cache info");

        let serialized = serde_json::to_vec(&self.info)?;
        crypto::encrypt_to_file(&self.info_file, &self.passphrase, serialized).await?;

        log::info!("Successfully persisted cache info");

        Ok(())
    }

    pub fn sync_token(&self) -> &Option<String> {
        &self.info.sync_token
    }

    pub fn sync_token_mut(&mut self) -> &mut Option<String> {
        &mut self.info.sync_token
    }

    pub fn user_status(&self) -> &Option<UserStatus> {
        &self.info.user_status
    }

    pub fn user_status_mut(&mut self) -> &mut Option<UserStatus> {
        &mut self.info.user_status
    }

    pub fn cache_room(&mut self, room: Room) {
        let rooms = self.cached_rooms.get_or_insert_default();

        if let Some(existing) = rooms.iter_mut().find(|p| p.room_id == room.room_id) {
            *existing = room;
        } else {
            rooms.push(room);
        }
    }

    pub fn update_room(&mut self, event: RoomChangeEvent) {
        let Some(room) = self.get_room_mut(&event.room_id) else {
            log::debug!("Room has not been cached before, nothing to do");
            return;
        };

        event.update_room(room);
    }

    pub fn remove_room(&mut self, room_id: &str) {
        if let Some(rooms) = &mut self.cached_rooms {
            rooms.retain(|f| f.room_id != room_id);
        }
    }

    pub fn room_list(&self) -> Option<&Vec<Room>> {
        self.cached_rooms.as_ref()
    }

    fn get_room_mut(&mut self, room_id: &str) -> Option<&mut Room> {
        if let Some(rooms) = &mut self.cached_rooms {
            rooms.iter_mut().find(|p| p.room_id == room_id)
        } else {
            None
        }
    }

    pub fn cache_user(&mut self, user: User) {
        let users = self.cached_users.get_or_insert_default();

        if let Some(existing) = users.iter_mut().find(|p| p.user_id == user.user_id) {
            *existing = user;
        } else {
            users.push(user);
        }
    }

    pub fn update_user(&mut self, event: UserChangeEvent) {
        let Some(user) = self.get_user_mut(&event.user_id) else {
            log::debug!("User has not been cached before, nothing to do");
            return;
        };

        event.update_user(user);
    }

    pub fn get_user(&self, user_id: &str) -> Option<&User> {
        if let Some(users) = &self.cached_users {
            users.iter().find(|p| p.user_id == user_id)
        } else {
            None
        }
    }

    fn get_user_mut(&mut self, user_id: &str) -> Option<&mut User> {
        if let Some(users) = &mut self.cached_users {
            users.iter_mut().find(|p| p.user_id == user_id)
        } else {
            None
        }
    }
}

/// Encodes a list of proto messages.
fn encode_proto_messages<T: prost::Message>(messages: &[T]) -> Vec<u8> {
    let mut result = Vec::new();

    for msg in messages {
        let mut encoded = msg.encode_to_vec();
        let size = (encoded.len() as u64).to_le_bytes();
        result.extend(size);
        result.append(&mut encoded);
    }

    result
}

/// Decodes a list of proto messages.
fn decode_proto_messages<T: prost::Message + Default>(mut encoded: &[u8]) -> Result<Vec<T>> {
    let mut result = Vec::new();

    while !encoded.is_empty() {
        if encoded.len() < 8 {
            log::error!("Error decoding proto messages: not enough bytes to fill size buffer");
            return Err(ProtoCacheError::DecodeError);
        }

        let bytes = encoded[..8]
            .try_into()
            .map_err(|_| ProtoCacheError::Unknown)?;
        let size = u64::from_le_bytes(bytes) as usize;
        encoded = &encoded[8..];

        if encoded.len() < size {
            log::error!("Error decoding proto messages: not enough bytes to fill object buffer");
            return Err(ProtoCacheError::DecodeError);
        }

        let message = &encoded[..size];
        result.push(T::decode(message)?);

        encoded = &encoded[size..];
    }

    Ok(result)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use gouda_proto::chat::LoginUsernamePasswordRequest;

    use super::*;

    const TEMPDIR_PREFIX: &str = "gouda_matrix_adapter";

    struct TestData {
        pub cache: ProtoCache,
        pub cache_dir: PathBuf,
        _temp_dir: tempdir::TempDir,
    }

    impl TestData {
        pub async fn new() -> TestData {
            let temp_dir = tempdir::TempDir::new(TEMPDIR_PREFIX).unwrap();
            let cache_dir = temp_dir.path().join("cache");

            Self {
                cache: ProtoCache::new(cache_dir.clone(), "secret123").await,
                cache_dir,
                _temp_dir: temp_dir,
            }
        }

        pub async fn read_info(&self, secret: &str) -> Info {
            let encoded = crypto::decrypt_file(self.info_path(), secret)
                .await
                .unwrap();

            serde_json::from_slice(&encoded).unwrap()
        }

        pub async fn write_info(&self, info: Info, secret: &str) {
            let encoded = serde_json::to_vec(&info).unwrap();
            crypto::encrypt_to_file(self.info_path(), secret, encoded)
                .await
                .unwrap();
        }

        pub async fn read_rooms(&self, secret: &str) -> Vec<Room> {
            let encoded = crypto::decrypt_file(self.rooms_path(), secret)
                .await
                .unwrap();

            decode_proto_messages::<Room>(&encoded).unwrap()
        }

        pub async fn write_rooms(&self, rooms: Vec<Room>, secret: &str) {
            let encoded = encode_proto_messages::<Room>(&rooms);
            crypto::encrypt_to_file(self.rooms_path(), secret, encoded)
                .await
                .unwrap();
        }

        pub async fn read_users(&self, secret: &str) -> Vec<User> {
            let encoded = crypto::decrypt_file(self.users_path(), secret)
                .await
                .unwrap();

            decode_proto_messages::<User>(&encoded).unwrap()
        }

        pub async fn write_users(&self, users: Vec<User>, secret: &str) {
            let encoded = encode_proto_messages::<User>(&users);
            crypto::encrypt_to_file(self.users_path(), secret, encoded)
                .await
                .unwrap();
        }

        fn info_path(&self) -> PathBuf {
            self.cache_dir.join(CACHED_INFO_FILE)
        }

        fn rooms_path(&self) -> PathBuf {
            self.cache_dir.join(CACHED_ROOMS_FILE)
        }

        fn users_path(&self) -> PathBuf {
            self.cache_dir.join(CACHED_USERS_FILE)
        }
    }

    #[test]
    fn test_encode_proto_messages() {
        // Arrange
        let messages = vec![
            LoginUsernamePasswordRequest {
                password: "myverygoodsecret".to_owned(),
                username: "someuser1".to_owned(),
            },
            LoginUsernamePasswordRequest {
                password: "myverygoodsecret2".to_owned(),
                username: "someuser2".to_owned(),
            },
        ];

        #[rustfmt::skip]
        let data: &[u8] = &[
            // Size 1
            0x1D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Message 1
            0x0A, 0x09, 0x73, 0x6F, 0x6D, 0x65, 0x75, 0x73, 0x65, 0x72, 0x31, 0x12, 0x10, 0x6D,
            0x79, 0x76, 0x65, 0x72, 0x79, 0x67, 0x6F, 0x6F, 0x64, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74,
            // Size 2
            0x1E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Message 2
            0x0A, 0x09, 0x73, 0x6F, 0x6D, 0x65, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x11, 0x6D,
            0x79, 0x76, 0x65, 0x72, 0x79, 0x67, 0x6F, 0x6F, 0x64, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x32
        ];

        // Act
        let encoded = encode_proto_messages(&messages);

        // Assert
        assert_eq!(encoded, data);
    }

    #[test]
    fn test_decode_proto_messages() {
        // Arrange
        #[rustfmt::skip]
        let encoded: &[u8] = &[
            // Size 1
            0x1D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Message 1
            0x0A, 0x09, 0x73, 0x6F, 0x6D, 0x65, 0x75, 0x73, 0x65, 0x72, 0x31, 0x12, 0x10, 0x6D,
            0x79, 0x76, 0x65, 0x72, 0x79, 0x67, 0x6F, 0x6F, 0x64, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74,
            // Size 2
            0x1E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Message 2
            0x0A, 0x09, 0x73, 0x6F, 0x6D, 0x65, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x11, 0x6D,
            0x79, 0x76, 0x65, 0x72, 0x79, 0x67, 0x6F, 0x6F, 0x64, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x32
        ];

        let expected = vec![
            LoginUsernamePasswordRequest {
                password: "myverygoodsecret".to_owned(),
                username: "someuser1".to_owned(),
            },
            LoginUsernamePasswordRequest {
                password: "myverygoodsecret2".to_owned(),
                username: "someuser2".to_owned(),
            },
        ];

        // Act
        let decoded = decode_proto_messages::<LoginUsernamePasswordRequest>(encoded).unwrap();

        // Assert
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn test_proto_cache_inner_save() {
        // Arrange
        let test_data = TestData::new().await;

        let mut cache_inner =
            ProtoCacheInner::from_directory(test_data.cache_dir.clone(), "secret-123".to_owned())
                .await;

        let expected_sync_token = Some("some-sync-token".to_owned());
        let expected_user_status = Some(UserStatus {
            state: 1,
            status_message: Some("Hello World".to_owned()),
        });

        let expected_room = Room {
            display_name: Some("Room 1".to_owned()),
            ..Default::default()
        };

        let expected_user = User {
            display_name: Some("User 1".to_owned()),
            ..Default::default()
        };

        *cache_inner.sync_token_mut() = expected_sync_token.clone();
        *cache_inner.user_status_mut() = expected_user_status.clone();
        cache_inner.cache_room(expected_room.clone());
        cache_inner.cache_user(expected_user.clone());

        // Act
        cache_inner.save().await;

        // Assert
        let info = test_data.read_info("secret-123").await;
        let rooms = test_data.read_rooms("secret-123").await;
        let users = test_data.read_users("secret-123").await;

        assert_eq!(
            info,
            Info {
                sync_token: expected_sync_token,
                user_status: expected_user_status
            }
        );
        assert_eq!(rooms, vec![expected_room]);
        assert_eq!(users, vec![expected_user]);
    }

    #[tokio::test]
    async fn test_proto_cache_inner_load() {
        // Arrange
        let test_data = TestData::new().await;

        let expected_info = Info {
            sync_token: Some("some-sync-token".to_owned()),
            user_status: Some(UserStatus {
                state: 2,
                status_message: Some("Msg".to_owned()),
            }),
        };

        let expected_rooms = vec![Room {
            display_name: Some("Room 1".to_owned()),
            ..Default::default()
        }];

        let expected_users = vec![User {
            display_name: Some("User 1".to_owned()),
            ..Default::default()
        }];

        test_data
            .write_info(expected_info.clone(), "secret-123")
            .await;
        test_data
            .write_rooms(expected_rooms.clone(), "secret-123")
            .await;
        test_data
            .write_users(expected_users.clone(), "secret-123")
            .await;

        let mut cache_inner =
            ProtoCacheInner::from_directory(test_data.cache_dir.clone(), "secret-123".to_owned())
                .await;

        // Act
        cache_inner.read_from_file_system().await;

        // Assert
        assert_eq!(cache_inner.info, expected_info);
        assert_eq!(cache_inner.cached_rooms, Some(expected_rooms));
        assert_eq!(cache_inner.cached_users, Some(expected_users));
    }

    #[tokio::test]
    async fn test_proto_cache_sync_token() {
        let TestData { cache, .. } = TestData::new().await;

        cache.set_sync_token("some-sync-token".to_owned()).await;

        // TODO: Test saving

        assert_eq!(cache.sync_token().await, Some("some-sync-token".to_owned()));
    }
}
