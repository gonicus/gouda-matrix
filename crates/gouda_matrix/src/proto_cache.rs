use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;

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

    #[error("lock poisoined")]
    Poisoined,

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

impl<T> From<std::sync::PoisonError<T>> for ProtoCacheError {
    fn from(_value: std::sync::PoisonError<T>) -> Self {
        Self::Poisoined
    }
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
    inner: Arc<ProtoCacheInner>,
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
            inner: Arc::new(inner),
        };

        obj.clone().begin_save_interval();

        obj
    }

    fn begin_save_interval(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(SAVE_INTERVAL_SECONDS)).await;
                self.inner.save().await;
            }
        });
    }

    pub fn sync_token(&self) -> Option<String> {
        self.inner
            .sync_token()
            .inspect_err(|err| log::error!("Error retrieving sync token: {err}"))
            .ok()
            .flatten()
    }

    pub fn set_sync_token(&self, sync_token: String) {
        log::debug!("Updating sync token");

        if let Err(err) = self.inner.set_sync_token(sync_token) {
            log::error!("Error caching sync token: {err}");
        }
    }

    pub fn user_status(&self) -> Option<UserStatus> {
        self.inner
            .user_status()
            .inspect_err(|err| log::error!("Error retrieving user status: {err}"))
            .ok()
            .flatten()
    }

    pub fn set_user_status(&self, user_status: UserStatus) {
        log::debug!("Caching user status: {user_status:?}");

        if let Err(err) = self.inner.set_user_status(user_status) {
            log::error!("Error caching user status: {err}");
        }
    }

    /// Caches the specified response content.
    pub fn cache_response_content(&self, content: &ResponseContent) {
        match content {
            ResponseContent::RoomListResponse(room_list) => {
                self.cache_rooms(room_list.room_list.clone());
            }
            ResponseContent::RoomCreatedEvent(room) => {
                self.cache_room(room.clone());
            }
            ResponseContent::RoomLeftEvent(event) => {
                self.remove_room(&event.room_id);
            }
            ResponseContent::RoomChangeEvent(event) => {
                self.update_room(event.clone());
            }
            ResponseContent::UserResponse(user) => {
                self.cache_user(user.clone());
            }
            ResponseContent::UserChangeEvent(event) => {
                self.update_user(event.clone());
            }
            _ => (),
        }
    }

    /// Gets all cached rooms.
    /// None is returned if no rooms have been cached previously.
    pub fn cached_rooms(&self) -> Option<Vec<Room>> {
        self.inner
            .room_list()
            .inspect_err(|err| log::error!("Error retrieving cached room list: {err}"))
            .ok()
            .flatten()
    }

    /// Gets a cached user.
    pub fn cached_user(&self, user_id: impl AsRef<str>) -> Option<User> {
        self.inner
            .get_user(user_id.as_ref())
            .inspect_err(|err| log::error!("Error retrieving cached user: {err}"))
            .ok()
            .flatten()
    }

    /// Cache the specified room.
    fn cache_room(&self, room: Room) {
        log::debug!("Caching room: {room:?}");

        if let Err(err) = self.inner.cache_room(room) {
            log::error!("Error caching room: {err}");
        }
    }

    /// Update an already cached room.
    fn update_room(&self, event: RoomChangeEvent) {
        log::debug!("Updating room: {event:?}");

        if let Err(err) = self.inner.update_room(event) {
            log::error!("Error updating room: {err}");
        }
    }

    /// Caches many rooms.
    fn cache_rooms(&self, rooms: Vec<Room>) {
        log::debug!("Caching room list: {rooms:?}");

        for room in rooms {
            if let Err(err) = self.inner.cache_room(room) {
                log::error!("Error caching room: {err}");
            }
        }
    }

    /// Removes a previously cached room.
    fn remove_room(&self, room_id: impl AsRef<str>) {
        let room_id = room_id.as_ref();
        log::debug!("Removing cached room: {room_id:?}");

        if let Err(err) = self.inner.remove_room(room_id) {
            log::error!("Error removing cached room: {err}");
        }
    }

    /// Caches a user.
    fn cache_user(&self, user: User) {
        log::debug!("Caching user: {user:?}");

        if let Err(err) = self.inner.cache_user(user) {
            log::error!("Error caching user: {err}");
        }
    }

    /// Updates an already cached user, if the user exists.
    fn update_user(&self, event: UserChangeEvent) {
        log::debug!("Updating user: {event:?}");

        if let Err(err) = self.inner.update_user(event) {
            log::error!("Error updating user: {err}");
        }
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
    info: Mutex<Info>,
    /// The rooms that have been cached.
    cached_rooms: Mutex<Option<Vec<Room>>>,
    /// The users that have been cached.
    cached_users: Mutex<Option<Vec<User>>>,
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

        let obj = Self {
            passphrase: cache_passphrase,
            info_file: cache_directory.join(CACHED_INFO_FILE),
            rooms_file: cache_directory.join(CACHED_ROOMS_FILE),
            users_file: cache_directory.join(CACHED_USERS_FILE),

            info: Mutex::new(Info::default()),
            cached_rooms: Mutex::new(None),
            cached_users: Mutex::new(None),
        };

        obj.read_from_file_system().await;

        obj
    }

    async fn initialize_directory(cache_directory: &Path) {
        if let Err(err) = tokio::fs::create_dir_all(cache_directory).await {
            log::error!("Error creating cache directory: {err}");
        }
    }

    async fn read_from_file_system(&self) {
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
    async fn read_info(&self) -> Result<()> {
        log::info!("Reading cache info from: {:?}", &self.info_file);

        let decrypted = crypto::decrypt_file(&self.info_file, &self.passphrase).await?;
        let storage: Info = serde_json::from_slice(&decrypted)?;

        log::info!("Successfully read cache info");

        let mut guard = self.info.lock()?;
        *guard = storage;

        Ok(())
    }

    /// Reads the rooms from the file system.
    async fn read_rooms(&self) -> Result<()> {
        log::info!("Reading cached rooms from: {:?}", &self.rooms_file);

        let encoded = crypto::decrypt_file(&self.rooms_file, &self.passphrase).await?;
        let decoded = decode_proto_messages::<Room>(&encoded)?;

        log::info!("Successfully read cached rooms");

        let mut guard = self.cached_rooms.lock()?;
        *guard = Some(decoded);

        Ok(())
    }

    /// Reads the cached users from the file system.
    async fn read_users(&self) -> Result<()> {
        log::info!("Reading cached users from: {:?}", &self.users_file);

        let encoded = crypto::decrypt_file(&self.users_file, &self.passphrase).await?;
        let decoded = decode_proto_messages::<User>(&encoded)?;

        log::info!("Successfully read cached users");

        let mut guard = self.cached_users.lock()?;
        *guard = Some(decoded);

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

        let encoded = {
            let guard = self.cached_rooms.lock()?;

            let Some(rooms) = guard.as_ref() else {
                log::info!("No rooms to cache, nothing to do");
                return Ok(());
            };

            encode_proto_messages(rooms)
        };

        crypto::encrypt_to_file(&self.rooms_file, &self.passphrase, encoded).await?;

        Ok(())
    }

    async fn write_users(&self) -> Result<()> {
        log::info!("Persisting cached users");

        let encoded = {
            let guard = self.cached_users.lock()?;

            let Some(users) = guard.as_ref() else {
                log::info!("No users to cache, nothing to do");
                return Ok(());
            };

            encode_proto_messages(users)
        };

        crypto::encrypt_to_file(&self.users_file, &self.passphrase, encoded).await?;

        Ok(())
    }

    async fn write_info(&self) -> Result<()> {
        log::info!("Persisting cache info");

        let serialized = {
            let guard = self.info.lock()?;
            serde_json::to_vec(&*guard)?
        };

        crypto::encrypt_to_file(&self.info_file, &self.passphrase, serialized).await?;

        log::info!("Successfully persisted cache info");

        Ok(())
    }

    pub fn sync_token(&self) -> Result<Option<String>> {
        Ok(self.info.lock()?.sync_token.clone())
    }

    pub fn set_sync_token(&self, sync_token: String) -> Result<()> {
        self.info.lock()?.sync_token = Some(sync_token);
        Ok(())
    }

    pub fn user_status(&self) -> Result<Option<UserStatus>> {
        Ok(self.info.lock()?.user_status.clone())
    }

    pub fn set_user_status(&self, user_status: UserStatus) -> Result<()> {
        self.info.lock()?.user_status = Some(user_status);
        Ok(())
    }

    pub fn cache_room(&self, room: Room) -> Result<()> {
        let mut guard = self.cached_rooms.lock()?;
        let rooms = guard.get_or_insert_default();

        if let Some(existing) = rooms.iter_mut().find(|p| p.room_id == room.room_id) {
            *existing = room;
        } else {
            rooms.push(room);
        }

        Ok(())
    }

    pub fn update_room(&self, event: RoomChangeEvent) -> Result<()> {
        let mut guard = self.cached_rooms.lock()?;

        let Some(rooms) = &mut *guard else {
            log::debug!("Room has not been cached before, nothing to do");
            return Ok(());
        };

        let Some(room) = rooms.iter_mut().find(|p| p.room_id == event.room_id) else {
            log::debug!("Room has not been cached before, nothing to do");
            return Ok(());
        };

        event.update_into_room(room);

        Ok(())
    }

    pub fn remove_room(&self, room_id: &str) -> Result<()> {
        let mut guard = self.cached_rooms.lock()?;

        if let Some(rooms) = guard.as_mut() {
            rooms.retain(|f| f.room_id != room_id);
        }

        Ok(())
    }

    pub fn room_list(&self) -> Result<Option<Vec<Room>>> {
        Ok(self.cached_rooms.lock()?.clone())
    }

    pub fn cache_user(&self, user: User) -> Result<()> {
        let mut guard = self.cached_users.lock()?;
        let users = guard.get_or_insert_default();

        if let Some(existing) = users.iter_mut().find(|p| p.user_id == user.user_id) {
            *existing = user;
        } else {
            users.push(user);
        }

        Ok(())
    }

    pub fn update_user(&self, event: UserChangeEvent) -> Result<()> {
        let mut guard = self.cached_users.lock()?;

        let Some(users) = &mut *guard else {
            log::debug!("User has not been cached before, nothing to do");
            return Ok(());
        };

        let Some(user) = users.iter_mut().find(|p| p.user_id == event.user_id) else {
            log::debug!("User has not been cached before, nothing to do");
            return Ok(());
        };

        event.update_into_user(user);

        Ok(())
    }

    pub fn get_user(&self, user_id: &str) -> Result<Option<User>> {
        let guard = self.cached_users.lock()?;

        if let Some(users) = &*guard {
            Ok(users.iter().find(|p| p.user_id == user_id).cloned())
        } else {
            Ok(None)
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

        let cache_inner =
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

        cache_inner
            .set_sync_token(expected_sync_token.clone().unwrap())
            .unwrap();
        cache_inner
            .set_user_status(expected_user_status.clone().unwrap())
            .unwrap();
        cache_inner.cache_room(expected_room.clone()).unwrap();
        cache_inner.cache_user(expected_user.clone()).unwrap();

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

        let cache_inner =
            ProtoCacheInner::from_directory(test_data.cache_dir.clone(), "secret-123".to_owned())
                .await;

        // Act
        cache_inner.read_from_file_system().await;

        // Assert
        assert_eq!(*cache_inner.info.lock().unwrap(), expected_info);
        assert_eq!(
            *cache_inner.cached_rooms.lock().unwrap(),
            Some(expected_rooms)
        );
        assert_eq!(
            *cache_inner.cached_users.lock().unwrap(),
            Some(expected_users)
        );
    }

    #[tokio::test]
    async fn test_proto_cache_sync_token() {
        let TestData { cache, .. } = TestData::new().await;
        cache.set_sync_token("some-sync-token".to_owned());
        assert_eq!(cache.sync_token(), Some("some-sync-token".to_owned()));
    }

    #[tokio::test]
    async fn test_proto_cache_sync_token_none() {
        let TestData { cache, .. } = TestData::new().await;
        assert_eq!(cache.sync_token(), None);
    }

    #[tokio::test]
    async fn test_proto_cache_user_status() {
        let TestData { cache, .. } = TestData::new().await;

        let status = UserStatus {
            state: 1,
            status_message: Some("msg".to_owned()),
        };

        cache.set_user_status(status.clone());

        assert_eq!(cache.user_status(), Some(status));
    }

    #[tokio::test]
    async fn test_proto_cache_user_status_none() {
        let TestData { cache, .. } = TestData::new().await;
        assert_eq!(cache.user_status(), None);
    }

    #[tokio::test]
    async fn test_proto_cache_cached_rooms() {
        let TestData { cache, .. } = TestData::new().await;

        let expected_rooms = vec![
            Room {
                display_name: Some("Room 1".to_owned()),
                ..Default::default()
            },
            Room {
                display_name: Some("Room 2".to_owned()),
                ..Default::default()
            },
        ];

        *cache.inner.cached_rooms.lock().unwrap() = Some(expected_rooms.clone());

        assert_eq!(cache.cached_rooms(), Some(expected_rooms));
    }

    #[tokio::test]
    async fn test_proto_cache_cached_rooms_none() {
        let TestData { cache, .. } = TestData::new().await;
        assert_eq!(cache.cached_rooms(), None);
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_room_list_response() {
        let TestData { cache, .. } = TestData::new().await;

        let rooms = vec![
            Room {
                room_id: "room-1".to_owned(),
                display_name: Some("Room 1".to_owned()),
                ..Default::default()
            },
            Room {
                room_id: "room-2".to_owned(),
                display_name: Some("Room 2".to_owned()),
                ..Default::default()
            },
        ];

        let response = ResponseContent::RoomListResponse(RoomListResponse {
            room_list: rooms.clone(),
        });

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_rooms(), Some(rooms));
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_room_created_event() {
        let TestData { cache, .. } = TestData::new().await;

        let room = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1".to_owned()),
            ..Default::default()
        };

        let response = ResponseContent::RoomCreatedEvent(room.clone());

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_rooms(), Some(vec![room]));
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_room_left_event() {
        let TestData { cache, .. } = TestData::new().await;

        let room = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1".to_owned()),
            ..Default::default()
        };

        let response = ResponseContent::RoomLeftEvent(RoomLeftEvent {
            room_id: "room-1".to_owned(),
            ..Default::default()
        });

        *cache.inner.cached_rooms.lock().unwrap() = Some(vec![room.clone()]);

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_rooms(), Some(vec![]));
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_room_change_event() {
        let TestData { cache, .. } = TestData::new().await;

        let room = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1".to_owned()),
            ..Default::default()
        };

        *cache.inner.cached_rooms.lock().unwrap() = Some(vec![room.clone()]);

        let mut updated_room = room.clone();
        updated_room.display_name = Some("Room 1 New Name".to_owned());

        let response = ResponseContent::RoomChangeEvent(RoomChangeEvent {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1 New Name".to_owned()),
            ..Default::default()
        });

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_rooms(), Some(vec![updated_room]));
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_user_response() {
        let TestData { cache, .. } = TestData::new().await;

        let user = User {
            user_id: "user-1".to_owned(),
            display_name: Some("User 1".to_owned()),
            ..Default::default()
        };

        let response = ResponseContent::UserResponse(user.clone());

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_user("user-1"), Some(user));
    }

    #[tokio::test]
    async fn test_proto_cache_cache_response_content_user_change_event() {
        let TestData { cache, .. } = TestData::new().await;

        let user = User {
            user_id: "user-1".to_owned(),
            display_name: Some("User 1".to_owned()),
            ..Default::default()
        };

        *cache.inner.cached_users.lock().unwrap() = Some(vec![user.clone()]);

        let mut user_updated = user.clone();
        user_updated.display_name = Some("User 1 New Name".to_owned());

        let response = ResponseContent::UserChangeEvent(UserChangeEvent {
            user_id: "user-1".to_owned(),
            display_name: Some("User 1 New Name".to_owned()),
            ..Default::default()
        });

        cache.cache_response_content(&response);

        assert_eq!(cache.cached_user("user-1"), Some(user_updated));
    }

    #[tokio::test]
    async fn test_proto_cache_cached_user() {
        let TestData { cache, .. } = TestData::new().await;

        let user = User {
            user_id: "user-1".to_owned(),
            display_name: Some("User 1".to_owned()),
            ..Default::default()
        };

        *cache.inner.cached_users.lock().unwrap() = Some(vec![user.clone()]);

        assert_eq!(cache.cached_user("user-1"), Some(user));
    }

    #[tokio::test]
    async fn test_proto_cache_cached_user_none() {
        let TestData { cache, .. } = TestData::new().await;
        assert_eq!(cache.cached_user("user-1"), None);
    }
}
