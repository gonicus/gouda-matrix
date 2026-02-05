use std::path::{Path, PathBuf};

use async_trait::async_trait;
use matrix_sdk::deserialized_responses::SyncOrStrippedState;
use matrix_sdk::media::{MediaFormat, MediaRequestParameters};
use matrix_sdk::ruma::events::room::avatar::RoomAvatarEventContent;
use matrix_sdk::ruma::events::room::MediaSource;
use matrix_sdk::Room;
use thiserror::Error;
use tokio::fs;

use crate::{unwrap_or_log_return_err, unwrap_or_log_return_option, utils};

const INFO_FILE_SUFFIX: &str = "_info";

const ROOM_AVATARS_FOLDER: &str = "room_avatars";

#[derive(Error, Debug)]
pub enum MediaError {
    #[error("the requested resource was not found")]
    NotFound,

    #[error("the previously downloaded resource has been removed from the upstream server")]
    Removed,

    #[error("the file extension of the data could not be determined")]
    UnableToDetermineFileExtension,

    #[error("the mime type of the data could not be determined")]
    UnableToDetermineMimeType,

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("serde json error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("matrix error")]
    MatrixError(#[from] matrix_sdk::Error),
}

type Result<T> = std::result::Result<T, MediaError>;

#[derive(Clone, Debug)]
pub struct MediaManager {
    /// The root directory where data should be stored.
    data_root_dir: PathBuf,
    /// The relative path from the `data_root_dir` where room avatars are stored.
    room_avatars_dir: PathBuf,
}

impl MediaManager {
    pub async fn new(data_root_dir: impl Into<PathBuf>) -> Self {
        let obj = Self {
            data_root_dir: data_root_dir.into(),
            room_avatars_dir: PathBuf::from(ROOM_AVATARS_FOLDER),
        };

        obj.init_dirs().await
    }

    /// Creates all necessary data directories if they do not already exist.
    async fn init_dirs(self) -> Self {
        self.init_dir(&self.data_root_dir.join(&self.room_avatars_dir))
            .await;
        self
    }

    /// Creates a directory if it does not already exist.
    async fn init_dir(&self, dir: &Path) {
        if let Err(err) = fs::create_dir(dir).await {
            if err.kind() != tokio::io::ErrorKind::AlreadyExists {
                log::error!("Error initializing directory {dir:?}: {err}");
            }
        } else {
            log::info!("Initialized directory: {dir:?}");
        }
    }

    /// Returns the relative path to the room's avatar, starting in the
    /// data root directory. This method downloads the avatar
    /// if it doesn't exist, or updates the existing one if a new avatar
    /// has been uploaded to the Matrix server.
    /// Returns None if no avatar is set for the room.
    pub async fn get_room_avatar_path(&self, room: &Room) -> Option<String> {
        log::debug!("Receiving avatar for room: {}", room.room_id());

        let mut asset = RoomAvatarAsset::new(room.clone());

        if !asset.is_available().await {
            log::debug!("No avatar event found for the room");
            return None;
        }

        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.room_avatars_dir.clone(),
            Box::new(asset),
        );

        asset_manager.download().await.ok()
    }

    /// Uploads a new avatar to the specified room.
    /// This method moves the avatar to the correct directory after upload.
    ///
    /// # Arguments
    ///
    /// * `room` - The room for which the avatar should be uploaded.
    /// * `avatar_path` - The relative path to the avatar starting from
    ///   the data root directory.
    ///
    /// # Returns
    ///
    /// Returns the relative path starting from the data root directory to the
    /// uploaded asset.
    /// Note that this path will not match the passed `avatar_path`, as the avatar
    /// will be moved to the correct directory after upload.
    pub async fn upload_room_avatar(
        &self,
        room: &Room,
        avatar_path: impl AsRef<Path>,
    ) -> Result<String> {
        let src = avatar_path.as_ref();

        log::info!("Uploading room avatar {src:?} to room {}", room.room_id());

        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.room_avatars_dir.clone(),
            Box::new(RoomAvatarAsset::new(room.clone())),
        );

        asset_manager.upload(src).await
    }
}

/// Manages a single asset.
struct AssetManager {
    /// The root directory where data is stored.
    data_root_dir: PathBuf,
    /// The relative path starting from the `data_root_dir` to the directory
    /// where the asset is stored.
    asset_dir: PathBuf,
    /// The asset that is being managed.
    asset: Box<dyn Asset>,
}

impl AssetManager {
    /// Creates a new asset manager.
    ///
    /// # Arguments
    ///
    /// * `data_root_dir` - The absolute path to the data root directory.
    /// * `asset_dir` - The relative path to the asset directory,
    ///   starting from the data root directory.
    /// * `asset` - The asset that is being managed.
    pub fn new(
        data_root_dir: impl Into<PathBuf>,
        asset_dir: impl Into<PathBuf>,
        asset: Box<dyn Asset>,
    ) -> Self {
        Self {
            data_root_dir: data_root_dir.into(),
            asset_dir: asset_dir.into(),
            asset,
        }
    }

    /// Downloads the asset if it hasn't been done already.
    /// This method will check if the previously downloaded asset is
    /// still up-to-date and if not, downloads the updated asset from
    /// the upstream server.
    /// Returns the relative file path starting from the data root
    /// directory to the downloaded asset.
    pub async fn download(&mut self) -> Result<String> {
        if let Some(path) = self.has_been_downloaded_before().await {
            path
        } else {
            self.download_asset().await
        }
    }

    /// Uploads the specified asset to the upstream server and replaces
    /// the current asset, if one exists.
    /// Returns the relative path starting from the data root directory
    /// to the uploaded asset.
    /// Note that this method will move the source file to the correct asset
    /// directory after upload.
    pub async fn upload(&mut self, src: impl Into<PathBuf>) -> Result<String> {
        let src = self.data_root_dir.join(src.into());

        log::debug!("Uploading asset: {src:?}");

        let extension = self.asset.upload(src.clone()).await?;

        log::debug!("Successfully uploaded asset");

        if let Some(info) = self.read_info().await {
            log::debug!(
                "Old version of the asset exists, replacing it with the newly uploaded asset"
            );
            self.delete_asset_and_info(&info).await;
        }

        let asset_file_name = self.get_asset_file_name(&extension);
        let info = AssetInfo {
            file: asset_file_name.clone(),
            download_ts: utils::get_unix_timestamp_seconds(),
        };

        let info_path_absolute = self.get_info_path_absolute();
        let asset_path_absolute = self.get_asset_path_absolute(&asset_file_name);

        log::debug!("Moving uploaded asset from {src:?} to {asset_path_absolute:?}");
        tokio::fs::rename(src, asset_path_absolute).await?;

        self.write_info(&info_path_absolute, info).await?;

        let asset_path_relative = self.get_asset_path_relative(&asset_file_name);

        Ok(asset_path_relative.to_string_lossy().to_string())
    }

    /// Checks if the asset has been downloaded before and if so, if it is
    /// still up-to-date.
    async fn has_been_downloaded_before(&mut self) -> Option<Result<String>> {
        let Some(info) = self.read_info().await else {
            log::debug!("Asset hasn't been downloaded yet");
            return None;
        };

        log::debug!("Asset has already been downloaded before");

        if self.is_up_to_date(info.download_ts).await {
            log::debug!("Asset is still up-to-date");
            let path = self.get_asset_path_relative(&info.file);
            return Some(Ok(path.to_string_lossy().to_string()));
        }

        log::debug!("Downloaded asset is no longer up-to-date");

        if self.asset.was_removed().await {
            log::debug!("Asset was removed from upstream server");
            self.delete_asset_and_info(&info).await;
            Some(Err(MediaError::Removed))
        } else {
            None
        }
    }

    /// Checks if the asset downloaded at the specified timestamp is up-to-date.
    async fn is_up_to_date(&mut self, downloaded_at: u64) -> bool {
        let Ok(upload_ts) = self.asset.upload_timestamp().await else {
            log::warn!(
                "Unable to retrieve the assets upload timestamp, assuming it's still up-to-date"
            );

            return true;
        };

        upload_ts <= downloaded_at
    }

    /// Deletes the downloaded asset as well as its info file from the file system.
    async fn delete_asset_and_info(&mut self, info: &AssetInfo) {
        log::info!("Deleting asset with ID '{}'", self.asset.asset_id());

        let info_path = self.get_info_path_absolute();
        let asset_path = self.get_asset_path_absolute(&info.file);

        log::debug!("Deleting asset info file: {info_path:?}");

        if let Err(err) = tokio::fs::remove_file(info_path).await {
            log::error!("Error deleting asset info file: {err}");
        }

        log::debug!("Deleting asset file: {asset_path:?}");

        if let Err(err) = tokio::fs::remove_file(asset_path).await {
            log::error!("Error deleting asset file: {err}");
        }
    }

    /// Downloads the actual asset using `Self::downloader`.
    /// Returns the relative path from the data root directory to the downloaded asset.
    /// This method writes the downloaded asset as well as its info to the file system.
    /// If the asset has already been downloaded, it will be overwritten with
    /// the newly downloaded asset.
    async fn download_asset(&mut self) -> Result<String> {
        log::debug!("Downloading asset");

        let (data, extension) = self.asset.download().await?;

        if let Some(existing_info) = self.read_info().await {
            log::debug!("Replacing the already downloaded asset");
            self.delete_asset_and_info(&existing_info).await;
        }

        let data_file_name = self.get_asset_file_name(&extension);

        let info = AssetInfo {
            file: data_file_name.clone(),
            download_ts: utils::get_unix_timestamp_seconds(),
        };

        self.write_info_and_asset(info, data).await?;

        let relative_asset_path = self.get_asset_path_relative(&data_file_name);

        Ok(relative_asset_path.to_string_lossy().to_string())
    }

    /// Reads the asset information from the file system.
    /// Returns None if the requested asset hasn't been downloaded yet.
    async fn read_info(&self) -> Option<AssetInfo> {
        let path = self.get_info_path_absolute();

        if !path.exists() {
            return None;
        }

        let content = unwrap_or_log_return_option!(
            tokio::fs::read(&path).await,
            format!("Error reading asset info at {path:?}")
        );

        let info: AssetInfo = unwrap_or_log_return_option!(
            serde_json::from_slice(&content),
            format!("Error deserializing asset info at {path:?}")
        );

        Some(info)
    }

    /// Writes the asset information as well as the actual downloaded asset
    /// to the file system.
    async fn write_info_and_asset(&self, info: AssetInfo, asset: Vec<u8>) -> Result<()> {
        let info_path = self.get_info_path_absolute();
        let item_path = self.get_asset_path_absolute(&info.file);

        self.write_asset(&item_path, &asset).await?;
        self.write_info(&info_path, info).await?;

        Ok(())
    }

    /// Writes the asset information to the file system.
    async fn write_info(&self, path: &Path, info: AssetInfo) -> Result<()> {
        log::debug!("Writing asset info {info:?} to: {path:?}");

        let serialized = unwrap_or_log_return_err!(
            serde_json::to_string_pretty(&info),
            "Error serializing asset information"
        );

        unwrap_or_log_return_err!(
            tokio::fs::write(path, serialized).await,
            "Error writing asset information"
        );

        Ok(())
    }

    /// Writes the asset to the file system.
    async fn write_asset(&self, path: &Path, data: &[u8]) -> Result<()> {
        log::debug!("Writing asset to: {path:?}");

        unwrap_or_log_return_err!(tokio::fs::write(path, data).await, "Error writing asset");

        Ok(())
    }

    /// Gets the file name of the asset information file.
    fn get_info_file_name(&self) -> String {
        format!("{}{INFO_FILE_SUFFIX}.json", self.asset.asset_id())
    }

    /// Gets the file name of the downloaded asset.
    fn get_asset_file_name(&self, extension: &str) -> String {
        format!("{}.{}", self.asset.asset_id(), extension)
    }

    /// Builds the relative path to the download directory starting from
    /// the data root directory.
    fn get_download_dir_relative(&self) -> PathBuf {
        self.asset_dir.clone()
    }

    /// Gets the absolute path to the download directory.
    fn get_download_dir_absolute(&self) -> PathBuf {
        self.data_root_dir.join(self.get_download_dir_relative())
    }

    /// Gets the absolute path to the asset information file.
    fn get_info_path_absolute(&self) -> PathBuf {
        let file_name = self.get_info_file_name();
        self.get_download_dir_absolute().join(file_name)
    }

    /// Gets the relative path to the asset information file starting
    /// from the data root directory.
    fn get_info_path_relative(&self) -> PathBuf {
        let file_name = self.get_info_file_name();
        self.get_download_dir_relative().join(file_name)
    }

    /// Gets the absolute path to the asset.
    fn get_asset_path_absolute(&self, file_name: &str) -> PathBuf {
        let mut path = self.get_info_path_absolute();
        path.set_file_name(file_name);
        path
    }

    /// Gets the relative path to the asset starting from the data root directory.
    fn get_asset_path_relative(&self, file_name: &str) -> PathBuf {
        let mut path = self.get_info_path_relative();
        path.set_file_name(file_name);
        path
    }
}

/// Contains metadata of a downloaded asset, which is to be permanently
/// stored alongside the actual downloaded asset.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct AssetInfo {
    /// The name of the downloaded asset file, which is located in the same
    /// folder as this information.
    file: String,
    /// The UNIX timestamp in seconds at which we downloaded the asset.
    /// This is used to check if the asset is still up-to-date.
    download_ts: u64,
}

/// Trait that represents an object that actually implements an asset upload
/// and download.
#[async_trait]
trait Asset: Send + Sync {
    /// Gets the ID of the asset.
    fn asset_id(&self) -> &str;

    /// Retrieves the timestamp of when the asset was uploaded to the upstream server.
    async fn upload_timestamp(&mut self) -> Result<u64>;

    /// If the asset was removed from the upstream server.
    async fn was_removed(&mut self) -> bool;

    /// Downloads the asset from the upstream server.
    /// The first value of the tuple is the downloaded asset, while the second
    /// specifies the file extension to be used.
    /// Returns None if the asset was not found on the upstream server or if
    /// a download error occurred.
    async fn download(&mut self) -> Result<(Vec<u8>, String)>;

    /// Uploads the specified asset to the upstream server.
    /// Returns the file extension to be used for the asset file.
    async fn upload(&mut self, src: PathBuf) -> Result<String>;
}

/// Manages the download and upload of an avatar from a specific room.
struct RoomAvatarAsset {
    /// The room from which the avatar is to be downloaded or uploaded.
    room: Room,
    /// Cached `RoomAvatarEventContent` and its origin server timestamp in seconds.
    cached_event: Option<(RoomAvatarEventContent, u64)>,
}

impl RoomAvatarAsset {
    pub fn new(room: Room) -> Self {
        Self {
            room,
            cached_event: None,
        }
    }

    async fn is_available(&mut self) -> bool {
        self.get_avatar_event_content().await.is_some()
    }

    /// Retrieves the content of the last room avatar event, including the server timestamp
    /// of the event in seconds.
    async fn get_avatar_event_content(&mut self) -> Option<(RoomAvatarEventContent, u64)> {
        if let Some(cached) = self.cached_event.clone() {
            return Some(cached);
        }

        let event = self
            .room
            .get_state_event_static::<RoomAvatarEventContent>()
            .await
            .ok()??
            .deserialize()
            .ok()?;

        let timestamp: u64 = event.origin_server_ts()?.as_secs().into();

        let content = match event {
            SyncOrStrippedState::Stripped(event) => event.content,
            SyncOrStrippedState::Sync(event) => event.as_original()?.content.clone(),
        };

        self.cached_event = Some((content, timestamp));
        self.cached_event.clone()
    }
}

#[async_trait]
impl Asset for RoomAvatarAsset {
    fn asset_id(&self) -> &str {
        self.room.room_id().as_str()
    }

    async fn upload_timestamp(&mut self) -> Result<u64> {
        Ok(self
            .get_avatar_event_content()
            .await
            .ok_or(MediaError::NotFound)?
            .1)
    }

    async fn was_removed(&mut self) -> bool {
        if let Some(event) = self.get_avatar_event_content().await {
            event.0.url.is_none()
        } else {
            false
        }
    }

    async fn download(&mut self) -> Result<(Vec<u8>, String)> {
        let event_content = self
            .get_avatar_event_content()
            .await
            .ok_or_else(|| {
                log::debug!("No room avatar event was found for this room");
                MediaError::NotFound
            })?
            .0;

        log::info!("Downloading avatar image for room: {}", self.room.room_id());

        let client = self.room.client();

        let request = MediaRequestParameters {
            source: MediaSource::Plain(event_content.url.ok_or(MediaError::NotFound)?),
            format: MediaFormat::File,
        };

        let content = unwrap_or_log_return_err!(
            client.media().get_media_content(&request, true).await,
            "Error downloading room avatar"
        );

        let extension = determine_data_file_extension(&content)
            .ok_or(MediaError::UnableToDetermineFileExtension)?;

        Ok((content, extension))
    }

    async fn upload(&mut self, src: PathBuf) -> Result<String> {
        log::info!("Uploading room avatar: {src:?}");

        let data =
            unwrap_or_log_return_err!(tokio::fs::read(&src).await, "Error reading source file");

        let file_extension = src
            .extension()
            .map(|f| f.to_string_lossy().to_string())
            .or_else(|| determine_data_file_extension(&data))
            .ok_or(MediaError::UnableToDetermineFileExtension)?;

        let mime = mime_guess::from_ext(&file_extension)
            .first()
            .ok_or(MediaError::UnableToDetermineMimeType)?;

        unwrap_or_log_return_err!(
            self.room.upload_avatar(&mime, data, None).await,
            "Error uploading room avatar"
        );

        Ok(file_extension)
    }
}

/// Attempts to determine the file extension of the specified data.
fn determine_data_file_extension(data: &[u8]) -> Option<String> {
    match infer::get(data) {
        Some(kind) => Some(kind.extension().to_string()),
        None => {
            log::error!("Error determining data file extension");
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempdir::TempDir;

    use super::*;
    use crate::test_utils;

    const TEMPDIR_PREFIX: &str = "mrhc_matrix_adapter";
    const DATA_ROOT_DIR: &str = "data_root_dir";
    const ASSET_DIR: &str = "assets";
    const UPLOAD_DIR: &str = "uploads";
    const TIMESTAMP_PRECISION: u64 = 5;

    type MockedResult<T> = Arc<dyn Fn() -> Result<T> + Send + Sync>;

    struct AssetMock {
        asset_id: String,
        upload_timestamp: MockedResult<u64>,
        was_removed: bool,
        download: MockedResult<(Vec<u8>, String)>,
        upload: MockedResult<String>,
    }

    impl AssetMock {
        pub fn new(asset_id: impl Into<String>) -> Self {
            Self {
                asset_id: asset_id.into(),
                upload_timestamp: Arc::new(|| Err(MediaError::NotFound)),
                was_removed: false,
                download: Arc::new(|| Err(MediaError::NotFound)),
                upload: Arc::new(|| Err(MediaError::NotFound)),
            }
        }

        pub fn upload_timestamp(mut self, upload_timestamp: MockedResult<u64>) -> Self {
            self.upload_timestamp = upload_timestamp;
            self
        }

        pub fn was_removed(mut self, was_removed: bool) -> Self {
            self.was_removed = was_removed;
            self
        }

        pub fn download(mut self, download: MockedResult<(Vec<u8>, String)>) -> Self {
            self.download = download;
            self
        }

        pub fn upload(mut self, upload: MockedResult<String>) -> Self {
            self.upload = upload;
            self
        }

        /// Helper method for `Self::download` to return the specified data and extension.
        pub fn download_result(self, data: Vec<u8>, extension: String) -> Self {
            self.download(Arc::new(move || Ok((data.clone(), extension.to_owned()))))
        }

        /// Helper method for `Self::upload` to return the specified file extension.
        pub fn upload_result(self, extension: String) -> Self {
            self.upload(Arc::new(move || Ok(extension.clone())))
        }
    }

    #[async_trait]
    impl Asset for AssetMock {
        fn asset_id(&self) -> &str {
            &self.asset_id
        }

        async fn upload_timestamp(&mut self) -> Result<u64> {
            (self.upload_timestamp)()
        }

        async fn was_removed(&mut self) -> bool {
            self.was_removed
        }

        async fn download(&mut self) -> Result<(Vec<u8>, String)> {
            (self.download)()
        }

        async fn upload(&mut self, _src: PathBuf) -> Result<String> {
            (self.upload)()
        }
    }

    struct Directories {
        /// Handle to the temporary directory.
        /// The temporary directory will be removed once this is dropped, making
        /// all the created directories invalid.
        _temp_dir_handle: TempDir,
        /// The absolute path to the data root directory.
        pub data_root_dir: PathBuf,

        pub asset_dir_relative: PathBuf,
        pub asset_dir_absolute: PathBuf,
        pub upload_dir_relative: PathBuf,
        pub upload_dir_absolute: PathBuf,
    }

    /// Creates the temporary directories for the tests.
    fn setup_directories() -> Directories {
        let temp_dir = TempDir::new(TEMPDIR_PREFIX).unwrap();

        let data_root_dir = temp_dir.path().join(DATA_ROOT_DIR);

        let asset_dir_relative = PathBuf::from(ASSET_DIR);
        let asset_dir_absolute = data_root_dir.join(&asset_dir_relative);
        let upload_dir_relative = PathBuf::from(UPLOAD_DIR);
        let upload_dir_absolute = data_root_dir.join(&upload_dir_relative);

        fs::create_dir(&data_root_dir).unwrap();
        fs::create_dir(&asset_dir_absolute).unwrap();
        fs::create_dir(&upload_dir_absolute).unwrap();

        Directories {
            _temp_dir_handle: temp_dir,
            data_root_dir,
            asset_dir_relative,
            asset_dir_absolute,
            upload_dir_relative,
            upload_dir_absolute,
        }
    }

    /// Creates the asset manager.
    fn setup_asset_manager(dirs: &Directories, asset: Box<dyn Asset>) -> AssetManager {
        AssetManager::new(&dirs.data_root_dir, &dirs.asset_dir_relative, asset)
    }

    /// Creates the info and item file.
    fn setup_asset(dir: impl AsRef<Path>, id: &str, content: Vec<u8>, extension: &str) {
        let asset_file_name = format!("{id}.{extension}");

        let info_path = dir.as_ref().join(format!("{id}{INFO_FILE_SUFFIX}.json"));
        let asset_path = dir.as_ref().join(&asset_file_name);

        let info = AssetInfo {
            file: asset_file_name,
            download_ts: utils::get_unix_timestamp_seconds(),
        };

        let serialized = serde_json::to_string_pretty(&info).unwrap();

        fs::write(info_path, serialized).unwrap();
        fs::write(asset_path, content).unwrap();
    }

    fn assert_info_file(info_file: impl AsRef<Path>, asset_file_name: &str) {
        let content = fs::read(info_file).unwrap();
        let info: AssetInfo = serde_json::from_slice(&content).unwrap();

        assert_eq!(&info.file, asset_file_name);

        // Make sure the download timestamp was within the allowed seconds and not in the future.
        let now = utils::get_unix_timestamp_seconds();

        assert!(
            info.download_ts <= now,
            "Timestamp of download is in the future"
        );

        assert!(
            now - info.download_ts <= TIMESTAMP_PRECISION,
            "Timestamp of download is older than {TIMESTAMP_PRECISION} seconds"
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_new() {
        // Arrange
        let dirs = setup_directories();

        let data: Vec<u8> = vec![1, 2, 3, 4];
        let extension: String = "png".to_owned();

        let asset = AssetMock::new("some_asset").download_result(data.clone(), extension);
        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&asset_path_absolute, &data);

        assert_info_file(info_path_absolute, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_up_to_date() {
        // Arrange
        let dirs = setup_directories();

        let content = vec![2, 3, 4, 5];

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            content.clone(),
            "png",
        );

        let asset = AssetMock::new("some_asset")
            .upload_timestamp(Arc::new(|| Ok(utils::get_unix_timestamp_seconds() - 10000)));

        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&asset_path_absolute, &content);

        assert_info_file(info_path_absolute, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_no_longer_up_to_date() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(&dirs.asset_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&dirs.asset_dir_absolute, "some_asset2", vec![5, 2], "png");

        let content_new = vec![10, 20];
        let extension = "png".to_owned();

        let asset = AssetMock::new("some_asset")
            .download_result(content_new.clone(), extension)
            .upload_timestamp(Arc::new(|| Ok(utils::get_unix_timestamp_seconds() + 10)));

        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec![
                "some_asset_info.json",
                "some_asset.png",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(&asset_path_absolute, &content_new);

        assert_info_file(info_path_absolute, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_new_file_extension() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(&dirs.asset_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&dirs.asset_dir_absolute, "some_asset2", vec![5, 2], "png");

        let content_new = vec![10, 20];
        let extension = "jpeg".to_owned();

        let asset = AssetMock::new("some_asset")
            .download_result(content_new.clone(), extension)
            .upload_timestamp(Arc::new(|| Ok(utils::get_unix_timestamp_seconds() + 10)));

        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.jpeg");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.jpeg"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec![
                "some_asset_info.json",
                "some_asset.jpeg",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(&asset_path_absolute, &content_new);

        assert_info_file(info_path_absolute, "some_asset.jpeg");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_removed() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(&dirs.asset_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&dirs.asset_dir_absolute, "some_asset2", vec![5, 2], "png");

        let asset = AssetMock::new("some_asset")
            .was_removed(true)
            .upload_timestamp(Arc::new(|| Ok(utils::get_unix_timestamp_seconds() + 10000)));

        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.download().await;

        // Assert
        assert!(matches!(result, Err(MediaError::Removed)));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset2_info.json", "some_asset2.png"],
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_upload_new() {
        // Arrange
        let dirs = setup_directories();

        let upload_path_absolute = dirs.upload_dir_absolute.join("some_asset.png");
        let upload_path_relative = dirs.upload_dir_relative.join("some_asset.png");
        let asset_content = vec![5, 6, 7];

        fs::write(dirs.upload_dir_absolute.join("other.jpg"), vec![1, 2, 3]).unwrap();
        fs::write(&upload_path_absolute, &asset_content).unwrap();

        let asset = AssetMock::new("some_asset").upload_result("png".to_owned());
        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_upload_existing() {
        // Arrange
        let dirs = setup_directories();

        let upload_path_absolute = dirs.upload_dir_absolute.join("some_asset.png");
        let upload_path_relative = dirs.upload_dir_relative.join("some_asset.png");
        let asset_content = vec![5, 6, 7];

        fs::write(dirs.upload_dir_absolute.join("other.jpg"), vec![1, 2, 3]).unwrap();
        fs::write(&upload_path_absolute, &asset_content).unwrap();

        setup_asset(&dirs.asset_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&dirs.asset_dir_absolute, "some_asset2", vec![5, 2], "png");

        let asset = AssetMock::new("some_asset").upload_result("png".to_owned());
        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec![
                "some_asset_info.json",
                "some_asset.png",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_upload_new_file_extension() {
        // Arrange
        let dirs = setup_directories();

        let upload_path_absolute = dirs.upload_dir_absolute.join("some_asset.jpeg");
        let upload_path_relative = dirs.upload_dir_relative.join("some_asset.jpeg");
        let asset_content = vec![5, 6, 7];

        fs::write(dirs.upload_dir_absolute.join("other.jpg"), vec![1, 2, 3]).unwrap();
        fs::write(&upload_path_absolute, &asset_content).unwrap();

        setup_asset(&dirs.asset_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&dirs.asset_dir_absolute, "some_asset2", vec![5, 2], "png");

        let asset = AssetMock::new("some_asset").upload_result("jpeg".to_owned());
        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.jpeg");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.jpeg"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec![
                "some_asset_info.json",
                "some_asset.jpeg",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.jpeg");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_upload_new_different_upload_name() {
        // Arrange
        let dirs = setup_directories();

        let upload_path_absolute = dirs.upload_dir_absolute.join("upload_file.png");
        let upload_path_relative = dirs.upload_dir_relative.join("upload_file.png");
        let asset_content = vec![5, 6, 7];

        fs::write(dirs.upload_dir_absolute.join("other.jpg"), vec![1, 2, 3]).unwrap();
        fs::write(&upload_path_absolute, &asset_content).unwrap();

        let asset = AssetMock::new("some_asset").upload_result("png".to_owned());
        let mut manager = setup_asset_manager(&dirs, Box::new(asset));

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.png");
    }
}
