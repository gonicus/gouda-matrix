use std::path::{Path, PathBuf};

use async_trait::async_trait;
use matrix_sdk::deserialized_responses::SyncOrStrippedState;
use matrix_sdk::media::{MediaFormat, MediaRequestParameters};
use matrix_sdk::ruma::events::room::avatar::RoomAvatarEventContent;
use matrix_sdk::ruma::events::room::MediaSource;
use matrix_sdk::Room;
use tokio::fs;

use crate::{unwrap_or_log_return_option, utils};

const INFO_FILE_SUFFIX: &str = "_info";

const ROOM_AVATARS_FOLDER: &str = "room_avatars";

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

        let mut downloader = RoomAvatarDownloader::new(room.clone());

        if !downloader.is_available().await {
            log::debug!("No avatar event found for the room");
            return None;
        }

        let mut download_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.room_avatars_dir.clone(),
            Box::new(downloader),
        );

        download_manager.get_asset().await
    }
}

/// Manages a single asset.
struct AssetManager {
    /// The root directory where data is stored.
    data_root_dir: PathBuf,
    /// The relative path starting from the `data_root_dir` to the directory
    /// where the asset is stored.
    download_dir: PathBuf,
    /// Downloader to download the requested asset if it is not cached.
    downloader: Box<dyn AssetDownloader>,
}

impl AssetManager {
    pub fn new(
        data_root_dir: impl Into<PathBuf>,
        download_dir: impl Into<PathBuf>,
        downloader: Box<dyn AssetDownloader>,
    ) -> Self {
        Self {
            data_root_dir: data_root_dir.into(),
            download_dir: download_dir.into(),
            downloader,
        }
    }

    /// Returns the relative file path starting from the data root directory
    /// to the requested asset.
    /// Which asset is downloaded and requested is specified using `Self::downloader`.
    /// This function checks if the requested asset has been downloaded before
    /// and downloads the asset if it is not, or if the downloaded asset is outdated.
    ///
    /// Returns None if the asset has been removed or was not found on the Matrix server.
    pub async fn get_asset(&mut self) -> Option<String> {
        if let Some(path) = self.has_been_downloaded_before().await {
            path
        } else {
            self.download_asset().await
        }
    }

    /// Checks if the asset has been downloaded before and if so, if it is
    /// still up-to-date.
    async fn has_been_downloaded_before(&mut self) -> Option<Option<String>> {
        let Some(info) = self.read_info().await else {
            log::debug!("Asset hasn't been downloaded yet");
            return None;
        };

        log::debug!("Asset has already been downloaded before");

        if self.is_up_to_date(info.download_ts).await {
            log::debug!("Asset is still up-to-date");
            let path = self.get_asset_path_relative(&info.file);
            return Some(Some(path.to_string_lossy().to_string()));
        }

        log::debug!("Downloaded asset is no longer up-to-date");

        if self.downloader.was_removed().await {
            log::debug!("Asset was removed from upstream server");
            self.delete_asset_and_info(&info).await;
            Some(None)
        } else {
            None
        }
    }

    /// Checks if the asset downloaded at the specified timestamp is up-to-date.
    async fn is_up_to_date(&mut self, downloaded_at: u64) -> bool {
        let Some(upload_ts) = self.downloader.upload_timestamp().await else {
            log::warn!(
                "Unable to retrieve the assets upload timestamp, assuming it's still up-to-date"
            );

            return true;
        };

        upload_ts <= downloaded_at
    }

    /// Deletes the downloaded asset as well as its info file from the file system.
    async fn delete_asset_and_info(&mut self, info: &AssetInfo) {
        log::info!("Deleting asset with ID '{}'", self.downloader.asset_id());

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
    async fn download_asset(&mut self) -> Option<String> {
        log::debug!("Downloading asset");

        let (data, extension) = self.downloader.download().await?;

        if let Some(existing_info) = self.read_info().await {
            log::debug!("Replacing the already downloaded asset");
            self.delete_asset_and_info(&existing_info).await;
        }

        let data_file_name = self.get_asset_file_name(&extension);

        let info = AssetInfo {
            file: data_file_name.clone(),
            download_ts: utils::get_unix_timestamp_seconds(),
        };

        self.write_info_and_asset(info, data).await.ok()?;

        let relative_asset_path = self.get_asset_path_relative(&data_file_name);

        Some(relative_asset_path.to_string_lossy().to_string())
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
    async fn write_info_and_asset(&self, info: AssetInfo, asset: Vec<u8>) -> Result<(), ()> {
        let info_path = self.get_info_path_absolute();
        let item_path = self.get_asset_path_absolute(&info.file);

        log::debug!("Writing downloaded asset to: {item_path:?}");
        self.write_asset(&item_path, &asset).await?;

        log::debug!("Writing {info:?} to: {info_path:?}");
        self.write_info(&info_path, info).await?;

        Ok(())
    }

    /// Writes the asset information to the file system.
    async fn write_info(&self, path: &Path, info: AssetInfo) -> Result<(), ()> {
        let Ok(serialized) = serde_json::to_string_pretty(&info) else {
            log::error!("Error serializing asset information");
            return Err(());
        };

        if let Err(err) = tokio::fs::write(path, serialized).await {
            log::error!("Error writing asset information: {err}");
            return Err(());
        }

        Ok(())
    }

    /// Writes the asset to the file system.
    async fn write_asset(&self, path: &Path, data: &[u8]) -> Result<(), ()> {
        if let Err(err) = tokio::fs::write(path, data).await {
            log::error!("Error writing asset: {err}");
            return Err(());
        }

        Ok(())
    }

    /// Gets the file name of the asset information file.
    fn get_info_file_name(&self) -> String {
        format!("{}{INFO_FILE_SUFFIX}.json", self.downloader.asset_id())
    }

    /// Gets the file name of the downloaded asset.
    fn get_asset_file_name(&self, extension: &str) -> String {
        format!("{}.{}", self.downloader.asset_id(), extension)
    }

    /// Builds the relative path to the download directory starting from
    /// the data root directory.
    fn get_download_dir_relative(&self) -> PathBuf {
        self.download_dir.clone()
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

/// Trait that represents an object that actually implements the asset download.
#[async_trait]
trait AssetDownloader: Send + Sync {
    /// If the asset was removed from the upstream server.
    async fn was_removed(&mut self) -> bool;

    /// Downloads the asset from the upstream server.
    /// The first value of the tuple is the downloaded asset, while the second
    /// specifies the file extension to be used.
    /// Returns None if the asset was not found on the upstream server or if
    /// a download error occurred.
    async fn download(&mut self) -> Option<(Vec<u8>, String)>;

    /// Gets the ID of the asset.
    fn asset_id(&self) -> &str;

    /// Retrieves the timestamp of when the asset was uploaded to the upstream server,
    /// provided the asset is still available.
    async fn upload_timestamp(&mut self) -> Option<u64>;
}

/// Downloads the avatar of a specific room from the matrix server.
struct RoomAvatarDownloader {
    /// The room from which the avatar is to be downloaded.
    room: Room,
    /// Cached `RoomAvatarEventContent` and its origin server timestamp in seconds.
    cached_event: Option<(RoomAvatarEventContent, u64)>,
}

impl RoomAvatarDownloader {
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
impl AssetDownloader for RoomAvatarDownloader {
    async fn was_removed(&mut self) -> bool {
        if let Some(event) = self.get_avatar_event_content().await {
            event.0.url.is_none()
        } else {
            false
        }
    }

    async fn download(&mut self) -> Option<(Vec<u8>, String)> {
        let event_content = match self.get_avatar_event_content().await {
            Some(content) => content.0,
            None => {
                log::debug!("No room avatar event was found for this room");
                return None;
            }
        };

        log::info!("Downloading avatar image for room: {}", self.room.room_id());

        let client = self.room.client();

        let request = MediaRequestParameters {
            source: MediaSource::Plain(event_content.url?),
            format: MediaFormat::File,
        };

        let content = match client.media().get_media_content(&request, true).await {
            Ok(content) => content,
            Err(err) => {
                log::error!("Error downloading room avatar: {err}");
                return None;
            }
        };

        let extension = determine_data_file_extension(&content)?;

        Some((content, extension))
    }

    fn asset_id(&self) -> &str {
        self.room.room_id().as_str()
    }

    async fn upload_timestamp(&mut self) -> Option<u64> {
        Some(self.get_avatar_event_content().await?.1)
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

    use tempdir::TempDir;

    use super::*;
    use crate::test_utils;

    const TEMPDIR_PREFIX: &str = "mrhc_matrix_adapter";
    const DATA_ROOT_DIR: &str = "data_root_dir";
    const DOWNLOAD_DIR: &str = "download_dir";

    struct DownloaderMock {
        item_id: String,
        was_removed: bool,
        data: Option<(Vec<u8>, String)>,
        last_upload_timestamp: Option<u64>,
    }

    impl DownloaderMock {
        pub fn new(item_id: impl Into<String>) -> Self {
            Self {
                item_id: item_id.into(),
                was_removed: false,
                data: None,
                last_upload_timestamp: None,
            }
        }

        pub fn was_removed(mut self, was_removed: bool) -> Self {
            self.was_removed = was_removed;
            self
        }

        pub fn data(mut self, data: (Vec<u8>, String)) -> Self {
            self.data = Some(data);
            self
        }

        pub fn upload_timestamp(mut self, last_upload_timestamp: u64) -> Self {
            self.last_upload_timestamp = Some(last_upload_timestamp);
            self
        }
    }

    #[async_trait]
    impl AssetDownloader for DownloaderMock {
        async fn was_removed(&mut self) -> bool {
            self.was_removed
        }

        async fn download(&mut self) -> Option<(Vec<u8>, String)> {
            self.data.clone()
        }

        fn asset_id(&self) -> &str {
            &self.item_id
        }

        async fn upload_timestamp(&mut self) -> Option<u64> {
            self.last_upload_timestamp
        }
    }

    /// Creates the temporary directories for the tests. Returns the TempDir handle,
    /// the absolute root directory, and the relative download directory.
    /// Ensure that the TempDir handle is not dropped before the test is complete.
    fn setup_temp_directories() -> (TempDir, PathBuf, PathBuf) {
        let dir = TempDir::new(TEMPDIR_PREFIX).unwrap();
        let data_root_dir = dir.path().join(DATA_ROOT_DIR);
        let download_dir = data_root_dir.join(DOWNLOAD_DIR);

        fs::create_dir(&data_root_dir).unwrap();
        fs::create_dir(&download_dir).unwrap();

        (dir, data_root_dir, PathBuf::from(DOWNLOAD_DIR))
    }

    /// Creates the info and item file.
    fn setup_asset(dir: impl AsRef<Path>, id: &str, content: Vec<u8>, extension: &str) {
        let item_file_name = format!("{id}.{extension}");

        let info_path = dir.as_ref().join(format!("{id}{INFO_FILE_SUFFIX}.json"));
        let item_path = dir.as_ref().join(&item_file_name);

        let info = AssetInfo {
            file: item_file_name,
            download_ts: utils::get_unix_timestamp_seconds(),
        };

        let serialized = serde_json::to_string_pretty(&info).unwrap();

        fs::write(info_path, serialized).unwrap();
        fs::write(item_path, content).unwrap();
    }

    fn assert_info_file(info_file: impl AsRef<Path>, item_file_name: &str) {
        let content = fs::read(info_file).unwrap();
        let info: AssetInfo = serde_json::from_slice(&content).unwrap();

        assert_eq!(&info.file, item_file_name);

        // Make sure the download timestamp was within the last 3 seconds
        // and not in the future.
        let now = utils::get_unix_timestamp_seconds();

        assert!(
            info.download_ts <= now,
            "Timestamp of download is in the future"
        );

        assert!(
            now - info.download_ts <= 3,
            "Timestamp of download is older than 3 seconds"
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_get_asset_with_new_item() {
        // Arrange
        let (_tmpdir, data_root_dir, download_dir_relative) = setup_temp_directories();

        let data: Vec<u8> = vec![1, 2, 3, 4];
        let extension: String = "png".to_owned();

        let downloader = DownloaderMock::new("some_asset").data((data.clone(), extension));

        let mut manager =
            AssetManager::new(&data_root_dir, &download_dir_relative, Box::new(downloader));

        // Act
        let result = manager.get_asset().await;

        // Assert
        let download_dir_str = download_dir_relative.to_string_lossy();
        let asset_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset.png");
        let info_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset_info.json");

        assert_eq!(result, Some(format!("{download_dir_str}/some_asset.png")));

        test_utils::assert_directory(
            data_root_dir.join(download_dir_relative),
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&asset_path, &data);

        assert_info_file(info_path, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_get_asset_with_up_to_date_item() {
        // Arrange
        let (_tmpdir, data_root_dir, download_dir_relative) = setup_temp_directories();
        let download_dir_absolute = data_root_dir.join(&download_dir_relative);

        let content = vec![2, 3, 4, 5];

        setup_asset(&download_dir_absolute, "some_asset", content.clone(), "png");

        let downloader = DownloaderMock::new("some_asset")
            .upload_timestamp(utils::get_unix_timestamp_seconds() - 10000);

        let mut manager =
            AssetManager::new(&data_root_dir, &download_dir_relative, Box::new(downloader));

        // Act
        let result = manager.get_asset().await;

        // Assert
        let download_dir_str = download_dir_relative.to_string_lossy();
        let item_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset.png");
        let asset_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset_info.json");

        assert_eq!(result, Some(format!("{download_dir_str}/some_asset.png")));

        test_utils::assert_directory(
            data_root_dir.join(download_dir_relative),
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&item_path, &content);

        assert_info_file(asset_path, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_get_asset_with_no_longer_up_to_date_item() {
        // Arrange
        let (_tmpdir, data_root_dir, download_dir_relative) = setup_temp_directories();
        let download_dir_absolute = data_root_dir.join(&download_dir_relative);

        setup_asset(&download_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&download_dir_absolute, "some_asset2", vec![5, 2], "png");

        let content_new = vec![10, 20];
        let extension = "png".to_owned();

        let downloader = DownloaderMock::new("some_asset")
            .data((content_new.clone(), extension))
            .upload_timestamp(utils::get_unix_timestamp_seconds() + 10);

        let mut manager =
            AssetManager::new(&data_root_dir, &download_dir_relative, Box::new(downloader));

        // Act
        let result = manager.get_asset().await;

        // Assert
        let download_dir_str = download_dir_relative.to_string_lossy();
        let item_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset.png");
        let asset_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset_info.json");

        assert_eq!(result, Some(format!("{download_dir_str}/some_asset.png")));

        test_utils::assert_directory(
            data_root_dir.join(download_dir_relative),
            vec![
                "some_asset_info.json",
                "some_asset.png",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(&item_path, &content_new);

        assert_info_file(asset_path, "some_asset.png");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_get_asset_with_no_longer_up_to_date_item_and_new_extension() {
        // Arrange
        let (_tmpdir, data_root_dir, download_dir_relative) = setup_temp_directories();
        let download_dir_absolute = data_root_dir.join(&download_dir_relative);

        setup_asset(&download_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&download_dir_absolute, "some_asset2", vec![5, 2], "png");

        let content_new = vec![10, 20];
        let extension = "jpeg".to_owned();

        let downloader = DownloaderMock::new("some_asset")
            .data((content_new.clone(), extension))
            .upload_timestamp(utils::get_unix_timestamp_seconds() + 10);

        let mut manager =
            AssetManager::new(&data_root_dir, &download_dir_relative, Box::new(downloader));

        // Act
        let result = manager.get_asset().await;

        // Assert
        let download_dir_str = download_dir_relative.to_string_lossy();
        let item_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset.jpeg");
        let asset_path = data_root_dir
            .join(&download_dir_relative)
            .join("some_asset_info.json");

        assert_eq!(result, Some(format!("{download_dir_str}/some_asset.jpeg")));

        test_utils::assert_directory(
            data_root_dir.join(download_dir_relative),
            vec![
                "some_asset_info.json",
                "some_asset.jpeg",
                "some_asset2_info.json",
                "some_asset2.png",
            ],
        );

        test_utils::assert_file_content(&item_path, &content_new);

        assert_info_file(asset_path, "some_asset.jpeg");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_get_asset_with_removed_item_upstream() {
        // Arrange
        let (_tmpdir, data_root_dir, download_dir_relative) = setup_temp_directories();
        let download_dir_absolute = data_root_dir.join(&download_dir_relative);

        setup_asset(&download_dir_absolute, "some_asset", vec![2, 3, 4], "png");
        setup_asset(&download_dir_absolute, "some_asset2", vec![5, 2], "png");

        let downloader = DownloaderMock::new("some_asset")
            .was_removed(true)
            .upload_timestamp(utils::get_unix_timestamp_seconds() + 10000);

        let mut manager =
            AssetManager::new(&data_root_dir, &download_dir_relative, Box::new(downloader));

        // Act
        let result = manager.get_asset().await;

        // Assert
        let download_dir_absolute = data_root_dir.join(download_dir_relative);

        assert_eq!(result, None);

        test_utils::assert_directory(
            download_dir_absolute,
            vec!["some_asset2_info.json", "some_asset2.png"],
        );
    }
}
