use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use gouda_proto::chat::Error as ChatError;
use matrix_sdk::attachment::AttachmentConfig;
use matrix_sdk::media::{MediaEventContent, MediaFormat, MediaRequestParameters};
use matrix_sdk::room::reply::Reply;
use matrix_sdk::ruma::api::client::user_directory::search_users::v3::User;
use matrix_sdk::ruma::events::room::avatar::ImageInfo;
use matrix_sdk::ruma::events::room::MediaSource;
use matrix_sdk::{Client, Room};
use mime::Mime;
use ruma_common::{EventId, MxcUri, OwnedEventId, OwnedMxcUri, OwnedUserId};
use thiserror::Error;
use tokio::fs;

use crate::error::chat_err;
use crate::{
    debug_assert_or_log, unwrap_or_log_return, unwrap_or_log_return_err,
    unwrap_or_log_return_option, user, utils,
};

const ROOM_AVATARS_FOLDER: &str = "room_avatars";
const USER_AVATARS_FOLDER: &str = "user_avatars";
const ATTACHMENTS_FOLDER: &str = "attachments";

const INFO_FILE_SUFFIX: &str = "_info";

const DEFAULT_MIME: mime::Mime = mime::APPLICATION_OCTET_STREAM;

macro_rules! log_avatar_result {
    ($result:expr, $object:literal) => {
        if let Err(err) = &$result {
            if matches!(err, MediaError::NotFound) {
                log::debug!(concat!($object, " does not have an avatar"));
            } else {
                log::error!("Error downloading avatar: {err}");
            }
        }
    };
}

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

    #[error("the requested operation is not allowed")]
    NotAllowed,

    #[error("unable to get the avatar uri of the requested user")]
    UnableToGetAvatarUri,

    // This error is most likely a bug in the code!
    #[error("the id of the asset is not specified")]
    AssetIdNotSpecified,

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("serde json error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("matrix error")]
    MatrixError(#[from] matrix_sdk::Error),
}

impl From<MediaError> for ChatError {
    // TODO: Improve error handling
    fn from(value: MediaError) -> ChatError {
        chat_err!(Unknown, value)
    }
}

type Result<T> = std::result::Result<T, MediaError>;

/// Manages persistent media.
///
/// All of the state is held in an `Arc` so the `MediaManager` can be cloned freely.
#[derive(Clone, Debug)]
pub struct MediaManager {
    inner: Arc<MediaManagerInner>,
}

impl MediaManager {
    pub async fn new(
        client: Client,
        data_root_dir: impl Into<PathBuf>,
        media_dir_relative: impl Into<PathBuf>,
    ) -> Self {
        let data_root_dir = data_root_dir.into();
        let media_dir_relative = media_dir_relative.into();

        let inner = MediaManagerInner {
            client,
            data_root_dir,

            room_avatars_dir: media_dir_relative.join(ROOM_AVATARS_FOLDER),
            user_avatars_dir: media_dir_relative.join(USER_AVATARS_FOLDER),
            attachments_dir: media_dir_relative.join(ATTACHMENTS_FOLDER),
        }
        .init_dirs()
        .await;

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns the relative path to the room's avatar, starting in the
    /// data root directory. This method downloads the avatar
    /// if it doesn't exist, or updates the existing one if a new avatar
    /// has been uploaded to the Matrix server.
    /// Returns None if no avatar is set for the room.
    pub async fn get_room_avatar_path(&self, room: &Room) -> Option<String> {
        log::info!("Receiving avatar for room: {}", room.room_id());
        self.inner.get_room_avatar_path(room).await
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
        self.inner.upload_room_avatar(room, src).await
    }

    /// Returns the relative path to the users avatar, starting in the
    /// data root directory. This method downloads the avatar
    /// if it doesn't exist, or updates the existing one if a new avatar
    /// has been uploaded to the Matrix server.
    /// Returns None if no avatar is set for the user.
    pub async fn get_user_avatar_path(&self, user_id: OwnedUserId) -> Option<String> {
        log::info!("Receiving avatar for user: {user_id}");
        self.inner.get_user_avatar_path(user_id).await
    }

    /// Returns the relative path to the users avatar, starting in the
    /// data root directory. This method downloads the avatar
    /// if it doesn't exist, or updates the existing one if a new avatar
    /// has been uploaded to the Matrix server.
    /// Returns None if no avatar is set for the user.
    pub async fn get_user_directory_user_avatar_path(&self, user: &User) -> Option<String> {
        log::info!("Receiving avatar for user: {}", &user.user_id);
        self.inner
            .get_user_avatar_path(user.user_id.to_owned())
            .await
    }

    /// Uploads and sends a new attachment to the specified room.
    /// This method moves the attachment to the correct directory after upload.
    ///
    /// # Arguments
    ///
    /// * `room` - The room for which the attachment should be uploaded.
    /// * `attachment_path` - The relative path to the attachment starting from
    ///   the data root directory.
    ///
    /// # Returns
    ///
    /// Returns the ID of the created message.
    pub async fn send_room_attachment(
        &self,
        room: &Room,
        attachment_path: impl AsRef<Path>,
        file_name: Option<String>,
        related_event: Option<OwnedEventId>,
    ) -> Result<String> {
        let src = attachment_path.as_ref();
        log::info!("Sending room attachment {src:?} to room {}", room.room_id());
        self.inner
            .send_room_attachment(room, src, file_name, related_event)
            .await
    }

    /// Downloads the attachment from a media event.
    ///
    /// # Arguments
    ///
    /// * `room` - The room in which the event was send in
    /// * `event_id` - The ID of the event.
    /// * `content` - The actual content of the event, e.g. `ImageMessageEventContent`.
    ///
    /// # Returns
    ///
    /// Returns the relative path starting from the data root directory to the
    /// downloaded media file.
    pub async fn download_from_media_event_content<C: MediaEventContent + Send + Sync>(
        &self,
        room: &Room,
        event_id: &EventId,
        content: &C,
        file_name: Option<&str>,
    ) -> Result<String> {
        log::info!(
            "Downloading media event content from room: {}",
            room.room_id()
        );
        self.inner
            .download_from_media_event_content(room, event_id, content, file_name)
            .await
    }
}

#[derive(Debug)]
pub struct MediaManagerInner {
    /// The matrix_sdk client to use.
    client: Client,
    /// The root directory where data should be stored.
    data_root_dir: PathBuf,

    /// The relative path from the `data_root_dir` where room avatars are stored.
    room_avatars_dir: PathBuf,
    /// The relative path from the `data_root_dir` where user avatars are stored.
    user_avatars_dir: PathBuf,
    /// The relative path from the `data_root_dir` where attachments are stored.
    attachments_dir: PathBuf,
}

impl MediaManagerInner {
    /// Creates all necessary data directories if they do not already exist.
    async fn init_dirs(self) -> Self {
        self.init_dir_relative(&self.room_avatars_dir).await;
        self.init_dir_relative(&self.user_avatars_dir).await;
        self.init_dir_relative(&self.attachments_dir).await;
        self
    }

    /// Creates the directory with the specified relative path starting from the
    /// data root directory.
    async fn init_dir_relative(&self, dir: &Path) {
        let absolute = self.data_root_dir.join(dir);

        if let Err(err) = fs::create_dir_all(absolute).await {
            if err.kind() != tokio::io::ErrorKind::AlreadyExists {
                log::error!("Error initializing directory {dir:?}: {err}");
            }
        } else {
            log::info!("Initialized directory: {dir:?}");
        }
    }

    pub async fn get_room_avatar_path(&self, room: &Room) -> Option<String> {
        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.room_avatars_dir.clone(),
            RoomAvatarAsset::new(room.clone()),
        );

        let result = asset_manager.download().await;

        log_avatar_result!(result, "Room");

        result.ok()
    }

    pub async fn upload_room_avatar(&self, room: &Room, avatar_path: &Path) -> Result<String> {
        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.room_avatars_dir.clone(),
            RoomAvatarAsset::new(room.clone()),
        );

        match asset_manager.upload(avatar_path).await {
            Ok(path) => Ok(path),
            Err(err) => {
                log::error!("Error uploading avatar: {err}");
                Err(err)
            }
        }
    }

    pub async fn get_user_avatar_path(&self, user_id: OwnedUserId) -> Option<String> {
        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            self.user_avatars_dir.clone(),
            UserAvatarAsset::new(self.client.clone(), user_id).await,
        );

        let result = asset_manager.download().await;

        log_avatar_result!(result, "User");

        result.ok()
    }

    pub async fn send_room_attachment(
        &self,
        room: &Room,
        attachment_path: &Path,
        file_name: Option<String>,
        related_event: Option<OwnedEventId>,
    ) -> Result<String> {
        let attachments_room_dir = self.attachments_dir.join(room.room_id().as_str());
        self.init_dir_relative(&attachments_room_dir).await;

        let mut asset = RoomAttachmentAsset::new(room.clone(), file_name);

        if let Some(event_id) = related_event {
            asset = asset.reply_to(event_id);
        }

        let mut asset_manager =
            AssetManager::new(self.data_root_dir.clone(), attachments_room_dir, asset);

        if let Err(err) = asset_manager.upload(attachment_path).await {
            log::error!("Error uploading and sending room attachment: {err}");
            return Err(err);
        }

        asset_manager.asset().asset_id()
    }

    pub async fn download_from_media_event_content<C: MediaEventContent + Send + Sync>(
        &self,
        room: &Room,
        event_id: &EventId,
        content: &C,
        file_name: Option<&str>,
    ) -> Result<String> {
        let attachments_room_dir = self.attachments_dir.join(room.room_id().as_str());
        self.init_dir_relative(&attachments_room_dir).await;

        let mut asset_manager = AssetManager::new(
            self.data_root_dir.clone(),
            attachments_room_dir,
            MediaEventAsset::new(
                self.client.clone(),
                event_id.to_string(),
                content,
                file_name,
            ),
        );

        asset_manager.download().await
    }
}

/// Manages a single asset.
struct AssetManager<T>
where
    T: Asset,
{
    /// The root directory where data is stored.
    data_root_dir: PathBuf,
    /// The relative path starting from the `data_root_dir` to the directory
    /// where the asset is stored.
    asset_dir: PathBuf,
    /// The asset that is being managed.
    asset: T,
}

impl<T> AssetManager<T>
where
    T: Asset,
{
    /// Creates a new asset manager.
    ///
    /// # Arguments
    ///
    /// * `data_root_dir` - The absolute path to the data root directory.
    /// * `asset_dir` - The relative path to the asset directory,
    ///   starting from the data root directory.
    /// * `asset` - The asset that is being managed.
    pub fn new(data_root_dir: impl Into<PathBuf>, asset_dir: impl Into<PathBuf>, asset: T) -> Self {
        Self {
            data_root_dir: data_root_dir.into(),
            asset_dir: asset_dir.into(),
            asset,
        }
    }

    /// Returns a reference to the asset being managed.
    pub fn asset(&self) -> &T {
        &self.asset
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

        log::info!("Uploading asset: {src:?}");

        let upload = self.asset.upload(src.clone()).await?;

        log::debug!("Successfully uploaded asset");

        if let Some(info) = self.read_info().await {
            log::debug!(
                "Old version of the asset exists, replacing it with the newly uploaded asset"
            );
            self.delete_asset_and_info(&info).await;
        }

        let asset_file_name = self.get_asset_file_name(upload.file_extension.as_deref())?;

        let info = AssetInfo {
            file: asset_file_name.clone(),
            download_ts: utils::get_unix_timestamp_seconds(),
            upstream_url: upload.upstream_url,
        };

        let info_path_absolute = self.get_info_path_absolute()?;
        let asset_path_absolute = self.get_asset_path_absolute(&asset_file_name)?;

        log::debug!("Moving uploaded asset from {src:?} to {asset_path_absolute:?}");
        tokio::fs::rename(src, asset_path_absolute).await?;

        self.write_info(&info_path_absolute, info).await?;

        let asset_path_relative = self.get_asset_path_relative(&asset_file_name)?;

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

        if self.is_up_to_date(&info).await {
            log::debug!("Asset is still up-to-date");
            return match self.get_asset_path_relative(&info.file) {
                Ok(path) => Some(Ok(path.to_string_lossy().to_string())),
                Err(err) => Some(Err(err)),
            };
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
    async fn is_up_to_date(&mut self, info: &AssetInfo) -> bool {
        let Ok(result) = self.asset.is_up_to_date(info).await else {
            log::warn!("Error checking if the asset is still up-to-date, assuming it is");
            return true;
        };

        result
    }

    /// Deletes the downloaded asset as well as its info file from the file system.
    async fn delete_asset_and_info(&mut self, info: &AssetInfo) {
        log::info!("Deleting asset with ID '{:?}'", self.asset.asset_id());

        let info_path =
            unwrap_or_log_return!(self.get_info_path_absolute(), "Error retrieving asset ID");

        let asset_path = unwrap_or_log_return!(
            self.get_asset_path_absolute(&info.file),
            "Error retrieving asset ID"
        );

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

        let download = self.asset.download().await?;

        if let Some(existing_info) = self.read_info().await {
            log::debug!("Replacing the already downloaded asset");
            self.delete_asset_and_info(&existing_info).await;
        }

        let data_file_name = self.get_asset_file_name(download.file_extension.as_deref())?;

        let info = AssetInfo {
            file: data_file_name.clone(),
            download_ts: utils::get_unix_timestamp_seconds(),
            upstream_url: download.upstream_url,
        };

        self.write_info_and_asset(info, download.data).await?;

        let relative_asset_path = self.get_asset_path_relative(&data_file_name)?;

        Ok(relative_asset_path.to_string_lossy().to_string())
    }

    /// Reads the asset information from the file system.
    /// Returns None if the requested asset hasn't been downloaded yet.
    async fn read_info(&self) -> Option<AssetInfo> {
        let path = self.get_info_path_absolute().ok()?;

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
        let info_path = self.get_info_path_absolute()?;
        let item_path = self.get_asset_path_absolute(&info.file)?;

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
    fn get_info_file_name(&self) -> Result<String> {
        let asset_id =
            unwrap_or_log_return_err!(self.asset.asset_id(), "Error retrieving asset ID");

        Ok(format!("{asset_id}{INFO_FILE_SUFFIX}.json"))
    }

    /// Gets the file name of the downloaded asset.
    fn get_asset_file_name(&self, extension: Option<&str>) -> Result<String> {
        let asset_id =
            unwrap_or_log_return_err!(self.asset.asset_id(), "Error retrieving asset ID");

        if let Some(extension) = extension {
            Ok(format!("{asset_id}.{extension}"))
        } else {
            Ok(asset_id)
        }
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
    fn get_info_path_absolute(&self) -> Result<PathBuf> {
        let file_name = self.get_info_file_name()?;
        Ok(self.get_download_dir_absolute().join(file_name))
    }

    /// Gets the relative path to the asset information file starting
    /// from the data root directory.
    fn get_info_path_relative(&self) -> Result<PathBuf> {
        let file_name = self.get_info_file_name()?;
        Ok(self.get_download_dir_relative().join(file_name))
    }

    /// Gets the absolute path to the asset.
    fn get_asset_path_absolute(&self, file_name: &str) -> Result<PathBuf> {
        let mut path = self.get_info_path_absolute()?;
        path.set_file_name(file_name);
        Ok(path)
    }

    /// Gets the relative path to the asset starting from the data root directory.
    fn get_asset_path_relative(&self, file_name: &str) -> Result<PathBuf> {
        let mut path = self.get_info_path_relative()?;
        path.set_file_name(file_name);
        Ok(path)
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
    /// The upstream URL to the media file.
    upstream_url: String,
}

/// Contains information of a download.
#[derive(Debug, Clone)]
struct Download {
    /// The downloaded data.
    data: Vec<u8>,
    /// The file extension to use for the downloaded data.
    file_extension: Option<String>,
    /// The upstream URL to the media file.
    upstream_url: String,
}

/// Contains information of an upload.
#[derive(Debug, Clone)]
struct Upload {
    /// The file extension to use for the uploaded data.
    file_extension: Option<String>,
    /// The upstream URL to the uploaded media file.
    upstream_url: String,
}

/// Trait that represents an object that actually implements an asset upload
/// and download.
#[async_trait]
trait Asset: Send + Sync {
    /// Gets the ID of the asset.
    fn asset_id(&self) -> Result<String>;

    /// Checks if the already downloaded asset is still up to date.
    async fn is_up_to_date(&mut self, info: &AssetInfo) -> Result<bool>;

    /// If the asset was removed from the upstream server.
    async fn was_removed(&mut self) -> bool;

    /// Downloads the asset from the upstream server.
    async fn download(&mut self) -> Result<Download>;

    /// Uploads the specified asset to the upstream server.
    async fn upload(&mut self, src: PathBuf) -> Result<Upload>;
}

/// Manages the download and upload of an avatar from a specific room.
struct RoomAvatarAsset {
    /// The matrix sdk client to use.
    client: Client,
    /// The room from which the avatar is to be downloaded or uploaded.
    room: Room,
    /// The ID of the room.
    id: String,
}

impl RoomAvatarAsset {
    pub fn new(room: Room) -> Self {
        let id = room.room_id().to_string();

        Self {
            client: room.client(),
            room,
            id,
        }
    }
}

#[async_trait]
impl Asset for RoomAvatarAsset {
    fn asset_id(&self) -> Result<String> {
        Ok(self.id.clone())
    }

    async fn is_up_to_date(&mut self, info: &AssetInfo) -> Result<bool> {
        let url = self.room.avatar_url().map(|f| f.to_string());

        match url {
            Some(url) => Ok(url == info.upstream_url),
            None => Ok(false),
        }
    }

    async fn was_removed(&mut self) -> bool {
        self.room.avatar_url().is_none()
    }

    async fn download(&mut self) -> Result<Download> {
        log::info!("Downloading avatar image for room: {}", &self.id);

        let avatar_url = self.room.avatar_url().ok_or(MediaError::NotFound)?;

        let content = unwrap_or_log_return_err!(
            download_mxc(&self.client, &avatar_url).await,
            "Error downloading room avatar"
        );

        let extension = determine_file_extension_from_data(&content);

        let download = Download {
            data: content,
            file_extension: extension,
            upstream_url: avatar_url.to_string(),
        };

        Ok(download)
    }

    async fn upload(&mut self, src: PathBuf) -> Result<Upload> {
        log::info!("Uploading room avatar: {src:?}");

        let data =
            unwrap_or_log_return_err!(tokio::fs::read(&src).await, "Error reading source file");

        let (file_extension, mime) = determine_file_extension_and_mime(&data, &src);

        let response = unwrap_or_log_return_err!(
            self.room.client().media().upload(&mime, data, None).await,
            "Error uploading room avatar image to the matrix server"
        );

        let mut info = ImageInfo::default();
        info.blurhash = response.blurhash;
        info.mimetype = Some(mime.to_string());

        unwrap_or_log_return_err!(
            self.room
                .set_avatar_url(&response.content_uri, Some(info))
                .await,
            "Error sending room avatar state event"
        );

        let upload = Upload {
            file_extension,
            upstream_url: response.content_uri.to_string(),
        };

        Ok(upload)
    }
}

/// Manages the download of an user's avatar.
struct UserAvatarAsset {
    /// The matrix sdk client to use.
    client: Client,
    /// The ID of the room member.
    user_id: OwnedUserId,
    /// The cached user avatar uri.
    /// If none, the avatar uri hasn't been successfully retrieved yet.
    /// If the inner option is none, the user does not have an avatar set.
    avatar_uri: Option<Option<OwnedMxcUri>>,
}

impl UserAvatarAsset {
    pub async fn new(client: Client, user_id: OwnedUserId) -> Self {
        let avatar_uri = user::fetch_avatar_uri(&client, user_id.clone()).await.ok();

        Self {
            client,
            user_id,
            avatar_uri,
        }
    }

    async fn get_avatar_uri(&mut self) -> Result<Option<OwnedMxcUri>> {
        if let Some(uri) = &self.avatar_uri {
            return Ok(uri.clone());
        }

        let result = user::fetch_avatar_uri(&self.client, self.user_id.clone()).await;

        if let Ok(uri) = &result {
            self.avatar_uri = Some(uri.clone());
        }

        result.map_err(|_| MediaError::UnableToGetAvatarUri)
    }
}

#[async_trait]
impl Asset for UserAvatarAsset {
    fn asset_id(&self) -> Result<String> {
        Ok(self.user_id.to_string())
    }

    async fn is_up_to_date(&mut self, info: &AssetInfo) -> Result<bool> {
        self.get_avatar_uri()
            .await
            .map(|opt| opt.map(|uri| uri == info.upstream_url).unwrap_or(false))
    }

    async fn was_removed(&mut self) -> bool {
        self.get_avatar_uri()
            .await
            .map(|opt| opt.is_none())
            .unwrap_or(false)
    }

    async fn download(&mut self) -> Result<Download> {
        log::info!("Downloading user avatar for user: {}", &self.user_id);

        let avatar_uri = self.get_avatar_uri().await?.ok_or(MediaError::NotFound)?;

        let content = unwrap_or_log_return_err!(
            download_mxc(&self.client, &avatar_uri).await,
            "Error downloading user avatar"
        );

        let extension = determine_file_extension_from_data(&content);

        let download = Download {
            data: content,
            file_extension: extension,
            upstream_url: avatar_uri.to_string(),
        };

        Ok(download)
    }

    async fn upload(&mut self, _src: PathBuf) -> Result<Upload> {
        debug_assert_or_log!(false, "Uploading an avatar for another user is not allowed");
        Err(MediaError::NotAllowed)
    }
}

/// Manages the upload of a room attachment.
struct RoomAttachmentAsset {
    /// The room from which the avatar is to be downloaded or uploaded.
    room: Room,
    /// The ID of the attachment.
    asset_id: Option<String>,
    /// The actual name of the file.
    file_name: Option<String>,
    /// If the attachment is a reply to another event.
    reply_to: Option<OwnedEventId>,
}

impl RoomAttachmentAsset {
    pub fn new(room: Room, file_name: Option<String>) -> Self {
        Self {
            room,
            asset_id: None,
            file_name,
            reply_to: None,
        }
    }

    pub fn reply_to(mut self, event_id: OwnedEventId) -> Self {
        self.reply_to = Some(event_id);
        self
    }

    fn generate_attachment_config(&self) -> AttachmentConfig {
        let mut config = AttachmentConfig::default();

        if let Some(event_id) = &self.reply_to {
            let reply = Reply {
                event_id: event_id.clone(),
                enforce_thread: matrix_sdk::room::reply::EnforceThread::MaybeThreaded,
                add_mentions: matrix_sdk::ruma::events::room::message::AddMentions::No,
            };

            config.reply = Some(reply);
        }

        config
    }
}

#[async_trait]
impl Asset for RoomAttachmentAsset {
    fn asset_id(&self) -> Result<String> {
        self.asset_id.clone().ok_or(MediaError::AssetIdNotSpecified)
    }

    async fn is_up_to_date(&mut self, _info: &AssetInfo) -> Result<bool> {
        Ok(true)
    }

    async fn was_removed(&mut self) -> bool {
        false
    }

    async fn download(&mut self) -> Result<Download> {
        debug_assert_or_log!(
            false,
            "Downloading using the RoomAttachmentAsset is not allowed"
        );
        Err(MediaError::NotAllowed)
    }

    async fn upload(&mut self, src: PathBuf) -> Result<Upload> {
        log::info!("Uploading and sending room attachment: {src:?}");

        let data =
            unwrap_or_log_return_err!(tokio::fs::read(&src).await, "Error reading source file");

        let file_name = self.file_name.clone().unwrap_or_else(|| {
            src.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string()
        });

        let (file_extension, mime) = determine_file_extension_and_mime(&data, &src);
        let config = self.generate_attachment_config();

        let response = unwrap_or_log_return_err!(
            self.room
                .send_attachment(file_name, &mime, data, config)
                .await,
            "Error uploading room avatar image to the matrix server"
        );

        self.asset_id = Some(response.event_id.to_string());

        let upload = Upload {
            file_extension,
            upstream_url: String::new(),
        };

        Ok(upload)
    }
}

/// Manages the download of a media event.
struct MediaEventAsset<'a, C: MediaEventContent + Send + Sync> {
    /// The matrix sdk client to use.
    client: Client,
    /// The ID of the media.
    asset_id: String,
    /// The event content of the media.
    content: &'a C,
    /// The file name of the media.
    file_name: Option<&'a str>,
}

impl<'a, C: MediaEventContent + Send + Sync> MediaEventAsset<'a, C> {
    pub fn new(
        client: Client,
        asset_id: String,
        content: &'a C,
        file_name: Option<&'a str>,
    ) -> Self {
        Self {
            client,
            asset_id,
            content,
            file_name,
        }
    }

    fn get_upstream_url(&self) -> Result<String> {
        let source = self.content.source().ok_or(MediaError::NotFound)?;

        match source {
            MediaSource::Plain(url) => Ok(url.to_string()),
            MediaSource::Encrypted(info) => Ok(info.url.to_string()),
        }
    }
}

#[async_trait]
impl<'a, C: MediaEventContent + Send + Sync> Asset for MediaEventAsset<'a, C> {
    fn asset_id(&self) -> Result<String> {
        Ok(self.asset_id.clone())
    }

    async fn is_up_to_date(&mut self, info: &AssetInfo) -> Result<bool> {
        Ok(self.get_upstream_url()? == info.upstream_url)
    }

    async fn was_removed(&mut self) -> bool {
        matches!(self.get_upstream_url(), Err(MediaError::NotFound))
    }

    async fn download(&mut self) -> Result<Download> {
        log::info!("Downloading asset of media event: {}", self.asset_id);

        let url = self.get_upstream_url()?;

        let data = self
            .client
            .media()
            .get_file(self.content, true)
            .await?
            .ok_or(MediaError::NotFound)?;

        let extension = if let Some(file_name) = self.file_name {
            determine_file_extension_with_path(&data, file_name)
        } else {
            determine_file_extension_from_data(&data)
        };

        let download = Download {
            data,
            file_extension: extension,
            upstream_url: url,
        };

        Ok(download)
    }

    async fn upload(&mut self, _src: PathBuf) -> Result<Upload> {
        debug_assert_or_log!(false, "Uploading using the MediaEventAsset is not allowed");
        Err(MediaError::NotAllowed)
    }
}

/// Downloads the mrx resources at the specified URI.
async fn download_mxc(client: &Client, mxc_uri: &MxcUri) -> Result<Vec<u8>> {
    let request = MediaRequestParameters {
        source: MediaSource::Plain(mxc_uri.to_owned()),
        format: MediaFormat::File,
    };

    let content = client.media().get_media_content(&request, false).await?;

    Ok(content)
}

/// Attempts to determine the file extension of the specified data.
fn determine_file_extension_from_data(data: &[u8]) -> Option<String> {
    match infer::get(data) {
        Some(kind) => Some(kind.extension().to_string()),
        None => {
            log::error!("Error determining data file extension");
            None
        }
    }
}

/// Attempts to determine the file extension with the specified path, using
/// the data as a fallback.
fn determine_file_extension_with_path(data: &[u8], path: impl AsRef<Path>) -> Option<String> {
    path.as_ref()
        .extension()
        .map(|f| f.to_string_lossy().to_string())
        .or_else(|| determine_file_extension_from_data(data))
}

/// Attempts to determine the file extension as well as the mime type
/// given the specific data and path.
/// Will return a default value for the MIME type if we couldn't guess it.
fn determine_file_extension_and_mime(data: &[u8], path: &Path) -> (Option<String>, Mime) {
    let Some(file_extension) = determine_file_extension_with_path(data, path) else {
        return (None, DEFAULT_MIME);
    };

    let mime = mime_guess::from_ext(&file_extension)
        .first()
        .unwrap_or(DEFAULT_MIME);

    (Some(file_extension), mime)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempdir::TempDir;

    use super::*;
    use crate::test_utils;

    const TEMPDIR_PREFIX: &str = "gouda_matrix_adapter";
    const DATA_ROOT_DIR: &str = "data_root_dir";
    const ASSET_DIR: &str = "assets";
    const UPLOAD_DIR: &str = "uploads";
    const TIMESTAMP_PRECISION: u64 = 5;

    type MockedResult<T> = Arc<dyn Fn() -> Result<T> + Send + Sync>;

    struct AssetMock {
        asset_id: MockedResult<String>,
        is_up_to_date: MockedResult<bool>,
        was_removed: bool,
        download: MockedResult<Download>,
        upload: MockedResult<Upload>,
    }

    impl AssetMock {
        pub fn new(asset_id: impl Into<String>) -> Self {
            let asset_id = asset_id.into();

            Self {
                asset_id: Arc::new(move || Ok(asset_id.clone())),
                is_up_to_date: Arc::new(|| Err(MediaError::NotFound)),
                was_removed: false,
                download: Arc::new(|| Err(MediaError::NotFound)),
                upload: Arc::new(|| Err(MediaError::NotFound)),
            }
        }

        pub fn up_to_date(mut self, is_up_to_date: MockedResult<bool>) -> Self {
            self.is_up_to_date = is_up_to_date;
            self
        }

        pub fn was_removed(mut self, was_removed: bool) -> Self {
            self.was_removed = was_removed;
            self
        }

        pub fn download(mut self, download: MockedResult<Download>) -> Self {
            self.download = download;
            self
        }

        pub fn upload(mut self, upload: MockedResult<Upload>) -> Self {
            self.upload = upload;
            self
        }

        /// Helper method for `Self::is_up_to_date` to return the specified result.
        pub fn up_to_date_result(self, is_up_to_date: bool) -> Self {
            self.up_to_date(Arc::new(move || Ok(is_up_to_date)))
        }

        /// Helper method for `Self::download` to return the specified download.
        pub fn download_result(self, download: Download) -> Self {
            self.download(Arc::new(move || Ok(download.clone())))
        }

        /// Helper method for `Self::upload` to return the specified file extension.
        pub fn upload_result(self, upload: Upload) -> Self {
            self.upload(Arc::new(move || Ok(upload.clone())))
        }
    }

    #[async_trait]
    impl Asset for AssetMock {
        fn asset_id(&self) -> Result<String> {
            (self.asset_id)()
        }

        async fn is_up_to_date(&mut self, _info: &AssetInfo) -> Result<bool> {
            (self.is_up_to_date)()
        }

        async fn was_removed(&mut self) -> bool {
            self.was_removed
        }

        async fn download(&mut self) -> Result<Download> {
            (self.download)()
        }

        async fn upload(&mut self, _src: PathBuf) -> Result<Upload> {
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
    fn setup_asset_manager<T>(dirs: &Directories, asset: T) -> AssetManager<T>
    where
        T: Asset,
    {
        AssetManager::new(&dirs.data_root_dir, &dirs.asset_dir_relative, asset)
    }

    /// Creates the info and item file.
    fn setup_asset(
        dir: impl AsRef<Path>,
        id: &str,
        content: Vec<u8>,
        extension: &str,
        upstream_url: impl Into<String>,
    ) {
        let asset_file_name = format!("{id}.{extension}");

        let info_path = dir.as_ref().join(format!("{id}{INFO_FILE_SUFFIX}.json"));
        let asset_path = dir.as_ref().join(&asset_file_name);

        let info = AssetInfo {
            file: asset_file_name,
            download_ts: utils::get_unix_timestamp_seconds(),
            upstream_url: upstream_url.into(),
        };

        let serialized = serde_json::to_string_pretty(&info).unwrap();

        fs::write(info_path, serialized).unwrap();
        fs::write(asset_path, content).unwrap();
    }

    fn assert_info_file(info_file: impl AsRef<Path>, asset_file_name: &str, upstream_url: &str) {
        let content = fs::read(info_file).unwrap();
        let info: AssetInfo = serde_json::from_slice(&content).unwrap();

        assert_eq!(&info.file, asset_file_name);
        assert_eq!(&info.upstream_url, upstream_url);

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

        let download_result = Download {
            data: data.clone(),
            file_extension: Some(extension),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset").download_result(download_result);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&asset_path_absolute, &data);

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
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
            "mxc://some_asset",
        );

        let asset = AssetMock::new("some_asset").up_to_date_result(true);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(&asset_path_absolute, &content);

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_no_longer_up_to_date() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            vec![2, 3, 4],
            "png",
            "mrx://some_asset",
        );

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset2",
            vec![5, 2],
            "png",
            "mrx://some_asset2",
        );

        let content_new = vec![10, 20];
        let extension = "png".to_owned();

        let download_result = Download {
            data: content_new.clone(),
            file_extension: Some(extension),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset")
            .download_result(download_result)
            .up_to_date_result(false);

        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

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

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_new_file_extension() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            vec![2, 3, 4],
            "png",
            "mxc://some_asset",
        );

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset2",
            vec![5, 2],
            "png",
            "mxc://some_asset2",
        );

        let content_new = vec![10, 20];
        let extension = "jpeg".to_owned();

        let download_result = Download {
            data: content_new.clone(),
            file_extension: Some(extension),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset")
            .download_result(download_result)
            .up_to_date_result(false);

        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.download().await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.jpeg");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.jpeg"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.jpeg"));

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

        assert_info_file(info_path_absolute, "some_asset.jpeg", "mxc://some_asset");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_asset_manager_download_removed() {
        // Arrange
        let dirs = setup_directories();

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            vec![2, 3, 4],
            "png",
            "mxc://some_asset",
        );

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset2",
            vec![5, 2],
            "png",
            "mxc://some_asset",
        );

        let asset = AssetMock::new("some_asset")
            .was_removed(true)
            .up_to_date_result(false);

        let mut manager = setup_asset_manager(&dirs, asset);

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

        let upload_result = Upload {
            file_extension: Some("png".to_owned()),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset").upload_result(upload_result);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
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

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            vec![2, 3, 4],
            "png",
            "mxc://some_asset",
        );

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset2",
            vec![5, 2],
            "png",
            "mxc://some_asset2",
        );

        let upload_result = Upload {
            file_extension: Some("png".to_owned()),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset").upload_result(upload_result);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

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

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
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

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset",
            vec![2, 3, 4],
            "png",
            "mxc://some_asset",
        );

        setup_asset(
            &dirs.asset_dir_absolute,
            "some_asset2",
            vec![5, 2],
            "png",
            "mxc://some_asset2",
        );

        let upload_result = Upload {
            file_extension: Some("jpeg".to_owned()),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset").upload_result(upload_result);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.jpeg");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.jpeg"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.jpeg"));

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

        assert_info_file(info_path_absolute, "some_asset.jpeg", "mxc://some_asset");
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

        let upload_result = Upload {
            file_extension: Some("png".to_owned()),
            upstream_url: "mxc://some_asset".to_owned(),
        };

        let asset = AssetMock::new("some_asset").upload_result(upload_result);
        let mut manager = setup_asset_manager(&dirs, asset);

        // Act
        let result = manager.upload(upload_path_relative).await.unwrap();

        // Assert
        let asset_path_absolute = dirs.asset_dir_absolute.join("some_asset.png");
        let info_path_absolute = dirs.asset_dir_absolute.join("some_asset_info.json");

        #[cfg(not(windows))]
        assert_eq!(result, format!("{ASSET_DIR}/some_asset.png"));
        #[cfg(windows)]
        assert_eq!(result, format!("{ASSET_DIR}\\some_asset.png"));

        test_utils::assert_directory(dirs.upload_dir_absolute, vec!["other.jpg"]);
        test_utils::assert_directory(
            dirs.asset_dir_absolute,
            vec!["some_asset_info.json", "some_asset.png"],
        );

        test_utils::assert_file_content(asset_path_absolute, &asset_content);

        assert_info_file(info_path_absolute, "some_asset.png", "mxc://some_asset");
    }
}
