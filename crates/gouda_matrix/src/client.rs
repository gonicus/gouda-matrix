use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use gouda_core::{Client as ClientAbstraction, RequestContext, Result};
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;
use matrix_sdk::encryption::{BackupDownloadStrategy, EncryptionSettings};
use matrix_sdk::room::edit::EditedContent;
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContentWithoutRelation;
use matrix_sdk::ruma::{assign, OwnedUserId, RoomId, UserId};
use matrix_sdk::Client;
use ruma_common::{EventId, OwnedEventId};
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;
use url::Url;

use crate::events::EventManager;
use crate::media::MediaManager;
use crate::memory_cache::{self, MemoryCache};
use crate::proto_cache::ProtoCache;
use crate::session::Session;
use crate::user::UserManager;
use crate::verification::{self, VerificationManager};
use crate::{debug_assert_or_log, errors, messages, rooms, user};

const SESSION_DIR: &str = "session";
const MEDIA_DIR: &str = "media";
const CACHE_DIR: &str = "cache";
const AUTH_FILE: &str = "auth";

macro_rules! try_lock {
    ($lock_result:expr) => {{
        $lock_result.map_err(|_| errors::create_unknown("lock poisoined"))?
    }};
}

pub struct MatrixClient {
    inner: OnceCell<MatrixClientInner>,
}

impl Default for MatrixClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MatrixClient {
    pub fn new() -> Self {
        Self {
            inner: OnceCell::new(),
        }
    }

    fn inner(&self) -> Result<&MatrixClientInner> {
        let Some(inner) = self.inner.get() else {
            return Err(errors::create_error_msg(
                ErrorType::NotInitialized,
                "The client is not initialized",
            ));
        };

        Ok(inner)
    }
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    async fn initialize(
        &self,
        ctx: RequestContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        if self.inner.initialized() {
            return Err(errors::create_error(ErrorType::AlreadyInitialized));
        }

        let (client, result) = MatrixClientInner::new(ctx, request).await?;

        if let Err(err) = self.inner.set(client) {
            log::error!("Error when initializting client: {err}");
            return Err(errors::create_unknown("Unknown error initializing client"));
        }

        Ok(result)
    }

    async fn on_response(&self, response: ResponseContainer) {
        let Ok(inner) = self.inner() else {
            log::error!("Received response while client is not initialized");
            return;
        };

        let Some(content) = response.content else {
            log::error!("Received response with no content: {response:?}");
            return;
        };

        inner.on_response(content).await;
    }

    async fn get_login_flows(&self, ctx: RequestContext) -> Result<LoginFlowsResponse> {
        self.inner()?.get_login_flows(ctx).await
    }

    async fn get_identity_providers(
        &self,
        ctx: RequestContext,
    ) -> Result<IdentityProvidersResponse> {
        self.inner()?.get_identity_providers(ctx).await
    }

    async fn login_username_password(
        &self,
        ctx: RequestContext,
        request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        self.inner()?.login_username_password(ctx, request).await
    }

    async fn login_sso(
        &self,
        ctx: RequestContext,
        request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        self.inner()?.login_sso(ctx, request).await
    }

    async fn recovery_key_verification(
        &self,
        ctx: RequestContext,
        request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        self.inner()?.recovery_key_verification(ctx, request).await
    }

    async fn cross_signing_start(
        &self,
        ctx: RequestContext,
        request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        self.inner()?.cross_signing_start(ctx, request).await
    }

    async fn cross_signing_select_method(
        &self,
        ctx: RequestContext,
        request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        self.inner()?
            .cross_signing_select_method(ctx, request)
            .await
    }

    async fn cross_signing_confirm(
        &self,
        ctx: RequestContext,
        request: CrossSigningConfirmRequest,
    ) -> Result<()> {
        self.inner()?.cross_signing_confirm(ctx, request).await
    }

    async fn abort_verification(
        &self,
        ctx: RequestContext,
        request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        self.inner()?.abort_verification(ctx, request).await
    }

    async fn get_user(&self, ctx: RequestContext, request: UserRequest) -> Result<User> {
        self.inner()?.get_user(ctx, request).await
    }

    async fn search_users(
        &self,
        ctx: RequestContext,
        request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        self.inner()?.search_users(ctx, request).await
    }

    async fn set_status(&self, ctx: RequestContext, request: UserStatus) -> Result<()> {
        self.inner()?.set_status(ctx, request).await
    }

    async fn get_public_rooms(
        &self,
        ctx: RequestContext,
        request: PublicRoomListRequest,
    ) -> Result<PublicRoomListResponse> {
        self.inner()?.get_public_rooms(ctx, request).await
    }

    async fn invite(
        &self,
        ctx: RequestContext,
        request: InvitationRequest,
    ) -> Result<RoomChangeEvent> {
        self.inner()?.invite(ctx, request).await
    }

    async fn invitation_reply(&self, ctx: RequestContext, request: InvitedReply) -> Result<()> {
        self.inner()?.invitation_reply(ctx, request).await
    }

    async fn get_rooms(
        &self,
        ctx: RequestContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        self.inner()?.get_rooms(ctx, request).await
    }

    async fn create_group_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        self.inner()?.create_group_room(ctx, request).await
    }

    async fn create_direct_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        self.inner()?.create_direct_room(ctx, request).await
    }

    async fn change_room(
        &self,
        ctx: RequestContext,
        request: RoomChangeRequest,
    ) -> Result<RoomChangeEvent> {
        self.inner()?.change_room(ctx, request).await
    }

    async fn leave_room(
        &self,
        ctx: RequestContext,
        request: RoomLeaveRequest,
    ) -> Result<RoomLeftEvent> {
        self.inner()?.leave_room(ctx, request).await
    }

    async fn join_room(&self, ctx: RequestContext, request: RoomJoinRequest) -> Result<Room> {
        self.inner()?.join_room(ctx, request).await
    }

    async fn knock_room(&self, ctx: RequestContext, request: RoomKnockRequest) -> Result<()> {
        self.inner()?.knock_room(ctx, request).await
    }

    async fn get_room_messages(
        &self,
        ctx: RequestContext,
        request: RoomMessagesRequest,
    ) -> Result<()> {
        self.inner()?.get_room_messages(ctx, request).await
    }

    async fn mark_as_read(
        &self,
        ctx: RequestContext,
        request: RoomMarkAsReadRequest,
    ) -> Result<RoomChangeEvent> {
        self.inner()?.mark_as_read(ctx, request).await
    }

    async fn send_message(
        &self,
        ctx: RequestContext,
        request: MessageSendRequest,
    ) -> Result<MessageSendResponse> {
        self.inner()?.send_message(ctx, request).await
    }

    async fn remove_message(
        &self,
        ctx: RequestContext,
        request: MessageRemoveRequest,
    ) -> Result<()> {
        self.inner()?.remove_message(ctx, request).await
    }

    async fn change_message(
        &self,
        ctx: RequestContext,
        request: MessageChangeRequest,
    ) -> Result<()> {
        self.inner()?.change_message(ctx, request).await
    }

    async fn create_reaction(&self, ctx: RequestContext, request: Reaction) -> Result<()> {
        self.inner()?.create_reaction(ctx, request).await
    }

    async fn remove_reaction(&self, ctx: RequestContext, request: Reaction) -> Result<()> {
        self.inner()?.remove_reaction(ctx, request).await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Contains session data not shared across multiple different sessions.
#[derive(Clone)]
pub struct SessionContext {
    /// The actual matrix client.
    pub client: Client,
    /// Contains the in memory cache.
    pub memory_cache: MemoryCache,
    /// The persistent proto cache storing proto messages for the next application start.
    pub proto_cache: ProtoCache,
    /// Manages data stored on the file system, like avatars or downloaded chat images.
    pub media_manager: MediaManager,
    /// Manages incoming events.
    pub event_manager: EventManager,
}

impl SessionContext {
    pub async fn new(
        ctx: RequestContext,
        homeserver_url: Url,
        data_root_dir: PathBuf,
        encryption_secret: String,
        database_secret: &str,
    ) -> Result<Self> {
        let client = build_client(
            &homeserver_url,
            &get_session_dir(&data_root_dir),
            database_secret,
        )
        .await?;

        let proto_cache = ProtoCache::new(get_cache_dir(&data_root_dir), encryption_secret).await;

        let media_manager = MediaManager::new(
            client.clone(),
            data_root_dir.clone(),
            PathBuf::from(MEDIA_DIR),
        )
        .await;

        let memory_cache = MemoryCache::new(ctx.clone(), media_manager.clone());

        let event_manager = EventManager::new(
            client.clone(),
            ctx,
            memory_cache.clone(),
            media_manager.clone(),
        );

        Ok(Self {
            client,
            memory_cache,
            proto_cache,
            media_manager,
            event_manager,
        })
    }
}

struct MatrixClientInner {
    /// The context of the session.
    session: RwLock<Arc<SessionContext>>,

    /// Contains cached identity providers. The idps are cached when `Self::get_login_flows`
    /// is called, as this method already retrieves the available idps.
    cached_idps: Mutex<Option<Vec<String>>>,
    /// The current active verification processes.
    verification_requests: Mutex<Vec<VerificationManager>>,

    /// The homeserver url.
    homeserver_url: Url,
    /// The display name of this device.
    device_display_name: String,
    /// The absolute path to the root directory where data is stored.
    data_root_dir: PathBuf,
    /// The passphrase used to encrypt the session data.
    encryption_secret: String,
    /// The passphrase used to encrypt the database.
    database_secret: String,
}

impl MatrixClientInner {
    pub async fn new(
        ctx: RequestContext,
        request: InitializationRequest,
    ) -> Result<(Self, StatusUpdate)> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let data_root_dir = PathBuf::from(&request.data_root_path);

        let session = SessionContext::new(
            ctx.clone(),
            homeserver_url.clone(),
            data_root_dir.clone(),
            request.encryption_secret.clone(),
            &request.persistent_storage_secret,
        )
        .await?;

        let obj = Self {
            session: RwLock::new(Arc::new(session)),

            homeserver_url,
            device_display_name: request.device_display_name,
            data_root_dir,
            encryption_secret: request.encryption_secret,
            database_secret: request.persistent_storage_secret,

            cached_idps: Mutex::new(None),
            verification_requests: Mutex::new(Vec::new()),
        };

        if obj.get_auth_file().exists() {
            match obj.restore_session(ctx).await {
                Ok(()) => {
                    return Ok((
                        obj,
                        StatusUpdate {
                            code: status_update::StatusCode::LoggedIn as i32,
                        },
                    ))
                }
                Err(err) => log::error!("Error restoring session: {err:?}"),
            }
        }

        let status = StatusUpdate {
            code: status_update::StatusCode::Connected as i32,
        };

        Ok((obj, status))
    }

    /// Checks if the client is still logged in.
    /// This method sends a request to the Matrix server.
    async fn is_logged_in(&self) -> Result<bool> {
        let result = self
            .session()?
            .client
            .whoami()
            .await
            .map_err(errors::convert_http_error);

        if let Err(err) = result {
            if err.r#type == ErrorType::Authorization as i32 {
                return Ok(false);
            }

            return Err(err);
        }

        Ok(true)
    }

    /// Deletes the persisted session and resets the matrix client.
    async fn reset_session(&self, ctx: RequestContext) -> Result<()> {
        log::info!("Resetting session");

        self.remove_session_data()?;

        let session = SessionContext::new(
            ctx,
            self.homeserver_url.clone(),
            self.data_root_dir.clone(),
            self.encryption_secret.clone(),
            &self.database_secret,
        )
        .await?;

        let mut writer = try_lock!(self.session.write());
        *writer = Arc::new(session);

        log::info!("Successfully reset session");

        Ok(())
    }

    /// Restores the session from the session file.
    async fn restore_session(&self, ctx: RequestContext) -> Result<()> {
        let session_file = self.get_auth_file();

        log::debug!("Previous session found in '{session_file:?}'");

        let session = Session::read_from_file(session_file, self.encryption_secret.clone()).await?;

        log::info!(
            "Restoring session for {}",
            session.user_session.meta.user_id
        );

        self.session()?
            .client
            .restore_session(session.user_session.clone())
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        let session_context = self.session()?;

        session
            .sync(ctx.clone(), (*session_context).clone())
            .await?;

        log::info!(
            "Successfully restored session as {:?}",
            session_context.client.user_id()
        );

        Ok(())
    }

    /// Removes all data related to the current session.
    /// Including authentication, cache and media data.
    /// This method blocks until all data has been removed.
    /// A new login is required afterwards and the current session context
    /// should be reset.
    /// This method intentionally uses the std::fs methods instead of the async tokio::fs
    /// methods, in order to block until all data has been removed.
    fn remove_session_data(&self) -> Result<()> {
        log::info!("Removing session data");

        remove_directory(self.get_session_dir())?;
        remove_directory(self.get_media_dir())?;
        remove_directory(self.get_cache_dir())?;

        Ok(())
    }

    /// Gets a `matrix_sdk::Room` room by its id.
    /// Returns an `Err` when the room was not found or the ID is invalid.
    async fn get_matrix_room(&self, room_id: &str) -> Result<matrix_sdk::Room> {
        let room_id =
            RoomId::parse(room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = self
            .session()?
            .client
            .get_room(&room_id)
            .ok_or(errors::create_error(ErrorType::RoomNotFound))?;

        Ok(room)
    }

    /// Caches the given identity providers.
    /// This will overwrite existing ones.
    fn cache_idps(&self, idps: Vec<String>) -> Result<()> {
        let mut guard = try_lock!(self.cached_idps.lock());
        *guard = Some(idps);
        Ok(())
    }

    /// Gets the currently cached identity providers, if any.
    fn get_cached_idps(&self) -> Result<Option<Vec<String>>> {
        let lock = try_lock!(self.cached_idps.lock());
        Ok(lock.clone())
    }

    /// Gets the absolute path to the directory where session data is stored.
    fn get_session_dir(&self) -> PathBuf {
        get_session_dir(&self.data_root_dir)
    }

    /// Gets the absolute path to the directory where media files are stored.
    fn get_media_dir(&self) -> PathBuf {
        get_media_dir(&self.data_root_dir)
    }

    /// Gets the absolute path to the directory where cached data is stored.
    fn get_cache_dir(&self) -> PathBuf {
        get_cache_dir(&self.data_root_dir)
    }

    /// Gets the absolute path to the file where authentication for the
    /// current session is stored.
    /// This includes auth tokens as well as refresh tokens.
    fn get_auth_file(&self) -> PathBuf {
        self.get_session_dir().join(AUTH_FILE)
    }

    /// Gets the session context.
    /// Only returns an error when the session lock is poisoined.
    fn session(&self) -> Result<Arc<SessionContext>> {
        let reader = try_lock!(self.session.read());
        Ok((*reader).clone())
    }

    /// Pushes a new ongoing verification flow to the managed verification flows.
    fn push_verification_request(&self, manager: VerificationManager) -> Result<()> {
        let mut guard = try_lock!(self.verification_requests.lock());
        guard.push(manager);
        Ok(())
    }

    /// Removes all finished verification requests.
    fn cleanup_verifications(&self) -> Result<()> {
        let mut guard = try_lock!(self.verification_requests.lock());
        guard.retain(|f| f.is_active());
        Ok(())
    }
}

/// Contains the actual client implementation methods.
impl MatrixClientInner {
    async fn on_response(&self, content: ResponseContent) {
        match self.session() {
            Ok(session) => session.proto_cache.cache_response_content(content),
            Err(err) => log::error!("Unable to cache response content: {err:?}"),
        }
    }

    async fn get_login_flows(&self, _ctx: RequestContext) -> Result<LoginFlowsResponse> {
        use matrix_sdk::ruma::api::client::session::get_login_types::v3;
        use v3::LoginType as MatrixLoginType;

        let session = self.session()?;
        let SessionContext { client, .. } = session.as_ref();

        let login_types = client
            .matrix_auth()
            .get_login_types()
            .await
            .map_err(|err| errors::create_error_msg(ErrorType::Network, err))?;

        let mut response = LoginFlowsResponse::default();

        for flow in &login_types.flows {
            match flow {
                MatrixLoginType::Password(_) => {
                    response.push_login_flows(login_flows_response::LoginFlow::UsernamePassword)
                }
                MatrixLoginType::Sso(sso) => {
                    // We already have access to the available identity providers and store
                    // them in the cache so that `Self::get identity_providers` does not
                    // have to retrieve them again.
                    let idps = sso
                        .identity_providers
                        .iter()
                        .map(|f| f.id.to_owned())
                        .collect();

                    self.cache_idps(idps)?;

                    response.push_login_flows(login_flows_response::LoginFlow::Sso)
                }
                _ => (),
            }
        }

        Ok(response)
    }

    async fn get_identity_providers(
        &self,
        ctx: RequestContext,
    ) -> Result<IdentityProvidersResponse> {
        // Check if the idps have been retrieved before
        if let Some(idps) = self.get_cached_idps()? {
            return Ok(IdentityProvidersResponse {
                identity_providers: idps,
            });
        }

        // We can use the `Self::get_login_flows` method to retrieve the idps as it saves
        // them to the cache. This method would ultimately only fetch the login flows too.
        let _ = self.get_login_flows(ctx).await?;

        // If there is still nothing in the cache, no idps are available or single sign-on
        // is not supported by the server. In this case we can just return an empty list.
        let idps = self.get_cached_idps()?.unwrap_or_default();

        Ok(IdentityProvidersResponse {
            identity_providers: idps,
        })
    }

    async fn login_username_password(
        &self,
        ctx: RequestContext,
        request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        if self.is_logged_in().await? {
            return Err(errors::create_error(ErrorType::AlreadyLoggedIn));
        }

        self.reset_session(ctx.clone()).await?;

        let session_context = self.session()?;

        session_context
            .client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(&self.device_display_name)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        log::info!(
            "Successfully logged in as {:?}",
            session_context.client.user_id()
        );

        let session = Session::new(
            &session_context.client,
            self.get_auth_file(),
            self.encryption_secret.clone(),
        )?;

        session.save().await?;

        session
            .sync(ctx.clone(), (*session_context).clone())
            .await?;

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &self,
        ctx: RequestContext,
        request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        if self.is_logged_in().await? {
            return Err(errors::create_error(ErrorType::AlreadyLoggedIn));
        }

        self.reset_session(ctx.clone()).await?;

        let session_context = self.session()?;

        // Create a channel so we can receive the login url from the async closure
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut login_builder = session_context
            .client
            .matrix_auth()
            .login_sso(|url| async move {
                #[allow(clippy::expect_used)]
                tx.send(url).expect("Receiver of the login url dropped");
                Ok(())
            })
            .initial_device_display_name(&self.device_display_name);

        if let Some(idp) = request.identity_provider {
            login_builder = login_builder.identity_provider_id(&idp);
        }

        // Clone the data so we can move it into the tokio task
        let session_context = (*session_context).clone();
        let session_passphrase = self.encryption_secret.clone();
        let session_file = self.get_auth_file();

        // Spawn a tokio task which waits for the successful login in order to send
        // a status update to the application.
        tokio::spawn(async move {
            if let Err(err) = login_builder.await {
                ctx.send_error(errors::convert_matrix_sdk_error(err)).await;
                return;
            }

            log::info!(
                "Successfully logged in as {:?}",
                session_context.client.user_id()
            );

            let Ok(session) =
                Session::new(&session_context.client, session_file, session_passphrase)
            else {
                ctx.send_error(errors::create_unknown("Error creating session"))
                    .await;
                return;
            };

            if let Err(err) = session.save().await {
                ctx.send_error(err).await;
                return;
            }

            let result = session.sync(ctx.clone(), session_context).await;

            if let Err(err) = result {
                ctx.send_error(err).await;
                return;
            }

            ctx.send_event(ResponseContent::StatusUpdate(StatusUpdate {
                code: status_update::StatusCode::LoggedIn as i32,
            }))
            .await;
        });

        // Wait until the asynchronous closure sends the received login URL, so
        // we can return it to the application.
        let login_url = rx.await.map_err(|_| {
            errors::create_unknown("InternalError: Sender of the login url dropped")
        })?;

        Ok(LoginSsoResponse { login_url })
    }

    async fn recovery_key_verification(
        &self,
        _ctx: RequestContext,
        request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        let session = self.session()?;
        let SessionContext { client, .. } = session.as_ref();

        client
            .encryption()
            .recovery()
            .recover(&request.recovery_key)
            .await
            .map_err(errors::convert_recovery_error)?;

        Ok(VerificationEndEvent {
            verification_flow_id: None,
            result: Some(verification_end_event::Result::Successful(true)),
        })
    }

    async fn cross_signing_start(
        &self,
        ctx: RequestContext,
        request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        self.cleanup_verifications()?;

        let session = self.session()?;
        let SessionContext { client, .. } = session.as_ref();

        let Some(user_id) = client.user_id() else {
            return Err(errors::create_unknown(
                "InternalError: Client not logged in",
            ));
        };

        let user_identity = client
            .encryption()
            .get_user_identity(user_id)
            .await
            .map_err(errors::convert_crypto_store_error)?
            .ok_or(errors::create_unknown(
                "InternalError: User identity not found",
            ))?;

        let methods = verification::cross_signing_methods_to_matrix(request.supported_methods);

        let request = user_identity
            .request_verification_with_methods(methods)
            .await
            .map_err(errors::convert_request_verification_error)?;

        let verification_flow_id = request.flow_id().to_owned();
        let manager = VerificationManager::from_verification_request(ctx, request);

        self.push_verification_request(manager)?;

        Ok(CrossSigningStartResponse {
            verification_flow_id,
        })
    }

    async fn cross_signing_select_method(
        &self,
        _ctx: RequestContext,
        request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        self.cleanup_verifications()?;

        let CrossSigningMethodSelectedRequest {
            verification_flow_id,
            selected_method,
        } = request;

        let mut guard = try_lock!(self.verification_requests.lock());
        let manager = guard
            .iter_mut()
            .find(|p| p.flow_id() == verification_flow_id);

        let Some(manager) = manager else {
            return Err(errors::create_error_msg(
                ErrorType::VerificationFlowNotFound,
                "Verification flow with the given ID not found",
            ));
        };

        let Ok(method) = CrossSigningMethod::try_from(selected_method) else {
            return Err(errors::create_unknown("Unsupported cross signing method"));
        };

        manager.select_method(method);

        Ok(())
    }

    async fn cross_signing_confirm(
        &self,
        _ctx: RequestContext,
        request: CrossSigningConfirmRequest,
    ) -> Result<()> {
        self.cleanup_verifications()?;

        let CrossSigningConfirmRequest {
            verification_flow_id,
        } = request;

        let mut guard = try_lock!(self.verification_requests.lock());
        let manager = guard
            .iter_mut()
            .find(|p| p.flow_id() == verification_flow_id);

        let Some(manager) = manager else {
            return Err(errors::create_error_msg(
                ErrorType::VerificationFlowNotFound,
                "Verification flow with the given ID not found",
            ));
        };

        manager.confirm();

        Ok(())
    }

    async fn abort_verification(
        &self,
        _ctx: RequestContext,
        request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        self.cleanup_verifications()?;

        let VerificationAbortRequest {
            verification_flow_id,
        } = request;

        let mut guard = try_lock!(self.verification_requests.lock());
        let position = guard
            .iter()
            .position(|p| p.flow_id() == verification_flow_id);

        if let Some(index) = position {
            let manager = guard.swap_remove(index);

            manager.cancel();

            Ok(VerificationEndEvent {
                verification_flow_id: Some(verification_flow_id.clone()),
                result: Some(verification_end_event::Result::Successful(false)),
            })
        } else {
            Err(errors::create_error_msg(
                ErrorType::VerificationFlowNotFound,
                "Verification flow with the given ID not found",
            ))
        }
    }

    async fn get_user(&self, ctx: RequestContext, request: UserRequest) -> Result<User> {
        let user_id = UserId::parse(request.user_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidUserId))?
            .to_owned();

        let session = self.session()?;

        let user_manager = UserManager::from_session(ctx, session.as_ref());

        user_manager.get_and_sync_user(user_id).await
    }

    async fn search_users(
        &self,
        _ctx: RequestContext,
        request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        let UserSearchRequest { query, limit } = request;

        let session = self.session()?;
        let SessionContext {
            client,
            media_manager,
            ..
        } = session.as_ref();

        let user_list = client
            .search_users(&query, limit as u64)
            .await
            .map_err(errors::convert_http_error)?;

        let mut result = Vec::new();

        for user in user_list.results {
            result.push(User {
                user_id: user.user_id.to_string(),
                display_name: user.display_name.clone(),
                avatar_path: media_manager
                    .get_user_directory_user_avatar_path(&user)
                    .await,
                status: user::fetch_status(client, &user.user_id).await.ok(),
            });
        }

        Ok(UserSearchResponse { user_list: result })
    }

    async fn set_status(&self, _ctx: RequestContext, request: UserStatus) -> Result<()> {
        use matrix_sdk::ruma::api::client::presence::set_presence::v3::Request;

        let session = self.session()?;
        let SessionContext {
            client,
            proto_cache,
            ..
        } = session.as_ref();

        let Some(user_id) = client.user_id() else {
            debug_assert_or_log!(false, "User ID not set");
            return Err(errors::create_unknown("User ID not set"));
        };

        let presence_state = user::chat_presence_state_to_matrix(request.state());

        let Some(state) = presence_state else {
            return Err(errors::create_unknown("Invalid presence state"));
        };

        let mut matrix_request = Request::new(user_id.to_owned(), state);
        matrix_request.status_msg = request.status_message.clone();

        client
            .send(matrix_request)
            .await
            .map_err(errors::convert_http_error)?;

        proto_cache.set_user_status(request);

        Ok(())
    }

    async fn get_public_rooms(
        &self,
        _ctx: RequestContext,
        request: PublicRoomListRequest,
    ) -> Result<PublicRoomListResponse> {
        use matrix_sdk::ruma::api::client::directory::get_public_rooms_filtered;
        use ruma_common::directory::Filter;

        let session = self.session()?;
        let SessionContext { client, .. } = session.as_ref();

        let PublicRoomListRequest {
            limit,
            since,
            generic_search_term,
        } = request;

        let filter = assign!(Filter::default(), {
            generic_search_term: generic_search_term,
        });

        let request = assign!(get_public_rooms_filtered::v3::Request::default(), {
            limit: limit.map(|f| f.into()),
            since: since,
            filter: filter,
        });

        let result = client
            .public_rooms_filtered(request)
            .await
            .map_err(errors::convert_http_error)?;

        let rooms = rooms::convert_public_rooms_chunk(result.chunk);

        Ok(PublicRoomListResponse {
            room_list: rooms,
            next_batch: result.next_batch,
        })
    }

    async fn invite(
        &self,
        _ctx: RequestContext,
        request: InvitationRequest,
    ) -> Result<RoomChangeEvent> {
        let room = self.get_matrix_room(&request.room_id).await?;

        let invitees: Vec<OwnedUserId> = request
            .invitees
            .into_iter()
            .map(|id| UserId::parse(&id).map_err(errors::convert_id_parse_error))
            .collect::<Result<Vec<OwnedUserId>>>()?;

        for invite in invitees {
            if let Err(err) = room.invite_user_by_id(&invite).await {
                log::error!("Error inviting user: {err}");
            };
        }

        // Refresh the room
        let room = self.get_matrix_room(&request.room_id).await?;
        let members = rooms::get_members(&room).await?;

        Ok(
            builder::RoomChangeEventBuilder::new(request.room_id.clone())
                .change_user_id_list(members)
                .to_proto(),
        )
    }

    async fn invitation_reply(&self, ctx: RequestContext, request: InvitedReply) -> Result<()> {
        let session = self.session()?;
        let SessionContext {
            client,
            media_manager,
            ..
        } = session.as_ref();

        let Some(user_id) = client.user_id() else {
            log::error!("Error retrieving the user ID of the current user");
            return Err(errors::create_unknown(
                "Error retrieving the user ID of the current user",
            ));
        };

        let InvitedReply { room_id, accepted } = request;

        let room = self.get_matrix_room(&room_id).await?;

        if !accepted {
            room.leave()
                .await
                .map_err(errors::convert_matrix_sdk_error)?;

            log::info!("Successfully declined invitation for room: {room_id:?}");

            return Ok(());
        }

        room.join()
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        log::info!("Successfully accepted invitation for room: {room_id:?}");

        let proto = rooms::convert_to_proto(media_manager, room, user_id).await?;

        ctx.send_event(ResponseContent::RoomCreatedEvent(proto))
            .await;

        Ok(())
    }

    async fn get_rooms(
        &self,
        ctx: RequestContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        let session = self.session()?;

        let Some(user_id) = session.client.user_id() else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let room_manager = rooms::RoomManager::from_session(ctx, session.as_ref());
        let room_list = room_manager.get_and_sync_rooms().await?;

        let mut result = Vec::new();

        for room in room_list {
            let joined = room
                .user_id_list
                .iter()
                .find(|p| p.0 == user_id.as_str())
                .map(|f| *f.1 == UserRoomState::Joined as i32)
                .unwrap_or(false);

            if request.include_joined && joined {
                result.push(room);
                continue;
            }

            if request.include_unjoined && !joined {
                result.push(room);
                continue;
            }
        }

        Ok(RoomListResponse { room_list: result })
    }

    async fn create_group_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        let session = self.session()?;
        let SessionContext {
            client,
            media_manager,
            ..
        } = session.as_ref();

        let Some(user_id) = client.user_id() else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let RoomCreateGroupRequest {
            display_name,
            invitees,
            join_rule,
            avatar_path,
        } = request;

        let join_rule = RoomJoinRule::try_from(join_rule)
            .map_err(|_| errors::create_unknown("Invalid RoomJoinRule"))?;

        let invitees: Vec<OwnedUserId> = invitees
            .into_iter()
            .map(|id| UserId::parse(&id).map_err(errors::convert_id_parse_error))
            .collect::<Result<Vec<OwnedUserId>>>()?;

        let room_request = rooms::create_room_request(display_name.clone(), invitees, join_rule);

        let room = client
            .create_room(room_request)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        if let Some(avatar_path) = avatar_path {
            let result = media_manager
                .upload_room_avatar(&room, &PathBuf::from(avatar_path))
                .await
                .map_err(|_| errors::create_unknown("Error uploading room avatar"));

            if let Err(err) = result {
                ctx.send_error(err).await;
            }
        }

        rooms::convert_to_proto(media_manager, room, user_id).await
    }

    async fn create_direct_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        let session = self.session()?;
        let SessionContext {
            client,
            media_manager,
            ..
        } = session.as_ref();

        let Some(our_user_id) = client.user_id() else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let invitee_user_id =
            UserId::parse(&request.invitee).map_err(errors::convert_id_parse_error)?;

        let room_request =
            rooms::create_dm_room_request(request.display_name.clone(), invitee_user_id.to_owned());

        let room = client
            .create_room(room_request)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        if let Some(avatar_path) = request.avatar_path {
            let result = media_manager
                .upload_room_avatar(&room, &PathBuf::from(avatar_path))
                .await
                .map_err(|_| errors::create_unknown("Error uploading room avatar"));

            if let Err(err) = result {
                ctx.send_error(err).await;
            }
        }

        rooms::convert_to_proto(media_manager, room, our_user_id).await
    }

    async fn change_room(
        &self,
        ctx: RequestContext,
        request: RoomChangeRequest,
    ) -> Result<RoomChangeEvent> {
        let RoomChangeRequest {
            room_id,
            display_name,
            join_rule,
            avatar_path,
            is_favorite,
        } = request;

        let session = self.session()?;
        let SessionContext { media_manager, .. } = session.as_ref();

        let room = self.get_matrix_room(&room_id).await?;

        let mut response = builder::RoomChangeEventBuilder::new(room_id.to_string());

        if let Some(display_name) = display_name {
            room.set_name(display_name.clone())
                .await
                .map_err(errors::convert_matrix_sdk_error)?;

            response = response.change_display_name(display_name);
        }

        if let Some(join_rule) = join_rule {
            let join_rule = RoomJoinRule::try_from(join_rule)
                .map_err(|_| errors::create_unknown("Invalid JoinRule"))?;

            rooms::update_room_join_rule(&room, join_rule).await?;
            response = response.change_join_rule(join_rule);
        }

        if let Some(avatar_path) = avatar_path {
            let result = media_manager
                .upload_room_avatar(&room, &PathBuf::from(&avatar_path))
                .await
                .map_err(|_| errors::create_unknown("Error uploading room avatar"));

            if let Err(err) = result {
                ctx.send_error(err).await;
            } else {
                response = response.change_avatar_path(avatar_path);
            }
        }

        if let Some(is_favourite) = is_favorite {
            room.set_is_favourite(is_favourite, None)
                .await
                .map_err(errors::convert_matrix_sdk_error)?;

            response = response.change_is_favourite(is_favourite);
        }

        Ok(response.to_proto())
    }

    async fn leave_room(
        &self,
        _ctx: RequestContext,
        request: RoomLeaveRequest,
    ) -> Result<RoomLeftEvent> {
        let room = self.get_matrix_room(&request.room_id).await?;

        room.leave()
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(RoomLeftEvent {
            room_id: request.room_id,
            reason: room_left_event::RoomLeaveReason::User.into(),
            message: None,
        })
    }

    async fn join_room(&self, _ctx: RequestContext, request: RoomJoinRequest) -> Result<Room> {
        let session = self.session()?;
        let SessionContext {
            client,
            media_manager,
            ..
        } = session.as_ref();

        let Some(user_id) = client.user_id().map(|f| f.to_owned()) else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let room_id = RoomId::parse(&request.room_id)
            .map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = client
            .join_room_by_id(&room_id)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        rooms::convert_to_proto(media_manager, room, &user_id).await
    }

    async fn knock_room(&self, _ctx: RequestContext, request: RoomKnockRequest) -> Result<()> {
        let RoomKnockRequest { room_id, message } = request;

        let session = self.session()?;
        let SessionContext { client, .. } = session.as_ref();

        let room_id =
            RoomId::parse(&room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        client
            .knock(room_id.into(), message, Vec::new())
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    async fn get_room_messages(
        &self,
        ctx: RequestContext,
        request: RoomMessagesRequest,
    ) -> Result<()> {
        let room = self.get_matrix_room(request.room_id.as_str()).await?;

        let session = self.session()?;
        let SessionContext { memory_cache, .. } = session.as_ref();

        if request.order == Some(MessagesOrder::Forward.into()) {
            return Err(errors::create_unknown(
                "MessagesOrder::Forward is currently not supported",
            ));
        }

        let limit = request.limit.unwrap_or(10);

        let from_message_id = request
            .from_message_id
            .as_ref()
            .map(|v| {
                OwnedEventId::from_str(v)
                    .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))
            })
            .transpose()?;

        let query_options = memory_cache::QueryOptions {
            from_message_id,
            limit,
        };

        let mut stream = memory_cache
            .fetch_messages(room, query_options)
            .await
            .map_err(errors::convert_memory_cache_error)?;

        let multipart_response = ctx.begin_multipart_response();

        while let Some(result) = stream.next().await {
            let message = result.map_err(errors::convert_memory_cache_error)?;

            multipart_response
                .send_item(ResponseContent::MessageReceivedEvent(message))
                .await;
        }

        Ok(())
    }

    async fn mark_as_read(
        &self,
        _ctx: RequestContext,
        request: RoomMarkAsReadRequest,
    ) -> Result<RoomChangeEvent> {
        use matrix_sdk::ruma::api::client::receipt::create_receipt::v3::ReceiptType;
        use matrix_sdk::ruma::events::receipt::ReceiptThread;

        let room = self.get_matrix_room(&request.room_id).await?;
        let options = MessagesOptions::new(matrix_sdk::ruma::api::Direction::Backward);

        let messages = room
            .messages(options)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        let event = messages
            .chunk
            .first()
            .ok_or(errors::create_unknown("No event found"))?;

        let event_id = event
            .event_id()
            .ok_or(errors::create_unknown("Invalid event ID"))?;

        room.send_single_receipt(ReceiptType::Read, ReceiptThread::Unthreaded, event_id)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(
            builder::RoomChangeEventBuilder::new(request.room_id.clone())
                .change_unread_count(0)
                .to_proto(),
        )
    }

    async fn send_message(
        &self,
        _ctx: RequestContext,
        request: MessageSendRequest,
    ) -> Result<MessageSendResponse> {
        use message_send_request::Content;

        let MessageSendRequest {
            room_id,
            related_message_id,
            mentioned_user_ids,
            content,
        } = request;

        let session = self.session()?;
        let SessionContext { media_manager, .. } = session.as_ref();

        let room = self.get_matrix_room(&room_id).await?;

        let Some(content) = content else {
            return Err(errors::create_unknown("Message content not set"));
        };

        match content {
            Content::Text(content) => {
                messages::send_text_message(room, related_message_id, mentioned_user_ids, content)
                    .await
            }
            Content::Image(content) => {
                messages::send_image_message(media_manager, room, related_message_id, content).await
            }
            Content::File(content) => {
                messages::send_file_message(media_manager, room, related_message_id, content).await
            }
        }
    }

    async fn remove_message(
        &self,
        _ctx: RequestContext,
        request: MessageRemoveRequest,
    ) -> Result<()> {
        let MessageRemoveRequest {
            room_id,
            message_id,
        } = request;

        let room = self.get_matrix_room(&room_id).await?;

        let event_id = EventId::parse(message_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))?;

        room.redact(&event_id, None, None)
            .await
            .map_err(errors::convert_http_error)?;

        Ok(())
    }

    async fn change_message(
        &self,
        _ctx: RequestContext,
        request: MessageChangeRequest,
    ) -> Result<()> {
        use message_change_request::Content;

        let MessageChangeRequest {
            room_id,
            message_id,
            has_mentioned_user_ids_changed,
            mentioned_user_ids,
            content,
        } = request;

        let room = self.get_matrix_room(&room_id).await?;

        let event_id = EventId::parse(message_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))?;

        let Some(content) = content else {
            return Err(errors::create_unknown("Message content not set"));
        };

        let content = match content {
            Content::Text(text) => {
                let mut event = RoomMessageEventContentWithoutRelation::text_markdown(text.content);

                if has_mentioned_user_ids_changed {
                    let mentions =
                        messages::proto_mentions_to_matrix_mentions(&mentioned_user_ids)?;
                    event = event.add_mentions(mentions);
                }

                event
            }
            Content::Image(_) => {
                return Err(errors::create_error(ErrorType::NotImplemented));
            }
            Content::File(_) => {
                return Err(errors::create_error(ErrorType::NotImplemented));
            }
        };

        let event = room
            .make_edit_event(&event_id, EditedContent::RoomMessage(content))
            .await
            .map_err(errors::convert_edit_error)?;

        room.send(event)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    async fn create_reaction(&self, _ctx: RequestContext, request: Reaction) -> Result<()> {
        let Reaction {
            room_id,
            message_id,
            reaction,
            ..
        } = request;

        let room = self.get_matrix_room(&room_id).await?;

        let message_id = OwnedEventId::try_from(message_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))?;

        let event = ReactionEventContent::new(Annotation::new(message_id, reaction));

        room.send(event)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    async fn remove_reaction(&self, ctx: RequestContext, request: Reaction) -> Result<()> {
        let Reaction {
            room_id,
            message_id,
            reaction,
            user_id,
        } = request;

        let session = self.session()?;
        let SessionContext {
            client,
            memory_cache,
            ..
        } = session.as_ref();

        let room = self.get_matrix_room(&room_id).await?;

        let user_id =
            user_id.unwrap_or(client.user_id().map(|f| f.to_string()).unwrap_or_default());

        let cached_reaction =
            memory_cache.remove_reaction_by_emoji(&room_id, &message_id, &user_id, &reaction);

        let Some(cached_reaction) = cached_reaction else {
            return Err(errors::create_error(ErrorType::ReactionNotFound));
        };

        let Ok(event_id) = EventId::parse(&cached_reaction.reaction_id) else {
            log::error!("Unable to parse cached reaction ID to an event ID");
            return Err(errors::create_error(ErrorType::ReactionNotFound));
        };

        room.redact(&event_id, None, None)
            .await
            .map_err(errors::convert_http_error)?;

        let proto = Reaction {
            room_id,
            message_id,
            reaction,
            user_id: Some(user_id),
        };

        ctx.send_event(ResponseContent::ReactionRemovedEvent(proto))
            .await;

        Ok(())
    }
}

/// Builds and configures a new matrix client.
pub async fn build_client(
    homeserver: &Url,
    session_dir: &Path,
    session_db_passphrase: &str,
) -> Result<Client> {
    let client = Client::builder()
        .homeserver_url(homeserver)
        .sqlite_store(session_dir, Some(session_db_passphrase))
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            auto_enable_backups: true,
            backup_download_strategy: BackupDownloadStrategy::AfterDecryptionFailure,
        })
        .build()
        .await
        .map_err(errors::convert_client_build_error)?;

    if client.event_cache().subscribe().is_err() {
        log::error!("Error subscribing to event cache");
    }

    Ok(client)
}

/// Removes the given directory, if it exists.
/// Returns Ok when the directory does not exist.
fn remove_directory(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();

    log::info!("Removing directory: {path:?}");

    if let Err(err) = std::fs::remove_dir_all(path) {
        if err.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }

        return Err(errors::create_unknown("error removing session file"));
    }

    Ok(())
}

/// Gets the path to the session directory, starting from the given data root directory.
fn get_session_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(SESSION_DIR)
}

/// Gets the path to the media directory, starting from the given data root directory.
fn get_media_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(MEDIA_DIR)
}

/// Gets the path to the cache directory, starting from the given data root directory.
fn get_cache_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(CACHE_DIR)
}
