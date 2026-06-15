use std::path::{Path, PathBuf};
use std::str::FromStr;

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

fn get_session_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(SESSION_DIR)
}

fn get_media_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(MEDIA_DIR)
}

fn get_cache_dir(data_root_dir: impl AsRef<Path>) -> PathBuf {
    data_root_dir.as_ref().join(CACHE_DIR)
}

fn get_auth_file(data_root_dir: impl AsRef<Path>) -> PathBuf {
    get_session_dir(data_root_dir).join(AUTH_FILE)
}

#[derive(Clone)]
pub struct InitializedData {
    /// The initialized matrix client.
    pub client: Client,

    /// The homeserver url.
    pub homeserver_url: Url,
    /// The display name of this device.
    pub device_display_name: String,

    /// The passphrase used to encrypt the session data.
    pub encryption_passphrase: String,
    /// The passphrase used to encrypt the database.
    pub database_passphrase: String,

    /// The absolute path to the root directory where data is stored.
    pub data_root_dir: PathBuf,

    /// Contains the in memory cache.
    pub memory_cache: MemoryCache,
    /// The persistent proto cache storing proto messages for the next application start.
    pub proto_cache: ProtoCache,
    /// Manages data stored on the file system, like avatars or downloaded chat images.
    pub media_manager: MediaManager,
    /// Manages incoming events.
    pub event_manager: EventManager,
}

impl InitializedData {
    pub fn get_session_dir(&self) -> PathBuf {
        get_session_dir(&self.data_root_dir)
    }

    pub fn get_media_dir(&self) -> PathBuf {
        get_media_dir(&self.data_root_dir)
    }

    pub fn get_cache_dir(&self) -> PathBuf {
        get_cache_dir(&self.data_root_dir)
    }

    pub fn get_auth_file(&self) -> PathBuf {
        get_auth_file(&self.data_root_dir)
    }
}

#[derive(Default)]
pub struct MatrixClient {
    /// The inner matrix client. If `None`, the client has not yet been initialized
    /// using `Self::initialize`.
    initialized_data: Option<InitializedData>,

    /// Contains cached identity providers. The idps are cached when `Self::get_login_flows`
    /// is called, as this method already retrieves the available idps.
    cached_idps: Option<Vec<String>>,

    /// The current active verification processes.
    verification_requests: Vec<VerificationManager>,
}

impl MatrixClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the client if it has been initialized with `Self::initialize`.
    /// An error is returned if the client has not yet been initialized.
    fn get_client(&self) -> Result<&Client> {
        Ok(&self.get_initialized_data()?.client)
    }

    /// Returns the initialized data if it has been initialized with `Self::initialize`.
    /// An error is returned if the client has not yet been initialized.
    fn get_initialized_data(&self) -> Result<&InitializedData> {
        let data = self.initialized_data.as_ref().ok_or(Error {
            r#type: ErrorType::NotInitialized.into(),
            error_string: Some("The client has not been initialized".to_owned()),
        })?;

        Ok(data)
    }

    /// Returns the initialized data if it has been initialized with `Self::initialize`.
    /// An error is returned if the client has not yet been initialized.
    fn get_initialized_data_mut(&mut self) -> Result<&mut InitializedData> {
        let data = self.initialized_data.as_mut().ok_or(Error {
            r#type: ErrorType::NotInitialized.into(),
            error_string: Some("The client has not been initialized".to_owned()),
        })?;

        Ok(data)
    }

    /// Returns the initialized data, provided it was initialized with `Self::initialize`
    /// and the client is already logged in. Otherwise, an error is returned.
    async fn get_initialized_data_logged_in(&self) -> Result<&InitializedData> {
        let data = self.get_initialized_data()?;

        if !data.client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Authorization required",
            ));
        }

        Ok(data)
    }

    /// Returns the client if it was initialized with `Self::initialize` and logged in with
    /// either `Self::login_sso` or `Self::login_username_password`.
    /// An error is returned if the client is not yet initialized or is not currently logged in.
    async fn get_client_logged_in(&self) -> Result<&Client> {
        let client = self.get_client()?;

        if !client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Authorization required",
            ));
        }

        Ok(client)
    }

    /// Checks if the client is still logged in.
    /// This method sends a request to the Matrix server.
    async fn is_logged_in(&self) -> Result<bool> {
        let Some(initialized_data) = &self.initialized_data else {
            return Err(errors::create_error(ErrorType::NotInitialized));
        };

        let result = initialized_data
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

    /// Builds the `self.initialized_data` with the given values.
    async fn initialize_data(
        &mut self,
        ctx: RequestContext,
        request: InitializationRequest,
    ) -> Result<&InitializedData> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let data_root_dir = PathBuf::from(request.data_root_path);

        let client = build_client(
            &homeserver_url,
            &get_session_dir(&data_root_dir),
            &request.persistent_storage_secret,
        )
        .await?;

        let proto_cache = ProtoCache::new(
            get_cache_dir(&data_root_dir),
            request.encryption_secret.clone(),
        )
        .await;

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

        let data = InitializedData {
            client,

            homeserver_url,
            device_display_name: request.device_display_name,

            encryption_passphrase: request.encryption_secret,
            database_passphrase: request.persistent_storage_secret,

            data_root_dir,

            memory_cache,
            proto_cache,
            media_manager,
            event_manager,
        };

        self.initialized_data = Some(data);

        #[allow(clippy::unwrap_used)]
        Ok(self.initialized_data.as_ref().unwrap())
    }

    /// Deletes the persisted session and resets the matrix client.
    async fn reset_session(&mut self, ctx: RequestContext) -> Result<()> {
        log::info!("Resetting session");

        let initialized_data = self.get_initialized_data_mut()?;

        remove_session_data(initialized_data)?;

        initialized_data.client = build_client(
            &initialized_data.homeserver_url,
            &initialized_data.get_session_dir(),
            &initialized_data.database_passphrase,
        )
        .await?;

        initialized_data.media_manager = MediaManager::new(
            initialized_data.client.clone(),
            initialized_data.data_root_dir.clone(),
            PathBuf::from(MEDIA_DIR),
        )
        .await;

        initialized_data.memory_cache =
            MemoryCache::new(ctx.clone(), initialized_data.media_manager.clone());

        initialized_data.event_manager = EventManager::new(
            initialized_data.client.clone(),
            ctx,
            initialized_data.memory_cache.clone(),
            initialized_data.media_manager.clone(),
        );

        initialized_data.proto_cache = ProtoCache::new(
            initialized_data.get_cache_dir(),
            initialized_data.encryption_passphrase.clone(),
        )
        .await;

        log::info!("Successfully reset session");

        Ok(())
    }

    /// Restores the session from the session file.
    async fn restore_session(&self, ctx: RequestContext) -> Result<()> {
        let initialized_data = self.get_initialized_data()?;

        let client = &initialized_data.client;
        let session_passphrase = initialized_data.encryption_passphrase.clone();
        let session_file = initialized_data.get_auth_file();

        log::debug!("Previous session found in '{session_file:?}'");

        let session = Session::read_from_file(session_file, session_passphrase).await?;

        log::info!(
            "Restoring session for {}",
            session.user_session.meta.user_id
        );

        client
            .restore_session(session.user_session.clone())
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        session.sync(ctx, initialized_data.clone()).await?;

        log::info!("Successfully restored session as {:?}", client.user_id());

        Ok(())
    }

    /// Removes all finished verification requests.
    fn cleanup_verifications(&mut self) {
        self.verification_requests.retain(|f| f.is_active());
    }

    /// Gets the verification manager by its flow id.
    fn get_verification_manager_mut(&mut self, flow_id: &str) -> Option<&mut VerificationManager> {
        self.verification_requests
            .iter_mut()
            .find(|p| p.flow_id() == flow_id)
    }

    /// Gets a `matrix_sdk::Room` room by its id.
    /// Returns an `Err` when the room was not found or the ID is invalid.
    async fn get_matrix_room(&self, room_id: &str) -> Result<matrix_sdk::Room> {
        let client = self.get_client_logged_in().await?;

        let room_id =
            RoomId::parse(room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = client
            .get_room(&room_id)
            .ok_or(errors::create_error(ErrorType::RoomNotFound))?;

        Ok(room)
    }
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    async fn on_response(&mut self, response: &ResponseContainer) {
        let Some(content) = &response.content else {
            log::error!("Received response with no content: {response:?}");
            return;
        };

        let Ok(initialized_data) = self.get_initialized_data() else {
            log::error!("Received response while client is not initialized: {response:?}");
            return;
        };

        let InitializedData { proto_cache, .. } = initialized_data;

        proto_cache.cache_response_content(content);
    }

    async fn initialize(
        &mut self,
        ctx: RequestContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        if self.initialized_data.is_some() {
            return Err(errors::create_error(ErrorType::AlreadyInitialized));
        }

        let initialized_data = self.initialize_data(ctx.clone(), request).await?;

        if initialized_data.get_auth_file().exists() {
            match self.restore_session(ctx).await {
                Ok(()) => {
                    return Ok(StatusUpdate {
                        code: status_update::StatusCode::LoggedIn as i32,
                    })
                }
                Err(err) => log::error!("Error restoring session: {err:?}"),
            }
        }

        Ok(StatusUpdate {
            code: status_update::StatusCode::Connected as i32,
        })
    }

    async fn get_login_flows(&mut self, _ctx: RequestContext) -> Result<LoginFlowsResponse> {
        use matrix_sdk::ruma::api::client::session::get_login_types::v3;
        use v3::LoginType as MatrixLoginType;

        let client = self.get_client()?;

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

                    self.cached_idps = Some(idps);

                    response.push_login_flows(login_flows_response::LoginFlow::Sso)
                }
                _ => (),
            }
        }

        Ok(response)
    }

    async fn get_identity_providers(
        &mut self,
        ctx: RequestContext,
    ) -> Result<IdentityProvidersResponse> {
        // Check if the idps have been retrieved before
        if let Some(idps) = &self.cached_idps {
            return Ok(IdentityProvidersResponse {
                identity_providers: idps.clone(),
            });
        }

        // We can use the `Self::get_login_flows` method to retrieve the idps as it saves
        // them to the cache.
        // This method would ultimately only fetch the login flows too.
        let _ = self.get_login_flows(ctx).await?;

        // If there is still nothing in the cache, no idps are available or single sign-on
        // is not supported
        // by the server. In this case we can just return an empty list.
        let idps = if let Some(idps) = &self.cached_idps {
            idps.clone()
        } else {
            Vec::new()
        };

        Ok(IdentityProvidersResponse {
            identity_providers: idps,
        })
    }

    async fn login_username_password(
        &mut self,
        ctx: RequestContext,
        request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        if self.is_logged_in().await? {
            return Err(errors::create_error(ErrorType::AlreadyLoggedIn));
        }

        self.reset_session(ctx.clone()).await?;

        let initialized_data = self.get_initialized_data()?;

        initialized_data
            .client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(&initialized_data.device_display_name)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        log::info!(
            "Successfully logged in as {:?}",
            initialized_data.client.user_id()
        );

        let session = Session::new(
            &initialized_data.client,
            initialized_data.get_auth_file(),
            initialized_data.encryption_passphrase.clone(),
        )?;

        session.save().await?;
        session.sync(ctx.clone(), initialized_data.clone()).await?;

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &mut self,
        ctx: RequestContext,
        request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        if self.is_logged_in().await? {
            return Err(errors::create_error(ErrorType::AlreadyLoggedIn));
        }

        self.reset_session(ctx.clone()).await?;

        let initialized_data = self.get_initialized_data()?;

        // Create a channel so we can receive the login url from the async closure
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut login_builder = initialized_data
            .client
            .matrix_auth()
            .login_sso(|url| async move {
                #[allow(clippy::expect_used)]
                tx.send(url).expect("Receiver of the login url dropped");
                Ok(())
            })
            .initial_device_display_name(&initialized_data.device_display_name);

        if let Some(idp) = request.identity_provider {
            login_builder = login_builder.identity_provider_id(&idp);
        }

        // Clone the data so we can move it into the tokio task
        let initialized_data = initialized_data.clone();
        let session_passphrase = initialized_data.encryption_passphrase.clone();
        let session_file = initialized_data.get_auth_file();

        // Spawn a tokio task which waits for the successful login in order to send
        // a status update to the application.
        tokio::spawn(async move {
            if let Err(err) = login_builder.await {
                ctx.send_error(errors::convert_matrix_sdk_error(err)).await;
                return;
            }

            log::info!(
                "Successfully logged in as {:?}",
                initialized_data.client.user_id()
            );

            let Ok(session) =
                Session::new(&initialized_data.client, session_file, session_passphrase)
            else {
                ctx.send_error(errors::create_unknown("Error creating session"))
                    .await;
                return;
            };

            if let Err(err) = session.save().await {
                ctx.send_error(err).await;
                return;
            }

            if let Err(err) = session.sync(ctx.clone(), initialized_data.clone()).await {
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
        &mut self,
        _ctx: RequestContext,
        request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        let client = self.get_client_logged_in().await?;

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
        &mut self,
        ctx: RequestContext,
        request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        self.cleanup_verifications();

        let client = self.get_client_logged_in().await?;

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

        self.verification_requests.push(manager);

        Ok(CrossSigningStartResponse {
            verification_flow_id,
        })
    }

    async fn cross_signing_select_method(
        &mut self,
        _ctx: RequestContext,
        request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        let _ = self.get_client_logged_in().await?;

        self.cleanup_verifications();

        let CrossSigningMethodSelectedRequest {
            verification_flow_id,
            selected_method,
        } = request;

        let Some(manager) = self.get_verification_manager_mut(&verification_flow_id) else {
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
        &mut self,
        _ctx: RequestContext,
        request: CrossSigningConfirmRequest,
    ) -> Result<()> {
        let _ = self.get_client_logged_in().await?;

        self.cleanup_verifications();

        let CrossSigningConfirmRequest {
            verification_flow_id,
        } = request;

        let Some(manager) = self.get_verification_manager_mut(&verification_flow_id) else {
            return Err(errors::create_error_msg(
                ErrorType::VerificationFlowNotFound,
                "Verification flow with the given ID not found",
            ));
        };

        manager.confirm();

        Ok(())
    }

    async fn abort_verification(
        &mut self,
        _ctx: RequestContext,
        request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        let _ = self.get_client_logged_in().await?;

        self.cleanup_verifications();

        let VerificationAbortRequest {
            verification_flow_id,
        } = request;

        let position = self
            .verification_requests
            .iter()
            .position(|p| p.flow_id() == verification_flow_id);

        if let Some(index) = position {
            let manager = self.verification_requests.swap_remove(index);

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

    async fn get_user(&mut self, ctx: RequestContext, request: UserRequest) -> Result<User> {
        let initialized_data = self.get_initialized_data_logged_in().await?;

        let user_id = UserId::parse(request.user_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidUserId))?
            .to_owned();

        let user_manager = UserManager::from_initialized_data(ctx, initialized_data);
        user_manager.get_and_sync_user(user_id).await
    }

    async fn search_users(
        &mut self,
        _ctx: RequestContext,
        request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

        let UserSearchRequest { query, limit } = request;

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

    async fn set_status(&mut self, _ctx: RequestContext, request: UserStatus) -> Result<()> {
        use matrix_sdk::ruma::api::client::presence::set_presence::v3::Request;

        let InitializedData {
            client,
            proto_cache,
            ..
        } = self.get_initialized_data_logged_in().await?;

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
        &mut self,
        _ctx: RequestContext,
        request: PublicRoomListRequest,
    ) -> Result<PublicRoomListResponse> {
        use matrix_sdk::ruma::api::client::directory::get_public_rooms_filtered;
        use ruma_common::directory::Filter;

        let client = self.get_client_logged_in().await?;

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
        &mut self,
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

    async fn invitation_reply(&mut self, ctx: RequestContext, request: InvitedReply) -> Result<()> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

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
        &mut self,
        ctx: RequestContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        let initialized_data = self.get_initialized_data_logged_in().await?;

        let Some(user_id) = initialized_data.client.user_id() else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let room_manager = rooms::RoomManager::from_initialized_data(ctx, initialized_data);
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
        &mut self,
        ctx: RequestContext,
        request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

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

        Ok(rooms::convert_to_proto(media_manager, room, user_id).await?)
    }

    async fn create_direct_room(
        &mut self,
        ctx: RequestContext,
        request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

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

        Ok(rooms::convert_to_proto(media_manager, room, our_user_id).await?)
    }

    async fn change_room(
        &mut self,
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

        let InitializedData { media_manager, .. } = self.get_initialized_data_logged_in().await?;
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
        &mut self,
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

    async fn join_room(&mut self, _ctx: RequestContext, request: RoomJoinRequest) -> Result<Room> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

        let Some(user_id) = client.user_id().map(|f| f.to_owned()) else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let room_id = RoomId::parse(&request.room_id)
            .map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = client
            .join_room_by_id(&room_id)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(rooms::convert_to_proto(media_manager, room, &user_id).await?)
    }

    async fn knock_room(&mut self, _ctx: RequestContext, request: RoomKnockRequest) -> Result<()> {
        let client = self.get_client_logged_in().await?;

        let RoomKnockRequest { room_id, message } = request;

        let room_id =
            RoomId::parse(&room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        client
            .knock(room_id.into(), message, Vec::new())
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    async fn get_room_messages(
        &mut self,
        ctx: RequestContext,
        request: RoomMessagesRequest,
    ) -> Result<()> {
        let InitializedData { memory_cache, .. } = self.get_initialized_data_logged_in().await?;

        let room = self.get_matrix_room(request.room_id.as_str()).await?;

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
        &mut self,
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
        &mut self,
        _ctx: RequestContext,
        request: MessageSendRequest,
    ) -> Result<MessageSendResponse> {
        use message_send_request::Content;

        let InitializedData { media_manager, .. } = self.get_initialized_data_logged_in().await?;

        let MessageSendRequest {
            room_id,
            related_message_id,
            mentioned_user_ids,
            content,
        } = request;

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
            Content::AudioFile(content) => {
                messages::send_audio_message(media_manager, room, related_message_id, content).await
            }
            Content::VideoFile(content) => {
                messages::send_video_message(media_manager, room, related_message_id, content).await
            }
        }
    }

    async fn remove_message(
        &mut self,
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
        &mut self,
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
            Content::AudioFile(_) => {
                return Err(errors::create_error(ErrorType::NotImplemented));
            }
            Content::VideoFile(_) => {
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

    async fn create_reaction(&mut self, _ctx: RequestContext, request: Reaction) -> Result<()> {
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

    async fn remove_reaction(&mut self, ctx: RequestContext, request: Reaction) -> Result<()> {
        let Reaction {
            room_id,
            message_id,
            reaction,
            user_id,
        } = request;

        let InitializedData {
            client,
            memory_cache,
            ..
        } = self.get_initialized_data_logged_in().await?;

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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Builds and configures a matrix client.
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

fn remove_session_data(initialized_data: &InitializedData) -> Result<()> {
    log::info!("Removing session data");

    remove_directory(initialized_data.get_session_dir())?;
    remove_directory(initialized_data.get_media_dir())?;
    remove_directory(initialized_data.get_cache_dir())?;

    Ok(())
}

fn remove_directory(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();

    log::info!("Removing directory: {path:?}");

    // The use of the sync fs methods instead of tokio::fs is intended to block
    // the runtime until all session data has been removed.

    if let Err(err) = std::fs::remove_dir_all(path) {
        if err.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }

        return Err(errors::create_unknown("error removing session file"));
    }

    Ok(())
}
