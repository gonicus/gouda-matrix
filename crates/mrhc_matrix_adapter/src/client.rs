use std::path::{Path, PathBuf};

use async_trait::async_trait;
use matrix_sdk::encryption::{BackupDownloadStrategy, EncryptionSettings};
use matrix_sdk::room::edit::EditedContent;
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::api::client::profile::DisplayName;
use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContentWithoutRelation;
use matrix_sdk::ruma::{assign, OwnedRoomId, OwnedUserId, RoomId, UserId};
use matrix_sdk::Client;
use matrix_sdk_base::RoomStateFilter;
use mrhc_core::{Client as ClientAbstraction, ClientContext, Result};
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;
use ruma_common::{EventId, OwnedEventId};
use url::Url;

use crate::cache::Cache;
use crate::events::EventManager;
use crate::media::MediaManager;
use crate::session::Session;
use crate::verification::{self, VerificationManager};
use crate::{cache, errors, messages, rooms, user};

const SESSION_DIR: &str = "session_data";
const SESSION_FILE: &str = "session";

const CRYPTO_DB: &str = "matrix-sdk-crypto.sqlite3";
const EVENT_CACHE_DB: &str = "matrix-sdk-event-cache.sqlite3";
const MEDIA_DB: &str = "matrix-sdk-media.sqlite3";
const STATE_DB: &str = "matrix-sdk-state.sqlite3";

#[derive(Clone)]
pub struct InitializedData {
    /// The initialized matrix client.
    pub client: Client,

    /// The homeserver url.
    pub homeserver_url: Url,
    /// The display name of this device.
    pub device_display_name: String,

    /// The absolute path to the root directory where data is stored.
    pub data_root_dir: PathBuf,
    /// The absolute path to the directory where the session data is stored.
    pub session_dir: PathBuf,
    /// The absolute path to the file where the current session metadata is stored.
    pub session_file: PathBuf,
    /// The passphrase used to encrypt the session data.
    pub session_passphrase: String,
    /// The passphrase used to encrypt the database.
    pub database_passphrase: String,

    /// Contains the in memory cache.
    pub cache: cache::Cache,
    /// Manages data stored on the file system, like avatars or downloaded chat images.
    pub media_manager: MediaManager,
    /// Manages incoming events.
    pub event_manager: EventManager,
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
        ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<&InitializedData> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let data_root_dir = PathBuf::from(request.data_root_path);
        let session_dir = data_root_dir.join(SESSION_DIR);
        let session_file = session_dir.join(SESSION_FILE);

        let client = build_client(
            &homeserver_url,
            &session_dir,
            &request.persistent_storage_secret,
        )
        .await?;

        let cache = Cache::new();
        let media_manager = MediaManager::new(client.clone(), data_root_dir.clone()).await;
        let event_manager =
            EventManager::new(client.clone(), ctx, cache.clone(), media_manager.clone());

        let data = InitializedData {
            client,

            homeserver_url,
            device_display_name: request.device_display_name,

            data_root_dir,
            session_dir,
            session_file,
            session_passphrase: request.encryption_secret,
            database_passphrase: request.persistent_storage_secret,

            cache,
            media_manager,
            event_manager,
        };

        self.initialized_data = Some(data);

        #[allow(clippy::unwrap_used)]
        Ok(self.initialized_data.as_ref().unwrap())
    }

    /// Restores the session from the session file.
    async fn restore_session(&self, ctx: ClientContext) -> Result<()> {
        let initialized_data = self.get_initialized_data()?;

        let client = &initialized_data.client;
        let session_file = initialized_data.session_file.clone();
        let session_passphrase = initialized_data.session_passphrase.clone();

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

    /// Deletes the persisted session and resets the matrix client.
    async fn reset_session(&mut self, ctx: ClientContext) -> Result<()> {
        log::info!("Resetting session");

        let InitializedData {
            client,
            homeserver_url,
            data_root_dir,
            session_dir,
            session_file,
            database_passphrase,
            cache,
            media_manager,
            event_manager,
            ..
        } = self.get_initialized_data_mut()?;

        remove_session_file(session_file)?;
        remove_session_file(session_dir.join(CRYPTO_DB))?;
        remove_session_file(session_dir.join(EVENT_CACHE_DB))?;
        remove_session_file(session_dir.join(MEDIA_DB))?;
        remove_session_file(session_dir.join(STATE_DB))?;

        *client = build_client(homeserver_url, session_dir, database_passphrase).await?;

        *cache = Cache::new();
        *media_manager = MediaManager::new(client.clone(), data_root_dir.clone()).await;
        *event_manager =
            EventManager::new(client.clone(), ctx, cache.clone(), media_manager.clone());

        log::info!("Successfully reset session");

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
    async fn initialize(
        &mut self,
        ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        if self.initialized_data.is_some() {
            return Err(errors::create_error(ErrorType::AlreadyInitialized));
        }

        let initialized_data = self.initialize_data(ctx.clone(), request).await?;

        if initialized_data.session_file.exists() {
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

    async fn get_login_flows(&mut self, _ctx: ClientContext) -> Result<LoginFlowsResponse> {
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
        ctx: ClientContext,
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
        ctx: ClientContext,
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
            initialized_data.session_file.to_path_buf(),
            initialized_data.session_passphrase.clone(),
        )?;

        session.save().await?;
        session.sync(ctx.clone(), initialized_data.clone()).await?;

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &mut self,
        ctx: ClientContext,
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
        let session_passphrase = initialized_data.session_passphrase.clone();
        let session_file = initialized_data.session_file.clone();

        // Spawn a tokio task which waits for the successful login in order to send
        // a status update to the application.
        tokio::spawn(async move {
            if let Err(err) = login_builder.await {
                ctx.send_error(errors::convert_matrix_sdk_error(err));
            }

            log::info!(
                "Successfully logged in as {:?}",
                initialized_data.client.user_id()
            );

            let Ok(session) =
                Session::new(&initialized_data.client, session_file, session_passphrase)
            else {
                ctx.send_error(errors::create_unknown("Error creating session"));
                return;
            };

            if let Err(err) = session.save().await {
                ctx.send_error(err);
                return;
            }

            if let Err(err) = session.sync(ctx.clone(), initialized_data.clone()).await {
                ctx.send_error(err);
                return;
            }

            ctx.send_event(ResponseContent::StatusUpdate(StatusUpdate {
                code: status_update::StatusCode::LoggedIn as i32,
            }));
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
        _ctx: ClientContext,
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
        ctx: ClientContext,
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
        _ctx: ClientContext,
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
        _ctx: ClientContext,
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
        _ctx: ClientContext,
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

    async fn get_user(&mut self, _ctx: ClientContext, request: UserRequest) -> Result<User> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

        let user_id = UserId::parse(request.user_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidUserId))?
            .to_owned();

        let profile = client
            .account()
            .fetch_user_profile_of(&user_id)
            .await
            .map_err(|_| errors::create_error(ErrorType::UserNotFound))?;

        let display_name = profile.get_static::<DisplayName>().unwrap_or_default();

        let proto = User {
            user_id: user_id.to_string(),
            display_name,
            presence_state: Some(user::fetch_presence_state(client, &user_id).await.into()),
            avatar_path: media_manager.get_user_avatar_path(user_id.clone()).await,
        };

        Ok(proto)
    }

    async fn search_users(
        &mut self,
        _ctx: ClientContext,
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
            let presence = user::fetch_presence_state(client, &user.user_id).await;

            result.push(User {
                user_id: user.user_id.to_string(),
                display_name: user.display_name.clone(),
                presence_state: Some(presence.into()),
                avatar_path: media_manager
                    .get_user_directory_user_avatar_path(&user)
                    .await,
            });
        }

        Ok(UserSearchResponse { user_list: result })
    }

    async fn get_public_rooms(
        &mut self,
        _ctx: ClientContext,
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
        _ctx: ClientContext,
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

    async fn invitation_reply(&mut self, ctx: ClientContext, request: InvitedReply) -> Result<()> {
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

        ctx.send_event(ResponseContent::RoomCreatedEvent(proto));

        Ok(())
    }

    async fn get_rooms(
        &mut self,
        _ctx: ClientContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in().await?;

        let Some(user_id) = client.user_id() else {
            return Err(errors::create_unknown("Unable to retrieve user_id"));
        };

        let mut filter = RoomStateFilter::empty();

        if request.include_joined {
            filter |= RoomStateFilter::JOINED;
        }

        if request.include_unjoined {
            filter |= RoomStateFilter::INVITED
                | RoomStateFilter::LEFT
                | RoomStateFilter::KNOCKED
                | RoomStateFilter::BANNED;
        }

        let mut result = Vec::new();

        for room in client.rooms_filtered(filter) {
            if room.is_space() {
                continue;
            }

            result.push(rooms::convert_to_proto(media_manager, room, user_id).await?);
        }

        Ok(RoomListResponse { room_list: result })
    }

    async fn create_group_room(
        &mut self,
        ctx: ClientContext,
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
                ctx.send_error(err);
            }
        }

        Ok(rooms::convert_to_proto(media_manager, room, user_id).await?)
    }

    async fn create_direct_room(
        &mut self,
        ctx: ClientContext,
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
                ctx.send_error(err);
            }
        }

        Ok(rooms::convert_to_proto(media_manager, room, our_user_id).await?)
    }

    async fn change_room(
        &mut self,
        ctx: ClientContext,
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
                ctx.send_error(err);
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
        _ctx: ClientContext,
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

    async fn join_room(&mut self, _ctx: ClientContext, request: RoomJoinRequest) -> Result<Room> {
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

    async fn knock_room(&mut self, _ctx: ClientContext, request: RoomKnockRequest) -> Result<()> {
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
        ctx: &ClientContext,
        request: &RoomMessagesRequest,
    ) -> Result<()> {
        let InitializedData {
            media_manager,
            cache,
            ..
        } = self.get_initialized_data()?;

        let room = self.get_matrix_room(request.room_id.as_str()).await?;
        let room_id = OwnedRoomId::try_from(request.room_id.as_str())
            .map_err(|_| errors::create_unknown("invalid room id"))?;

        let key_change_rx =
            messages::setup_room_key_listener(&room_id, self.get_client_logged_in().await?).await?;

        let request_clone = request.clone();

        // use default backward sorting on any error or missing option
        let order = request
            .order
            .and_then(|v| MessagesOrder::try_from(v).ok())
            .unwrap_or(MessagesOrder::Backward);

        let limit = request_clone.limit.unwrap_or(10);

        // set limit of first fetch a little higher than requested limit
        let mut fetch_limit = messages::initial_fetch_limit(limit);

        let mut skip_first = true;
        let from_id = match request_clone.from_message_id {
            Some(val) => OwnedEventId::try_from(val)
                .map_err(|_| errors::create_unknown("invalid event ID"))?,
            None => {
                let (_, id) =
                    messages::fetch_messages_from_sdk(cache, order, &room, None, fetch_limit)
                        .await?;

                // reduce limit for subsequent fetches
                fetch_limit = messages::subsequent_fetch_limit(limit);

                // The first message is part of the response when no from_id has been specified
                skip_first = false;

                id.ok_or(errors::create_unknown("no messages in room"))?
            }
        };

        let cached_room =
            cache::get_or_create_room(cache, &room_id).map_err(errors::convert_cache_error)?;

        loop {
            let next_batch = cache::check_cached_enough(
                &cached_room.clone(),
                from_id.clone(),
                limit,
                order,
                skip_first,
            )
            .map_err(errors::convert_cache_error)?;

            match next_batch {
                None => break,
                Some(val) => {
                    log::debug!("Attempting to fetch further messages from sdk");
                    let (fetched, _) = messages::fetch_messages_from_sdk(
                        cache,
                        order,
                        &room,
                        Some(val),
                        fetch_limit,
                    )
                    .await?;

                    // reduce limit for subsequent fetches
                    fetch_limit = messages::subsequent_fetch_limit(limit);

                    if fetched == 0 {
                        break;
                    }
                }
            }
        }

        let room_client = cache::MatrixRoomClient::new(&room, media_manager.clone());

        // fetch events from sdk and assemble response
        let seq = cache::send_and_get_sequence_chunk(
            &cached_room.clone(),
            from_id.clone(),
            limit,
            order,
            skip_first,
            &room_client,
            cache,
            ctx,
        )
        .await
        .map_err(|err| mrhc_proto::chat::Error {
            r#type: 0,
            error_string: Some(err.to_string()),
        })?;

        if seq.is_complete {
            log::warn!("Sequence chunk was incomplete");
        }

        let ctx = ctx.clone();
        let room_id = room_id.clone();
        let room_client = room_client.clone();
        let cache = cache.clone();

        tokio::spawn(async move {
            let result = cache::retry_decryption(
                seq.messages,
                &room_id,
                &room_client,
                &cache,
                key_change_rx,
                &ctx,
            )
            .await;

            if let Err(err) = result {
                ctx.send_error(errors::convert_cache_error(err));
            }
        });

        Ok(())
    }

    async fn mark_as_read(
        &mut self,
        _ctx: ClientContext,
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
        _ctx: ClientContext,
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
        }
    }

    async fn remove_message(
        &mut self,
        _ctx: ClientContext,
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
        _ctx: ClientContext,
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

    async fn create_reaction(&mut self, _ctx: ClientContext, request: Reaction) -> Result<()> {
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

    async fn remove_reaction(&mut self, ctx: ClientContext, request: Reaction) -> Result<()> {
        let Reaction {
            room_id,
            message_id,
            reaction,
            user_id,
        } = request;

        let InitializedData { client, cache, .. } = self.get_initialized_data_logged_in().await?;

        let room = self.get_matrix_room(&room_id).await?;

        let user_id =
            user_id.unwrap_or(client.user_id().map(|f| f.to_string()).unwrap_or_default());

        let cached_reaction =
            cache.untrack_reaction_by_emoji(&room_id, &message_id, &user_id, &reaction);

        let Some(cached_reaction) = cached_reaction else {
            return Err(errors::create_error(ErrorType::ReactionNotFound));
        };

        room.redact(&cached_reaction.event_id, None, None)
            .await
            .map_err(errors::convert_http_error)?;

        let proto = Reaction {
            room_id,
            message_id,
            reaction,
            user_id: Some(user_id),
        };

        ctx.send_event(ResponseContent::ReactionRemovedEvent(proto));

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

/// Removes the session file at the specified path.
/// Blocks until the file has been removed.
fn remove_session_file(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();

    log::info!("Removing session file: {path:?}");

    // The use of the sync fs methods instead of tokio::fs is intended to block
    // the runtime until all session data has been removed.

    if let Err(err) = std::fs::remove_file(path) {
        if err.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }

        return Err(errors::create_unknown("error removing session file"));
    }

    Ok(())
}
