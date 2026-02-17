use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures_util::StreamExt;
use matrix_sdk::room::edit::EditedContent;
use matrix_sdk::room::{MessagesOptions, Room as MatrixRoom};
use matrix_sdk::ruma::api::client::filter::RoomEventFilter;
use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::message::{
    RoomMessageEventContent, RoomMessageEventContentWithoutRelation,
};
use matrix_sdk::ruma::{assign, OwnedRoomId, OwnedUserId, RoomId, UInt, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_base::RoomStateFilter;
use mrhc_core::{Client as ClientAbstraction, ClientContext, Result};
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;
use ruma_common::{EventId, OwnedEventId};
use tokio::sync::mpsc;
use url::Url;

use crate::chat_cache::{
    cache_room_messages_response, get_or_create_room, get_sequence_chunk, retry_decryption,
    CacheError, CachedData, MatrixRoomClient,
};
use crate::events::EventManager;
use crate::media::MediaManager;
use crate::session::Session;
use crate::verification::VerificationManager;
use crate::{errors, rooms, session, user, utils};

const SESSION_DIR: &str = "session_data";
const SESSION_FILE: &str = "session";

#[derive(Clone)]
pub struct InitializedData {
    /// The initialized matrix client.
    pub client: Client,
    /// The display name of this device.
    pub device_display_name: String,

    /// The file where the current session metadata is stored.
    pub session_file: PathBuf,
    /// The passphrase used to encrypt the session data.
    pub session_passphrase: String,

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
    // The chronologically-resolved room message cache
    cached_data: Arc<RwLock<CachedData>>,
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

    /// Returns the initialized data, provided it was initialized with `Self::initialize`
    /// and the client is already logged in. Otherwise, an error is returned.
    fn get_initialized_data_logged_in(&self) -> Result<&InitializedData> {
        let data = self.get_initialized_data()?;

        if data.client.matrix_auth().logged_in() {
            Ok(data)
        } else {
            Err(errors::create_error_msg(
                ErrorType::Authorization,
                "The client is not yet logged in",
            ))
        }
    }

    /// Returns the client if it was initialized with `Self::initialize` and logged in with
    /// either `Self::login_sso` or `Self::login_username_password`.
    /// An error is returned if the client is not yet initialized or is not currently logged in.
    fn get_client_logged_in(&self) -> Result<&Client> {
        let client = self.get_client()?;

        if client.matrix_auth().logged_in() {
            Ok(client)
        } else {
            Err(errors::create_error_msg(
                ErrorType::Authorization,
                "The client is not yet logged in",
            ))
        }
    }

    /// Builds the `self.initialized_data` with the given values.
    async fn initialize_data(
        &mut self,
        ctx: ClientContext,
        request: InitializationRequest,
        client: Client,
        session_file: PathBuf,
    ) -> InitializedData {
        let media_manager =
            MediaManager::new(client.clone(), PathBuf::from(request.data_root_path)).await;

        let event_manager = EventManager::new(client.clone(), ctx, media_manager.clone());

        let data = InitializedData {
            client,
            device_display_name: request.device_display_name,

            session_file,
            session_passphrase: request.encryption_secret,

            media_manager,
            event_manager,
        };

        self.initialized_data = Some(data.clone());

        data
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
    fn get_matrix_room(&self, room_id: &str) -> Result<matrix_sdk::Room> {
        let client = self.get_client_logged_in()?;

        let room_id =
            RoomId::parse(room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = client
            .get_room(&room_id)
            .ok_or(errors::create_error(ErrorType::RoomNotFound))?;

        Ok(room)
    }

    async fn fetch_messages_from_sdk(
        cached_data: Arc<RwLock<CachedData>>,
        order: MessagesOrder,
        room: &MatrixRoom,
        next: String,
        limit: u32,
    ) -> Result<usize> {
        let mut options: MessagesOptions;
        let chronological: bool;

        match order {
            MessagesOrder::Forward => {
                options = MessagesOptions::forward();
                chronological = true;
            }
            MessagesOrder::Backward => {
                options = MessagesOptions::backward();
                chronological = false;
            }
        }

        options.from = Some(next);
        options.filter = RoomEventFilter::default();
        options.limit = UInt::from((limit * 3).div_ceil(2));

        let messages = room
            .messages(options)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        if messages.chunk.is_empty() {
            log::debug!("Reached end of room data - returning N < limit messages");

            return Ok(0);
        }

        cache_room_messages_response(
            cached_data.clone(),
            &messages,
            room.room_id().to_owned(),
            chronological,
        )
        .map_err(errors::convert_cache_error)?;

        Ok(messages.chunk.len())
    }

    async fn setup_room_key_listener(
        room_id: &OwnedRoomId,
        client: &Client,
    ) -> Result<mpsc::Receiver<()>> {
        log::debug!("setting up key listener for room {room_id}");

        let (tx, rx) = mpsc::channel(100);
        let key_stream = client
            .encryption()
            .backups()
            .room_keys_for_room_stream(room_id);

        tokio::spawn(async move {
            // pinning is needed before calling next
            tokio::pin!(key_stream);

            log::debug!("Now listening on room keys");

            while let Some(result) = key_stream.next().await {
                match result {
                    Ok(session_ids) => {
                        // session_ids is a mapping of sender_key to set of session_ids
                        let total_keys: usize = session_ids.values().map(|s| s.len()).sum();
                        log::info!(
                            "Room keys downloaded from backup: {} sessions keys from {} senders",
                            total_keys,
                            session_ids.len()
                        );
                        log::debug!("Downloaded session keys: {session_ids:#?}");
                        // Notify listener that new keys have arrived
                        let _ = tx.send(()).await;
                    }
                    Err(e) => {
                        log::warn!("Error receiving room key notification: {e:?}");
                    }
                }
            }

            log::debug!("Ending room key listener");
        });

        Ok(rx)
    }
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    async fn initialize(
        &mut self,
        mut ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let session_dir = PathBuf::from(&request.data_root_path).join(SESSION_DIR);
        let session_file = session_dir.join(SESSION_FILE);
        let session_passphrase = request.encryption_secret.clone();
        let cached_data = self.cached_data.clone();

        if session_file.exists() {
            let result = session::restore_session(
                &homeserver_url,
                session_file.clone(),
                session_passphrase.clone(),
                &session_dir,
                &request.persistent_storage_secret,
                cached_data,
            )
            .await;

            match result {
                Ok((client, mut session)) => {
                    let initialized_data = self
                        .initialize_data(ctx.clone(), request, client.clone(), session_file.clone())
                        .await;

                    session.initial_sync(&mut ctx, &client).await?;

                    session.start_background_sync(initialized_data, ctx)?;

                    return Ok(StatusUpdate {
                        code: status_update::StatusCode::LoggedIn as i32,
                    });
                }
                Err(err) => log::error!("Error restoring session: {err:?}"),
            }
        }

        let client = session::build_client(
            &homeserver_url,
            &session_dir,
            &request.persistent_storage_secret,
        )
        .await?;

        self.initialize_data(ctx, request, client, session_file)
            .await;

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
        mut ctx: ClientContext,
        request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        let initialized_data = self.get_initialized_data()?;

        if initialized_data.client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Client is already logged in",
            ));
        }

        initialized_data
            .client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(&initialized_data.device_display_name)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        let cached_data = self.cached_data.clone();

        log::info!(
            "Successfully logged in as {:?}",
            initialized_data.client.user_id()
        );

        let mut session = Session::new(
            &initialized_data.client,
            initialized_data.session_file.to_path_buf(),
            initialized_data.session_passphrase.clone(),
            cached_data,
        )?;

        session.save().await?;

        session
            .initial_sync(&mut ctx, &initialized_data.client)
            .await?;

        session.start_background_sync(initialized_data.clone(), ctx)?;

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &mut self,
        mut ctx: ClientContext,
        request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        let initialized_data = self.get_initialized_data()?;

        let cached_data = self.cached_data.clone();

        if initialized_data.client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Client is already logged in",
            ));
        }

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

            let Ok(mut session) = Session::new(
                &initialized_data.client,
                session_file,
                session_passphrase,
                cached_data,
            ) else {
                ctx.send_error(errors::create_unknown("Error creating session"));
                return;
            };

            if let Err(err) = session.save().await {
                ctx.send_error(err);
                return;
            }

            if let Err(err) = session
                .initial_sync(&mut ctx, &initialized_data.client)
                .await
            {
                ctx.send_error(err);
                return;
            }

            if session
                .start_background_sync(initialized_data, ctx.clone())
                .is_err()
            {
                ctx.send_error(errors::create_unknown("Error starting background sync"));
                return;
            };

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
        let client = self.get_client_logged_in()?;

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

        let client = self.get_client_logged_in()?;

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

        let methods = utils::cross_signing_methods_to_matrix(request.supported_methods);

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
        let _ = self.get_client_logged_in()?;

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
        let _ = self.get_client_logged_in()?;

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
        let _ = self.get_client_logged_in()?;

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

    async fn get_users(&mut self, _ctx: ClientContext) -> Result<UserListResponse> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in()?;

        // It is not possible to retrieve all known users using the matrix-sdk.
        // As a workaround, we retrieve all rooms and get their members.
        let rooms = client.rooms();

        let mut result: Vec<User> = Vec::new();

        for room in rooms {
            let members = room
                .members(RoomMemberships::all())
                .await
                .map_err(errors::convert_matrix_sdk_error)?;

            for member in members {
                // Skip duplicates
                if result.iter().any(|m| m.user_id == *member.user_id()) {
                    continue;
                }

                let presence = user::request_user_presence(client, member.user_id()).await;

                result.push(User {
                    user_id: member.user_id().to_string(),
                    display_name: member.display_name().map(str::to_string),
                    presence_state: Some(presence.into()),
                    avatar_path: media_manager.get_room_member_avatar_path(&member).await,
                });
            }
        }

        Ok(UserListResponse { user_list: result })
    }

    async fn get_room_messages(
        &mut self,
        ctx: &ClientContext,
        request: &RoomMessagesRequest,
    ) -> Result<RoomMessagesResponse> {
        let room = self.get_matrix_room(request.room_id.as_str())?;
        let room_id = OwnedRoomId::try_from(request.room_id.as_str())
            .map_err(|_| errors::create_unknown("invalid room id"))?;

        let key_change_rx =
            MatrixClient::setup_room_key_listener(&room_id, self.get_client_logged_in()?).await?;

        let request_clone = request.clone();

        let from_id = request_clone.from_message_id;
        let limit = request_clone.limit.unwrap_or(5);

        let cached_room = get_or_create_room(self.cached_data.clone(), &room_id)
            .map_err(errors::convert_cache_error)?;

        // use default backward sorting on any error or missing option
        let order = request
            .order
            .and_then(|v| MessagesOrder::try_from(v).ok())
            .unwrap_or(MessagesOrder::Backward);

        let room_client = MatrixRoomClient::new(&room);

        let response;
        let mut seq;

        loop {
            seq = get_sequence_chunk(
                &cached_room.clone(),
                from_id.as_deref(),
                limit,
                order,
                &room_client,
            )
            .await
            .map_err(|err| mrhc_proto::chat::Error {
                r#type: 0,
                error_string: Some(err.to_string()),
            })?;

            if seq.is_complete {
                log::debug!("Retrieved all requested messages from SequenceChunk");

                response = RoomMessagesResponse {
                    message_list: seq
                        .messages
                        .clone()
                        .ok_or_else(|| errors::convert_cache_error(CacheError::Unexpected))?,
                };

                break;
            }

            match (seq.messages.as_ref(), seq.next) {
                (None, _) => {
                    // can happen when a room has no displayable messages
                    // OR when an unknown message ID is requested - as long as follow-up context request is not implemented
                    log::debug!("Retrieved empty messages from SequenceChunk");

                    response = RoomMessagesResponse {
                        message_list: vec![],
                    };

                    break;
                }
                (Some(val), None) => {
                    log::warn!("No sync token available for further fetching");

                    response = RoomMessagesResponse {
                        message_list: val.clone(),
                    };

                    break;
                }
                (Some(val), Some(next)) => {
                    log::debug!("Attempting to fetch further messages from sdk");
                    let fetched = MatrixClient::fetch_messages_from_sdk(
                        self.cached_data.clone(),
                        order,
                        &room,
                        next,
                        limit,
                    )
                    .await?;

                    if fetched == 0 {
                        response = RoomMessagesResponse {
                            message_list: val.clone(),
                        };

                        break;
                    }
                }
            }
        }

        let ctx = ctx.clone();
        let room_id = room_id.clone();
        let room_client = room_client.clone();
        let cached_data = self.cached_data.clone();

        tokio::spawn(async move {
            let result = retry_decryption(
                seq.messages,
                &room_id,
                &room_client,
                cached_data,
                key_change_rx,
                &ctx,
            )
            .await;

            if let Err(err) = result {
                ctx.send_error(errors::convert_cache_error(err));
            }
        });

        Ok(response)
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
        } = self.get_initialized_data_logged_in()?;

        let UserSearchRequest { query, limit } = request;

        let user_list = client
            .search_users(&query, limit as u64)
            .await
            .map_err(errors::convert_http_error)?;

        let mut result = Vec::new();

        for user in user_list.results {
            let presence = user::request_user_presence(client, &user.user_id).await;

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

        let client = self.get_client_logged_in()?;

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
        let room = self.get_matrix_room(&request.room_id)?;

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
        let room = self.get_matrix_room(&request.room_id)?;
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
        } = self.get_initialized_data_logged_in()?;

        let Some(user_id) = client.user_id() else {
            log::error!("Error retrieving the user ID of the current user");
            return Err(errors::create_unknown(
                "Error retrieving the user ID of the current user",
            ));
        };

        let InvitedReply { room_id, accepted } = request;

        let room = self.get_matrix_room(&room_id)?;

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
        } = self.get_initialized_data_logged_in()?;

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
        _ctx: ClientContext,
        request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in()?;

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
            // TODO: Error handling
            let _ = media_manager
                .upload_room_avatar(&room, &PathBuf::from(avatar_path))
                .await;
        }

        Ok(rooms::convert_to_proto(media_manager, room, user_id).await?)
    }

    async fn create_direct_room(
        &mut self,
        _ctx: ClientContext,
        request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        let InitializedData {
            client,
            media_manager,
            ..
        } = self.get_initialized_data_logged_in()?;

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
            // TODO: Error handling
            let _ = media_manager
                .upload_room_avatar(&room, &PathBuf::from(avatar_path))
                .await;
        }

        Ok(rooms::convert_to_proto(media_manager, room, our_user_id).await?)
    }

    async fn change_room(
        &mut self,
        _ctx: ClientContext,
        request: RoomChangeRequest,
    ) -> Result<RoomChangeEvent> {
        let RoomChangeRequest {
            room_id,
            display_name,
            join_rule,
            avatar_path,
            is_favorite,
        } = request;

        let InitializedData { media_manager, .. } = self.get_initialized_data_logged_in()?;
        let room = self.get_matrix_room(&room_id)?;

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
            // TODO: Error handling
            let _ = media_manager
                .upload_room_avatar(&room, &PathBuf::from(&avatar_path))
                .await;

            response = response.change_avatar_path(avatar_path);
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
        let room = self.get_matrix_room(&request.room_id)?;

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
        } = self.get_initialized_data_logged_in()?;

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
        let client = self.get_client_logged_in()?;

        let RoomKnockRequest { room_id, message } = request;

        let room_id =
            RoomId::parse(&room_id).map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        client
            .knock(room_id.into(), message, Vec::new())
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    async fn mark_as_read(
        &mut self,
        _ctx: ClientContext,
        request: RoomMarkAsReadRequest,
    ) -> Result<RoomChangeEvent> {
        use matrix_sdk::ruma::api::client::receipt::create_receipt::v3::ReceiptType;
        use matrix_sdk::ruma::events::receipt::ReceiptThread;

        let room = self.get_matrix_room(&request.room_id)?;
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

        let room = self.get_matrix_room(&request.room_id)?;

        let Some(content) = request.content else {
            return Err(errors::create_unknown("Message content not set"));
        };

        let event = match content {
            Content::Text(text) => RoomMessageEventContent::text_plain(text.content),
            Content::Image(_) => {
                return Err(errors::create_error(ErrorType::NotImplemented));
            }
        };

        let re = room
            .send(event)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(MessageSendResponse {
            message_id: re.event_id.to_string(),
        })
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

        let room = self.get_matrix_room(&room_id)?;

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
            content,
        } = request;

        let room = self.get_matrix_room(&room_id)?;

        let event_id = EventId::parse(message_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))?;

        let Some(content) = content else {
            return Err(errors::create_unknown("Message content not set"));
        };

        let content = match content {
            Content::Text(text) => RoomMessageEventContentWithoutRelation::text_plain(text.content),
            Content::Image(_) => {
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

        let room = self.get_matrix_room(&room_id)?;

        let message_id = OwnedEventId::try_from(message_id)
            .map_err(|_| errors::create_error(ErrorType::InvalidMessageId))?;

        let event = ReactionEventContent::new(Annotation::new(message_id, reaction));

        room.send(event)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
