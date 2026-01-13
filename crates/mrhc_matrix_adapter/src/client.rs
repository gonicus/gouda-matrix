use std::path::PathBuf;

use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedUserId, RoomId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use matrix_sdk_base::RoomStateFilter;
use mrhc_core::{Client as ClientAbstraction, ClientContext, Result};
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;
use url::Url;

use crate::session::Session;
use crate::verification::VerificationManager;
use crate::{errors, rooms, session, utils};

#[derive(Clone)]
struct InitializedData {
    /// The initialized matrix client.
    pub client: Client,
    /// The display name of this device.
    pub device_display_name: String,

    /// The file where the current session metadata is stored.
    pub session_file: PathBuf,
    /// The passphrase used to encrypt the session data.
    pub session_passphrase: String,
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
    #[inline]
    fn get_client(&self) -> Result<&Client> {
        Ok(&self.get_initialized_data()?.client)
    }

    /// Returns the initialized data if it has been initialized with `Self::initialize`.
    /// An error is returned if the client has not yet been initialized.
    #[inline]
    fn get_initialized_data(&self) -> Result<&InitializedData> {
        let data = self.initialized_data.as_ref().ok_or(Error {
            r#type: ErrorType::NotInitialized.into(),
            error_string: Some("The client has not been initialized".to_owned()),
        })?;
        Ok(data)
    }

    /// Returns the client if it was initialized with `Self::initialize` and logged in with
    /// either `Self::login_sso` or `Self::login_username_password`.
    /// An error is returned if the client is not yet initialized or is not currently logged in.
    #[inline]
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
    #[inline]
    fn initialize_data(
        &mut self,
        request: InitializationRequest,
        client: Client,
        session_file: PathBuf,
    ) {
        let data = InitializedData {
            client,
            device_display_name: request.device_display_name,
            session_file,
            session_passphrase: request.encryption_secret,
        };

        self.initialized_data = Some(data);
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
    fn get_matrix_room(&mut self, room_id: &str) -> Result<matrix_sdk::Room> {
        let client = self.get_client()?;

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
        mut ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let session_dir = PathBuf::from(&request.data_root_path).join("session_data");
        let session_file = session_dir.join("session");
        let session_passphrase = request.encryption_secret.clone();

        if session_file.exists() {
            let result = session::restore_session(
                &homeserver_url,
                session_file.clone(),
                session_passphrase.clone(),
                &session_dir,
                &request.persistent_storage_secret,
            )
            .await;

            match result {
                Ok((client, mut session)) => {
                    self.initialize_data(request, client.clone(), session_file.clone());

                    let sync_settings = SyncSettings::new();

                    session
                        .initial_sync(&mut ctx, &client, sync_settings.clone())
                        .await?;
                    session.start_background_sync(ctx, client, sync_settings)?;

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

        self.initialize_data(request, client, session_file);

        Ok(StatusUpdate {
            code: status_update::StatusCode::Connected as i32,
        })
    }

    async fn get_login_flows(&mut self, _ctx: ClientContext) -> Result<LoginFlowsResponse> {
        use matrix_sdk::ruma::api::client::session::get_login_types::v3::LoginType as MatrixLoginType;

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

    async fn login_username_password(
        &mut self,
        mut ctx: ClientContext,
        request: UsernamePasswordLoginRequest,
    ) -> Result<StatusUpdate> {
        let InitializedData {
            client,
            device_display_name,
            session_file,
            session_passphrase,
            ..
        } = self.get_initialized_data()?;

        if client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Client is already logged in",
            ));
        }

        let result = client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(device_display_name)
            .await;

        if let Err(err) = result {
            return Err(errors::convert_matrix_sdk_error(err));
        }

        log::info!("Successfully logged in as {:?}", client.user_id());

        let mut session = Session::new(
            client,
            session_file.to_path_buf(),
            session_passphrase.clone(),
        )?;
        session.save().await?;

        let sync_settings = SyncSettings::new();

        session
            .initial_sync(&mut ctx, client, sync_settings.clone())
            .await?;
        session.start_background_sync(ctx, client.clone(), sync_settings)?;

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &mut self,
        mut ctx: ClientContext,
        request: SsoLoginRequest,
    ) -> Result<SsoLoginResponse> {
        let InitializedData {
            client,
            device_display_name,
            session_file,
            session_passphrase,
            ..
        } = self.get_initialized_data()?;

        if client.matrix_auth().logged_in() {
            return Err(errors::create_error_msg(
                ErrorType::Authorization,
                "Client is already logged in",
            ));
        }

        // Create a channel so we can receive the login url from the async closure
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut login_builder = client
            .matrix_auth()
            .login_sso(|url| async move {
                #[allow(clippy::expect_used)]
                tx.send(url).expect("Receiver of the login url dropped");
                Ok(())
            })
            .initial_device_display_name(device_display_name);

        if let Some(idp) = request.identity_provider {
            login_builder = login_builder.identity_provider_id(&idp);
        }

        // Clone the data so we can move it into the tokio task
        let client = client.clone();
        let session_file = session_file.clone();
        let session_passphrase = session_passphrase.clone();

        // Spawn a tokio task which waits for the successful login in order to send
        // a status update to the application.
        tokio::spawn(async move {
            if let Err(err) = login_builder.await {
                ctx.send_error(errors::convert_matrix_sdk_error(err));
            }

            log::info!("Successfully logged in as {:?}", client.user_id());

            let Ok(mut session) = Session::new(&client, session_file, session_passphrase) else {
                ctx.send_error(errors::create_unknown("Error creating session"));
                return;
            };

            if let Err(err) = session.save().await {
                ctx.send_error(err);
                return;
            }

            let sync_settings = SyncSettings::new();

            if let Err(err) = session
                .initial_sync(&mut ctx, &client, sync_settings.clone())
                .await
            {
                ctx.send_error(err);
                return;
            }

            if session
                .start_background_sync(ctx.clone(), client.clone(), sync_settings)
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

        Ok(SsoLoginResponse { login_url })
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

    async fn get_rooms(
        &mut self,
        _ctx: ClientContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        let client = self.get_client_logged_in()?;

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

            result.push(rooms::convert_to_proto(room).await?);
        }

        Ok(RoomListResponse { room_list: result })
    }

    async fn send_message(
        &mut self,
        _ctx: ClientContext,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let room = self.get_matrix_room(&request.room_id)?;
        let event = RoomMessageEventContent::text_plain(request.content);

        let re = room
            .send(event)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(SendMessageResponse {
            message_id: re.event_id.to_string(),
        })
    }

    async fn get_users(&mut self, _ctx: ClientContext) -> Result<UserListResponse> {
        let client = self.get_client_logged_in()?;

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

                // TODO: Presence state

                result.push(User {
                    user_id: member.user_id().to_string(),
                    display_name: member
                        .display_name()
                        .map(str::to_string)
                        .unwrap_or_default(),
                    presence_state: None,
                });
            }
        }

        Ok(UserListResponse { user_list: result })
    }

    async fn search_users(
        &mut self,
        _ctx: ClientContext,
        request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        let client = self.get_client_logged_in()?;

        let UserSearchRequest { query, limit } = request;

        let user_list = client
            .search_users(&query, limit as u64)
            .await
            .map_err(errors::convert_http_error)?;

        let mut result = Vec::new();

        for user in user_list.results {
            // TODO: Presence state

            result.push(User {
                user_id: user.user_id.to_string(),
                display_name: user.display_name.unwrap_or_default(),
                presence_state: None,
            });
        }

        Ok(UserSearchResponse { user_list: result })
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
        request: CrossSigningAcceptRequest,
    ) -> Result<()> {
        let _ = self.get_client_logged_in()?;

        self.cleanup_verifications();

        let CrossSigningAcceptRequest {
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

    async fn create_direct_room(
        &mut self,
        _ctx: ClientContext,
        request: CreateDirectRoomRequest,
    ) -> Result<Room> {
        let client = self.get_client_logged_in()?;

        let user_id = UserId::parse(&request.invitee).map_err(errors::convert_id_parse_error)?;

        let room_request =
            rooms::create_dm_room_request(request.display_name.clone(), user_id.to_owned());

        let room = client
            .create_room(room_request)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        // The room name is a separate event retrieved after the room is created and
        // the SDK returns the room object. Therefore, `room.display_name()` always
        // returns an empty name after creation, even if we specified a name in
        // the request. To avoid waiting for the name event, we simply override
        // the display name in the response.
        let mut response = rooms::convert_to_proto(room).await?;
        response.display_name = request.display_name;

        Ok(response)
    }

    async fn create_group_room(
        &mut self,
        _ctx: ClientContext,
        request: CreateGroupRoomRequest,
    ) -> Result<Room> {
        let client = self.get_client_logged_in()?;

        let invitees: Vec<OwnedUserId> = request
            .invitees
            .into_iter()
            .map(|id| UserId::parse(&id).map_err(errors::convert_id_parse_error))
            .collect::<Result<Vec<OwnedUserId>>>()?;

        let mut room_request = rooms::create_room_request(request.display_name.clone(), invitees);

        room_request.visibility = if request.is_public {
            matrix_sdk::ruma::api::client::room::Visibility::Public
        } else {
            matrix_sdk::ruma::api::client::room::Visibility::Private
        };

        let room = client
            .create_room(room_request)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        // The room name is a separate event retrieved after the room is created and
        // the SDK returns the room object. Therefore, `room.display_name()` always
        // returns an empty name after creation, even if we specified a name in
        // the request. To avoid waiting for the name event, we simply override
        // the display name in the response.
        let mut response = rooms::convert_to_proto(room).await?;
        response.display_name = request.display_name;

        Ok(response)
    }

    async fn mark_as_read(
        &mut self,
        _ctx: ClientContext,
        request: MarkAsReadRequest,
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
                .into_proto(),
        )
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
                .into_proto(),
        )
    }

    async fn change_room(
        &mut self,
        _ctx: ClientContext,
        request: ChangeRoomRequest,
    ) -> Result<RoomChangeEvent> {
        let ChangeRoomRequest {
            room_id,
            display_name,
            is_public,
        } = request;

        let room = self.get_matrix_room(&room_id)?;

        let mut response = builder::RoomChangeEventBuilder::new(room_id.to_string());

        if let Some(display_name) = display_name {
            room.set_name(display_name.clone())
                .await
                .map_err(errors::convert_matrix_sdk_error)?;

            response = response.change_display_name(display_name);
        }

        if let Some(is_public) = is_public {
            rooms::update_room_visibility(&room, is_public).await?;
        }

        Ok(response.into_proto())
    }

    async fn leave_room(
        &mut self,
        _ctx: ClientContext,
        request: LeaveRoomRequest,
    ) -> Result<RoomLeftEvent> {
        let room = self.get_matrix_room(&request.room_id)?;

        room.leave()
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        Ok(RoomLeftEvent {
            room_id: request.room_id,
            reason: room_left_event::RoomLeaveReason::User.into(),
            message: String::new(),
        })
    }

    async fn public_rooms(
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

        let filter = {
            let mut filter = Filter::default();
            if !generic_search_term.is_empty() {
                filter.generic_search_term = Some(generic_search_term);
            }
            filter
        };

        let request = {
            let mut request = get_public_rooms_filtered::v3::Request::default();
            request.limit = limit.map(|f| f.into());
            request.since = since;
            request.filter = filter;
            request
        };

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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
