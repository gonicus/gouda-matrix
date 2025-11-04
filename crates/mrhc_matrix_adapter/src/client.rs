use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::RoomId;
use matrix_sdk::Client;
use matrix_sdk::RoomMemberships;
use matrix_sdk_base::RoomStateFilter;
use url::Url;

use mrhc_core::Client as ClientAbstraction;
use mrhc_core::ClientContext;
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;

use crate::errors;
use crate::login;
use crate::rooms;

// TODO: Make configurable inside the initialization request
const INITIAL_DEVICE_DISPLAY_NAME: &str = "matrix-rust-headless-client";

struct InitializedData {
    // The initialized matrix client.
    pub client: Client,
    // The path where to store shared data between this client and the application.
    pub data_root_path: String,
}

#[derive(Default)]
pub struct MatrixClient {
    // The inner matrix client. If `None`, the client has not yet been initialized
    // using `Self::initialize`.
    initialized_data: Option<InitializedData>,
    // Contains cached identity providers. The idps are cached when `Self::get_login_flows` is called,
    // as this method already retrieves the available idps.
    cached_idps: Option<Vec<String>>,
}

impl MatrixClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the client if it has been initialized with `Self::initialize`.
    /// An error is returned if the client has not yet been initialized.
    #[inline]
    fn get_client(&self) -> Result<&Client> {
        let data = self.initialized_data.as_ref().ok_or(Error {
            r#type: ErrorType::NotInitialized as i32,
            error_string: Some("The client has not been initialized".to_owned()),
        })?;
        Ok(&data.client)
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
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    async fn get_capabilities(&mut self, _ctx: ClientContext) -> Result<CapabilityResponse> {
        Ok(CapabilityResponse {
            direct_rooms: false,
            group_rooms: true,
            sub_threads: true,
            user_search: true,
            invitations: true,
            mime_types: Vec::new(),
        })
    }

    async fn initialize(
        &mut self,
        _ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        let homeserver_url = Url::parse(&request.backend_url)
            .map_err(|err| errors::create_error_msg(ErrorType::InvalidUrl, err))?;

        let client = Client::new(homeserver_url)
            .await
            .map_err(|err| errors::convert_client_build_error(err))?;

        let data = InitializedData {
            client,
            data_root_path: request.data_root_path,
        };

        self.initialized_data = Some(data);

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
                    // We already have access to the available identity providers and store them in the cache so
                    // that `Self::get identity_providers` does not have to retrieve them again.
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
        ctx: ClientContext,
        request: UsernamePasswordLoginRequest,
    ) -> Result<StatusUpdate> {
        let client = self.get_client()?;

        let result = client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(INITIAL_DEVICE_DISPLAY_NAME)
            .await;

        if let Err(err) = result {
            return Err(errors::convert_matrix_sdk_error(err));
        }

        log::info!("Successfully logged in as {:?}", client.user_id());
        log::info!("Waiting for initial sync to finish");

        let sync_settings = SyncSettings::new();

        login::initial_sync(client, sync_settings.clone()).await?;
        login::start_background_sync(ctx, client.clone(), sync_settings);

        Ok(StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        })
    }

    async fn login_sso(
        &mut self,
        mut ctx: ClientContext,
        request: SsoLoginRequest,
    ) -> Result<SsoLoginResponse> {
        let client = self.get_client()?;

        // Create a channel so we can receive the login url from the async closure
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut login_builder = client.matrix_auth().login_sso(|url| async move {
            #[allow(clippy::expect_used)]
            tx.send(url).expect("Receiver of the login url dropped");
            Ok(())
        });

        if let Some(idp) = request.identity_provider {
            login_builder = login_builder.identity_provider_id(&idp);
        }

        // Clone the client so we can move it into the tokio task
        let client = client.clone();

        // Spawn a tokio task which waits for the successful login in order to send a status update
        // to the application.
        tokio::spawn(async move {
            if let Err(err) = login_builder.await {
                ctx.send_error(errors::convert_matrix_sdk_error(
                    err,
                ));
            }

            log::info!("Successfully logged in as {:?}", client.user_id());
            log::info!("Waiting for initial sync");

            let sync_settings = SyncSettings::new();

            if let Err(err) = login::initial_sync(&client, sync_settings.clone()).await {
                ctx.send_error(err);
            }

            ctx.send_event(ResponseContent::StatusUpdate(StatusUpdate {
                code: status_update::StatusCode::LoggedIn as i32,
            }));

            login::start_background_sync(ctx, client.clone(), sync_settings);
        });

        // Wait until the asynchronous closure sends the received login URL, so
        // we can return it to the application.
        let login_url = rx.await.map_err(|_| {
            errors::create_error_msg(
                ErrorType::Unknown,
                "InternalError: Sender of the login url dropped",
            )
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

        // We can use the `Self::get_login_flows` method to retrieve the idps as it saves them to the cache.
        // This method would ultimately only fetch the login flows too.
        let _ = self.get_login_flows(ctx).await?;

        // If there is still nothing in the cache, no idps are available or single sign-on is not supported
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
            result.push(rooms::convert_to_proto(room).await?);
        }

        Ok(RoomListResponse { room_list: result })
    }

    async fn send_message(
        &mut self,
        _ctx: ClientContext,
        request: Message,
    ) -> Result<SendMessageResponse> {
        let client = self.get_client_logged_in()?;

        let room_id = <&RoomId>::try_from(request.room_id.as_str())
            .map_err(|_| errors::create_error(ErrorType::RoomNotFound))?;

        let room = client
            .get_room(room_id)
            .ok_or(errors::create_error(ErrorType::RoomNotFound))?;

        let event = RoomMessageEventContent::text_plain(request.content);

        let re = room
            .send(event)
            .await
            .map_err(|err| errors::convert_matrix_sdk_error(err))?;

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
                .map_err(|err| errors::convert_matrix_sdk_error(err))?;

            for member in members {
                // Skip duplicates
                if result.iter().any(|m| m.user_id == *member.user_id()) {
                    continue;
                }

                result.push(User {
                    user_id: member.user_id().to_string(),
                    display_name: member.display_name().map(str::to_string),
                });
            }
        }

        Ok(UserListResponse { user_list: result })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
