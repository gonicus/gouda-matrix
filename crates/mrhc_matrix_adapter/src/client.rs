use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent;
use matrix_sdk::Client;
use mrhc_proto::chat::*;
use url::Url;

use mrhc_core::Client as ClientAbstraction;
use mrhc_core::ClientContext;
use mrhc_core::Result;
use mrhc_core::{create_error, create_error_msg};
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;

// TODO: Make configurable inside the initialization request
const INITIAL_DEVICE_DISPLAY_NAME: &str = "matrix-rust-headless-client";

#[derive(Default)]
pub struct MatrixClient {
    client: Option<Client>,
    cached_idps: Option<Vec<String>>,
}

impl MatrixClient {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn get_client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or(Error {
            r#type: ErrorType::NotInitialized as i32,
            error_string: Some("The client has not been initialized.".to_owned()),
        })
    }
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    async fn get_capabilities(&mut self, _ctx: ClientContext) -> Result<CapabilityResponse> {
        Ok(CapabilityResponse {
            direct_rooms: false,
            group_rooms: false,
            sub_threads: false,
            mime_types: Vec::new(),
        })
    }

    async fn initialize(
        &mut self,
        _ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        let homeserver_url =
            Url::parse(&request.backend_url).map_err(|_| create_error(ErrorType::InvalidUrl))?;

        let client = Client::new(homeserver_url)
            .await
            .map_err(|err| create_error_msg(ErrorType::Unknown, err))?;

        self.client = Some(client);

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
            .map_err(|err| Error {
                r#type: ErrorType::Network as i32,
                error_string: Some(err.to_string()),
            })?;

        let mut response = LoginFlowsResponse::default();

        for flow in &login_types.flows {
            match flow {
                MatrixLoginType::Password(_) => {
                    response.push_login_flows(login_flows_response::LoginFlow::UsernamePassword)
                }
                MatrixLoginType::Sso(sso) => {
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
        _ctx: ClientContext,
        request: UsernamePasswordLoginRequest,
    ) -> Result<StatusUpdate> {
        let client = self.get_client()?;

        let result = client
            .matrix_auth()
            .login_username(request.username, &request.password)
            .initial_device_display_name(INITIAL_DEVICE_DISPLAY_NAME)
            .await;

        match result {
            Ok(_) => Ok(StatusUpdate {
                code: status_update::StatusCode::LoggedIn as i32,
            }),
            Err(err) => Err(create_error_msg(ErrorType::Authorization, err)),
        }
    }

    async fn login_sso(
        &mut self,
        mut ctx: ClientContext,
        request: SsoLoginRequest,
    ) -> Result<SsoLoginResponse> {
        let client = self.get_client()?;

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
            match login_builder.await {
                Ok(_) => {
                    ctx.send_event(ResponseContent::StatusUpdate(StatusUpdate {
                        code: status_update::StatusCode::LoggedIn as i32,
                    }));
                }
                Err(err) => {
                    ctx.send_event(ResponseContent::Error(create_error_msg(
                        ErrorType::Authorization,
                        err,
                    )));
                    return;
                }
            }

            log::info!("Successfully logged in as {:?}", client.user_id());

            client.add_event_handler_context(ctx.clone());
            client.add_event_handler(event_handler);

            tokio::spawn(async move {
                if let Err(err) = client.sync(SyncSettings::new()).await {
                    // TODO: Check for error type
                    ctx.send_event(ResponseContent::Error(create_error_msg(
                        ErrorType::Unknown,
                        err,
                    )));
                }
            });
        });

        let login_url = rx.await.map_err(|_| create_error(ErrorType::Unknown))?;

        Ok(SsoLoginResponse { login_url })
    }

    async fn get_identity_providers(
        &mut self,
        _ctx: ClientContext,
    ) -> Result<IdentityProvidersResponse> {
        use matrix_sdk::ruma::api::client::session::get_login_types::v3::LoginType as MatrixLoginType;

        let client = self.get_client()?;

        if let Some(idps) = &self.cached_idps {
            return Ok(IdentityProvidersResponse {
                identity_providers: idps.clone(),
            });
        }

        let login_types = client
            .matrix_auth()
            .get_login_types()
            .await
            .map_err(|err| Error {
                r#type: ErrorType::Network as i32,
                error_string: Some(err.to_string()),
            })?;

        let mut idps = Vec::new();

        for flow in &login_types.flows {
            if let MatrixLoginType::Sso(sso) = flow {
                idps = sso
                    .identity_providers
                    .iter()
                    .map(|idp| idp.id.to_owned())
                    .collect();
                break;
            }
        }

        Ok(IdentityProvidersResponse {
            identity_providers: idps,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

async fn event_handler(ev: SyncRoomMessageEvent, _ctx: Ctx<ClientContext>) {
    log::info!("Received event: {ev:?}");

    // ctx.clone()
    //     .send_event(ResponseContent::StatusUpdate(StatusUpdate { code: 1 }));
}
