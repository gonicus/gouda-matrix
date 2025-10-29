use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent;
use matrix_sdk::Client;
use matrix_sdk::Room;
use mrhc_proto::chat::StatusUpdate;
use tokio::sync::mpsc::UnboundedSender;
use url::Url;

use mrhc_core::create_error_msg;
use mrhc_core::Client as ClientAbstraction;
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{CapabilityResponse, LoginRequest, LoginResponse, ResponseContainer};

#[derive(Default)]
pub struct MatrixClient {
    client: Option<Client>,
    output_sender: Option<UnboundedSender<ResponseContainer>>,
}

impl MatrixClient {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ClientAbstraction for MatrixClient {
    fn set_output_sender(&mut self, sender: UnboundedSender<ResponseContainer>) {
        self.output_sender = Some(sender);
    }

    async fn get_capabilities(&mut self) -> CapabilityResponse {
        CapabilityResponse {
            direct_rooms: false,
            group_rooms: false,
            sub_threads: false,
            mime_types: Vec::new(),
        }
    }

    async fn login_request(&mut self, request: LoginRequest) -> Result<LoginResponse> {
        let homeserver_url = request.backend_url.ok_or(create_error_msg(
            ErrorType::Authorization,
            "Backend URL missing".to_owned(),
        ))?;

        let homserver_url = Url::parse(&homeserver_url).map_err(|_| {
            create_error_msg(ErrorType::Authorization, "Invalid backend URL".to_owned())
        })?;

        let client = Client::new(homserver_url)
            .await
            .map_err(|err| create_error_msg(ErrorType::Authorization, err.to_string()))?;

        let mut login_builder = client.matrix_auth().login_sso(|url| async move {
            log::info!("Open this URL in your browser: {url}");
            Ok(())
        });

        login_builder = login_builder.identity_provider_id("oidc-keycloak");

        login_builder
            .await
            .map_err(|err| create_error_msg(ErrorType::Authorization, err.to_string()))?;

        log::info!("Logged in as {}", client.user_id().unwrap());

        let output_sender = self.output_sender.clone();

        client.add_event_handler_context(output_sender.expect("Output sender has not been set"));
        client.add_event_handler(event_handler);

        self.client = Some(client.clone());

        tokio::spawn(async move {
            client.sync(SyncSettings::new()).await.unwrap();
        });

        Ok(LoginResponse::default())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

async fn event_handler(
    ev: SyncRoomMessageEvent,
    _room: Room,
    output_sender: Ctx<UnboundedSender<ResponseContainer>>,
) {
    log::info!("Received event: {ev:?}");

    output_sender
        .send(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::StatusUpdate(StatusUpdate { code: 1 })),
        })
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_login() {
        let mut client = MatrixClient::new();

        let response = client
            .login_request(LoginRequest {
                user_id: String::new(),
                backend_url: Some("https://matrix.gonicus.de".to_owned()),
            })
            .await;

        println!("RECEIVED_RESPONSE: {response:?}");
    }
}
