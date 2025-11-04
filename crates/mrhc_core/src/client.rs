use async_trait::async_trait;
use std::any::Any;
use tokio::sync::mpsc::UnboundedSender;

use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;

use crate::output_processor::OutputTask;
use crate::{create_error_msg, Result};

#[inline]
fn not_implemented_error<T>() -> Result<T> {
    Err(Error {
        r#type: ErrorType::NotImplemented as i32,
        error_string: Some("The requested feature is not implemented by the client".to_owned()),
    })
}

#[derive(Clone)]
pub struct ClientContext {
    output_sender: UnboundedSender<OutputTask>,
}

impl ClientContext {
    pub fn new(output_sender: UnboundedSender<OutputTask>) -> Self {
        Self { output_sender }
    }

    pub fn send_response(&mut self, tag: u64, content: ResponseContent) {
        self.output_sender
            .send(OutputTask::Response(Box::new(ResponseContainer {
                tag,
                content: Some(content),
            })))
            .expect("Receiver of the output sender dropped");
    }

    pub fn send_event(&mut self, content: ResponseContent) {
        self.output_sender
            .send(OutputTask::Response(Box::new(ResponseContainer {
                tag: 0,
                content: Some(content),
            })))
            .expect("Receiver of the output sender dropped");
    }

    pub fn send_error_msg<M: std::fmt::Display>(&mut self, ty: ErrorType, msg: M) {
        self.output_sender
            .send(OutputTask::Response(Box::new(ResponseContainer {
                tag: 0,
                content: Some(ResponseContent::Error(create_error_msg(ty, msg))),
            })))
            .expect("Receiver of the output sender dropped");
    }
}

#[async_trait]
pub trait Client: Send {
    async fn get_capabilities(&mut self, ctx: ClientContext) -> Result<CapabilityResponse>;

    async fn initialize(
        &mut self,
        ctx: ClientContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate>;

    async fn get_login_flows(&mut self, ctx: ClientContext) -> Result<LoginFlowsResponse>;

    #[allow(unused_variables)]
    async fn login_username_password(
        &mut self,
        ctx: ClientContext,
        request: UsernamePasswordLoginRequest,
    ) -> Result<StatusUpdate> {
        not_implemented_error()
    }

    #[allow(unused_variables)]
    async fn login_sso(
        &mut self,
        ctx: ClientContext,
        request: SsoLoginRequest,
    ) -> Result<SsoLoginResponse> {
        not_implemented_error()
    }

    #[allow(unused_variables)]
    async fn get_identity_providers(
        &mut self,
        ctx: ClientContext,
    ) -> Result<IdentityProvidersResponse> {
        not_implemented_error()
    }

    #[allow(unused_variables)]
    async fn get_rooms(
        &mut self,
        ctx: ClientContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        not_implemented_error()
    }

    #[allow(unused_variables)]
    async fn send_message(
        &mut self,
        ctx: ClientContext,
        request: Message,
    ) -> Result<SendMessageResponse> {
        not_implemented_error()
    }

    async fn get_users(&mut self, ctx: ClientContext) -> Result<UserListResponse>;

    /// This method is currently used only for testing purposes to downcast a `dyn Client`.
    /// Implement this method as follows:
    /// ```ignore
    /// use mrhc_core::Client;
    ///
    /// struct MyClient;
    ///
    /// impl Client for MyClient {
    ///     fn as_any(&self) -> &dyn std::any::Any {
    ///         self
    ///     }
    /// }
    /// ```
    fn as_any(&self) -> &dyn Any;
}
