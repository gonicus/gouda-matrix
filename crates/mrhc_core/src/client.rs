use async_trait::async_trait;
use std::any::Any;
use tokio::sync::mpsc::UnboundedSender;

use mrhc_proto::chat::{CapabilityResponse, LoginRequest, LoginResponse, ResponseContainer};

use crate::Result;

#[async_trait]
pub trait Client: Send {
    fn set_output_sender(&mut self, sender: UnboundedSender<ResponseContainer>);

    async fn get_capabilities(&mut self) -> CapabilityResponse;
    async fn login_request(&mut self, request: LoginRequest) -> Result<LoginResponse>;

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
