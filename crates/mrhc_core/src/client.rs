use async_trait::async_trait;
use std::any::Any;

use mrhc_proto::chat::CapabilityResponse;

#[async_trait]
pub trait Client: Send {
    async fn get_capabilities(&mut self) -> CapabilityResponse;

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
