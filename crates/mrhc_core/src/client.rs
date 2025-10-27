use async_trait::async_trait;
use mrhc_proto::chat::CapabilityResponse;

#[async_trait]
pub trait Client: Send {
    async fn get_capabilities(&mut self) -> CapabilityResponse;
}
