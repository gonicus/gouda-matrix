use async_trait::async_trait;

use mrhc_core::Client;
use mrhc_proto::chat::CapabilityResponse;

pub struct MatrixClient;

#[async_trait]
impl Client for MatrixClient {
    async fn get_capabilities(&mut self) -> CapabilityResponse {
        CapabilityResponse {
            direct_rooms: false,
            group_rooms: false,
            sub_threads: false,
            mime_types: Vec::new(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
