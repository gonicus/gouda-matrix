use mrhc_proto::chat::CapabilityResponse;

use crate::Client;

#[derive(Default)]
pub struct ClientMock {
    pub get_capabilities_call_count: u32,
}

impl ClientMock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assert_get_capabilities_called_n(&self, n: u32) {
        assert!(self.get_capabilities_call_count == n);
    }
}

#[async_trait::async_trait]
impl Client for ClientMock {
    async fn get_capabilities(&mut self) -> CapabilityResponse {
        self.get_capabilities_call_count += 1;
        CapabilityResponse::default()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
