use mrhc_proto::chat::{CapabilityResponse, LoginRequest, LoginResponse};

use crate::{Client, Result};

pub struct ClientMock {
    /// The result returned from the `Self::get_capabilities` method.
    pub get_capabilities_response: CapabilityResponse,
    /// How many times the `Self::get_capabilities` method was called.
    pub get_capabilities_call_count: u32,

    /// The result returned from the `Self::login_request` method.
    pub login_request_response: Result<LoginResponse>,
    /// How many times the `Self::login_request` method was called.
    pub login_request_call_count: u32,
}

impl Default for ClientMock {
    fn default() -> Self {
        Self {
            get_capabilities_response: CapabilityResponse::default(),
            get_capabilities_call_count: 0,

            login_request_response: Ok(LoginResponse::default()),
            login_request_call_count: 0,
        }
    }
}

impl ClientMock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assert_login_request_called_n(&self, n: u32) {
        assert!(self.login_request_call_count == n)
    }

    pub fn assert_get_capabilities_called_n(&self, n: u32) {
        assert!(self.get_capabilities_call_count == n);
    }
}

#[async_trait::async_trait]
impl Client for ClientMock {
    async fn get_capabilities(&mut self) -> CapabilityResponse {
        self.get_capabilities_call_count += 1;
        self.get_capabilities_response.clone()
    }

    async fn login_request(&mut self, _request: LoginRequest) -> Result<LoginResponse> {
        self.login_request_call_count += 1;
        self.login_request_response.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
