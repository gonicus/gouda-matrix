use async_trait::async_trait;

use mrhc_core::Client;
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::{CapabilityResponse, Error, LoginRequest, LoginResponse};

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

    async fn login_request(&mut self, _request: LoginRequest) -> Result<LoginResponse> {
        Err(Error {
            r#type: ErrorType::Authorization as i32,
            error_string: Some("".to_owned()),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
