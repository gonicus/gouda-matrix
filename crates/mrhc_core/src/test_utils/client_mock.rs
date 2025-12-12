use mrhc_proto::chat::*;

use crate::{Client, ClientContext, Result};

pub struct ClientMock {
    pub initialize_response: Result<StatusUpdate>,
    pub initialize_call_count: u32,

    pub get_login_flows_response: Result<LoginFlowsResponse>,
    pub get_login_flows_call_count: u32,

    pub login_username_password_response: Result<StatusUpdate>,
    pub login_username_password_call_count: u32,

    pub login_sso_response: Result<SsoLoginResponse>,
    pub login_sso_call_count: u32,

    pub get_identity_providers_response: Result<IdentityProvidersResponse>,
    pub get_identity_providers_call_count: u32,

    pub get_rooms_response: Result<RoomListResponse>,
    pub get_rooms_call_count: u32,

    pub send_message_response: Result<SendMessageResponse>,
    pub send_message_call_count: u32,

    pub get_users_response: Result<UserListResponse>,
    pub get_users_call_count: u32,

    pub recovery_key_verification_response: Result<VerificationEndEvent>,
    pub recovery_key_verification_call_count: u32,

    pub cross_signing_start_response: Result<CrossSigningStartResponse>,
    pub cross_signing_start_call_count: u32,

    pub cross_signing_select_method_response: Result<()>,
    pub cross_signing_select_method_call_count: u32,

    pub cross_signing_confirm_response: Result<()>,
    pub cross_signing_confirm_call_count: u32,

    pub abort_verification_response: Result<VerificationEndEvent>,
    pub abort_verification_call_count: u32,

    pub received_ctx: Option<ClientContext>,
}

impl Default for ClientMock {
    fn default() -> Self {
        Self {
            initialize_response: Ok(StatusUpdate::default()),
            initialize_call_count: 0,

            get_login_flows_response: Ok(LoginFlowsResponse::default()),
            get_login_flows_call_count: 0,

            login_username_password_response: Ok(StatusUpdate::default()),
            login_username_password_call_count: 0,

            login_sso_response: Ok(SsoLoginResponse::default()),
            login_sso_call_count: 0,

            get_identity_providers_response: Ok(IdentityProvidersResponse::default()),
            get_identity_providers_call_count: 0,

            get_rooms_response: Ok(RoomListResponse::default()),
            get_rooms_call_count: 0,

            send_message_response: Ok(SendMessageResponse::default()),
            send_message_call_count: 0,

            get_users_response: Ok(UserListResponse::default()),
            get_users_call_count: 0,

            recovery_key_verification_response: Ok(VerificationEndEvent::default()),
            recovery_key_verification_call_count: 0,

            cross_signing_start_response: Ok(CrossSigningStartResponse::default()),
            cross_signing_start_call_count: 0,

            cross_signing_select_method_response: Ok(()),
            cross_signing_select_method_call_count: 0,

            cross_signing_confirm_response: Ok(()),
            cross_signing_confirm_call_count: 0,

            abort_verification_response: Ok(VerificationEndEvent::default()),
            abort_verification_call_count: 0,

            received_ctx: None,
        }
    }
}

impl ClientMock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assert_initialize_called_n(&self, n: u32) {
        assert!(self.initialize_call_count == n);
    }

    pub fn assert_get_login_flows_called_n(&self, n: u32) {
        assert!(self.get_login_flows_call_count == n);
    }

    pub fn assert_login_username_password_called_n(&self, n: u32) {
        assert!(self.login_username_password_call_count == n);
    }

    pub fn assert_login_sso_called_n(&self, n: u32) {
        assert!(self.login_sso_call_count == n);
    }

    pub fn assert_get_identity_providers_called_n(&self, n: u32) {
        assert!(self.get_identity_providers_call_count == n);
    }

    pub fn assert_get_rooms_called_n(&self, n: u32) {
        assert!(self.get_rooms_call_count == n);
    }

    pub fn assert_send_message_called_n(&self, n: u32) {
        assert!(self.send_message_call_count == n);
    }

    pub fn assert_get_users_called_n(&self, n: u32) {
        assert!(self.get_users_call_count == n);
    }

    pub fn assert_recovery_key_verification_called_n(&self, n: u32) {
        assert!(self.recovery_key_verification_call_count == n);
    }

    pub fn assert_cross_signing_start_called_n(&self, n: u32) {
        assert!(self.cross_signing_start_call_count == n);
    }

    pub fn assert_cross_signing_select_method_called_n(&self, n: u32) {
        assert!(self.cross_signing_select_method_call_count == n);
    }

    pub fn assert_cross_signing_confirm_called_n(&self, n: u32) {
        assert!(self.cross_signing_confirm_call_count == n);
    }

    pub fn assert_abort_verification_called_n(&self, n: u32) {
        assert!(self.abort_verification_call_count == n);
    }
}

#[async_trait::async_trait]
impl Client for ClientMock {
    async fn initialize(
        &mut self,
        ctx: ClientContext,
        _request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        self.received_ctx = Some(ctx);
        self.initialize_call_count += 1;
        self.initialize_response.clone()
    }

    async fn get_login_flows(&mut self, ctx: ClientContext) -> Result<LoginFlowsResponse> {
        self.received_ctx = Some(ctx);
        self.get_login_flows_call_count += 1;
        self.get_login_flows_response.clone()
    }

    async fn login_username_password(
        &mut self,
        ctx: ClientContext,
        _request: UsernamePasswordLoginRequest,
    ) -> Result<StatusUpdate> {
        self.received_ctx = Some(ctx);
        self.login_username_password_call_count += 1;
        self.login_username_password_response.clone()
    }

    async fn login_sso(
        &mut self,
        ctx: ClientContext,
        _request: SsoLoginRequest,
    ) -> Result<SsoLoginResponse> {
        self.received_ctx = Some(ctx);
        self.login_sso_call_count += 1;
        self.login_sso_response.clone()
    }

    async fn get_identity_providers(
        &mut self,
        ctx: ClientContext,
    ) -> Result<IdentityProvidersResponse> {
        self.received_ctx = Some(ctx);
        self.get_identity_providers_call_count += 1;
        self.get_identity_providers_response.clone()
    }

    async fn get_rooms(
        &mut self,
        ctx: ClientContext,
        _request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        self.received_ctx = Some(ctx);
        self.get_rooms_call_count += 1;
        self.get_rooms_response.clone()
    }

    async fn send_message(
        &mut self,
        ctx: ClientContext,
        _request: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        self.received_ctx = Some(ctx);
        self.send_message_call_count += 1;
        self.send_message_response.clone()
    }

    async fn get_users(&mut self, ctx: ClientContext) -> Result<UserListResponse> {
        self.received_ctx = Some(ctx);
        self.get_users_call_count += 1;
        self.get_users_response.clone()
    }

    async fn recovery_key_verification(
        &mut self,
        ctx: ClientContext,
        _request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        self.received_ctx = Some(ctx);
        self.recovery_key_verification_call_count += 1;
        self.recovery_key_verification_response.clone()
    }

    async fn cross_signing_start(
        &mut self,
        ctx: ClientContext,
        _request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        self.received_ctx = Some(ctx);
        self.cross_signing_start_call_count += 1;
        self.cross_signing_start_response.clone()
    }

    async fn cross_signing_select_method(
        &mut self,
        ctx: ClientContext,
        _request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.cross_signing_select_method_call_count += 1;
        self.cross_signing_select_method_response.clone()
    }

    async fn cross_signing_confirm(
        &mut self,
        ctx: ClientContext,
        _request: CrossSigningAcceptRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.cross_signing_confirm_call_count += 1;
        self.cross_signing_confirm_response.clone()
    }

    async fn abort_verification(
        &mut self,
        ctx: ClientContext,
        _request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        self.received_ctx = Some(ctx);
        self.abort_verification_call_count += 1;
        self.abort_verification_response.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
