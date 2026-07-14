#![allow(clippy::unwrap_used)]

use std::sync::Mutex;

use gouda_proto::chat::*;

use crate::{Client, RequestContext};

/// Custom result for the ClientMock so we can implement a default value.
#[derive(Clone)]
struct Result<T>(crate::Result<T>);

impl<T> Default for Result<T>
where
    T: Default,
{
    fn default() -> Self {
        Self(crate::Result::Ok(T::default()))
    }
}

impl<T> From<crate::Result<T>> for Result<T>
where
    T: Default,
{
    fn from(value: crate::Result<T>) -> Self {
        Self(value)
    }
}

impl<T> From<Result<T>> for crate::Result<T>
where
    T: Default,
{
    fn from(value: Result<T>) -> Self {
        match value.0 {
            Ok(val) => crate::Result::Ok(val),
            Err(err) => crate::Result::Err(err),
        }
    }
}

/// Mocks the `Client` trait.
#[derive(Default)]
pub struct ClientMock {
    /// The context received from the latest request.
    received_ctx: Mutex<Option<RequestContext>>,
    /// The response we have received with [`Self::on_response`]
    received_response: Mutex<Option<ResponseContainer>>,

    initialize_response: Mutex<Result<StatusUpdate>>,
    initialize_call_count: Mutex<u32>,

    get_login_flows_response: Mutex<Result<LoginFlowsResponse>>,
    get_login_flows_call_count: Mutex<u32>,

    get_identity_providers_response: Mutex<Result<IdentityProvidersResponse>>,
    get_identity_providers_call_count: Mutex<u32>,

    login_username_password_response: Mutex<Result<StatusUpdate>>,
    login_username_password_call_count: Mutex<u32>,

    login_sso_response: Mutex<Result<LoginSsoResponse>>,
    login_sso_call_count: Mutex<u32>,

    recovery_key_verification_response: Mutex<Result<VerificationEndEvent>>,
    recovery_key_verification_call_count: Mutex<u32>,

    cross_signing_start_response: Mutex<Result<CrossSigningStartResponse>>,
    cross_signing_start_call_count: Mutex<u32>,

    cross_signing_select_method_response: Mutex<Result<()>>,
    cross_signing_select_method_call_count: Mutex<u32>,

    cross_signing_confirm_response: Mutex<Result<()>>,
    cross_signing_confirm_call_count: Mutex<u32>,

    abort_verification_response: Mutex<Result<VerificationEndEvent>>,
    abort_verification_call_count: Mutex<u32>,

    get_global_settings_response: Mutex<Result<GlobalSettings>>,
    get_global_settings_call_count: Mutex<u32>,

    get_user_response: Mutex<Result<User>>,
    get_user_call_count: Mutex<u32>,

    search_users_response: Mutex<Result<UserSearchResponse>>,
    search_users_call_count: Mutex<u32>,

    set_status_response: Mutex<Result<()>>,
    set_status_call_count: Mutex<u32>,

    get_public_rooms_response: Mutex<Result<PublicRoomListResponse>>,
    get_public_rooms_call_count: Mutex<u32>,

    invite_response: Mutex<Result<RoomChangeEvent>>,
    invite_call_count: Mutex<u32>,

    invitation_reply_response: Mutex<Result<()>>,
    invitation_reply_call_count: Mutex<u32>,

    get_rooms_response: Mutex<Result<RoomListResponse>>,
    get_rooms_call_count: Mutex<u32>,

    create_group_room_response: Mutex<Result<Room>>,
    create_group_room_call_count: Mutex<u32>,

    create_direct_room_response: Mutex<Result<Room>>,
    create_direct_room_call_count: Mutex<u32>,

    change_room_response: Mutex<Result<RoomChangeEvent>>,
    change_room_call_count: Mutex<u32>,

    leave_room_response: Mutex<Result<RoomLeftEvent>>,
    leave_room_call_count: Mutex<u32>,

    join_room_response: Mutex<Result<Room>>,
    join_room_call_count: Mutex<u32>,

    knock_room_response: Mutex<Result<()>>,
    knock_room_call_count: Mutex<u32>,

    get_room_messages_response: Mutex<Result<()>>,
    get_room_messages_call_count: Mutex<u32>,

    mark_as_read_response: Mutex<Result<RoomChangeEvent>>,
    mark_as_read_call_count: Mutex<u32>,

    activate_typing_notice_response: Mutex<Result<()>>,
    activate_typing_notice_call_count: Mutex<u32>,

    send_message_response: Mutex<Result<MessageSendResponse>>,
    send_message_call_count: Mutex<u32>,

    remove_message_response: Mutex<Result<()>>,
    remove_message_call_count: Mutex<u32>,

    change_message_response: Mutex<Result<()>>,
    change_message_call_count: Mutex<u32>,

    create_reaction_response: Mutex<Result<()>>,
    create_reaction_call_count: Mutex<u32>,

    remove_reaction_response: Mutex<Result<()>>,
    remove_reaction_call_count: Mutex<u32>,

    get_message_response: Mutex<Result<Message>>,
    get_message_call_count: Mutex<u32>,
}

impl ClientMock {
    /// Creates a new `ClientMock` object with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// The response received with the [`Self::on_response`] event handler.
    pub fn assert_received_response(&self, response: ResponseContainer) {
        assert_eq!(*self.received_response.lock().unwrap(), Some(response));
    }

    /// The response [`Self::initialize`] should return.
    pub fn initialize_response(mut self, response: crate::Result<StatusUpdate>) -> Self {
        self.initialize_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::initialize`] was called `n` times.
    pub fn assert_initialize_called_n(&self, n: u32) {
        assert!(*self.initialize_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_login_flows`] should return.
    pub fn get_login_flows_response(mut self, response: crate::Result<LoginFlowsResponse>) -> Self {
        self.get_login_flows_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_login_flows`] was called `n` times.
    pub fn assert_get_login_flows_called_n(&self, n: u32) {
        assert!(*self.get_login_flows_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_identity_providers`] should return.
    pub fn get_identity_providers_response(
        mut self,
        response: crate::Result<IdentityProvidersResponse>,
    ) -> Self {
        self.get_identity_providers_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_identity_providers`] was called `n` times.
    pub fn assert_get_identity_providers_called_n(&self, n: u32) {
        assert!(*self.get_identity_providers_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::login_username_password`] should return.
    pub fn login_username_password_response(
        mut self,
        response: crate::Result<StatusUpdate>,
    ) -> Self {
        self.login_username_password_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::login_username_password`] was called `n` times.
    pub fn assert_login_username_password_called_n(&self, n: u32) {
        assert!(*self.login_username_password_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::login_sso`] should return.
    pub fn login_sso_response(mut self, response: crate::Result<LoginSsoResponse>) -> Self {
        self.login_sso_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::login_sso`] was called `n` times.
    pub fn assert_login_sso_called_n(&self, n: u32) {
        assert!(*self.login_sso_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::recovery_key_verification`] should return.
    pub fn recovery_key_verification_response(
        mut self,
        response: crate::Result<VerificationEndEvent>,
    ) -> Self {
        self.recovery_key_verification_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::recovery_key_verification`] was called `n` times.
    pub fn assert_recovery_key_verification_called_n(&self, n: u32) {
        assert!(*self.recovery_key_verification_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::cross_signing_start`] should return.
    pub fn cross_signing_start_response(
        mut self,
        response: crate::Result<CrossSigningStartResponse>,
    ) -> Self {
        self.cross_signing_start_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::cross_signing_start`] was called `n` times.
    pub fn assert_cross_signing_start_called_n(&self, n: u32) {
        assert!(*self.cross_signing_start_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::cross_signing_select_method`] should return.
    pub fn cross_signing_select_method_response(mut self, response: crate::Result<()>) -> Self {
        self.cross_signing_select_method_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::cross_signing_select_method`] was called `n` times.
    pub fn assert_cross_signing_select_method_called_n(&self, n: u32) {
        assert!(*self.cross_signing_select_method_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::cross_signing_confirm`] should return.
    pub fn cross_signing_confirm_response(mut self, response: crate::Result<()>) -> Self {
        self.cross_signing_confirm_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::cross_signing_confirm`] was called `n` times.
    pub fn assert_cross_signing_confirm_called_n(&self, n: u32) {
        assert!(*self.cross_signing_confirm_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::abort_verification`] should return.
    pub fn abort_verification_response(
        mut self,
        response: crate::Result<VerificationEndEvent>,
    ) -> Self {
        self.abort_verification_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::abort_verification`] was called `n` times.
    pub fn assert_abort_verification_called_n(&self, n: u32) {
        assert!(*self.abort_verification_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_global_settings`] should return.
    pub fn get_global_settings_response(mut self, response: crate::Result<GlobalSettings>) -> Self {
        self.get_global_settings_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_global_settings`] was called `n` times.
    pub fn assert_get_global_settings_called_n(&self, n: u32) {
        assert!(*self.get_global_settings_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_user`] should return.
    pub fn get_user_response(mut self, response: crate::Result<User>) -> Self {
        self.get_user_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_user`] was called `n` times.
    pub fn assert_get_user_called_n(&self, n: u32) {
        assert!(*self.get_user_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::search_users`] should return.
    pub fn search_users_response(mut self, response: crate::Result<UserSearchResponse>) -> Self {
        self.search_users_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::search_users`] was called `n` times.
    pub fn assert_search_users_called_n(&self, n: u32) {
        assert!(*self.search_users_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::set_status`] should return.
    pub fn set_status_response(mut self, response: crate::Result<()>) -> Self {
        self.set_status_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::set_status`] was called `n` times.
    pub fn assert_set_status_called_n(&self, n: u32) {
        assert!(*self.set_status_call_count.lock().unwrap() == n)
    }

    /// The response [`Self::get_public_rooms`] should return.
    pub fn get_public_rooms_response(
        mut self,
        response: crate::Result<PublicRoomListResponse>,
    ) -> Self {
        self.get_public_rooms_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_public_rooms`] was called `n` times.
    pub fn assert_get_public_rooms_called_n(&self, n: u32) {
        assert!(*self.get_public_rooms_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::invite`] should return.
    pub fn invite_response(mut self, response: crate::Result<RoomChangeEvent>) -> Self {
        self.invite_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::invite`] was called `n` times.
    pub fn assert_invite_called_n(&self, n: u32) {
        assert!(*self.invite_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::invitation_reply`] should return.
    pub fn invitation_reply_response(mut self, response: crate::Result<()>) -> Self {
        self.invitation_reply_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::invitation_reply`] was called `n` times.
    pub fn assert_invitation_reply_called_n(&self, n: u32) {
        assert!(*self.invitation_reply_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_rooms`] should return.
    pub fn get_rooms_response(mut self, response: crate::Result<RoomListResponse>) -> Self {
        self.get_rooms_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_rooms`] was called `n` times.
    pub fn assert_get_rooms_called_n(&self, n: u32) {
        assert!(*self.get_rooms_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::create_group_room`] should return.
    pub fn create_group_room_response(mut self, response: crate::Result<Room>) -> Self {
        self.create_group_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::create_group_room`] was called `n` times.
    pub fn assert_create_group_room_called_n(&self, n: u32) {
        assert!(*self.create_group_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::create_direct_room`] should return.
    pub fn create_direct_room_response(mut self, response: crate::Result<Room>) -> Self {
        self.create_direct_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::create_direct_room`] was called `n` times.
    pub fn assert_create_direct_room_called_n(&self, n: u32) {
        assert!(*self.create_direct_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::change_room`] should return.
    pub fn change_room_response(mut self, response: crate::Result<RoomChangeEvent>) -> Self {
        self.change_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::change_room`] was called `n` times.
    pub fn assert_change_room_called_n(&self, n: u32) {
        assert!(*self.change_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::leave_room`] should return.
    pub fn leave_room_response(mut self, response: crate::Result<RoomLeftEvent>) -> Self {
        self.leave_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::leave_room`] was called `n` times.
    pub fn assert_leave_room_called_n(&self, n: u32) {
        assert!(*self.leave_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::join_room`] should return.
    pub fn join_room_response(mut self, response: crate::Result<Room>) -> Self {
        self.join_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::join_room`] was called `n` times.
    pub fn assert_join_room_called_n(&self, n: u32) {
        assert!(*self.join_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::knock_room`] should return.
    pub fn knock_room_response(mut self, response: crate::Result<()>) -> Self {
        self.knock_room_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::knock_room`] was called `n` times.
    pub fn assert_knock_room_called_n(&self, n: u32) {
        assert!(*self.knock_room_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_room_messages`] should return.
    pub fn get_room_messages_response(mut self, response: crate::Result<()>) -> Self {
        self.get_room_messages_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_room_messages`] was called `n` times.
    pub fn assert_get_room_messages_called_n(&self, n: u32) {
        assert!(*self.get_room_messages_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::mark_as_read`] should return.
    pub fn mark_as_read_response(mut self, response: crate::Result<RoomChangeEvent>) -> Self {
        self.mark_as_read_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::mark_as_read`] was called `n` times.
    pub fn assert_mark_as_read_called_n(&self, n: u32) {
        assert!(*self.mark_as_read_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::activate_typing_notice`] should return.
    pub fn activate_typing_notice_response(mut self, response: crate::Result<()>) -> Self {
        self.activate_typing_notice_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::activate_typing_notice`] was called `n` times.
    pub fn assert_activate_typing_notice_called_n(&self, n: u32) {
        assert!(*self.activate_typing_notice_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::send_message`] should return.
    pub fn send_message_response(mut self, response: crate::Result<MessageSendResponse>) -> Self {
        self.send_message_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::send_message`] was called `n` times.
    pub fn assert_send_message_called_n(&self, n: u32) {
        assert!(*self.send_message_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::remove_message`] should return.
    pub fn remove_message_response(mut self, response: crate::Result<()>) -> Self {
        self.remove_message_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::remove_message`] was called `n` times.
    pub fn assert_remove_message_called_n(&self, n: u32) {
        assert!(*self.remove_message_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::change_message`] should return.
    pub fn change_message_response(mut self, response: crate::Result<()>) -> Self {
        self.change_message_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::change_message`] was called `n` times.
    pub fn assert_change_message_called_n(&self, n: u32) {
        assert!(*self.change_message_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::create_reaction`] should return.
    pub fn create_reaction_response(mut self, response: crate::Result<()>) -> Self {
        self.create_reaction_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::create_reaction`] was called `n` times.
    pub fn assert_create_reaction_called_n(&self, n: u32) {
        assert!(*self.create_reaction_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::remove_reaction`] should return.
    pub fn remove_reaction_response(mut self, response: crate::Result<()>) -> Self {
        self.remove_reaction_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::remove_reaction`] was called `n` times.
    pub fn assert_remove_reaction_called_n(&self, n: u32) {
        assert!(*self.remove_reaction_call_count.lock().unwrap() == n);
    }

    /// The response [`Self::get_message`] should return.
    pub fn get_message_response(mut self, response: crate::Result<Message>) -> Self {
        self.get_message_response = Mutex::new(response.into());
        self
    }

    /// Assert [`Self::get_message`] was called `n` times.
    pub fn assert_get_message_called_n(&self, n: u32) {
        assert!(*self.get_message_call_count.lock().unwrap() == n);
    }
}

#[async_trait::async_trait]
impl Client for ClientMock {
    async fn on_response(&self, response: ResponseContainer) {
        *self.received_response.lock().unwrap() = Some(response);
    }

    async fn initialize(
        &self,
        ctx: RequestContext,
        _request: InitializationRequest,
    ) -> crate::Result<StatusUpdate> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.initialize_call_count.lock().unwrap() += 1;
        self.initialize_response.lock().unwrap().clone().into()
    }

    async fn get_login_flows(&self, ctx: RequestContext) -> crate::Result<LoginFlowsResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_login_flows_call_count.lock().unwrap() += 1;
        self.get_login_flows_response.lock().unwrap().clone().into()
    }

    async fn get_identity_providers(
        &self,
        ctx: RequestContext,
    ) -> crate::Result<IdentityProvidersResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_identity_providers_call_count.lock().unwrap() += 1;
        self.get_identity_providers_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn login_username_password(
        &self,
        ctx: RequestContext,
        _request: LoginUsernamePasswordRequest,
    ) -> crate::Result<StatusUpdate> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.login_username_password_call_count.lock().unwrap() += 1;
        self.login_username_password_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn login_sso(
        &self,
        ctx: RequestContext,
        _request: LoginSsoRequest,
    ) -> crate::Result<LoginSsoResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.login_sso_call_count.lock().unwrap() += 1;
        self.login_sso_response.lock().unwrap().clone().into()
    }

    async fn recovery_key_verification(
        &self,
        ctx: RequestContext,
        _request: RecoveryKeyVerificationRequest,
    ) -> crate::Result<VerificationEndEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.recovery_key_verification_call_count.lock().unwrap() += 1;
        self.recovery_key_verification_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn cross_signing_start(
        &self,
        ctx: RequestContext,
        _request: CrossSigningStartRequest,
    ) -> crate::Result<CrossSigningStartResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.cross_signing_start_call_count.lock().unwrap() += 1;
        self.cross_signing_start_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn cross_signing_select_method(
        &self,
        ctx: RequestContext,
        _request: CrossSigningMethodSelectedRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.cross_signing_select_method_call_count.lock().unwrap() += 1;
        self.cross_signing_select_method_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn cross_signing_confirm(
        &self,
        ctx: RequestContext,
        _request: CrossSigningConfirmRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.cross_signing_confirm_call_count.lock().unwrap() += 1;
        self.cross_signing_confirm_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn abort_verification(
        &self,
        ctx: RequestContext,
        _request: VerificationAbortRequest,
    ) -> crate::Result<VerificationEndEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.abort_verification_call_count.lock().unwrap() += 1;
        self.abort_verification_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn get_global_settings(
        &self,
        ctx: RequestContext,
        _request: GlobalSettingsRequest,
    ) -> crate::Result<GlobalSettings> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_global_settings_call_count.lock().unwrap() += 1;
        self.get_global_settings_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn get_user(&self, ctx: RequestContext, _request: UserRequest) -> crate::Result<User> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_user_call_count.lock().unwrap() += 1;
        self.get_user_response.lock().unwrap().clone().into()
    }

    async fn search_users(
        &self,
        ctx: RequestContext,
        _request: UserSearchRequest,
    ) -> crate::Result<UserSearchResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.search_users_call_count.lock().unwrap() += 1;
        self.search_users_response.lock().unwrap().clone().into()
    }

    async fn set_status(&self, ctx: RequestContext, _request: UserStatus) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.set_status_call_count.lock().unwrap() += 1;
        self.set_status_response.lock().unwrap().clone().into()
    }

    async fn get_public_rooms(
        &self,
        ctx: RequestContext,
        _request: PublicRoomListRequest,
    ) -> crate::Result<PublicRoomListResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_public_rooms_call_count.lock().unwrap() += 1;
        self.get_public_rooms_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn invite(
        &self,
        ctx: RequestContext,
        _request: InvitationRequest,
    ) -> crate::Result<RoomChangeEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.invite_call_count.lock().unwrap() += 1;
        self.invite_response.lock().unwrap().clone().into()
    }

    async fn invitation_reply(
        &self,
        ctx: RequestContext,
        _request: InvitedReply,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.invitation_reply_call_count.lock().unwrap() += 1;
        self.invitation_reply_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn get_rooms(
        &self,
        ctx: RequestContext,
        _request: RoomListRequest,
    ) -> crate::Result<RoomListResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_rooms_call_count.lock().unwrap() += 1;
        self.get_rooms_response.lock().unwrap().clone().into()
    }

    async fn create_group_room(
        &self,
        ctx: RequestContext,
        _request: RoomCreateGroupRequest,
    ) -> crate::Result<Room> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.create_group_room_call_count.lock().unwrap() += 1;
        self.create_group_room_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn create_direct_room(
        &self,
        ctx: RequestContext,
        _request: RoomCreateDirectRequest,
    ) -> crate::Result<Room> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.create_direct_room_call_count.lock().unwrap() += 1;
        self.create_direct_room_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn change_room(
        &self,
        ctx: RequestContext,
        _request: RoomChangeRequest,
    ) -> crate::Result<RoomChangeEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.change_room_call_count.lock().unwrap() += 1;
        self.change_room_response.lock().unwrap().clone().into()
    }

    async fn leave_room(
        &self,
        ctx: RequestContext,
        _request: RoomLeaveRequest,
    ) -> crate::Result<RoomLeftEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.leave_room_call_count.lock().unwrap() += 1;
        self.leave_room_response.lock().unwrap().clone().into()
    }

    async fn join_room(
        &self,
        ctx: RequestContext,
        _request: RoomJoinRequest,
    ) -> crate::Result<Room> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.join_room_call_count.lock().unwrap() += 1;
        self.join_room_response.lock().unwrap().clone().into()
    }

    async fn knock_room(
        &self,
        ctx: RequestContext,
        _request: RoomKnockRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.knock_room_call_count.lock().unwrap() += 1;
        self.knock_room_response.lock().unwrap().clone().into()
    }

    async fn get_room_messages(
        &self,
        ctx: RequestContext,
        _request: RoomMessagesRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx.clone());
        *self.get_room_messages_call_count.lock().unwrap() += 1;
        self.get_room_messages_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn mark_as_read(
        &self,
        ctx: RequestContext,
        _request: RoomMarkAsReadRequest,
    ) -> crate::Result<RoomChangeEvent> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.mark_as_read_call_count.lock().unwrap() += 1;
        self.mark_as_read_response.lock().unwrap().clone().into()
    }

    async fn activate_typing_notice(
        &self,
        ctx: RequestContext,
        _request: RoomTypingRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.activate_typing_notice_call_count.lock().unwrap() += 1;
        self.activate_typing_notice_response
            .lock()
            .unwrap()
            .clone()
            .into()
    }

    async fn send_message(
        &self,
        ctx: RequestContext,
        _request: MessageSendRequest,
    ) -> crate::Result<MessageSendResponse> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.send_message_call_count.lock().unwrap() += 1;
        self.send_message_response.lock().unwrap().clone().into()
    }

    async fn remove_message(
        &self,
        ctx: RequestContext,
        _request: MessageRemoveRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.remove_message_call_count.lock().unwrap() += 1;
        self.remove_message_response.lock().unwrap().clone().into()
    }

    async fn change_message(
        &self,
        ctx: RequestContext,
        _request: MessageChangeRequest,
    ) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.change_message_call_count.lock().unwrap() += 1;
        self.change_message_response.lock().unwrap().clone().into()
    }

    async fn create_reaction(&self, ctx: RequestContext, _request: Reaction) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.create_reaction_call_count.lock().unwrap() += 1;
        self.create_reaction_response.lock().unwrap().clone().into()
    }

    async fn remove_reaction(&self, ctx: RequestContext, _request: Reaction) -> crate::Result<()> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.remove_reaction_call_count.lock().unwrap() += 1;
        self.remove_reaction_response.lock().unwrap().clone().into()
    }

    async fn get_message(
        &self,
        ctx: RequestContext,
        _request: MessageRequest,
    ) -> crate::Result<Message> {
        *self.received_ctx.lock().unwrap() = Some(ctx);
        *self.get_message_call_count.lock().unwrap() += 1;
        self.get_message_response.lock().unwrap().clone().into()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
