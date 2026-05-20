use mrhc_proto::chat::*;

use crate::{Client, RequestContext, Result};

/// Mocks the `Client` trait.
pub struct ClientMock {
    /// The context received from the latest request.
    pub received_ctx: Option<RequestContext>,

    /// The response [`Self::initialize`] should return.
    pub initialize_response: Result<StatusUpdate>,
    /// The response [`Self::get_login_flows`] should return.
    pub get_login_flows_response: Result<LoginFlowsResponse>,
    /// The response [`Self::get_identity_providers`] should return.
    pub get_identity_providers_response: Result<IdentityProvidersResponse>,
    /// The response [`Self::login_username_password`] should return.
    pub login_username_password_response: Result<StatusUpdate>,
    /// The response [`Self::login_sso`] should return.
    pub login_sso_response: Result<LoginSsoResponse>,
    /// The response [`Self::recovery_key_verification`] should return.
    pub recovery_key_verification_response: Result<VerificationEndEvent>,
    /// The response [`Self::cross_signing_start`] should return.
    pub cross_signing_start_response: Result<CrossSigningStartResponse>,
    /// The response [`Self::cross_signing_select_method`] should return.
    pub cross_signing_select_method_response: Result<()>,
    /// The response [`Self::cross_signing_confirm`] should return.
    pub cross_signing_confirm_response: Result<()>,
    /// The response [`Self::abort_verification`] should return.
    pub abort_verification_response: Result<VerificationEndEvent>,
    /// The response [`Self::get_user`] should return.
    pub get_user_response: Result<User>,
    /// The response [`Self::search_users`] should return.
    pub search_users_response: Result<UserSearchResponse>,
    /// The response [`Self::set_status`] should return.
    pub set_status_response: Result<()>,
    /// The response [`Self::get_public_rooms`] should return.
    pub get_public_rooms_response: Result<PublicRoomListResponse>,
    /// The response [`Self::invite`] should return.
    pub invite_response: Result<RoomChangeEvent>,
    /// The response [`Self::invitation_reply`] should return.
    pub invitation_reply_response: Result<()>,
    /// The response [`Self::get_rooms`] should return.
    pub get_rooms_response: Result<RoomListResponse>,
    /// The response [`Self::create_group_room`] should return.
    pub create_group_room_response: Result<Room>,
    /// The response [`Self::create_direct_room`] should return.
    pub create_direct_room_response: Result<Room>,
    /// The response [`Self::change_room`] should return.
    pub change_room_response: Result<RoomChangeEvent>,
    /// The response [`Self::leave_room`] should return.
    pub leave_room_response: Result<RoomLeftEvent>,
    /// The response [`Self::join_room`] should return.
    pub join_room_response: Result<Room>,
    /// The response [`Self::knock_room`] should return.
    pub knock_room_response: Result<()>,
    /// The response [`Self::get_room_messages`] should return.
    pub get_room_messages_response: Result<()>,
    /// The response [`Self::mark_as_read`] should return.
    pub mark_as_read_response: Result<RoomChangeEvent>,
    /// The response [`Self::send_message`] should return.
    pub send_message_response: Result<MessageSendResponse>,
    /// The response [`Self::remove_message`] should return.
    pub remove_message_response: Result<()>,
    /// The response [`Self::change_message`] should return.
    pub change_message_response: Result<()>,
    /// The response [`Self::create_reaction`] should return.
    pub create_reaction_response: Result<()>,
    /// The response [`Self::remove_reaction`] should return.
    pub remove_reaction_response: Result<()>,

    initialize_call_count: u32,
    get_login_flows_call_count: u32,
    get_identity_providers_call_count: u32,
    login_username_password_call_count: u32,
    login_sso_call_count: u32,
    recovery_key_verification_call_count: u32,
    cross_signing_start_call_count: u32,
    cross_signing_select_method_call_count: u32,
    cross_signing_confirm_call_count: u32,
    abort_verification_call_count: u32,
    get_user_call_count: u32,
    search_users_call_count: u32,
    set_status_call_count: u32,
    get_public_rooms_call_count: u32,
    invite_call_count: u32,
    invitation_reply_call_count: u32,
    get_rooms_call_count: u32,
    create_group_room_call_count: u32,
    create_direct_room_call_count: u32,
    change_room_call_count: u32,
    leave_room_call_count: u32,
    join_room_call_count: u32,
    knock_room_call_count: u32,
    get_room_messages_call_count: u32,
    mark_as_read_call_count: u32,
    send_message_call_count: u32,
    remove_message_call_count: u32,
    change_message_call_count: u32,
    create_reaction_call_count: u32,
    remove_reaction_call_count: u32,
}

impl Default for ClientMock {
    fn default() -> Self {
        Self {
            initialize_response: Ok(StatusUpdate::default()),
            initialize_call_count: 0,

            get_login_flows_response: Ok(LoginFlowsResponse::default()),
            get_login_flows_call_count: 0,

            get_identity_providers_response: Ok(IdentityProvidersResponse::default()),
            get_identity_providers_call_count: 0,

            login_username_password_response: Ok(StatusUpdate::default()),
            login_username_password_call_count: 0,

            login_sso_response: Ok(LoginSsoResponse::default()),
            login_sso_call_count: 0,

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

            get_user_response: Ok(User::default()),
            get_user_call_count: 0,

            search_users_response: Ok(UserSearchResponse::default()),
            search_users_call_count: 0,

            set_status_response: Ok(()),
            set_status_call_count: 0,

            get_public_rooms_response: Ok(PublicRoomListResponse::default()),
            get_public_rooms_call_count: 0,

            invite_response: Ok(RoomChangeEvent::default()),
            invite_call_count: 0,

            invitation_reply_response: Ok(()),
            invitation_reply_call_count: 0,

            get_rooms_response: Ok(RoomListResponse::default()),
            get_rooms_call_count: 0,

            create_group_room_response: Ok(Room::default()),
            create_group_room_call_count: 0,

            create_direct_room_response: Ok(Room::default()),
            create_direct_room_call_count: 0,

            change_room_response: Ok(RoomChangeEvent::default()),
            change_room_call_count: 0,

            leave_room_response: Ok(RoomLeftEvent::default()),
            leave_room_call_count: 0,

            join_room_response: Ok(Room::default()),
            join_room_call_count: 0,

            knock_room_response: Ok(()),
            knock_room_call_count: 0,

            get_room_messages_response: Ok(()),
            get_room_messages_call_count: 0,

            mark_as_read_response: Ok(RoomChangeEvent::default()),
            mark_as_read_call_count: 0,

            send_message_response: Ok(MessageSendResponse::default()),
            send_message_call_count: 0,

            remove_message_response: Ok(()),
            remove_message_call_count: 0,

            change_message_response: Ok(()),
            change_message_call_count: 0,

            create_reaction_response: Ok(()),
            create_reaction_call_count: 0,

            remove_reaction_response: Ok(()),
            remove_reaction_call_count: 0,

            received_ctx: None,
        }
    }
}

impl ClientMock {
    /// Creates a new `ClientMock` object with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert [`Self::initialize`] was called `n` times.
    pub fn assert_initialize_called_n(&self, n: u32) {
        assert!(self.initialize_call_count == n);
    }

    /// Assert [`Self::get_login_flows`] was called `n` times.
    pub fn assert_get_login_flows_called_n(&self, n: u32) {
        assert!(self.get_login_flows_call_count == n);
    }

    /// Assert [`Self::get_identity_providers`] was called `n` times.
    pub fn assert_get_identity_providers_called_n(&self, n: u32) {
        assert!(self.get_identity_providers_call_count == n);
    }

    /// Assert [`Self::login_username_password`] was called `n` times.
    pub fn assert_login_username_password_called_n(&self, n: u32) {
        assert!(self.login_username_password_call_count == n);
    }

    /// Assert [`Self::login_sso`] was called `n` times.
    pub fn assert_login_sso_called_n(&self, n: u32) {
        assert!(self.login_sso_call_count == n);
    }

    /// Assert [`Self::recovery_key_verification`] was called `n` times.
    pub fn assert_recovery_key_verification_called_n(&self, n: u32) {
        assert!(self.recovery_key_verification_call_count == n);
    }

    /// Assert [`Self::cross_signing_start`] was called `n` times.
    pub fn assert_cross_signing_start_called_n(&self, n: u32) {
        assert!(self.cross_signing_start_call_count == n);
    }

    /// Assert [`Self::cross_signing_select_method`] was called `n` times.
    pub fn assert_cross_signing_select_method_called_n(&self, n: u32) {
        assert!(self.cross_signing_select_method_call_count == n);
    }

    /// Assert [`Self::cross_signing_confirm`] was called `n` times.
    pub fn assert_cross_signing_confirm_called_n(&self, n: u32) {
        assert!(self.cross_signing_confirm_call_count == n);
    }

    /// Assert [`Self::abort_verification`] was called `n` times.
    pub fn assert_abort_verification_called_n(&self, n: u32) {
        assert!(self.abort_verification_call_count == n);
    }

    /// Assert [`Self::get_user`] was called `n` times.
    pub fn assert_get_user_called_n(&self, n: u32) {
        assert!(self.get_user_call_count == n);
    }

    /// Assert [`Self::search_users`] was called `n` times.
    pub fn assert_search_users_called_n(&self, n: u32) {
        assert!(self.search_users_call_count == n);
    }

    /// Assert [`Self::set_status`] was called `n` times.
    pub fn assert_set_status_called_n(&self, n: u32) {
        assert!(self.set_status_call_count == n)
    }

    /// Assert [`Self::get_public_rooms`] was called `n` times.
    pub fn assert_get_public_rooms_called_n(&self, n: u32) {
        assert!(self.get_public_rooms_call_count == n);
    }

    /// Assert [`Self::invite`] was called `n` times.
    pub fn assert_invite_called_n(&self, n: u32) {
        assert!(self.invite_call_count == n);
    }

    /// Assert [`Self::invitation_reply`] was called `n` times.
    pub fn assert_invitation_reply_called_n(&self, n: u32) {
        assert!(self.invitation_reply_call_count == n);
    }

    /// Assert [`Self::get_rooms`] was called `n` times.
    pub fn assert_get_rooms_called_n(&self, n: u32) {
        assert!(self.get_rooms_call_count == n);
    }

    /// Assert [`Self::create_group_room`] was called `n` times.
    pub fn assert_create_group_room_called_n(&self, n: u32) {
        assert!(self.create_group_room_call_count == n);
    }

    /// Assert [`Self::create_direct_room`] was called `n` times.
    pub fn assert_create_direct_room_called_n(&self, n: u32) {
        assert!(self.create_direct_room_call_count == n);
    }

    /// Assert [`Self::change_room`] was called `n` times.
    pub fn assert_change_room_called_n(&self, n: u32) {
        assert!(self.change_room_call_count == n);
    }

    /// Assert [`Self::leave_room`] was called `n` times.
    pub fn assert_leave_room_called_n(&self, n: u32) {
        assert!(self.leave_room_call_count == n);
    }

    /// Assert [`Self::join_room`] was called `n` times.
    pub fn assert_join_room_called_n(&self, n: u32) {
        assert!(self.join_room_call_count == n);
    }

    /// Assert [`Self::knock_room`] was called `n` times.
    pub fn assert_knock_room_called_n(&self, n: u32) {
        assert!(self.knock_room_call_count == n);
    }

    /// Assert [`Self::get_room_messages`] was called `n` times.
    pub fn assert_get_room_messages_called_n(&self, n: u32) {
        assert!(self.get_room_messages_call_count == n);
    }

    /// Assert [`Self::mark_as_read`] was called `n` times.
    pub fn assert_mark_as_read_called_n(&self, n: u32) {
        assert!(self.mark_as_read_call_count == n);
    }

    /// Assert [`Self::send_message`] was called `n` times.
    pub fn assert_send_message_called_n(&self, n: u32) {
        assert!(self.send_message_call_count == n);
    }

    /// Assert [`Self::remove_message`] was called `n` times.
    pub fn assert_remove_message_called_n(&self, n: u32) {
        assert!(self.remove_message_call_count == n);
    }

    /// Assert [`Self::change_message`] was called `n` times.
    pub fn assert_change_message_called_n(&self, n: u32) {
        assert!(self.change_message_call_count == n);
    }

    /// Assert [`Self::create_reaction`] was called `n` times.
    pub fn assert_create_reaction_called_n(&self, n: u32) {
        assert!(self.create_reaction_call_count == n);
    }

    /// Assert [`Self::remove_reaction`] was called `n` times.
    pub fn assert_remove_reaction_called_n(&self, n: u32) {
        assert!(self.remove_reaction_call_count == n);
    }
}

#[async_trait::async_trait]
impl Client for ClientMock {
    async fn initialize(
        &mut self,
        ctx: RequestContext,
        _request: InitializationRequest,
    ) -> Result<StatusUpdate> {
        self.received_ctx = Some(ctx);
        self.initialize_call_count += 1;
        self.initialize_response.clone()
    }

    async fn get_login_flows(&mut self, ctx: RequestContext) -> Result<LoginFlowsResponse> {
        self.received_ctx = Some(ctx);
        self.get_login_flows_call_count += 1;
        self.get_login_flows_response.clone()
    }

    async fn get_identity_providers(
        &mut self,
        ctx: RequestContext,
    ) -> Result<IdentityProvidersResponse> {
        self.received_ctx = Some(ctx);
        self.get_identity_providers_call_count += 1;
        self.get_identity_providers_response.clone()
    }

    async fn login_username_password(
        &mut self,
        ctx: RequestContext,
        _request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        self.received_ctx = Some(ctx);
        self.login_username_password_call_count += 1;
        self.login_username_password_response.clone()
    }

    async fn login_sso(
        &mut self,
        ctx: RequestContext,
        _request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        self.received_ctx = Some(ctx);
        self.login_sso_call_count += 1;
        self.login_sso_response.clone()
    }

    async fn recovery_key_verification(
        &mut self,
        ctx: RequestContext,
        _request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        self.received_ctx = Some(ctx);
        self.recovery_key_verification_call_count += 1;
        self.recovery_key_verification_response.clone()
    }

    async fn cross_signing_start(
        &mut self,
        ctx: RequestContext,
        _request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        self.received_ctx = Some(ctx);
        self.cross_signing_start_call_count += 1;
        self.cross_signing_start_response.clone()
    }

    async fn cross_signing_select_method(
        &mut self,
        ctx: RequestContext,
        _request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.cross_signing_select_method_call_count += 1;
        self.cross_signing_select_method_response.clone()
    }

    async fn cross_signing_confirm(
        &mut self,
        ctx: RequestContext,
        _request: CrossSigningConfirmRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.cross_signing_confirm_call_count += 1;
        self.cross_signing_confirm_response.clone()
    }

    async fn abort_verification(
        &mut self,
        ctx: RequestContext,
        _request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        self.received_ctx = Some(ctx);
        self.abort_verification_call_count += 1;
        self.abort_verification_response.clone()
    }

    async fn get_user(&mut self, ctx: RequestContext, _request: UserRequest) -> Result<User> {
        self.received_ctx = Some(ctx);
        self.get_user_call_count += 1;
        self.get_user_response.clone()
    }

    async fn search_users(
        &mut self,
        ctx: RequestContext,
        _request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        self.received_ctx = Some(ctx);
        self.search_users_call_count += 1;
        self.search_users_response.clone()
    }

    async fn set_status(&mut self, ctx: RequestContext, _request: UserStatus) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.set_status_call_count += 1;
        self.set_status_response.clone()
    }

    async fn get_public_rooms(
        &mut self,
        ctx: RequestContext,
        _request: PublicRoomListRequest,
    ) -> Result<PublicRoomListResponse> {
        self.received_ctx = Some(ctx);
        self.get_public_rooms_call_count += 1;
        self.get_public_rooms_response.clone()
    }

    async fn invite(
        &mut self,
        ctx: RequestContext,
        _request: InvitationRequest,
    ) -> Result<RoomChangeEvent> {
        self.received_ctx = Some(ctx);
        self.invite_call_count += 1;
        self.invite_response.clone()
    }

    async fn invitation_reply(
        &mut self,
        ctx: RequestContext,
        _request: InvitedReply,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.invitation_reply_call_count += 1;
        self.invitation_reply_response.clone()
    }

    async fn get_rooms(
        &mut self,
        ctx: RequestContext,
        _request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        self.received_ctx = Some(ctx);
        self.get_rooms_call_count += 1;
        self.get_rooms_response.clone()
    }

    async fn create_group_room(
        &mut self,
        ctx: RequestContext,
        _request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        self.received_ctx = Some(ctx);
        self.create_group_room_call_count += 1;
        self.create_group_room_response.clone()
    }

    async fn create_direct_room(
        &mut self,
        ctx: RequestContext,
        _request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        self.received_ctx = Some(ctx);
        self.create_direct_room_call_count += 1;
        self.create_direct_room_response.clone()
    }

    async fn change_room(
        &mut self,
        ctx: RequestContext,
        _request: RoomChangeRequest,
    ) -> Result<RoomChangeEvent> {
        self.received_ctx = Some(ctx);
        self.change_room_call_count += 1;
        self.change_room_response.clone()
    }

    async fn leave_room(
        &mut self,
        ctx: RequestContext,
        _request: RoomLeaveRequest,
    ) -> Result<RoomLeftEvent> {
        self.received_ctx = Some(ctx);
        self.leave_room_call_count += 1;
        self.leave_room_response.clone()
    }

    async fn join_room(&mut self, ctx: RequestContext, _request: RoomJoinRequest) -> Result<Room> {
        self.received_ctx = Some(ctx);
        self.join_room_call_count += 1;
        self.join_room_response.clone()
    }

    async fn knock_room(&mut self, ctx: RequestContext, _request: RoomKnockRequest) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.knock_room_call_count += 1;
        self.knock_room_response.clone()
    }

    async fn get_room_messages(
        &mut self,
        ctx: &RequestContext,
        _request: &RoomMessagesRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx.clone());
        self.get_room_messages_call_count += 1;
        self.get_room_messages_response.clone()
    }

    async fn mark_as_read(
        &mut self,
        ctx: RequestContext,
        _request: RoomMarkAsReadRequest,
    ) -> Result<RoomChangeEvent> {
        self.received_ctx = Some(ctx);
        self.mark_as_read_call_count += 1;
        self.mark_as_read_response.clone()
    }

    async fn send_message(
        &mut self,
        ctx: RequestContext,
        _request: MessageSendRequest,
    ) -> Result<MessageSendResponse> {
        self.received_ctx = Some(ctx);
        self.send_message_call_count += 1;
        self.send_message_response.clone()
    }

    async fn remove_message(
        &mut self,
        ctx: RequestContext,
        _request: MessageRemoveRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.remove_message_call_count += 1;
        self.remove_message_response.clone()
    }

    async fn change_message(
        &mut self,
        ctx: RequestContext,
        _request: MessageChangeRequest,
    ) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.change_message_call_count += 1;
        self.change_message_response.clone()
    }

    async fn create_reaction(&mut self, ctx: RequestContext, _request: Reaction) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.create_reaction_call_count += 1;
        self.create_reaction_response.clone()
    }

    async fn remove_reaction(&mut self, ctx: RequestContext, _request: Reaction) -> Result<()> {
        self.received_ctx = Some(ctx);
        self.remove_reaction_call_count += 1;
        self.remove_reaction_response.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
