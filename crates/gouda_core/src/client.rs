use std::any::Any;

use async_trait::async_trait;
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::*;

use crate::{RequestContext, Result};

#[inline]
fn not_implemented_error<T>() -> Result<T> {
    Err(Error {
        r#type: ErrorType::NotImplemented.into(),
        error_string: Some("The requested feature is not implemented by this client".to_owned()),
    })
}

/// Represents a chat client.
#[async_trait]
pub trait Client: Send + Sync {
    /// Handler called for each response before the response is sent to the application.
    #[allow(unused_variables)]
    async fn on_response(&self, response: ResponseContainer) {}

    /// Initializes the client.
    async fn initialize(
        &self,
        ctx: RequestContext,
        request: InitializationRequest,
    ) -> Result<StatusUpdate>;

    /// Retrieves the available login flows.
    async fn get_login_flows(&self, ctx: RequestContext) -> Result<LoginFlowsResponse>;

    /// Retrieves available identity providers when SSO login is used.
    #[allow(unused_variables)]
    async fn get_identity_providers(
        &self,
        ctx: RequestContext,
    ) -> Result<IdentityProvidersResponse> {
        not_implemented_error()
    }

    /// Login with username and password.
    #[allow(unused_variables)]
    async fn login_username_password(
        &self,
        ctx: RequestContext,
        request: LoginUsernamePasswordRequest,
    ) -> Result<StatusUpdate> {
        not_implemented_error()
    }

    /// Login with SSO.
    /// An [`gouda_proto::chat::StatusUpdate`] is expected when the login flow is finished.
    #[allow(unused_variables)]
    async fn login_sso(
        &self,
        ctx: RequestContext,
        request: LoginSsoRequest,
    ) -> Result<LoginSsoResponse> {
        not_implemented_error()
    }

    /// Verifies this client using a recovery key.
    #[allow(unused_variables)]
    async fn recovery_key_verification(
        &self,
        ctx: RequestContext,
        request: RecoveryKeyVerificationRequest,
    ) -> Result<VerificationEndEvent> {
        not_implemented_error()
    }

    /// Verifies this client by starting a cross signing flow with another client.
    #[allow(unused_variables)]
    async fn cross_signing_start(
        &self,
        ctx: RequestContext,
        request: CrossSigningStartRequest,
    ) -> Result<CrossSigningStartResponse> {
        not_implemented_error()
    }

    /// Sets the method used for an ongoing cross signing flow.
    #[allow(unused_variables)]
    async fn cross_signing_select_method(
        &self,
        ctx: RequestContext,
        request: CrossSigningMethodSelectedRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Confirms an ongoing cross singing flow.
    #[allow(unused_variables)]
    async fn cross_signing_confirm(
        &self,
        ctx: RequestContext,
        request: CrossSigningConfirmRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Aborts an ongoing cross signing flow.
    #[allow(unused_variables)]
    async fn abort_verification(
        &self,
        ctx: RequestContext,
        request: VerificationAbortRequest,
    ) -> Result<VerificationEndEvent> {
        not_implemented_error()
    }

    /// Gets the global settings of the user.
    #[allow(unused_variables)]
    async fn get_global_settings(
        &self,
        ctx: RequestContext,
        request: GlobalSettingsRequest,
    ) -> Result<GlobalSettings> {
        not_implemented_error()
    }

    /// Gets a single user.
    #[allow(unused_variables)]
    async fn get_user(&self, ctx: RequestContext, request: UserRequest) -> Result<User> {
        not_implemented_error()
    }

    /// Searches the users by a string value.
    #[allow(unused_variables)]
    async fn search_users(
        &self,
        ctx: RequestContext,
        request: UserSearchRequest,
    ) -> Result<UserSearchResponse> {
        not_implemented_error()
    }

    /// Sets the status of the current user.
    #[allow(unused_variables)]
    async fn set_status(&self, ctx: RequestContext, request: UserStatus) -> Result<()> {
        not_implemented_error()
    }

    /// Gets public rooms.
    #[allow(unused_variables)]
    async fn get_public_rooms(
        &self,
        ctx: RequestContext,
        request: PublicRoomListRequest,
    ) -> Result<PublicRoomListResponse> {
        not_implemented_error()
    }

    /// Invite users to a specific room.
    #[allow(unused_variables)]
    async fn invite(
        &self,
        ctx: RequestContext,
        request: InvitationRequest,
    ) -> Result<RoomChangeEvent> {
        not_implemented_error()
    }

    /// Reply to an invitation for our own user.
    #[allow(unused_variables)]
    async fn invitation_reply(&self, ctx: RequestContext, request: InvitedReply) -> Result<()> {
        not_implemented_error()
    }

    /// Gets all known rooms.
    #[allow(unused_variables)]
    async fn get_rooms(
        &self,
        ctx: RequestContext,
        request: RoomListRequest,
    ) -> Result<RoomListResponse> {
        not_implemented_error()
    }

    /// Creates a new group room.
    #[allow(unused_variables)]
    async fn create_group_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateGroupRequest,
    ) -> Result<Room> {
        not_implemented_error()
    }

    /// Creates a new direct room.
    #[allow(unused_variables)]
    async fn create_direct_room(
        &self,
        ctx: RequestContext,
        request: RoomCreateDirectRequest,
    ) -> Result<Room> {
        not_implemented_error()
    }

    /// Changes a rooms settings.
    #[allow(unused_variables)]
    async fn change_room(
        &self,
        ctx: RequestContext,
        request: RoomChangeRequest,
    ) -> Result<RoomChangeEvent> {
        not_implemented_error()
    }

    /// Leaves a room.
    #[allow(unused_variables)]
    async fn leave_room(
        &self,
        ctx: RequestContext,
        request: RoomLeaveRequest,
    ) -> Result<RoomLeftEvent> {
        not_implemented_error()
    }

    /// Joins a room.
    #[allow(unused_variables)]
    async fn join_room(&self, ctx: RequestContext, request: RoomJoinRequest) -> Result<Room> {
        not_implemented_error()
    }

    /// Knocks on a room.
    #[allow(unused_variables)]
    async fn knock_room(&self, ctx: RequestContext, request: RoomKnockRequest) -> Result<()> {
        not_implemented_error()
    }

    /// Gets the messages of a room.
    #[allow(unused_variables)]
    async fn get_room_messages(
        &self,
        ctx: RequestContext,
        request: RoomMessagesRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Marks a room as read.
    #[allow(unused_variables)]
    async fn mark_as_read(
        &self,
        ctx: RequestContext,
        request: RoomMarkAsReadRequest,
    ) -> Result<RoomChangeEvent> {
        not_implemented_error()
    }

    /// Active typing notice for the current user in the specified room.
    #[allow(unused_variables)]
    async fn activate_typing_notice(
        &self,
        ctx: RequestContext,
        request: RoomTypingRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Send a message to a room.
    #[allow(unused_variables)]
    async fn send_message(
        &self,
        ctx: RequestContext,
        request: MessageSendRequest,
    ) -> Result<MessageSendResponse> {
        not_implemented_error()
    }

    /// Remove a message from a room.
    #[allow(unused_variables)]
    async fn remove_message(
        &self,
        ctx: RequestContext,
        request: MessageRemoveRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Change an already send message.
    #[allow(unused_variables)]
    async fn change_message(
        &self,
        ctx: RequestContext,
        request: MessageChangeRequest,
    ) -> Result<()> {
        not_implemented_error()
    }

    /// Creates a reaction to a message.
    #[allow(unused_variables)]
    async fn create_reaction(&self, ctx: RequestContext, request: Reaction) -> Result<()> {
        not_implemented_error()
    }

    /// Removes a reaction from a message.
    #[allow(unused_variables)]
    async fn remove_reaction(&self, ctx: RequestContext, request: Reaction) -> Result<()> {
        not_implemented_error()
    }

    /// Gets a single message of a room by it's ID.
    #[allow(unused_variables)]
    async fn get_message(&self, ctx: RequestContext, request: MessageRequest) -> Result<Message> {
        not_implemented_error()
    }

    /// This method is currently used only for testing purposes to downcast a `dyn Client`.
    /// Implement this method as follows:
    /// ```ignore
    /// use gouda_core::Client;
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
