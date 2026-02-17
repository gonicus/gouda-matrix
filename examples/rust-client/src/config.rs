use std::path::Path;

use mrhc_proto::chat::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub initialize: InitializationRequest,
    pub login_sso: LoginSsoRequest,
    pub recovery_key_verification: RecoveryKeyVerificationRequest,
    pub cross_signing_select_method: CrossSigningMethodSelectedRequest,
    pub cross_signing_start: CrossSigningStartRequest,
    pub cross_signing_confirm: CrossSigningConfirmRequest,
    pub abort_verification: VerificationAbortRequest,
    pub user_search: UserSearchRequest,
    pub public_room_list: PublicRoomListRequest,
    pub invite: InvitationRequest,
    pub invitation_reply: InvitedReply,
    pub room_list: RoomListRequest,
    pub create_group_room: RoomCreateGroupRequest,
    pub create_direct_room: RoomCreateDirectRequest,
    pub change_room: RoomChangeRequest,
    pub leave_room: RoomLeaveRequest,
    pub join_room: RoomJoinRequest,
    pub knock_room: RoomKnockRequest,
    pub room_messages: RoomMessagesRequest,
    pub mark_as_read: RoomMarkAsReadRequest,
    pub send_message: MessageSendRequest,
    pub remove_message: MessageRemoveRequest,
    pub change_message: MessageChangeRequest,
    pub create_reaction: Reaction,
}

impl Config {
    pub fn read_from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("Error reading config file");
        serde_json::from_str(&contents).expect("Error parsing config file")
    }
}
