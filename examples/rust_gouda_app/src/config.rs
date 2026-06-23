use std::path::Path;

use gouda_proto::chat::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub initialize: InitializationRequest,
    pub login_username_password: LoginUsernamePasswordRequest,
    pub login_sso: LoginSsoRequest,
    pub recovery_key_verification: RecoveryKeyVerificationRequest,
    pub cross_signing_select_method: CrossSigningMethodSelectedRequest,
    pub cross_signing_start: CrossSigningStartRequest,
    pub cross_signing_confirm: CrossSigningConfirmRequest,
    pub abort_verification: VerificationAbortRequest,
    pub get_global_settings: GlobalSettingsRequest,
    pub get_user: UserRequest,
    pub user_search: UserSearchRequest,
    pub set_user_status: UserStatus,
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
    pub activate_typing_notice: RoomTypingRequest,
    pub send_message: MessageSendRequest,
    pub remove_message: MessageRemoveRequest,
    pub change_message: MessageChangeRequest,
    pub create_reaction: Reaction,
    pub remove_reaction: Reaction,
    pub get_message: MessageRequest,
}

impl Config {
    pub fn read_from_file(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        println!("Reading config file at {path:?}");
        let json = std::fs::read_to_string(path).expect("Error reading config file");
        serde_json::from_str(&json).expect("Error parsing config file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        Config::read_from_file("./config.json");
    }
}
