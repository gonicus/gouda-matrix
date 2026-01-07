use std::path::Path;

use mrhc_proto::chat::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub initialize: InitializationRequest,
    pub login_sso: SsoLoginRequest,
    pub room_list: RoomListRequest,
    pub user_search: UserSearchRequest,
    pub send_message: SendMessageRequest,
    pub abort_verification: VerificationAbortRequest,
    pub recovery_key_verification: RecoveryKeyVerificationRequest,
    pub cross_signing_start: CrossSigningStartRequest,
    pub cross_signing_select_method: CrossSigningMethodSelectedRequest,
    pub cross_signing_accept: CrossSigningAcceptRequest,
    pub create_direct_room: CreateDirectRoomRequest,
    pub create_group_room: CreateGroupRoomRequest,
    pub mark_as_read: MarkAsReadRequest,
}

impl Config {
    pub fn read_from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("Error reading config file");
        serde_json::from_str(&contents).expect("Error parsing config file")
    }
}
