use matrix_sdk::ruma::events::room::member::MembershipState;

use mrhc_proto::chat::UserRoomState;

pub fn membership_state_to_user_room_state(membership_state: &MembershipState) -> UserRoomState {
    match membership_state {
        MembershipState::Ban => UserRoomState::Banned,
        MembershipState::Invite => UserRoomState::Invited,
        MembershipState::Join => UserRoomState::Joined,
        MembershipState::Knock => UserRoomState::Knocked,
        MembershipState::Leave => UserRoomState::Unjoined,
        _ => UserRoomState::Joined, // This is just a wyld guess
    }
}
