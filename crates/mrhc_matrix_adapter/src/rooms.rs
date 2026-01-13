use std::collections::HashMap;

use matrix_sdk::ruma::api::client::room::create_room::v3::Request as MatrixCreateRoomRequest;
use matrix_sdk::ruma::room::JoinRule as MatrixJoinRule;
use matrix_sdk::ruma::OwnedUserId;
use mrhc_core::Result;
use mrhc_proto::chat::*;
use ruma_common::directory::PublicRoomsChunk;
use ruma_common::room::JoinRuleKind as MatrixJoinRuleKind;

use crate::{errors, utils};

pub async fn convert_to_proto(room: matrix_sdk::Room) -> Result<Room> {
    let display_name = room
        .display_name()
        .await
        .unwrap_or(matrix_sdk::RoomDisplayName::Empty);

    let display_name = if matches!(display_name, matrix_sdk::RoomDisplayName::Empty) {
        "".to_owned()
    } else {
        display_name.to_string()
    };

    let unread_count =
        u32::try_from(room.unread_notification_counts().notification_count).unwrap_or(u32::MAX);

    let members = get_members(&room).await?;

    let is_direct = if members.len() > 2 {
        false
    } else {
        room.is_direct()
            .await
            .map_err(errors::convert_store_error)?
    };

    let join_rule = convert_join_rule(room.join_rule().unwrap_or(MatrixJoinRule::Invite));

    Ok(Room {
        room_id: room.room_id().to_string(),
        display_name,
        user_id_list: members,
        space_id: Vec::new(),
        is_public: room.is_public().unwrap_or_default(),
        unread_count,
        is_direct,
        join_rule: join_rule.into(),
    })
}

pub async fn get_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
    let members = room
        .members(matrix_sdk::RoomMemberships::all())
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    let mut result: HashMap<String, i32> = HashMap::new();

    for member in members {
        result.insert(
            member.user_id().to_string(),
            utils::membership_state_to_user_room_state(member.membership()).into(),
        );
    }

    Ok(result)
}

pub fn convert_join_rule(join_rule: MatrixJoinRule) -> RoomJoinRule {
    match join_rule {
        MatrixJoinRule::Invite => RoomJoinRule::Invite,
        MatrixJoinRule::Knock => RoomJoinRule::Knock,
        MatrixJoinRule::Public => RoomJoinRule::Public,
        _ => RoomJoinRule::Invite,
    }
}

pub fn convert_join_rule_kind(join_rule_kind: MatrixJoinRuleKind) -> RoomJoinRule {
    match join_rule_kind {
        MatrixJoinRuleKind::Invite => RoomJoinRule::Invite,
        MatrixJoinRuleKind::Knock => RoomJoinRule::Knock,
        MatrixJoinRuleKind::Public => RoomJoinRule::Public,
        _ => RoomJoinRule::Invite,
    }
}

pub fn convert_to_matrix_join_rule(join_rule: RoomJoinRule) -> MatrixJoinRule {
    match join_rule {
        RoomJoinRule::Invite => MatrixJoinRule::Invite,
        RoomJoinRule::Knock => MatrixJoinRule::Knock,
        RoomJoinRule::Public => MatrixJoinRule::Public,
    }
}

/// Creates a new `ruma::api::client::room::create_room::v3::Request` for a private room with
/// enabled encryption and recommended defaults.
pub fn create_room_request(
    display_name: String,
    invitees: Vec<OwnedUserId>,
    join_rule: RoomJoinRule,
) -> MatrixCreateRoomRequest {
    use matrix_sdk::ruma::events::room::encryption::RoomEncryptionEventContent;
    use matrix_sdk::ruma::events::room::history_visibility::{
        HistoryVisibility, RoomHistoryVisibilityEventContent,
    };
    use matrix_sdk::ruma::events::room::join_rules::RoomJoinRulesEventContent;
    use matrix_sdk::ruma::events::InitialStateEvent;

    let join_rule = convert_to_matrix_join_rule(join_rule);
    let visibility = if join_rule == MatrixJoinRule::Public || join_rule == MatrixJoinRule::Knock {
        matrix_sdk::ruma::api::client::room::Visibility::Public
    } else {
        matrix_sdk::ruma::api::client::room::Visibility::Private
    };

    let mut request = MatrixCreateRoomRequest::new();

    if !display_name.is_empty() {
        request.name = Some(display_name);
    }

    request.invite = invitees;
    request.visibility = visibility;
    request.initial_state = vec![
        InitialStateEvent::with_empty_state_key(
            RoomEncryptionEventContent::with_recommended_defaults(),
        )
        .to_raw_any(),
        InitialStateEvent::with_empty_state_key(RoomJoinRulesEventContent::new(join_rule))
            .to_raw_any(),
        InitialStateEvent::with_empty_state_key(RoomHistoryVisibilityEventContent::new(
            HistoryVisibility::Shared,
        ))
        .to_raw_any(),
    ];

    request
}

/// Creates a new `ruma::api::client::room::create_room::v3::Request` for a direct room
/// with another user.
pub fn create_dm_room_request(
    display_name: String,
    invitee: OwnedUserId,
) -> MatrixCreateRoomRequest {
    use matrix_sdk::ruma::api::client::room::create_room;

    let mut request = create_room_request(display_name, vec![invitee], RoomJoinRule::Invite);
    request.preset = Some(create_room::v3::RoomPreset::TrustedPrivateChat);
    request.is_direct = true;

    request
}

/// Updates the visibility of a room.
/// This changes the rooms `JoinRule` as well as the `Visibility`.
pub async fn update_room_visibility(room: &matrix_sdk::Room, is_public: bool) -> Result<()> {
    let join_rule = if is_public {
        matrix_sdk::ruma::room::JoinRule::Public
    } else {
        matrix_sdk::ruma::room::JoinRule::Invite
    };

    room.privacy_settings()
        .update_join_rule(join_rule)
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    let visibility = if is_public {
        matrix_sdk::ruma::api::client::room::Visibility::Public
    } else {
        matrix_sdk::ruma::api::client::room::Visibility::Private
    };

    room.privacy_settings()
        .update_room_visibility(visibility)
        .await
        .map_err(errors::convert_matrix_sdk_error)
}

pub fn convert_public_rooms_chunk(chunk: Vec<PublicRoomsChunk>) -> Vec<PublicRoom> {
    let mut result = Vec::new();

    for room in chunk {
        result.push(PublicRoom {
            display_name: room.name.unwrap_or_default(),
            num_joined_members: room.num_joined_members.try_into().unwrap_or(u32::MAX),
            room_id: room.room_id.to_string(),
            topic: room.topic.unwrap_or_default(),
            join_rule: convert_join_rule_kind(room.join_rule).into(),
        });
    }

    result
}
