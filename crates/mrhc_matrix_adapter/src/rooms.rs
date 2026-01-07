use std::collections::HashMap;

use mrhc_core::Result;
use mrhc_proto::chat::*;

use crate::{errors, utils};

pub async fn convert_to_proto(room: matrix_sdk::Room) -> Result<Room> {
    let display_name = room
        .display_name()
        .await
        .unwrap_or(matrix_sdk::RoomDisplayName::Empty);

    let display_name = if matches!(display_name, matrix_sdk::RoomDisplayName::Empty) {
        None
    } else {
        Some(display_name.to_string())
    };

    let unread_count =
        u32::try_from(room.unread_notification_counts().notification_count).unwrap_or(u32::MAX);

    let members = get_room_members(&room).await?;

    let is_direct = if members.len() > 2 {
        false
    } else {
        room.is_direct()
            .await
            .map_err(errors::convert_store_error)?
    };

    Ok(Room {
        room_id: room.room_id().to_string(),
        display_name,
        user_id_list: members,
        space_id: Vec::new(),
        is_public: room.is_public().unwrap_or_default(),
        unread_count,
        is_direct,
    })
}

async fn get_room_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
    let members = room
        .members(matrix_sdk::RoomMemberships::JOIN)
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
