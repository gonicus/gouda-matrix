use std::collections::HashMap;

use mrhc_core::Result;
use mrhc_proto::chat::*;

use crate::errors;
use crate::utils;

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

    Ok(Room {
        room_id: room.room_id().to_string(),
        display_name,
        user_id_list: get_room_members(&room).await?,
    })
}

async fn get_room_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
    let members = room
        .members(matrix_sdk::RoomMemberships::JOIN)
        .await
        .map_err(|err| errors::convert_matrix_sdk_error(err))?;

    let mut result: HashMap<String, i32> = HashMap::new();

    for member in members {
        result.insert(
            member.user_id().to_string(),
            utils::membership_state_to_user_room_state(member.membership()) as i32,
        );
    }

    Ok(result)
}
