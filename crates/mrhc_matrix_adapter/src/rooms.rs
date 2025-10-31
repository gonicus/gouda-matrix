use matrix_sdk::RoomMemberships;

use mrhc_core::create_error_msg;
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::*;

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
        participant_list: get_room_members(&room).await?,
    })
}

async fn get_room_members(room: &matrix_sdk::Room) -> Result<Vec<Buddy>> {
    // TODO:proper error type
    let members = room
        .members(RoomMemberships::JOIN)
        .await
        .map_err(|err| create_error_msg(ErrorType::Unknown, err))?;

    let mut result = Vec::new();

    for member in members {
        result.push(Buddy {
            buddy_id: member.user_id().to_string(),
            display_name: Some(member.name().to_owned()),
        })
    }

    Ok(result)
}
