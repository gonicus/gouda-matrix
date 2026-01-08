use std::collections::HashMap;

use mrhc_core::Result;
use mrhc_proto::chat::*;

use crate::{errors, utils};

/// Builder to easily create a `RoomChangeEvent` with desired changes.
pub struct RoomChangeEventBuilder {
    room_id: String,
    user_id_list: Option<HashMap<String, i32>>,
    typing_user_id_list: Option<Vec<String>>,
    display_name: Option<String>,
    unread_count: Option<u32>,
}

impl RoomChangeEventBuilder {
    pub fn new(room_id: String) -> Self {
        Self {
            room_id,
            user_id_list: None,
            typing_user_id_list: None,
            display_name: None,
            unread_count: None,
        }
    }

    pub fn change_user_id_list(mut self, user_id_list: HashMap<String, i32>) -> Self {
        self.user_id_list = Some(user_id_list);
        self
    }

    #[allow(unused)]
    pub fn change_typing_user_id_list(mut self, typing_user_id_list: Vec<String>) -> Self {
        self.typing_user_id_list = Some(typing_user_id_list);
        self
    }

    pub fn change_display_name(mut self, display_name: String) -> Self {
        self.display_name = Some(display_name);
        self
    }

    pub fn change_unread_count(mut self, unread_count: u32) -> Self {
        self.unread_count = Some(unread_count);
        self
    }

    pub fn into_proto(self) -> RoomChangeEvent {
        let mut event = RoomChangeEvent {
            room_id: self.room_id,
            display_name: self.display_name,
            unread_count: self.unread_count,
            ..Default::default()
        };

        if let Some(user_id_list) = self.user_id_list {
            event.has_user_id_list_changed = true;
            event.user_id_list = user_id_list;
        }

        if let Some(typing_user_id_list) = self.typing_user_id_list {
            event.has_typing_user_id_list_changed = true;
            event.typing_user_id_list = typing_user_id_list;
        }

        event
    }
}

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

pub async fn get_room_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
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
