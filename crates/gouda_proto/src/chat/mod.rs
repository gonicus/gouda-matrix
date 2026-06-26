pub mod builder;

const REDACTED_VALUE: &str = "<REDACTED>";

include!(concat!(env!("OUT_DIR"), "/de.gonicus.gonnect.rs"));

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = &self.error_string {
            f.write_str(&format!("{} {}", self.r#type, msg))
        } else {
            f.write_str(&self.r#type.to_string())
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Debug for InitializationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializationRequest")
            .field("backend_url", &self.backend_url)
            .field("data_root_path", &self.data_root_path)
            .field("persistent_storage_secret", &REDACTED_VALUE)
            .field("encryption_secret", &REDACTED_VALUE)
            .field("device_display_name", &self.device_display_name)
            .finish()
    }
}

impl std::fmt::Debug for RecoveryKeyVerificationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryKeyVerificationRequest")
            .field("recovery_key", &REDACTED_VALUE)
            .finish()
    }
}

impl std::fmt::Debug for LoginUsernamePasswordRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginUsernamePasswordRequest")
            .field("username", &self.username)
            .field("password", &REDACTED_VALUE)
            .finish()
    }
}

impl std::fmt::Debug for CrossSigningMethodSelectedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossSigningMethodSelectedEvent")
            .field("verification_flow_id", &self.verification_flow_id)
            .field("selected_method", &self.selected_method)
            .field("verification_code", &REDACTED_VALUE)
            .finish()
    }
}

impl std::fmt::Debug for MessageContentText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageContentText")
            .field("content", &REDACTED_VALUE)
            .finish()
    }
}

impl Default for message_send_request::Content {
    fn default() -> Self {
        Self::Text(MessageContentText::default())
    }
}

impl Default for message_change_request::Content {
    fn default() -> Self {
        Self::Text(MessageContentText::default())
    }
}

impl Default for message_change_event::Content {
    fn default() -> Self {
        Self::Text(MessageContentText::default())
    }
}

impl From<message::Content> for message_change_event::Content {
    fn from(value: message::Content) -> Self {
        match value {
            message::Content::Text(t) => Self::Text(t),
            message::Content::File(f) => Self::File(f),
            message::Content::MembershipChange(c) => Self::MembershipChange(c),
        }
    }
}

impl From<message_change_event::Content> for message::Content {
    fn from(value: message_change_event::Content) -> Self {
        match value {
            message_change_event::Content::Text(t) => Self::Text(t),
            message_change_event::Content::File(f) => Self::File(f),
            message_change_event::Content::MembershipChange(c) => Self::MembershipChange(c),
        }
    }
}

impl RoomChangeEvent {
    pub fn update_into_room(self, room: &mut Room) {
        if self.has_user_id_list_changed {
            room.user_id_list = self.user_id_list;
        }

        if let Some(display_name) = self.display_name {
            if display_name.is_empty() {
                room.display_name = None;
            } else {
                room.display_name = Some(display_name);
            }
        }

        if let Some(unread_count) = self.unread_count {
            room.unread_count = unread_count;
        }

        if let Some(join_rule) = self.join_rule {
            room.join_rule = join_rule;
        }

        if let Some(is_direct) = self.is_direct {
            room.is_direct = is_direct;
        }

        if let Some(permissions) = self.permissions {
            room.permissions = Some(permissions);
        }

        if let Some(avatar_path) = self.avatar_path {
            if avatar_path.is_empty() {
                room.avatar_path = None;
            } else {
                room.avatar_path = Some(avatar_path);
            }
        }

        if let Some(is_favorite) = self.is_favorite {
            room.is_favorite = is_favorite;
        }

        if let Some(settings) = self.room_settings {
            room.room_settings = Some(settings);
        }
    }
}

impl UserChangeEvent {
    pub fn update_into_user(self, user: &mut User) {
        if let Some(display_name) = self.display_name {
            if display_name.is_empty() {
                user.display_name = None;
            } else {
                user.display_name = Some(display_name);
            }
        }

        if let Some(avatar_path) = self.avatar_path {
            if avatar_path.is_empty() {
                user.avatar_path = None;
            } else {
                user.avatar_path = Some(avatar_path);
            }
        }

        if let Some(status) = self.status {
            user.status = Some(status);
        }
    }
}

impl MessageChangeEvent {
    pub fn update_into_message(self, message: &mut Message) {
        if let Some(is_pinned) = self.is_pinned {
            message.is_pinned = is_pinned;
        }

        if let Some(is_encrypted) = self.is_encrypted {
            message.is_encrypted = is_encrypted;
        }

        if self.has_mentioned_user_ids_changed {
            message.mentioned_user_ids = self.mentioned_user_ids;
        }

        if let Some(content) = self.content {
            message.content = Some(content.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_room_change_event_update_into_room() {
        let permissions = Some(RoomPermissions {
            can_edit: true,
            can_invite: false,
            can_kick: true,
            can_ban: false,
        });

        let event = RoomChangeEvent {
            room_id: "room-1".to_owned(),
            has_user_id_list_changed: true,
            has_typing_user_id_list_changed: true,
            user_id_list: HashMap::from([("user-1".to_string(), PresenceState::Online.into())]),
            typing_user_id_list: vec!["user-2".to_string()],
            display_name: Some("Room 1".to_string()),
            unread_count: Some(5),
            join_rule: Some(RoomJoinRule::Public.into()),
            is_direct: Some(true),
            permissions,
            avatar_path: Some("avatar.png".to_string()),
            is_favorite: Some(true),
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::AllMessages.into()),
            }),
        };

        let expected = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1".to_owned()),
            user_id_list: HashMap::from([("user-1".to_string(), PresenceState::Online.into())]),
            space_id: Vec::new(),
            unread_count: 5,
            is_direct: true,
            join_rule: RoomJoinRule::Public.into(),
            permissions,
            latest_message_timestamp: None,
            avatar_path: Some("avatar.png".to_string()),
            is_favorite: true,
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::AllMessages.into()),
            }),
        };

        let mut room = Room {
            room_id: "room-1".to_owned(),
            ..Default::default()
        };

        event.update_into_room(&mut room);

        assert_eq!(room, expected);
    }

    #[test]
    fn test_room_change_event_update_into_room_empty_display_name() {
        let event = RoomChangeEvent {
            room_id: "room-1".to_owned(),
            display_name: Some(String::new()),
            ..Default::default()
        };

        let expected = Room {
            room_id: "room-1".to_owned(),
            display_name: None,
            ..Default::default()
        };

        let mut room = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Old Display Name".to_string()),
            ..Default::default()
        };

        event.update_into_room(&mut room);

        assert_eq!(room, expected);
    }

    #[test]
    fn test_room_change_event_update_into_room_empty_avatar_path() {
        let event = RoomChangeEvent {
            room_id: "room-1".to_owned(),
            avatar_path: Some(String::new()),
            ..Default::default()
        };

        let expected = Room {
            room_id: "room-1".to_owned(),
            avatar_path: None,
            ..Default::default()
        };

        let mut room = Room {
            room_id: "room-1".to_owned(),
            avatar_path: Some("old-avatar.png".to_string()),
            ..Default::default()
        };

        event.update_into_room(&mut room);

        assert_eq!(room, expected);
    }

    #[test]
    fn test_user_change_event_update_into_user() {
        let status = Some(UserStatus {
            state: UserRoomState::Invited.into(),
            status_message: Some("hello world!".to_owned()),
        });

        let event = UserChangeEvent {
            user_id: "user-1".to_owned(),
            avatar_path: Some("avatar.png".to_owned()),
            display_name: Some("User 1".to_owned()),
            status: status.clone(),
        };

        let expected = User {
            user_id: "user-1".to_owned(),
            avatar_path: Some("avatar.png".to_owned()),
            display_name: Some("User 1".to_owned()),
            status,
        };

        let mut user = User {
            user_id: "user-1".to_owned(),
            ..Default::default()
        };

        event.update_into_user(&mut user);

        assert_eq!(user, expected);
    }

    #[test]
    fn test_room_change_event_update_into_user_empty_display_name() {
        let event = UserChangeEvent {
            user_id: "user-1".to_owned(),
            display_name: Some(String::new()),
            ..Default::default()
        };

        let expected = User {
            user_id: "user-1".to_owned(),
            display_name: None,
            ..Default::default()
        };

        let mut user = User {
            user_id: "user-1".to_owned(),
            display_name: Some("Old Display Name".to_string()),
            ..Default::default()
        };

        event.update_into_user(&mut user);

        assert_eq!(user, expected);
    }

    #[test]
    fn test_room_change_event_update_into_user_empty_avatar_path() {
        let event = UserChangeEvent {
            user_id: "user-1".to_owned(),
            avatar_path: Some(String::new()),
            ..Default::default()
        };

        let expected = User {
            user_id: "user-1".to_owned(),
            avatar_path: None,
            ..Default::default()
        };

        let mut user = User {
            user_id: "user-1".to_owned(),
            avatar_path: Some("old-avatar".to_string()),
            ..Default::default()
        };

        event.update_into_user(&mut user);

        assert_eq!(user, expected);
    }

    #[test]
    fn test_message_change_event_update_into_message() {
        let event = MessageChangeEvent {
            room_id: "room-1".to_owned(),
            message_id: "message-1".to_owned(),
            is_pinned: Some(true),
            is_encrypted: Some(true),
            has_mentioned_user_ids_changed: true,
            mentioned_user_ids: vec!["user-1".to_owned(), "user-2".to_owned()],
            content: Some(message_change_event::Content::Text(MessageContentText {
                content: "Hello world".to_owned(),
            })),
        };

        let expected = Message {
            room_id: "room-1".to_owned(),
            message_id: "message-1".to_owned(),
            sender_id: "user-5".to_owned(),
            timestamp: 0,
            related_message_id: None,
            is_pinned: true,
            is_encrypted: true,
            reactions: Vec::new(),
            mentioned_user_ids: vec!["user-1".to_owned(), "user-2".to_owned()],
            content: Some(message::Content::Text(MessageContentText {
                content: "Hello world".to_owned(),
            })),
        };

        let mut message = Message {
            room_id: "room-1".to_owned(),
            message_id: "message-1".to_owned(),
            sender_id: "user-5".to_owned(),
            ..Default::default()
        };

        event.update_into_message(&mut message);

        assert_eq!(message, expected);
    }
}
