use std::collections::HashMap;

use super::*;

/// Builder to easily create a `RoomChangeEvent` with desired changes.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct RoomChangeEventBuilder {
    room_id: String,
    user_id_list: Option<HashMap<String, i32>>,
    typing_user_id_list: Option<Vec<String>>,
    display_name: Option<String>,
    unread_count: Option<u32>,
    join_rule: Option<RoomJoinRule>,
    is_direct: Option<bool>,
    permissions: Option<RoomPermissions>,
    avatar_path: Option<String>,
    is_favourite: Option<bool>,
    room_settings: Option<RoomSettings>,
}

impl RoomChangeEventBuilder {
    pub fn new(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            ..Default::default()
        }
    }

    pub fn compare_rooms(old: &Room, new: &Room) -> Self {
        let mut obj = Self::new(new.room_id.clone());

        if old.display_name != new.display_name {
            obj = obj.change_display_name(new.display_name.clone().unwrap_or_default());
        }

        if old.user_id_list != new.user_id_list {
            obj = obj.change_user_id_list(new.user_id_list.clone());
        }

        if old.unread_count != new.unread_count {
            obj = obj.change_unread_count(new.unread_count);
        }

        if old.is_direct != new.is_direct {
            obj = obj.change_is_direct(new.is_direct);
        }

        if old.join_rule != new.join_rule {
            obj = obj.change_join_rule(new.join_rule.try_into().unwrap_or_default());
        }

        if old.permissions != new.permissions {
            obj = obj.change_permissions(new.permissions.unwrap_or_default());
        }

        if old.avatar_path != new.avatar_path {
            obj = obj.change_avatar_path(new.avatar_path.clone().unwrap_or_default());
        }

        if old.is_favorite != new.is_favorite {
            obj = obj.change_is_favourite(new.is_favorite);
        }

        if old.room_settings != new.room_settings {
            obj = obj.change_room_settings(new.room_settings.unwrap_or_default());
        }

        obj
    }

    pub fn change_user_id_list(mut self, user_id_list: HashMap<String, i32>) -> Self {
        self.user_id_list = Some(user_id_list);
        self
    }

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

    pub fn change_join_rule(mut self, join_rule: RoomJoinRule) -> Self {
        self.join_rule = Some(join_rule);
        self
    }

    pub fn change_is_direct(mut self, is_direct: bool) -> Self {
        self.is_direct = Some(is_direct);
        self
    }

    pub fn change_permissions(mut self, permissions: RoomPermissions) -> Self {
        self.permissions = Some(permissions);
        self
    }

    pub fn change_avatar_path(mut self, avatar_path: String) -> Self {
        self.avatar_path = Some(avatar_path);
        self
    }

    pub fn change_is_favourite(mut self, is_favourite: bool) -> Self {
        self.is_favourite = Some(is_favourite);
        self
    }

    pub fn change_room_settings(mut self, room_settings: RoomSettings) -> Self {
        self.room_settings = Some(room_settings);
        self
    }

    pub fn to_proto(self) -> RoomChangeEvent {
        let mut event = RoomChangeEvent {
            room_id: self.room_id,
            has_user_id_list_changed: false,
            has_typing_user_id_list_changed: false,
            user_id_list: HashMap::new(),
            typing_user_id_list: Vec::new(),
            display_name: self.display_name,
            unread_count: self.unread_count,
            join_rule: self.join_rule.map(|f| f.into()),
            is_direct: self.is_direct,
            permissions: self.permissions,
            avatar_path: self.avatar_path,
            is_favorite: self.is_favourite,
            room_settings: self.room_settings,
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

/// Builder to easily create a `UserChangeEvent` with desired changes.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct UserChangeEventBuilder {
    user_id: String,
    status: Option<UserStatus>,
    display_name: Option<String>,
    avatar_path: Option<String>,
}

impl UserChangeEventBuilder {
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            ..Default::default()
        }
    }

    pub fn compare_users(old: &User, new: &User) -> Self {
        let mut obj = Self::new(new.user_id.clone());

        if old.display_name != new.display_name {
            obj = obj.change_display_name(new.display_name.clone().unwrap_or_default());
        }

        if old.avatar_path != new.avatar_path {
            obj = obj.change_avatar_path(new.avatar_path.clone().unwrap_or_default());
        }

        if old.status != new.status {
            obj = obj.change_status(new.status.clone().unwrap_or_default());
        }

        obj
    }

    pub fn change_status(mut self, status: UserStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn change_display_name(mut self, display_name: String) -> Self {
        self.display_name = Some(display_name);
        self
    }

    pub fn change_avatar_path(mut self, avatar_path: String) -> Self {
        self.avatar_path = Some(avatar_path);
        self
    }

    pub fn to_proto(self) -> UserChangeEvent {
        UserChangeEvent {
            user_id: self.user_id,
            status: self.status,
            display_name: self.display_name,
            avatar_path: self.avatar_path,
        }
    }
}

/// Builder to easily create a `MessageChangeEvent` with desired changes.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct MessageChangeEventBuilder {
    room_id: String,
    message_id: String,
    is_pinned: Option<bool>,
    is_encrypted: Option<bool>,
    mentioned_user_ids: Option<Vec<String>>,
    content: Option<message_change_event::Content>,
}

impl MessageChangeEventBuilder {
    pub fn new(room_id: impl Into<String>, message_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            message_id: message_id.into(),
            ..Default::default()
        }
    }

    pub fn change_is_pinned(mut self, is_pinned: bool) -> Self {
        self.is_pinned = Some(is_pinned);
        self
    }

    pub fn change_is_encrypted(mut self, is_encrypted: bool) -> Self {
        self.is_encrypted = Some(is_encrypted);
        self
    }

    pub fn change_mentioned_user_ids(mut self, mentioned_user_ids: Vec<String>) -> Self {
        self.mentioned_user_ids = Some(mentioned_user_ids);
        self
    }

    pub fn change_content(mut self, content: message_change_event::Content) -> Self {
        self.content = Some(content);
        self
    }

    pub fn to_proto(self) -> MessageChangeEvent {
        let mut event = MessageChangeEvent {
            room_id: self.room_id,
            message_id: self.message_id,
            is_pinned: self.is_pinned,
            is_encrypted: self.is_encrypted,
            mentioned_user_ids: Vec::new(),
            has_mentioned_user_ids_changed: false,
            content: self.content,
        };

        if let Some(mentioned_user_ids) = self.mentioned_user_ids {
            event.has_mentioned_user_ids_changed = true;
            event.mentioned_user_ids = mentioned_user_ids;
        }

        event
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_room_change_event_builder_compare_rooms() {
        let old = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 1".to_owned()),
            user_id_list: HashMap::from([("user-1".to_string(), PresenceState::Online.into())]),
            space_id: Vec::new(),
            unread_count: 5,
            is_direct: true,
            join_rule: RoomJoinRule::Public.into(),
            permissions: Some(RoomPermissions {
                can_edit: true,
                can_invite: false,
                can_kick: true,
                can_ban: false,
            }),
            latest_message_timestamp: None,
            avatar_path: Some("avatar-1.png".to_string()),
            is_favorite: true,
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::AllMessages.into()),
            }),
        };

        let new = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Room 2".to_owned()),
            user_id_list: HashMap::from([("user-2".to_string(), PresenceState::Online.into())]),
            space_id: Vec::new(),
            unread_count: 6,
            is_direct: false,
            join_rule: RoomJoinRule::Invite.into(),
            permissions: Some(RoomPermissions {
                can_edit: false,
                can_invite: true,
                can_kick: false,
                can_ban: true,
            }),
            latest_message_timestamp: None,
            avatar_path: Some("avatar-2.png".to_string()),
            is_favorite: false,
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::Mute.into()),
            }),
        };

        let expected = RoomChangeEventBuilder {
            room_id: "room-1".to_string(),
            user_id_list: Some(HashMap::from([(
                "user-2".to_string(),
                PresenceState::Online.into(),
            )])),
            typing_user_id_list: None,
            display_name: Some("Room 2".to_owned()),
            unread_count: Some(6),
            join_rule: Some(RoomJoinRule::Invite),
            is_direct: Some(false),
            permissions: Some(RoomPermissions {
                can_edit: false,
                can_invite: true,
                can_kick: false,
                can_ban: true,
            }),
            avatar_path: Some("avatar-2.png".to_string()),
            is_favourite: Some(false),
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::Mute.into()),
            }),
        };

        let result = RoomChangeEventBuilder::compare_rooms(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_room_change_event_builder_compare_rooms_empty_display_name() {
        let old = Room {
            room_id: "room-1".to_owned(),
            display_name: Some("Old display name".to_owned()),
            ..Default::default()
        };

        let new = Room {
            room_id: "room-1".to_owned(),
            display_name: None,
            ..Default::default()
        };

        let expected = RoomChangeEventBuilder {
            room_id: "room-1".to_string(),
            display_name: Some(String::new()),
            ..Default::default()
        };

        let result = RoomChangeEventBuilder::compare_rooms(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_room_change_event_builder_compare_rooms_empty_avatar_path() {
        let old = Room {
            room_id: "room-1".to_owned(),
            avatar_path: Some("avatar".to_owned()),
            ..Default::default()
        };

        let new = Room {
            room_id: "room-1".to_owned(),
            avatar_path: None,
            ..Default::default()
        };

        let expected = RoomChangeEventBuilder {
            room_id: "room-1".to_string(),
            avatar_path: Some(String::new()),
            ..Default::default()
        };

        let result = RoomChangeEventBuilder::compare_rooms(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_room_change_event_builder_to_proto() {
        let builder = RoomChangeEventBuilder {
            room_id: "room-1".to_string(),
            user_id_list: Some(HashMap::from([(
                "user-2".to_string(),
                PresenceState::Online.into(),
            )])),
            typing_user_id_list: Some(vec!["user-1".to_owned()]),
            display_name: Some("Room 2".to_owned()),
            unread_count: Some(6),
            join_rule: Some(RoomJoinRule::Invite),
            is_direct: Some(false),
            permissions: Some(RoomPermissions {
                can_edit: false,
                can_invite: true,
                can_kick: false,
                can_ban: true,
            }),
            avatar_path: Some("avatar-2.png".to_string()),
            is_favourite: Some(false),
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::Mute.into()),
            }),
        };

        let expected = RoomChangeEvent {
            room_id: "room-1".to_owned(),
            has_user_id_list_changed: true,
            user_id_list: HashMap::from([("user-2".to_string(), PresenceState::Online.into())]),
            has_typing_user_id_list_changed: true,
            typing_user_id_list: vec!["user-1".to_owned()],
            display_name: Some("Room 2".to_owned()),
            unread_count: Some(6),
            join_rule: Some(RoomJoinRule::Invite.into()),
            is_direct: Some(false),
            permissions: Some(RoomPermissions {
                can_edit: false,
                can_invite: true,
                can_kick: false,
                can_ban: true,
            }),
            avatar_path: Some("avatar-2.png".to_string()),
            is_favorite: Some(false),
            room_settings: Some(RoomSettings {
                notification_setting: Some(NotificationSetting::Mute.into()),
            }),
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_room_change_event_builder_to_proto_no_changes() {
        let builder = RoomChangeEventBuilder::new("room-id".to_string());
        let expected = RoomChangeEvent {
            room_id: "room-id".to_owned(),
            ..Default::default()
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_user_change_event_builder_compare_users() {
        let old = User {
            user_id: "user-1".to_owned(),
            display_name: Some("User 1".to_owned()),
            avatar_path: Some("avatar-1.png".to_string()),
            status: Some(UserStatus {
                state: UserRoomState::Knocked.into(),
                status_message: Some("hello-world".to_owned()),
            }),
        };

        let new = User {
            user_id: "user-1".to_owned(),
            display_name: Some("User 2".to_owned()),
            avatar_path: Some("avatar-2.png".to_string()),
            status: Some(UserStatus {
                state: UserRoomState::Knocked.into(),
                status_message: Some("hello-world 2".to_owned()),
            }),
        };
        let expected = UserChangeEventBuilder {
            user_id: "user-1".to_string(),
            display_name: Some("User 2".to_owned()),
            avatar_path: Some("avatar-2.png".to_string()),
            status: Some(UserStatus {
                state: UserRoomState::Knocked.into(),
                status_message: Some("hello-world 2".to_owned()),
            }),
        };

        let result = UserChangeEventBuilder::compare_users(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_user_change_event_builder_compare_users_empty_display_name() {
        let old = User {
            user_id: "user-1".to_owned(),
            display_name: Some("Old display name".to_owned()),
            ..Default::default()
        };

        let new = User {
            user_id: "user-1".to_owned(),
            display_name: None,
            ..Default::default()
        };

        let expected = UserChangeEventBuilder {
            user_id: "user-1".to_string(),
            display_name: Some(String::new()),
            ..Default::default()
        };

        let result = UserChangeEventBuilder::compare_users(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_user_change_event_builder_compare_users_empty_avatar_path() {
        let old = User {
            user_id: "user-1".to_owned(),
            avatar_path: Some("avatar".to_owned()),
            ..Default::default()
        };

        let new = User {
            user_id: "user-1".to_owned(),
            avatar_path: None,
            ..Default::default()
        };

        let expected = UserChangeEventBuilder {
            user_id: "user-1".to_string(),
            avatar_path: Some(String::new()),
            ..Default::default()
        };

        let result = UserChangeEventBuilder::compare_users(&old, &new);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_user_change_event_builder_to_proto() {
        let builder = UserChangeEventBuilder {
            user_id: "user-1".to_string(),
            display_name: Some("User 2".to_owned()),
            avatar_path: Some("avatar-2.png".to_string()),
            status: Some(UserStatus {
                state: UserRoomState::Banned.into(),
                status_message: Some("Hello world".to_owned()),
            }),
        };

        let expected = UserChangeEvent {
            user_id: "user-1".to_owned(),
            display_name: Some("User 2".to_owned()),
            avatar_path: Some("avatar-2.png".to_string()),
            status: Some(UserStatus {
                state: UserRoomState::Banned.into(),
                status_message: Some("Hello world".to_owned()),
            }),
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_user_change_event_builder_to_proto_no_changes() {
        let builder = UserChangeEventBuilder::new("user-id".to_string());
        let expected = UserChangeEvent {
            user_id: "user-id".to_owned(),
            ..Default::default()
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_message_change_event_builder_to_proto() {
        let builder = MessageChangeEventBuilder {
            room_id: "room-1".to_owned(),
            message_id: "message-1".to_owned(),
            is_pinned: Some(true),
            is_encrypted: Some(false),
            mentioned_user_ids: Some(vec!["user-1".to_owned(), "user-2".to_owned()]),
            content: Some(message_change_event::Content::Text(MessageContentText {
                content: "new content".to_owned(),
            })),
        };

        let expected = MessageChangeEvent {
            room_id: "room-1".to_owned(),
            message_id: "message-1".to_owned(),
            is_pinned: Some(true),
            is_encrypted: Some(false),
            has_mentioned_user_ids_changed: true,
            mentioned_user_ids: vec!["user-1".to_owned(), "user-2".to_owned()],
            content: Some(message_change_event::Content::Text(MessageContentText {
                content: "new content".to_owned(),
            })),
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_message_change_event_builder_to_proto_no_changes() {
        let builder = MessageChangeEventBuilder::new("room-id".to_owned(), "message-id".to_owned());
        let expected = MessageChangeEvent {
            room_id: "room-id".to_owned(),
            message_id: "message-id".to_owned(),
            ..Default::default()
        };

        let result = builder.to_proto();

        assert_eq!(result, expected);
    }
}
