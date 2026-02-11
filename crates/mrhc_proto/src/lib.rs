pub mod chat {
    include!(concat!(env!("OUT_DIR"), "/de.gonicus.gonnect.rs"));

    pub mod builder {
        use std::collections::HashMap;

        use super::*;

        /// Builder to easily create a `RoomChangeEvent` with desired changes.
        #[derive(Default, PartialEq, Eq)]
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
        }

        impl RoomChangeEventBuilder {
            pub fn new(room_id: impl Into<String>) -> Self {
                Self {
                    room_id: room_id.into(),
                    ..Default::default()
                }
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
        #[derive(Default, PartialEq, Eq)]
        pub struct UserChangeEventBuilder {
            user_id: String,
            presence_state: Option<PresenceState>,
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

            pub fn change_presence_state(mut self, presence_state: PresenceState) -> Self {
                self.presence_state = Some(presence_state);
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
                    presence_state: self.presence_state.map(|f| f.into()),
                    display_name: self.display_name,
                    avatar_path: self.avatar_path,
                }
            }
        }
    }
}
