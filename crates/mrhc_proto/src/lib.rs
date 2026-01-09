pub mod chat {
    include!(concat!(env!("OUT_DIR"), "/de.gonicus.gonnect.rs"));

    pub mod builder {
        use std::collections::HashMap;

        use super::*;

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
    }
}
