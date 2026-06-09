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

impl RoomChangeEvent {
    pub fn update_room(&self, room: &mut Room) {
        if self.has_user_id_list_changed {
            room.user_id_list = self.user_id_list.clone();
        }

        if let Some(display_name) = self.display_name.clone() {
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

        if let Some(avatar_path) = &self.avatar_path {
            if avatar_path.is_empty() {
                room.avatar_path = None;
            } else {
                room.avatar_path = Some(avatar_path.clone());
            }
        }

        if let Some(is_favorite) = self.is_favorite {
            room.is_favorite = is_favorite;
        }
    }
}

impl UserChangeEvent {
    pub fn update_user(&self, user: &mut User) {
        if let Some(display_name) = &self.display_name {
            if display_name.is_empty() {
                user.display_name = None;
            } else {
                user.display_name = Some(display_name.clone());
            }
        }

        if let Some(avatar_path) = &self.avatar_path {
            if avatar_path.is_empty() {
                user.avatar_path = None;
            } else {
                user.avatar_path = Some(avatar_path.clone());
            }
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

impl From<message_change_event::Content> for message::Content {
    fn from(val: message_change_event::Content) -> Self {
        match val {
            message_change_event::Content::Text(text) => message::Content::Text(text),
            message_change_event::Content::Image(image) => message::Content::Image(image),
            message_change_event::Content::File(file) => message::Content::File(file),
            message_change_event::Content::MembershipChange(change) => {
                message::Content::MembershipChange(change)
            }
            message_change_event::Content::AudioFile(audio) => message::Content::AudioFile(audio),
            message_change_event::Content::VideoFile(video) => message::Content::VideoFile(video),
        }
    }
}
