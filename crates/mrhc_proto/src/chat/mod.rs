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

// impl std::fmt::Debug for MessageContentText {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("MessageContentText")
//             .field("content", &REDACTED_VALUE)
//             .finish()
//     }
// }

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
