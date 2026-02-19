use matrix_sdk::ruma::api::client::profile::ProfileFieldValue;
use matrix_sdk::Client;
use mrhc_core::Result;
use mrhc_proto::chat::*;
use ruma_common::presence::PresenceState as MatrixPresenceState;
use ruma_common::{OwnedMxcUri, OwnedUserId, UserId};

use crate::{errors, unwrap_or_log_return_err};

pub fn convert_presence_state(state: MatrixPresenceState) -> PresenceState {
    match state {
        MatrixPresenceState::Offline => PresenceState::Offline,
        MatrixPresenceState::Online => PresenceState::Online,
        MatrixPresenceState::Unavailable => PresenceState::Away,
        _ => PresenceState::Away,
    }
}

pub async fn fetch_display_name(client: &Client, user_id: OwnedUserId) -> Option<String> {
    use matrix_sdk::ruma::api::client::profile::ProfileFieldName;

    let result = client
        .account()
        .fetch_profile_field_of(user_id, ProfileFieldName::DisplayName)
        .await;

    let value = match result {
        Ok(value) => value,
        Err(err) => {
            log::error!("Error retreiving display name: {err}");
            return None;
        }
    }?;

    match value {
        ProfileFieldValue::DisplayName(display_name) => Some(display_name),
        _ => {
            log::error!("Received unexpected profile field value when fetching display name");
            return None;
        }
    }
}

/// Retrieves the avatar URL from the user profile using the specified user ID.
/// Returns Ok(None) if the user does not have an avatar set.
pub async fn fetch_avatar_uri(
    client: &Client,
    user_id: OwnedUserId,
) -> Result<Option<OwnedMxcUri>> {
    use matrix_sdk::ruma::api::client::profile::ProfileFieldName;

    let result = client
        .account()
        .fetch_profile_field_of(user_id, ProfileFieldName::AvatarUrl)
        .await;

    if let Err(err) = &result {
        if is_profile_field_expected_error(err) {
            log::debug!(
                "Retrieving the user's avatar URI resulted in the expected error if the \
                avatar was removed or not found"
            );
            return Ok(None);
        }
    }

    let result = result.map_err(errors::convert_matrix_sdk_error);

    match unwrap_or_log_return_err!(result, "Error retrieving user avatar uri") {
        Some(uri) => {
            if let ProfileFieldValue::AvatarUrl(url) = uri {
                log::debug!("Successfully received avatar URL from user profile");
                Ok(Some(url))
            } else {
                log::error!("Received unexpected profile field value");
                Err(errors::create_unknown(
                    "received unexpected profile field value",
                ))
            }
        }
        None => Ok(None),
    }
}

fn is_profile_field_expected_error(err: &matrix_sdk::Error) -> bool {
    // TODO: This should be made more robust in the future.
    err.to_string()
        .contains("invalid type: null, expected a string")
}

pub async fn fetch_presence_state(client: &Client, user_id: &UserId) -> PresenceState {
    use matrix_sdk::ruma::api::client::presence::get_presence;

    log::debug!("Requesting presence state for user {user_id:?}");

    let request = get_presence::v3::Request::new(user_id.to_owned());

    let response = match client.send(request).await {
        Ok(response) => response,
        Err(err) => {
            log::error!("Error retrieving presence state for user {user_id}: {err}");
            return PresenceState::Unknown;
        }
    };

    convert_presence_state(response.presence)
}
