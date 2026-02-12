use matrix_sdk::Client;
use mrhc_proto::chat::*;
use ruma_common::presence::PresenceState as MatrixPresenceState;
use ruma_common::UserId;

pub fn convert_presence_state(state: MatrixPresenceState) -> PresenceState {
    match state {
        MatrixPresenceState::Offline => PresenceState::Offline,
        MatrixPresenceState::Online => PresenceState::Online,
        MatrixPresenceState::Unavailable => PresenceState::Away,
        _ => PresenceState::Away,
    }
}

pub async fn request_user_presence(client: &Client, user_id: &UserId) -> PresenceState {
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
