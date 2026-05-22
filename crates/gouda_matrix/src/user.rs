use gouda_core::{RequestContext, Result};
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::message_content_membership_change::MembershipChange;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;
use matrix_sdk::ruma::api::client::profile::{DisplayName, ProfileFieldValue};
use matrix_sdk::ruma::events::room::member::{
    MembershipChange as MatrixMembershipChange, MembershipState,
};
use matrix_sdk::Client;
use ruma_common::presence::PresenceState as MatrixPresenceState;
use ruma_common::{OwnedMxcUri, OwnedUserId, UserId};

use crate::client::InitializedData;
use crate::media::MediaManager;
use crate::proto_cache::ProtoCache;
use crate::{errors, unwrap_or_log_return_err};

#[derive(Clone)]
pub struct UserManager {
    context: RequestContext,
    client: Client,
    proto_cache: ProtoCache,
    media_manager: MediaManager,
}

impl UserManager {
    pub fn from_initialized_data(
        context: RequestContext,
        initialized_data: &InitializedData,
    ) -> Self {
        Self {
            context,
            client: initialized_data.client.clone(),
            proto_cache: initialized_data.proto_cache.clone(),
            media_manager: initialized_data.media_manager.clone(),
        }
    }

    /// Gets and syncs a user.
    /// If the user is already stored in the cache, they will be retrieved from the cache and
    /// synced in the background. If the user has not yet been cached, thismethod
    /// retrieves the user from the server and blocks once all data has been retrieved.
    pub async fn get_and_sync_user(&self, user_id: OwnedUserId) -> Result<User> {
        match self.proto_cache.cached_user(&user_id).await {
            Some(user) => {
                log::debug!("User has already been cached before");
                self.clone().sync_cached_user(user_id);
                Ok(user)
            }
            None => {
                log::debug!("User has not been cached before");
                let user = self.fetch_user(&user_id).await?;
                self.proto_cache.cache_user(user.clone()).await;
                Ok(user)
            }
        }
    }

    /// Fetches a user from the matrix server. Blocks until all
    /// data is received.
    async fn fetch_user(&self, user_id: &UserId) -> Result<User> {
        log::debug!("Fetching user {user_id} from matrix server");

        let profile = self
            .client
            .account()
            .fetch_user_profile_of(user_id)
            .await
            .map_err(|_| errors::create_error(ErrorType::UserNotFound))?;

        let display_name = profile.get_static::<DisplayName>().unwrap_or_default();

        let proto = User {
            user_id: user_id.to_string(),
            display_name,
            presence_state: Some(fetch_presence_state(&self.client, user_id).await.into()),
            avatar_path: self
                .media_manager
                .get_user_avatar_path(user_id.to_owned())
                .await,
        };

        Ok(proto)
    }

    /// Sync the given user in the background.
    fn sync_cached_user(self, user_id: OwnedUserId) {
        tokio::spawn(async move {
            log::info!("Syncing cached user {user_id} in the background");

            let fetched = match self.fetch_user(&user_id).await {
                Ok(fetched) => fetched,
                Err(err) => {
                    log::error!("Error syncing cached user: {err}");
                    return;
                }
            };

            let cached = self
                .proto_cache
                .cached_user(user_id)
                .await
                .unwrap_or_default();

            if cached != fetched {
                log::debug!(
                    "Cached user is no longer up to date. Old: {cached:?} new: {fetched:?}"
                );

                let proto =
                    builder::UserChangeEventBuilder::compare_users(&cached, &fetched).to_proto();

                self.context
                    .send_event(ResponseContent::UserChangeEvent(proto))
                    .await;

                // We don't have to manually overwrite the cache here, as the user change event
                // send to the application will trigger the neccessary cache changes.
            } else {
                log::debug!("Cached user is still up to date");
            }
        });
    }
}

/// Converts a membership state to a room state.
pub fn membership_state_to_user_room_state(membership_state: &MembershipState) -> UserRoomState {
    match membership_state {
        MembershipState::Ban => UserRoomState::Banned,
        MembershipState::Invite => UserRoomState::Invited,
        MembershipState::Join => UserRoomState::Joined,
        MembershipState::Knock => UserRoomState::Knocked,
        MembershipState::Leave => UserRoomState::Unjoined,
        _ => UserRoomState::Joined, // This is just a wyld guess
    }
}

/// project a matrix_sdk::ruma::events::room::member::MembershipChange variant on
/// the less granular representation
/// gouda_proto::chat::message_content_membership_change::MembershipChange
pub fn convert_membership_change(change: &MatrixMembershipChange) -> Option<MembershipChange> {
    match change {
        MatrixMembershipChange::Joined
        | MatrixMembershipChange::InvitationAccepted
        | MatrixMembershipChange::KnockAccepted => Some(MembershipChange::Joined),
        MatrixMembershipChange::Left => Some(MembershipChange::Left),
        MatrixMembershipChange::Banned | MatrixMembershipChange::KickedAndBanned => {
            Some(MembershipChange::Banned)
        }
        MatrixMembershipChange::Kicked => Some(MembershipChange::Kicked),
        MatrixMembershipChange::Invited => Some(MembershipChange::Invited),
        MatrixMembershipChange::Knocked => Some(MembershipChange::Knocked),
        _ => None,
    }
}

pub fn matrix_presence_state_to_chat(state: MatrixPresenceState) -> PresenceState {
    match state {
        MatrixPresenceState::Offline => PresenceState::Offline,
        MatrixPresenceState::Online => PresenceState::Online,
        MatrixPresenceState::Unavailable => PresenceState::Away,
        _ => PresenceState::Away,
    }
}

pub fn chat_presence_state_to_matrix(state: PresenceState) -> Option<MatrixPresenceState> {
    match state {
        PresenceState::Away => Some(MatrixPresenceState::Unavailable),
        PresenceState::Offline => Some(MatrixPresenceState::Offline),
        PresenceState::Online => Some(MatrixPresenceState::Online),
        PresenceState::Unknown => None,
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

    matrix_presence_state_to_chat(response.presence)
}
