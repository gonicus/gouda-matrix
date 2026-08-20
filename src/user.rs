use gouda_core::RequestContext;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;
use matrix_sdk::Client;
use ruma_common::{OwnedMxcUri, OwnedUserId, UserId};

use crate::bridge::IntoChat;
use crate::client::SessionContext;
use crate::error::{Error, Result};
use crate::media::MediaManager;
use crate::proto_cache::ProtoCache;

#[derive(Clone)]
pub struct UserManager {
    context: RequestContext,
    client: Client,
    proto_cache: ProtoCache,
    media_manager: MediaManager,
}

impl UserManager {
    pub fn from_session(context: RequestContext, session: &SessionContext) -> Self {
        Self {
            context,
            client: session.client.clone(),
            proto_cache: session.proto_cache.clone(),
            media_manager: session.media_manager.clone(),
        }
    }

    /// Gets and syncs a user.
    /// If the user is already stored in the cache, they will be retrieved from the cache and
    /// synced in the background. If the user has not yet been cached, this method
    /// retrieves the user from the server and blocks once all data has been retrieved.
    pub async fn get_and_sync_user(&self, user_id: OwnedUserId) -> Result<User> {
        match self.proto_cache.cached_user(&user_id) {
            Some(user) => {
                log::debug!("User has already been cached before");
                self.clone().sync_cached_user(user_id);
                Ok(user)
            }
            None => {
                log::debug!("User has not been cached before");
                let user = self.fetch_user(&user_id).await?;
                Ok(user)
            }
        }
    }

    /// Fetches a user from the matrix server. Blocks until all
    /// data is received.
    async fn fetch_user(&self, user_id: &UserId) -> Result<User> {
        use matrix_sdk::ruma::api::client::profile::DisplayName;

        log::debug!("Fetching user {user_id} from matrix server");

        let profile = self
            .client
            .account()
            .fetch_user_profile_of(user_id)
            .await
            .map_err(|_| Error::UserNotFound)?;

        let display_name = profile.get_static::<DisplayName>().unwrap_or_default();

        let proto = User {
            user_id: user_id.to_string(),
            display_name,
            avatar_path: self
                .media_manager
                .get_user_avatar_path(user_id.to_owned())
                .await,
            status: fetch_status(&self.client, user_id).await.ok(),
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

            let cached = self.proto_cache.cached_user(user_id).unwrap_or_default();

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
                // send to the application will trigger the necessary cache changes.
            } else {
                log::debug!("Cached user is still up to date");
            }
        });
    }
}

/// Retrieves the avatar URL from the user profile using the specified user ID.
/// Returns Ok(None) if the user does not have an avatar set.
pub async fn fetch_avatar_uri(
    client: &Client,
    user_id: OwnedUserId,
) -> Result<Option<OwnedMxcUri>> {
    use ruma_common::profile::{ProfileFieldName, ProfileFieldValue};

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

    let result = result?;

    match result {
        Some(uri) => {
            if let ProfileFieldValue::AvatarUrl(url) = uri {
                log::debug!("Successfully received avatar URL from user profile");
                Ok(Some(url))
            } else {
                log::error!("Received unexpected profile field value");
                Err(Error::internal("received unexpected profile field value"))
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

pub async fn fetch_status(client: &Client, user_id: &UserId) -> Result<UserStatus> {
    Ok(UserStatus {
        state: fetch_user_presence(client, user_id).await.unwrap_or(PresenceState::Unknown).into(),
        status_message: fetch_user_status_message(client, user_id).await.unwrap_or_default(),
    })
}

async fn fetch_user_presence(client: &Client, user_id: &UserId) -> Result<PresenceState> {
    use matrix_sdk::ruma::api::client::presence::get_presence;

    log::debug!("Requesting presence state for user {user_id:?}");

    let request = get_presence::v3::Request::new(user_id.to_owned());

    let response = client.send(request).await.inspect_err(|err| {
        log::error!("Error retrieving presence state for user {user_id}: {err}")
    })?;

    Ok(response.presence.into_chat())
}

async fn fetch_user_status_message(client: &Client, user_id: &UserId) -> Result<Option<String>> {
    use ruma_common::profile::{ProfileFieldName, ProfileFieldValue};

    let result = client
        .account()
        .fetch_profile_field_of(user_id.to_owned(), ProfileFieldName::Status)
        .await?;

    let Some(field) = result else {
        return Ok(None);
    };

    if let ProfileFieldValue::Status(status) = field {
        return Ok(Some(format!("{} {}", status.emoji, status.text)));
    }

    Ok(None)
}
