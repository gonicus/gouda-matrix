use std::collections::HashMap;
use std::time::Duration;

use gouda_core::{RequestContext, Result};
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::*;
use matrix_sdk::deserialized_responses::TimelineEvent;
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as MatrixCreateRoomRequest;
use matrix_sdk::ruma::api::client::room::Visibility;
use matrix_sdk::ruma::events::{AnySyncStateEvent, StateEventType};
use matrix_sdk::ruma::room::JoinRule as MatrixJoinRule;
use matrix_sdk::ruma::{assign, OwnedUserId};
use matrix_sdk::{Client, Room as MatrixRoom};
use ruma_common::directory::PublicRoomsChunk;
use ruma_common::room::JoinRuleKind as MatrixJoinRuleKind;
use ruma_common::UserId;

use crate::client::InitializedData;
use crate::media::MediaManager;
use crate::proto_cache::ProtoCache;
use crate::utils::ComparisonResult;
use crate::{errors, user, utils};

#[derive(Clone)]
pub struct RoomManager {
    context: RequestContext,
    client: Client,
    proto_cache: ProtoCache,
    media_manager: MediaManager,
}

impl RoomManager {
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

    /// Gets and syncs all available rooms.
    /// If the rooms are already cached, they are retrieved from the cache and
    /// synced in the background. If the rooms are not yet cached, this method
    /// retrieves them from the server and blocks once all rooms have been retrieved.
    pub async fn get_and_sync_rooms(&self) -> Result<Vec<Room>> {
        match self.proto_cache.cached_rooms().await {
            Some(room_list) => {
                log::debug!("Rooms have already been cached before");
                self.clone().sync_cached_rooms();
                Ok(room_list)
            }
            None => {
                log::debug!("Rooms have not been cached before");
                let room_list = self.fetch_all_rooms().await?;
                Ok(room_list)
            }
        }
    }

    /// Fetches all rooms from the matrix server and blocks until
    /// all rooms have been retrieved and converted to proto objects.
    async fn fetch_all_rooms(&self) -> Result<Vec<Room>> {
        log::debug!("Fetching all known rooms from matrix server");

        let Some(user_id) = self.client.user_id() else {
            return Err(errors::create_error(ErrorType::Authorization));
        };

        let mut result = Vec::new();

        for room in self.client.rooms() {
            if room.is_space() {
                continue;
            }

            let proto = convert_to_proto(&self.media_manager, room, user_id).await?;

            result.push(proto);
        }

        Ok(result)
    }

    /// Syncs the rooms in the background.
    fn sync_cached_rooms(self) {
        tokio::spawn(async move {
            log::info!("Syncing cached rooms in the background");

            let fetched = match self.fetch_all_rooms().await {
                Ok(fetched) => fetched,
                Err(err) => {
                    log::error!("Error syncing cached rooms: {err}");
                    return;
                }
            };

            let cached = self.proto_cache.cached_rooms().await.unwrap_or_default();
            let result = utils::compare_lists(&cached, &fetched, |a, b| a.room_id == b.room_id);
            self.process_comparison_result(result).await;

            // We don't have to manually overwrite the cache here, as the events send to
            // the application with `process_comparions_result` will trigger
            // the neccessary cache changes.
        });
    }

    async fn process_comparison_result(&self, result: ComparisonResult<Room>) {
        log::debug!("Processing room sync changes: {result:?}");

        for new in result.new {
            self.context
                .send_event(ResponseContent::RoomCreatedEvent(new))
                .await;
        }

        for (old, new) in result.updated {
            let proto = builder::RoomChangeEventBuilder::compare_rooms(&old, &new).to_proto();
            self.context
                .send_event(ResponseContent::RoomChangeEvent(proto))
                .await;
        }

        for deleted in result.deleted {
            let proto = RoomLeftEvent {
                room_id: deleted.room_id,
                ..Default::default()
            };

            self.context
                .send_event(ResponseContent::RoomLeftEvent(proto))
                .await;
        }
    }
}

pub async fn convert_to_proto(
    media_manager: &MediaManager,
    room: matrix_sdk::Room,
    user_id: &UserId,
) -> Result<Room> {
    let display_name = room
        .display_name()
        .await
        .unwrap_or(matrix_sdk::RoomDisplayName::Empty);

    let display_name = if matches!(display_name, matrix_sdk::RoomDisplayName::Empty) {
        None
    } else {
        Some(display_name.to_string())
    };

    let unread_count =
        u32::try_from(room.unread_notification_counts().notification_count).unwrap_or(u32::MAX);

    let members = get_members(&room).await?;

    let is_direct = if members.len() > 2 {
        false
    } else {
        room.is_direct()
            .await
            .map_err(errors::convert_store_error)?
    };

    let join_rule = convert_join_rule(room.join_rule().unwrap_or(MatrixJoinRule::Invite));

    let latest_message_timestamp: Option<u64> = get_latest_event(&room)
        .await
        .and_then(|e| e.timestamp())
        .map(|t| t.0.into());

    Ok(Room {
        room_id: room.room_id().to_string(),
        display_name,
        user_id_list: members,
        space_id: Vec::new(),
        unread_count,
        is_direct,
        join_rule: join_rule.into(),
        permissions: Some(get_permissions(&room, user_id).await?),
        latest_message_timestamp,
        avatar_path: media_manager.get_room_avatar_path(&room).await,
        is_favorite: room.is_favourite(),
    })
}

pub async fn get_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
    let members = room
        .members(matrix_sdk::RoomMemberships::all())
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    let mut result: HashMap<String, i32> = HashMap::new();

    for member in members {
        result.insert(
            member.user_id().to_string(),
            user::membership_state_to_user_room_state(member.membership()).into(),
        );
    }

    Ok(result)
}

pub async fn get_permissions(room: &matrix_sdk::Room, user_id: &UserId) -> Result<RoomPermissions> {
    use matrix_sdk::ruma::events::StateEventType;

    let room_power_levels = room.power_levels_or_default().await;

    let can_edit = room_power_levels.user_can_send_state(user_id, StateEventType::RoomName)
        && room_power_levels.user_can_send_state(user_id, StateEventType::RoomJoinRules);

    Ok(RoomPermissions {
        can_edit,
        can_invite: room_power_levels.user_can_invite(user_id),
        can_kick: room_power_levels.user_can_kick(user_id),
        can_ban: room_power_levels.user_can_ban(user_id),
    })
}

async fn get_latest_event(room: &matrix_sdk::Room) -> Option<TimelineEvent> {
    let options = assign!(
        MessagesOptions::backward(), {
        limit: 1u8.into(),
        }
    );

    let messages = room.messages(options).await.ok()?;
    messages.chunk.first().cloned()
}

pub fn convert_join_rule(join_rule: MatrixJoinRule) -> RoomJoinRule {
    match join_rule {
        MatrixJoinRule::Invite => RoomJoinRule::Invite,
        MatrixJoinRule::Knock => RoomJoinRule::Knock,
        MatrixJoinRule::Public => RoomJoinRule::Public,
        _ => RoomJoinRule::Invite,
    }
}

pub fn convert_join_rule_kind(join_rule_kind: MatrixJoinRuleKind) -> RoomJoinRule {
    match join_rule_kind {
        MatrixJoinRuleKind::Invite => RoomJoinRule::Invite,
        MatrixJoinRuleKind::Knock => RoomJoinRule::Knock,
        MatrixJoinRuleKind::Public => RoomJoinRule::Public,
        _ => RoomJoinRule::Invite,
    }
}

pub fn convert_to_matrix_join_rule(join_rule: RoomJoinRule) -> MatrixJoinRule {
    match join_rule {
        RoomJoinRule::Invite => MatrixJoinRule::Invite,
        RoomJoinRule::Knock => MatrixJoinRule::Knock,
        RoomJoinRule::Public => MatrixJoinRule::Public,
    }
}

fn matrix_join_rule_to_visibility(join_rule: MatrixJoinRule) -> Visibility {
    if join_rule == MatrixJoinRule::Public || join_rule == MatrixJoinRule::Knock {
        matrix_sdk::ruma::api::client::room::Visibility::Public
    } else {
        matrix_sdk::ruma::api::client::room::Visibility::Private
    }
}

/// Creates a new `ruma::api::client::room::create_room::v3::Request` for a private room with
/// enabled encryption and recommended defaults.
pub fn create_room_request(
    display_name: Option<String>,
    invitees: Vec<OwnedUserId>,
    join_rule: RoomJoinRule,
) -> MatrixCreateRoomRequest {
    use matrix_sdk::ruma::events::room::encryption::RoomEncryptionEventContent;
    use matrix_sdk::ruma::events::room::history_visibility::{
        HistoryVisibility, RoomHistoryVisibilityEventContent,
    };
    use matrix_sdk::ruma::events::room::join_rules::RoomJoinRulesEventContent;
    use matrix_sdk::ruma::events::InitialStateEvent;

    let join_rule = convert_to_matrix_join_rule(join_rule);
    let visibility = matrix_join_rule_to_visibility(join_rule.clone());

    let mut request = MatrixCreateRoomRequest::new();

    request.name = display_name;
    request.invite = invitees;
    request.visibility = visibility;
    request.initial_state = vec![
        InitialStateEvent::with_empty_state_key(
            RoomEncryptionEventContent::with_recommended_defaults(),
        )
        .to_raw_any(),
        InitialStateEvent::with_empty_state_key(RoomJoinRulesEventContent::new(join_rule))
            .to_raw_any(),
        InitialStateEvent::with_empty_state_key(RoomHistoryVisibilityEventContent::new(
            HistoryVisibility::Shared,
        ))
        .to_raw_any(),
    ];

    request
}

/// Creates a new `ruma::api::client::room::create_room::v3::Request` for a direct room
/// with another user.
pub fn create_dm_room_request(
    display_name: Option<String>,
    invitee: OwnedUserId,
) -> MatrixCreateRoomRequest {
    use matrix_sdk::ruma::api::client::room::create_room;

    let mut request = create_room_request(display_name, vec![invitee], RoomJoinRule::Invite);
    request.preset = Some(create_room::v3::RoomPreset::TrustedPrivateChat);
    request.is_direct = true;

    request
}

/// Updates the visibility of a room.
/// This changes the rooms `JoinRule` as well as the `Visibility`.
pub async fn update_room_join_rule(room: &matrix_sdk::Room, join_rule: RoomJoinRule) -> Result<()> {
    let join_rule = convert_to_matrix_join_rule(join_rule);

    room.privacy_settings()
        .update_join_rule(join_rule.clone())
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    let visibility = matrix_join_rule_to_visibility(join_rule);

    room.privacy_settings()
        .update_room_visibility(visibility)
        .await
        .map_err(errors::convert_matrix_sdk_error)
}

/// Converts a chunk of public rooms received from the matrix sdk to a chunk of public rooms
/// usable in the chat interface.
pub fn convert_public_rooms_chunk(chunk: Vec<PublicRoomsChunk>) -> Vec<PublicRoom> {
    let mut result = Vec::new();

    for room in chunk {
        result.push(PublicRoom {
            display_name: room.name,
            num_joined_members: room.num_joined_members.try_into().unwrap_or(u32::MAX),
            room_id: room.room_id.to_string(),
            topic: room.topic,
            join_rule: convert_join_rule_kind(room.join_rule).into(),
        });
    }

    result
}

/// Waits until we have received all specified event types for the given room.
// We may need this function again in the future.
#[allow(dead_code)]
pub async fn wait_for_state_events(
    room: &MatrixRoom,
    events: Vec<StateEventType>,
    timeout: Duration,
) -> Result<()> {
    log::debug!(
        "Waiting to receive {:?} for room {:?}",
        events,
        room.room_id()
    );

    let mut received_tracker: Vec<bool> = vec![false; events.len()];
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StateEventType>();

    let _handle = room.add_event_handler(|ev: AnySyncStateEvent| async move {
        let _ = tx.send(ev.event_type());
    });

    // Check for events we might have received before attaching the event handler
    for (i, event) in events.iter().enumerate() {
        let events = room
            .get_state_events(event.clone())
            .await
            .unwrap_or_default();
        if !events.is_empty() {
            log::debug!("Event {:?} was already received", event);
            received_tracker[i] = true;
        }
    }

    loop {
        let result = tokio::time::timeout(timeout, rx.recv())
            .await
            .map_err(|_| {
                log::error!("Reached timeout waiting for requested events");
                errors::create_error(ErrorType::Timeout)
            })?;

        let Some(event) = result else {
            log::error!("Did not receive every requested event");
            return Err(errors::create_unknown(
                "Did not receive every requested event",
            ));
        };

        log::debug!("Received event: {:?}", event);

        let Some(index) = events.iter().position(|p| *p == event) else {
            continue;
        };

        received_tracker[index] = true;

        if received_tracker.iter().all(|f| *f) {
            log::debug!("Received all requested events");
            break;
        }
    }

    Ok(())
}
