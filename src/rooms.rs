use std::collections::HashMap;

use futures_util::stream::{self, StreamExt};
use gouda_proto::chat::*;
use matrix_sdk::ruma::api::client::room::create_room::v3::Request as MatrixCreateRoomRequest;
use matrix_sdk::ruma::api::client::room::Visibility;
use matrix_sdk::ruma::room::JoinRule as MatrixJoinRule;
use matrix_sdk::ruma::OwnedUserId;
use matrix_sdk::Client;
use ruma_common::directory::PublicRoomsChunk;
use ruma_common::UserId;

use crate::bridge::{IntoChat, IntoMatrix};
use crate::client::SessionContext;
use crate::error::{Error, Result};
use crate::media::MediaManager;
use crate::notifications;

/// How many rooms to fetch at most at the same time.
const MAX_CONCURRENT_ROOM_FETCHES: usize = 50;

#[derive(Clone)]
pub struct RoomsManager {
    client: Client,
    media_manager: MediaManager,
}

impl RoomsManager {
    pub fn new(client: Client, media_manager: MediaManager) -> Self {
        Self {
            client,
            media_manager,
        }
    }

    pub fn from_session(session: &SessionContext) -> Self {
        Self {
            client: session.client.clone(),
            media_manager: session.media_manager.clone(),
        }
    }

    /// Fetches all rooms from the matrix server and blocks until
    /// all rooms have been retrieved and converted to proto objects.
    pub async fn fetch_all_rooms(&self) -> Result<Vec<Room>> {
        log::debug!("Fetching all known rooms from matrix server");

        let rooms = stream::iter(
            self.client
                .rooms()
                .into_iter()
                .filter(|room| !room.is_space()),
        )
        .map(|room| {
            let manager = self.clone();
            async move { manager.assemble_chat_room(&room).await }
        })
        .buffer_unordered(MAX_CONCURRENT_ROOM_FETCHES)
        .filter_map(|result| async {
            match result {
                Ok(room) => Some(room),
                Err(err) => {
                    log::error!("Error fetching room: {err}");
                    None
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

        Ok(rooms)
    }

    /// Converts the given matrix room to a proto room.
    /// This will download all necessary data, including avatar image, users, etc.
    pub async fn assemble_chat_room(&self, room: &matrix_sdk::Room) -> Result<Room> {
        let display_name = room
            .display_name()
            .await
            .unwrap_or(matrix_sdk::RoomDisplayName::Empty);

        let Some(user_id) = self.client.user_id() else {
            return Err(Error::NotLoggedIn);
        };

        let display_name = if matches!(display_name, matrix_sdk::RoomDisplayName::Empty) {
            None
        } else {
            Some(display_name.to_string())
        };

        let unread_count = u32::try_from(room.num_unread_messages()).unwrap_or(u32::MAX);
        let members = get_room_members(room).await?;
        let join_rule = room
            .join_rule()
            .unwrap_or(MatrixJoinRule::Invite)
            .into_chat();
        let latest_message_timestamp: Option<u64> =
            room.latest_event_timestamp().map(|f| f.0.into());
        let avatar_path = self.media_manager.get_room_avatar_path(room).await;

        let is_direct = if members.len() > 2 {
            false
        } else {
            room.is_direct().await?
        };

        let pinned_messages = room
            .pinned_event_ids()
            .unwrap_or_default()
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>();

        Ok(Room {
            room_id: room.room_id().to_string(),
            display_name,
            user_id_list: members,
            space_id: Vec::new(),
            unread_count,
            is_direct,
            join_rule: join_rule.into(),
            permissions: Some(get_room_permissions(room, user_id).await?),
            latest_message_timestamp,
            avatar_path,
            is_favorite: room.is_favourite(),
            room_settings: Some(get_room_settings(room).await),
            invitation_text: None,
            pinned_messages,
            // Read markers are updated after retrieving room messages
            read_marker: HashMap::new(),
        })
    }
}

pub async fn get_room_members(room: &matrix_sdk::Room) -> Result<HashMap<String, i32>> {
    let members = room.members(matrix_sdk::RoomMemberships::all()).await?;

    let mut result: HashMap<String, i32> = HashMap::new();

    for member in members {
        result.insert(
            member.user_id().to_string(),
            member.membership().clone().into_chat().into(),
        );
    }

    Ok(result)
}

pub async fn get_room_permissions(
    room: &matrix_sdk::Room,
    user_id: &UserId,
) -> Result<RoomPermissions> {
    use matrix_sdk::ruma::events::StateEventType;

    let room_power_levels = room.power_levels_or_default().await;

    let can_edit = room_power_levels.user_can_send_state(user_id, StateEventType::RoomName)
        && room_power_levels.user_can_send_state(user_id, StateEventType::RoomJoinRules);

    let can_pin_messages =
        room_power_levels.user_can_send_state(user_id, StateEventType::RoomPinnedEvents);

    Ok(RoomPermissions {
        can_edit,
        can_invite: room_power_levels.user_can_invite(user_id),
        can_kick: room_power_levels.user_can_kick(user_id),
        can_ban: room_power_levels.user_can_ban(user_id),
        can_mention_room: room_power_levels.user_can_trigger_room_notification(user_id),
        can_pin_messages,
    })
}

async fn get_room_settings(room: &matrix_sdk::Room) -> RoomSettings {
    let notification = room
        .notification_mode()
        .await
        .map(notifications::matrix_notification_mode_to_chat_notification_settings)
        .map(|f| f.into());

    RoomSettings {
        notification_setting: notification,
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

    let join_rule = join_rule.into_matrix();
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
    let join_rule = join_rule.into_matrix();

    room.privacy_settings()
        .update_join_rule(join_rule.clone())
        .await?;

    let visibility = matrix_join_rule_to_visibility(join_rule);

    room.privacy_settings()
        .update_room_visibility(visibility)
        .await
        .map_err(|err| err.into())
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
            join_rule: room.join_rule.into_chat().into(),
        });
    }

    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use js_int::UInt;
    use matrix_sdk::ruma::api::client::room::create_room::v3::RoomPreset;
    use matrix_sdk::ruma::room::JoinRuleKind;
    use ruma_common::directory::{PublicRoomsChunk, PublicRoomsChunkInit};
    use ruma_common::owned_user_id;
    use ruma_common::room::Restricted;

    use super::*;

    fn initial_state_event(
        request: &MatrixCreateRoomRequest,
        event_type: &str,
    ) -> Option<serde_json::Value> {
        request.initial_state.iter().find_map(|event| {
            let value: serde_json::Value = serde_json::from_str(event.json().get()).ok()?;

            if value.get("type")?.as_str()? == event_type {
                Some(value)
            } else {
                None
            }
        })
    }

    fn public_rooms_chunk(
        room_id: &str,
        num_joined_members: UInt,
        join_rule: JoinRuleKind,
    ) -> PublicRoomsChunk {
        let mut chunk: PublicRoomsChunk = PublicRoomsChunkInit {
            num_joined_members,
            room_id: room_id.try_into().unwrap(),
            world_readable: false,
            guest_can_join: false,
        }
        .into();

        chunk.join_rule = join_rule;
        chunk
    }

    #[test]
    fn test_matrix_join_rule_to_visibility() {
        let value = matrix_join_rule_to_visibility(MatrixJoinRule::Public);
        assert_eq!(value, Visibility::Public);

        let value = matrix_join_rule_to_visibility(MatrixJoinRule::Knock);
        assert_eq!(value, Visibility::Public);

        let value = matrix_join_rule_to_visibility(MatrixJoinRule::Invite);
        assert_eq!(value, Visibility::Private);

        let value = matrix_join_rule_to_visibility(MatrixJoinRule::Private);
        assert_eq!(value, Visibility::Private);

        let value =
            matrix_join_rule_to_visibility(MatrixJoinRule::Restricted(Restricted::default()));
        assert_eq!(value, Visibility::Private);

        let value =
            matrix_join_rule_to_visibility(MatrixJoinRule::KnockRestricted(Restricted::default()));
        assert_eq!(value, Visibility::Private);
    }

    #[test]
    fn test_create_room_request_sets_name_and_invitees() {
        let invitees = vec![owned_user_id!("@alice:example.org")];

        let request = create_room_request(
            Some("Test room".to_owned()),
            invitees.clone(),
            RoomJoinRule::Invite,
        );

        assert_eq!(request.name, Some("Test room".to_owned()));
        assert_eq!(request.invite, invitees);
        assert!(!request.is_direct);
        assert_eq!(request.preset, None);
    }

    #[test]
    fn test_create_room_request_without_name_and_invitees() {
        let request = create_room_request(None, Vec::new(), RoomJoinRule::Invite);

        assert_eq!(request.name, None);
        assert_eq!(request.invite, Vec::<OwnedUserId>::new());
    }

    #[test]
    fn test_create_room_request_always_enables_encryption() {
        for join_rule in [
            RoomJoinRule::Invite,
            RoomJoinRule::Knock,
            RoomJoinRule::Public,
        ] {
            let request = create_room_request(None, Vec::new(), join_rule);

            assert!(
                initial_state_event(&request, "m.room.encryption").is_some(),
                "encryption is not enabled for join rule {join_rule:?}",
            );
        }
    }

    #[test]
    fn test_create_room_request_sets_history_visibility_to_shared() {
        let request = create_room_request(None, Vec::new(), RoomJoinRule::Invite);

        let event = initial_state_event(&request, "m.room.history_visibility").unwrap();

        assert_eq!(
            event["content"]["history_visibility"].as_str(),
            Some("shared")
        );
    }

    #[test]
    fn test_create_room_request_join_rule_and_visibility() {
        let cases = [
            (RoomJoinRule::Invite, "invite", Visibility::Private),
            (RoomJoinRule::Knock, "knock", Visibility::Public),
            (RoomJoinRule::Public, "public", Visibility::Public),
        ];

        for (join_rule, expected_rule, expected_visibility) in cases {
            let request = create_room_request(None, Vec::new(), join_rule);

            assert_eq!(
                request.visibility, expected_visibility,
                "unexpected visibility for join rule {join_rule:?}",
            );

            let event = initial_state_event(&request, "m.room.join_rules").unwrap();

            assert_eq!(
                event["content"]["join_rule"].as_str(),
                Some(expected_rule),
                "unexpected join rule content for join rule {join_rule:?}",
            );
        }
    }

    #[test]
    fn test_create_dm_room_request() {
        let invitee = owned_user_id!("@alice:example.org");

        let request = create_dm_room_request(Some("Alice".to_owned()), invitee.clone());

        assert_eq!(request.name, Some("Alice".to_owned()));
        assert_eq!(request.invite, vec![invitee]);
        assert!(request.is_direct);
        assert_eq!(request.preset, Some(RoomPreset::TrustedPrivateChat));

        assert_eq!(request.visibility, Visibility::Private);
        assert!(initial_state_event(&request, "m.room.encryption").is_some());

        let event = initial_state_event(&request, "m.room.join_rules").unwrap();
        assert_eq!(event["content"]["join_rule"].as_str(), Some("invite"));
    }

    #[test]
    fn test_convert_public_rooms_chunk() {
        let mut chunk =
            public_rooms_chunk("!room:example.org", UInt::from(7u32), JoinRuleKind::Public);
        chunk.name = Some("Test room".to_owned());
        chunk.topic = Some("Some topic".to_owned());

        let result = convert_public_rooms_chunk(vec![chunk]);

        assert_eq!(
            result,
            vec![PublicRoom {
                room_id: "!room:example.org".to_owned(),
                display_name: Some("Test room".to_owned()),
                topic: Some("Some topic".to_owned()),
                num_joined_members: 7,
                join_rule: RoomJoinRule::Public.into(),
            }]
        );
    }

    #[test]
    fn test_convert_public_rooms_chunk_without_optional_fields() {
        let chunk = public_rooms_chunk("!room:example.org", UInt::from(0u32), JoinRuleKind::Knock);

        let result = convert_public_rooms_chunk(vec![chunk]);

        assert_eq!(
            result,
            vec![PublicRoom {
                room_id: "!room:example.org".to_owned(),
                display_name: None,
                topic: None,
                num_joined_members: 0,
                join_rule: RoomJoinRule::Knock.into(),
            }]
        );
    }

    #[test]
    fn test_convert_public_rooms_chunk_saturates_member_count() {
        let chunk = public_rooms_chunk("!room:example.org", UInt::MAX, JoinRuleKind::Public);

        let result = convert_public_rooms_chunk(vec![chunk]);

        assert_eq!(result[0].num_joined_members, u32::MAX);
    }

    #[test]
    fn test_convert_public_rooms_chunk_empty() {
        assert_eq!(
            convert_public_rooms_chunk(Vec::new()),
            Vec::<PublicRoom>::new()
        );
    }

    #[test]
    fn test_convert_public_rooms_chunk_preserves_order() {
        let rooms = vec![
            public_rooms_chunk("!first:example.org", UInt::from(1u32), JoinRuleKind::Public),
            public_rooms_chunk(
                "!second:example.org",
                UInt::from(2u32),
                JoinRuleKind::Invite,
            ),
        ];

        let result = convert_public_rooms_chunk(rooms);

        let room_ids: Vec<&str> = result.iter().map(|f| f.room_id.as_str()).collect();
        assert_eq!(room_ids, vec!["!first:example.org", "!second:example.org"]);
    }
}
