use std::time::{SystemTime, UNIX_EPOCH};

use matrix_sdk::deserialized_responses::TimelineEventKind;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::reaction::OriginalSyncReactionEvent;
use matrix_sdk::ruma::events::room::avatar::OriginalSyncRoomAvatarEvent;
use matrix_sdk::ruma::events::room::join_rules::OriginalSyncRoomJoinRulesEvent;
use matrix_sdk::ruma::events::room::member::{MembershipChange, OriginalSyncRoomMemberEvent};
use matrix_sdk::ruma::events::room::message::{
    MessageType, OriginalSyncRoomMessageEvent, Relation,
};
use matrix_sdk::ruma::events::room::name::OriginalSyncRoomNameEvent;
use matrix_sdk::ruma::events::room::redaction::OriginalSyncRoomRedactionEvent;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEvent, AnySyncMessageLikeEvent, AnySyncTimelineEvent, AnyTimelineEvent,
};
use matrix_sdk::{Client, Room, RoomState};
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::room_left_event::RoomLeaveReason;
use mrhc_proto::chat::*;
use ruma_common::serde::Raw;
use ruma_common::MilliSecondsSinceUnixEpoch;

use crate::event_index::EventIndex;
use crate::media::MediaManager;
use crate::{rooms, unwrap_or_log_return};

// After how many seconds does an event count as historical?
const HISTORICAL_EVENT_TIMEOUT: u64 = 5;

fn is_historical_event(origin_server_ts: MilliSecondsSinceUnixEpoch) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    now.saturating_sub(origin_server_ts.as_secs().into()) > HISTORICAL_EVENT_TIMEOUT
}

/// Adds all required event handlers to the client.
pub fn setup_event_handlers(
    client: &Client,
    ctx: ClientContext,
    media_manager: MediaManager,
    event_index: EventIndex,
) {
    client.add_event_handler_context(ctx);
    client.add_event_handler_context(event_index);
    client.add_event_handler_context(media_manager);

    client.add_event_handler(redaction_event_handler);
    client.add_event_handler(room_name_event_handler);
    client.add_event_handler(room_member_event_handler);
    client.add_event_handler(room_join_rules_event_handler);
    client.add_event_handler(room_avatar_event_handler);
    client.add_event_handler(message_event_handler);
    client.add_event_handler(reaction_event_handler);
}

async fn redaction_event_handler(
    redact_event: OriginalSyncRoomRedactionEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
    event_index: Ctx<EventIndex>,
) {
    log::debug!("Received redaction event: {redact_event:?}");

    let Some(redact_id) = redact_event.redacts else {
        log::error!("Event redact id is not set");
        return;
    };

    let event = unwrap_or_log_return!(room.event(&redact_id, None).await);

    match event.kind {
        TimelineEventKind::Decrypted(decrypted) => {
            redact_any_timeline_event(ctx, room, event_index, decrypted.event).await;
        }
        TimelineEventKind::PlainText { event } => {
            redact_any_sync_timeline_event(ctx, room, event_index, event).await;
        }
        _ => {
            log::warn!("Event is not decrypted or plain text");
        }
    };
}

async fn redact_any_timeline_event(
    ctx: Ctx<ClientContext>,
    room: Room,
    event_index: Ctx<EventIndex>,
    redacted_event: Raw<AnyTimelineEvent>,
) {
    let redacted_event = unwrap_or_log_return!(redacted_event.deserialize());

    let AnyTimelineEvent::MessageLike(event) = redacted_event else {
        log::debug!("Ignoring event as it is not message like");
        return;
    };

    match event {
        AnyMessageLikeEvent::Reaction(event) => {
            event_index.redact_reaction(event.event_id().to_string());
        }
        AnyMessageLikeEvent::RoomEncrypted(event) => {
            // TODO: This doesn't necessarily have to be a text message event.
            redact_room_message(ctx, room, event.event_id().to_string());
        }
        _ => {
            log::debug!("Ignoring event as it is not implemented: {event:?}");
        }
    }
}

async fn redact_any_sync_timeline_event(
    ctx: Ctx<ClientContext>,
    room: Room,
    event_index: Ctx<EventIndex>,
    redacted_event: Raw<AnySyncTimelineEvent>,
) {
    let redacted_event = unwrap_or_log_return!(redacted_event.deserialize());

    let AnySyncTimelineEvent::MessageLike(event) = redacted_event else {
        log::debug!("Ignoring event as it is not message like");
        return;
    };

    match event {
        AnySyncMessageLikeEvent::Reaction(event) => {
            event_index.redact_reaction(event.event_id().to_string());
        }
        AnySyncMessageLikeEvent::RoomEncrypted(event) => {
            // TODO: This doesn't necessarily have to be a text message event.
            redact_room_message(ctx, room, event.event_id().to_string());
        }
        _ => {
            log::debug!("Ignoring event as it is not implemented: {event:?}");
        }
    }
}

fn redact_room_message(ctx: Ctx<ClientContext>, room: Room, event_id: String) {
    let proto = MessageRemoveEvent {
        room_id: room.room_id().to_string(),
        message_id: event_id,
    };

    ctx.send_event(ResponseContent::MessageRemoveEvent(proto));
}

async fn room_name_event_handler(
    event: OriginalSyncRoomNameEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::debug!("Received room name event: {event:?}");

    if is_historical_event(event.origin_server_ts) {
        log::debug!("Ignoring event as it is older than {HISTORICAL_EVENT_TIMEOUT} seconds");
        return;
    }

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_display_name(event.content.name.clone())
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn room_member_event_handler(
    event: OriginalSyncRoomMemberEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
    client: Client,
) {
    log::debug!("Received room member event: {event:?}");

    if is_historical_event(event.origin_server_ts) {
        log::debug!("Ignoring event as it is older than {HISTORICAL_EVENT_TIMEOUT} seconds");
        return;
    }

    // Check if our user's membership changed
    if Some(event.state_key.to_string()) == client.user_id().map(|f| f.to_string())
        && process_membership_change(ctx.clone(), &room, event).await
    {
        return;
    }

    let members = unwrap_or_log_return!(rooms::get_members(&room).await);

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_user_id_list(members)
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn process_membership_change(
    ctx: ClientContext,
    room: &Room,
    event: OriginalSyncRoomMemberEvent,
) -> bool {
    let reason = match event.membership_change() {
        MembershipChange::Left => RoomLeaveReason::User,
        MembershipChange::Kicked => RoomLeaveReason::Kicked,
        MembershipChange::Banned | MembershipChange::KickedAndBanned => RoomLeaveReason::Banned,
        _ => return false,
    };

    let proto = RoomLeftEvent {
        room_id: room.room_id().to_string(),
        reason: reason.into(),
        message: event.content.reason.clone(),
    };

    ctx.send_event(ResponseContent::RoomLeftEvent(proto));

    true
}

async fn room_join_rules_event_handler(
    event: OriginalSyncRoomJoinRulesEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::debug!("Received room join rules event: {event:?}");

    if is_historical_event(event.origin_server_ts) {
        log::debug!("Ignoring event as it is older than {HISTORICAL_EVENT_TIMEOUT} seconds");
        return;
    }

    let join_rule = rooms::convert_join_rule(event.content.join_rule.clone());

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_join_rule(join_rule)
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn room_avatar_event_handler(
    event: OriginalSyncRoomAvatarEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
    media_manager: Ctx<MediaManager>,
) {
    log::debug!("Received room avatar event: {event:?}");

    if is_historical_event(event.origin_server_ts) {
        log::debug!("Ignoring event as it is older than {HISTORICAL_EVENT_TIMEOUT} seconds");
        return;
    }

    let avatar_path = media_manager
        .get_room_avatar_path(&room)
        .await
        .unwrap_or_default();

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_avatar_path(avatar_path)
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn message_event_handler(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::debug!("Received message event: {event:?}");

    if room.state() != RoomState::Joined {
        return;
    }

    let MessageType::Text(text_content) = event.content.msgtype else {
        return;
    };

    if let Some(Relation::Replacement(relation)) = event.content.relates_to {
        let content = text_content.body.strip_prefix("* ");

        let proto = MessageChangeEvent {
            message_id: relation.event_id.to_string(),
            content: Some(content.unwrap_or(text_content.body.as_str()).to_string()),
            is_encrypted: None,
            is_pinned: None,
        };

        ctx.send_event(ResponseContent::MessageChangeEvent(proto));

        return;
    }

    let proto = MessageReceivedEvent {
        message_content: Some(Message {
            message_id: Some(event.event_id.to_string()),
            room_id: room.room_id().to_string(),
            sender_id: event.sender.to_string(),
            timestamp: event.origin_server_ts.get().into(),
            mime_type: "text/plain".to_owned(),
            content: text_content.body,
            related_message_id: None,
            is_pinned: false,
            is_encrypted: false,
            reactions: Vec::new(),
        }),
    };

    ctx.send_event(ResponseContent::MessageReceivedEvent(proto));
}

async fn reaction_event_handler(
    event: OriginalSyncReactionEvent,
    room: Room,
    event_index: Ctx<EventIndex>,
) {
    log::debug!("Received reaction event: {event:?}");

    let full_event = event.into_full_event(room.room_id().to_owned());
    event_index.add_reaction(full_event.into());
}
