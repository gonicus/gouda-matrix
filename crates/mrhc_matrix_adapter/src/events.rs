use std::time::{SystemTime, UNIX_EPOCH};

use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::join_rules::RoomJoinRulesEventContent;
use matrix_sdk::ruma::events::room::member::{MembershipState, RoomMemberEventContent};
use matrix_sdk::ruma::events::room::message::{MessageType, OriginalSyncRoomMessageEvent};
use matrix_sdk::ruma::events::room::name::RoomNameEventContent;
use matrix_sdk::ruma::events::SyncStateEvent;
use matrix_sdk::{Client, Room, RoomState};
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::room_left_event::RoomLeaveReason;
use mrhc_proto::chat::*;

use crate::rooms;

macro_rules! unwrap_or_log_return {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("Error: {e:?}");
                return;
            }
        }
    };
}

/// Adds all required event handlers to the client.
pub fn setup_event_handlers(ctx: ClientContext, client: &Client) {
    client.add_event_handler_context(ctx);
    client.add_event_handler_context(client.clone());
    client.add_event_handler(room_name_event_handler);
    client.add_event_handler(room_member_event_handler);
    client.add_event_handler(room_join_rules_event_handler);
    client.add_event_handler(message_event_handler);
}

async fn room_name_event_handler(
    event: SyncStateEvent<RoomNameEventContent>,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::debug!("Received room name event: {event:?}");

    let Some(original) = event.as_original() else {
        log::debug!("Event is redacted");
        return;
    };

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_display_name(original.content.name.clone())
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn room_member_event_handler(
    event: SyncStateEvent<RoomMemberEventContent>,
    room: Room,
    ctx: Ctx<ClientContext>,
    client: Client,
) {
    const EVENT_TIMEOUT: u64 = 5;

    log::debug!("Received room member event: {event:?}");

    let Some(original) = event.as_original() else {
        log::debug!("Event is redacted");
        return;
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if now.saturating_sub(original.origin_server_ts.as_secs().into()) > EVENT_TIMEOUT {
        log::debug!("Ignoring event as it is older than {EVENT_TIMEOUT} seconds");
        return;
    }

    if Some(event.state_key().to_string()) == client.user_id().map(|f| f.to_string()) {
        match event.membership() {
            MembershipState::Leave => {
                let reason = if Some(event.sender()) != client.user_id() {
                    RoomLeaveReason::Kicked
                } else {
                    RoomLeaveReason::User
                };

                ctx.send_event(ResponseContent::RoomLeftEvent(RoomLeftEvent {
                    room_id: room.room_id().to_string(),
                    reason: reason.into(),
                    message: original.content.reason.clone(),
                }));
                return;
            }
            MembershipState::Ban => {
                ctx.send_event(ResponseContent::RoomLeftEvent(RoomLeftEvent {
                    room_id: room.room_id().to_string(),
                    reason: RoomLeaveReason::Banned.into(),
                    message: original.content.reason.clone(),
                }));
                return;
            }
            _ => (),
        }
    }

    let members = unwrap_or_log_return!(rooms::get_members(&room).await);

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_user_id_list(members)
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn room_join_rules_event_handler(
    event: SyncStateEvent<RoomJoinRulesEventContent>,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::info!("Received room join rules event: {event:?}");

    let Some(original) = event.as_original() else {
        log::debug!("Event is redacted");
        return;
    };

    let join_rule = rooms::convert_join_rule(original.content.join_rule.clone());

    let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
        .change_join_rule(join_rule)
        .into_proto();

    ctx.send_event(ResponseContent::RoomChangeEvent(proto));
}

async fn message_event_handler(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::info!("Received event: {event:?}");

    if room.state() != RoomState::Joined {
        return;
    }

    let MessageType::Text(text_content) = event.content.msgtype else {
        return;
    };

    // TODO: Support related_message_id
    // TODO: Support is_pinned
    // TODO: Support other mime types

    ctx.send_event(ResponseContent::MessageReceivedEvent(
        MessageReceivedEvent {
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
            }),
        },
    ));
}
