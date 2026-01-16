use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::message::{MessageType, OriginalSyncRoomMessageEvent};
use matrix_sdk::ruma::events::room::name::RoomNameEventContent;
use matrix_sdk::ruma::events::SyncStateEvent;
use matrix_sdk::{Client, Room, RoomState};
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;

/// Adds all required event handlers to the client.
pub fn setup_event_handlers(ctx: ClientContext, client: &Client) {
    client.add_event_handler_context(ctx);
    client.add_event_handler_context(client.clone());
    client.add_event_handler(room_name_event_handler);
    client.add_event_handler(message_event_handler);
}

async fn room_name_event_handler(
    event: SyncStateEvent<RoomNameEventContent>,
    room: Room,
    ctx: Ctx<ClientContext>,
) {
    log::debug!("Received room name event: {event:?}");

    if let Some(original) = event.as_original() {
        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_display_name(original.content.name.clone())
            .into_proto();

        ctx.clone().send_event(ResponseContent::RoomChangeEvent(proto));
    } else {
        log::debug!("Event is redacted");
    }
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

    ctx.clone()
        .send_event(ResponseContent::MessageReceivedEvent(
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
