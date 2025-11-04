use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::message::{MessageType, OriginalSyncRoomMessageEvent};
use matrix_sdk::{Room, RoomState};

use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;

pub async fn event_handler(event: OriginalSyncRoomMessageEvent, room: Room, ctx: Ctx<ClientContext>) {
    log::info!("Received event: {event:?}");

    if room.state() != RoomState::Joined {
        return;
    }

    let MessageType::Text(text_content) = event.content.msgtype else {
        return;
    };

    // TODO: Support related_messag_id
    // TODO: Support is_pinned
    // TODO: Support other mime types

    ctx.clone().send_event(ResponseContent::MessageReceivedEvent(MessageReceivedEvent { 
        message_content: Some(Message {
            message_id: Some(event.event_id.to_string()),
            room_id: room.room_id().to_string(),
            sender_id: event.sender.to_string(),
            timestamp: event.origin_server_ts.as_secs().into(),
            mime_type: "text/plain".to_owned(),
            content: text_content.body,
            related_message_id: None,
            is_pinned: false,
        })
    }));
}
