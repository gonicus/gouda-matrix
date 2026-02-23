use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::Room;
use mrhc_core::Result;
use mrhc_proto::chat::*;

use crate::media::MediaManager;
use crate::{errors, media};

pub async fn send_text_message(
    room: Room,
    content: MessageContentText,
) -> Result<MessageSendResponse> {
    let event = RoomMessageEventContent::text_plain(content.content);

    let re = room
        .send(event)
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    Ok(MessageSendResponse {
        message_id: re.event_id.to_string(),
    })
}

pub async fn send_image_message(
    media_manager: &MediaManager,
    room: Room,
    content: MessageContentImage,
) -> Result<MessageSendResponse> {
    let message_id = media_manager
        .send_room_attachment(&room, content.image_path, None)
        .await
        .map_err(media::convert_error)?;

    Ok(MessageSendResponse { message_id })
}

pub async fn send_file_message(
    media_manager: &MediaManager,
    room: Room,
    content: MessageContentFile,
) -> Result<MessageSendResponse> {
    let message_id = media_manager
        .send_room_attachment(&room, content.file_path, content.file_name)
        .await
        .map_err(media::convert_error)?;

    Ok(MessageSendResponse { message_id })
}
