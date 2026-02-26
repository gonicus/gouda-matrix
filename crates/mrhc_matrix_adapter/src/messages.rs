use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::ruma::events::room::message::{ReplyMetadata, RoomMessageEventContent};
use matrix_sdk::Room;
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::*;
use ruma_common::{EventId, OwnedEventId, OwnedUserId};

use crate::media::MediaManager;
use crate::{errors, media};

pub async fn send_text_message(
    room: Room,
    related_message_id: Option<String>,
    content: MessageContentText,
) -> Result<MessageSendResponse> {
    let mut event = RoomMessageEventContent::text_plain(content.content);

    if let Some(related_message_id) = related_message_id {
        let metadata = generate_reply_metadata(&room, &related_message_id).await?;

        event = event.make_reply_to(
            metadata.metadata(),
            matrix_sdk::ruma::events::room::message::ForwardThread::Yes,
            matrix_sdk::ruma::events::room::message::AddMentions::Yes,
        );
    }

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
    related_message_id: Option<String>,
    content: MessageContentImage,
) -> Result<MessageSendResponse> {
    let related_message_id = convert_related_message_id(related_message_id)?;

    let message_id = media_manager
        .send_room_attachment(&room, content.image_path, None, related_message_id)
        .await
        .map_err(media::convert_error)?;

    Ok(MessageSendResponse { message_id })
}

pub async fn send_file_message(
    media_manager: &MediaManager,
    room: Room,
    related_message_id: Option<String>,
    content: MessageContentFile,
) -> Result<MessageSendResponse> {
    let related_message_id = convert_related_message_id(related_message_id)?;

    let message_id = media_manager
        .send_room_attachment(
            &room,
            content.file_path,
            content.file_name,
            related_message_id,
        )
        .await
        .map_err(media::convert_error)?;

    Ok(MessageSendResponse { message_id })
}

struct CustomReplyMetadata {
    event_id: OwnedEventId,
    sender_id: OwnedUserId,
}

impl CustomReplyMetadata {
    pub fn new(event_id: OwnedEventId, sender_id: OwnedUserId) -> Self {
        Self {
            event_id,
            sender_id,
        }
    }

    pub fn metadata(&self) -> ReplyMetadata<'_> {
        ReplyMetadata::new(&self.event_id, &self.sender_id, None)
    }
}

fn convert_related_message_id(related_message_id: Option<String>) -> Result<Option<OwnedEventId>> {
    let Some(related_message_id) = related_message_id else {
        return Ok(None);
    };

    let event_id = EventId::parse(related_message_id)
        .map_err(|_| errors::create_error(error::ErrorType::InvalidMessageId))?;

    Ok(Some(event_id))
}

async fn generate_reply_metadata(
    room: &Room,
    related_message_id: &str,
) -> Result<CustomReplyMetadata> {
    let event_id = EventId::parse(related_message_id)
        .map_err(|_| errors::create_error(error::ErrorType::InvalidMessageId))?;

    let event = room
        .event(&event_id, None)
        .await
        .map_err(|_| errors::create_error(ErrorType::MessageNotFound))?;

    let Some(event_id) = event.event_id() else {
        return Err(errors::create_unknown(
            "Related message does not have an event id",
        ));
    };

    let sender_id = sender_id_from_timeline_event(&event)?;

    Ok(CustomReplyMetadata::new(event_id, sender_id))
}

fn sender_id_from_timeline_event(event: &TimelineEvent) -> Result<OwnedUserId> {
    match &event.kind {
        TimelineEventKind::PlainText { event } => {
            let event = event
                .deserialize()
                .map_err(|_| errors::create_unknown("Error deserializing related message"))?;

            Ok(event.sender().to_owned())
        }
        TimelineEventKind::Decrypted(event) => {
            let event = event
                .event
                .deserialize()
                .map_err(|_| errors::create_unknown("Error deserializing related message"))?;

            Ok(event.sender().to_owned())
        }
        _ => Err(errors::create_unknown(
            "Related event is not plaintext or decrypted",
        )),
    }
}
