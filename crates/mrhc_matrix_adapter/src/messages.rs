use std::sync::{Arc, RwLock};

use futures_util::StreamExt;
use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::api::client::filter::RoomEventFilter;
use matrix_sdk::ruma::events::room::message::{ReplyMetadata, RoomMessageEventContent};
use matrix_sdk::{Client, Room};
use mrhc_core::Result;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::*;
use ruma_common::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId};
use tokio::sync::mpsc;

use crate::chat_cache::{cache_room_messages_response, CachedData};
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

pub async fn fetch_messages_from_sdk(
    cached_data: Arc<RwLock<CachedData>>,
    order: MessagesOrder,
    room: &Room,
    next: Option<String>,
    limit: u32,
) -> Result<(usize, Option<OwnedEventId>)> {
    let mut options: MessagesOptions;
    let chronological: bool;

    match order {
        MessagesOrder::Forward => {
            options = MessagesOptions::forward();
            chronological = true;
        }
        MessagesOrder::Backward => {
            options = MessagesOptions::backward();
            chronological = false;
        }
    }

    options.from = next;
    options.filter = RoomEventFilter::default();
    options.limit = limit.into();

    let messages = room
        .messages(options)
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    if messages.chunk.is_empty() {
        log::debug!("Reached end of room data");

        return Ok((0, None));
    }

    cache_room_messages_response(
        cached_data.clone(),
        &messages,
        room.room_id().to_owned(),
        chronological,
    )
    .map_err(errors::convert_cache_error)?;

    let len = messages.chunk.len();

    if let Some(msg) = messages.chunk.first() {
        if let Some(id) = msg.event_id() {
            Ok((len, Some(id)))
        } else {
            log::warn!("No eventId attached to TimelineEvent");
            Err(errors::create_error(ErrorType::Unknown))
        }
    } else {
        log::warn!("No events available in room");
        Ok((0, None))
    }
}

pub async fn setup_room_key_listener(
    room_id: &OwnedRoomId,
    client: &Client,
) -> Result<mpsc::Receiver<()>> {
    log::debug!("setting up key listener for room {room_id}");

    let (tx, rx) = mpsc::channel(100);
    let key_stream = client
        .encryption()
        .backups()
        .room_keys_for_room_stream(room_id);

    tokio::spawn(async move {
        // pinning is needed before calling next
        tokio::pin!(key_stream);

        log::debug!("Now listening on room keys");

        while let Some(result) = key_stream.next().await {
            match result {
                Ok(session_ids) => {
                    // session_ids is a mapping of sender_key to set of session_ids
                    let total_keys: usize = session_ids.values().map(|s| s.len()).sum();
                    log::info!(
                        "Room keys downloaded from backup: {} sessions keys from {} senders",
                        total_keys,
                        session_ids.len()
                    );
                    log::debug!("Downloaded session keys: {session_ids:#?}");
                    // Notify listener that new keys have arrived
                    let _ = tx.send(()).await;
                }
                Err(e) => {
                    log::warn!("Error receiving room key notification: {e:?}");
                }
            }
        }

        log::debug!("Ending room key listener");
    });

    Ok(rx)
}

pub fn initial_fetch_limit(limit: u32) -> u32 {
    ((limit as f32) * 1.2).ceil() as u32
}

pub fn subsequent_fetch_limit(limit: u32) -> u32 {
    ((limit as f32) * 0.1).ceil() as u32
}
