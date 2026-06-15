use gouda_core::Result;
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::*;
use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::ruma::events::room::message::{Relation, ReplyMetadata, RoomMessageEventContent};
use matrix_sdk::ruma::events::{Mentions, OriginalMessageLikeEvent};
use matrix_sdk::Room;
use ruma_common::{EventId, OwnedEventId, OwnedUserId, UserId};

use crate::media::MediaManager;
use crate::{errors, media};

macro_rules! download_image {
    ($image:expr, $media_manager:expr, $room:expr, $event_id:expr, $dest_proto_message:ident) => {{
        let result = $media_manager
            .download_from_media_event_content(
                &$room,
                &$event_id,
                &$image,
                $image.filename.as_deref().or(Some(&$image.body)),
            )
            .await;

        match result {
            Ok(path) => Some($dest_proto_message::Content::Image(
                gouda_proto::chat::MessageContentImage { image_path: path },
            )),
            Err(err) => {
                log::error!("Error downloading attached image: {err}");
                None
            }
        }
    }};
}

macro_rules! download_file {
    ($file:expr, $media_manager:expr, $room:expr, $event_id:expr, $dest_proto_message:ident) => {{
        let file_name = $file.filename.clone().unwrap_or($file.body.clone());

        let result = $media_manager
            .download_from_media_event_content(&$room, &$event_id, &$file, Some(&file_name))
            .await;

        match result {
            Ok(path) => Some($dest_proto_message::Content::File(
                gouda_proto::chat::MessageContentFile {
                    file_path: path,
                    file_name: Some(file_name),
                },
            )),
            Err(err) => {
                log::error!("Error downloading attached file: {err}");
                None
            }
        }
    }};
}

macro_rules! download_audio {
    ($audio:expr, $media_manager:expr, $room:expr, $event_id:expr, $dest_proto_message:ident) => {{
        let file_name = $audio.filename.clone().unwrap_or($audio.body.clone());

        let result = $media_manager
            .download_from_media_event_content(&$room, &$event_id, &$audio, Some(&file_name))
            .await;

        match result {
            Ok(path) => Some($dest_proto_message::Content::AudioFile(
                gouda_proto::chat::MessageContentAudio {
                    file_path: path,
                    file_name: Some(file_name),
                },
            )),
            Err(err) => {
                log::error!("Error downloading attached audio: {err}");
                None
            }
        }
    }};
}

macro_rules! download_video {
    ($video:expr, $media_manager:expr, $room:expr, $event_id:expr, $dest_proto_message:ident) => {{
        let file_name = $video.filename.clone().unwrap_or($video.body.clone());

        let result = $media_manager
            .download_from_media_event_content(&$room, &$event_id, &$video, Some(&file_name))
            .await;

        match result {
            Ok(path) => Some($dest_proto_message::Content::VideoFile(
                gouda_proto::chat::MessageContentVideo {
                    file_path: path,
                    file_name: Some(file_name),
                },
            )),
            Err(err) => {
                log::error!("Error downloading attached video: {err}");
                None
            }
        }
    }};
}

macro_rules! convert_location {
    ($location:expr, $dest_proto_message:ident) => {{
        let msg = if let Some(content) = $location.location {
            content.uri
        } else {
            $location.geo_uri
        };

        Some($dest_proto_message::Content::Text(
            gouda_proto::chat::MessageContentText { content: msg },
        ))
    }};
}

macro_rules! generate_message_content {
    ($media_manager:expr, $room:expr, $event_id:expr, $msgtype:expr, $dest_proto_message:ident) => {
        match $msgtype {
            matrix_sdk::ruma::events::room::message::MessageType::Audio(audio) => {
                crate::messages::download_audio!(
                    audio,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            matrix_sdk::ruma::events::room::message::MessageType::Emote(emote) => Some(
                $dest_proto_message::Content::Text(gouda_proto::chat::MessageContentText {
                    content: emote.body,
                }),
            ),
            matrix_sdk::ruma::events::room::message::MessageType::File(file) => {
                crate::messages::download_file!(
                    file,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            matrix_sdk::ruma::events::room::message::MessageType::Image(image) => {
                crate::messages::download_image!(
                    image,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            matrix_sdk::ruma::events::room::message::MessageType::Location(location) => {
                crate::messages::convert_location!(location, $dest_proto_message)
            }
            matrix_sdk::ruma::events::room::message::MessageType::Notice(notice) => Some(
                $dest_proto_message::Content::Text(gouda_proto::chat::MessageContentText {
                    content: notice.body,
                }),
            ),
            matrix_sdk::ruma::events::room::message::MessageType::ServerNotice(notice) => Some(
                $dest_proto_message::Content::Text(gouda_proto::chat::MessageContentText {
                    content: notice.body,
                }),
            ),
            matrix_sdk::ruma::events::room::message::MessageType::Text(text) => Some(
                $dest_proto_message::Content::Text(gouda_proto::chat::MessageContentText {
                    content: text.body.to_string(),
                }),
            ),
            matrix_sdk::ruma::events::room::message::MessageType::Video(video) => {
                crate::messages::download_video!(
                    video,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            _ => {
                log::warn!("Unsupported message type");
                None
            }
        }
    };
}

pub(crate) use convert_location;
pub(crate) use download_audio;
pub(crate) use download_file;
pub(crate) use download_image;
pub(crate) use download_video;
pub(crate) use generate_message_content;

pub async fn message_from_event(
    media_manager: &MediaManager,
    room: &Room,
    event: &OriginalMessageLikeEvent<RoomMessageEventContent>,
) -> Message {
    let related_message_id = get_related_message_id(event);
    let mentioned_user_ids = matrix_mentions_to_proto_mentions(&event.content.mentions);

    let content = generate_message_content!(
        media_manager,
        room,
        event.event_id,
        event.content.msgtype.clone(),
        message
    );

    Message {
        message_id: event.event_id.to_string(),
        room_id: event.room_id.to_string(),
        sender_id: event.sender.to_string(),
        timestamp: event.origin_server_ts.get().into(),
        content,
        related_message_id,
        is_pinned: false,
        is_encrypted: false,
        reactions: Vec::new(),
        mentioned_user_ids,
    }
}

fn get_related_message_id(
    event: &OriginalMessageLikeEvent<RoomMessageEventContent>,
) -> Option<String> {
    let Some(relation) = &event.content.relates_to else {
        return None;
    };

    let Relation::Reply(reply) = relation else {
        return None;
    };

    Some(reply.in_reply_to.event_id.to_string())
}

pub async fn send_text_message(
    room: Room,
    related_message_id: Option<String>,
    mentioned_user_ids: Vec<String>,
    content: MessageContentText,
) -> Result<MessageSendResponse> {
    let mut event = RoomMessageEventContent::text_markdown(content.content);

    if let Some(related_message_id) = related_message_id {
        let metadata = generate_reply_metadata(&room, &related_message_id).await?;

        event = event.make_reply_to(
            metadata.metadata(),
            matrix_sdk::ruma::events::room::message::ForwardThread::Yes,
            matrix_sdk::ruma::events::room::message::AddMentions::Yes,
        );
    }

    if !mentioned_user_ids.is_empty() {
        let mentions = proto_mentions_to_matrix_mentions(&mentioned_user_ids)?;
        event = event.add_mentions(mentions);
    }

    let re = room
        .send(event)
        .await
        .map_err(errors::convert_matrix_sdk_error)?;

    Ok(MessageSendResponse {
        message_id: re.response.event_id.to_string(),
    })
}

pub fn proto_mentions_to_matrix_mentions(mentioned_user_ids: &[String]) -> Result<Mentions> {
    let user_ids: Vec<OwnedUserId> = mentioned_user_ids
        .iter()
        .map(|f| {
            UserId::parse(f)
                .map(|f| f.to_owned())
                .map_err(|_| errors::create_error(ErrorType::InvalidUserId))
        })
        .collect::<Result<Vec<OwnedUserId>>>()?;

    Ok(Mentions::with_user_ids(user_ids))
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

pub async fn send_audio_message(
    media_manager: &MediaManager,
    room: Room,
    related_message_id: Option<String>,
    content: MessageContentAudio,
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

pub async fn send_video_message(
    media_manager: &MediaManager,
    room: Room,
    related_message_id: Option<String>,
    content: MessageContentVideo,
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

pub fn matrix_mentions_to_proto_mentions(mentions: &Option<Mentions>) -> Vec<String> {
    let Some(mentions) = &mentions else {
        return Vec::new();
    };

    mentions.user_ids.iter().map(|f| f.to_string()).collect()
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
