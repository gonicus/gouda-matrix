use gouda_proto::chat::*;
use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::ruma::events::relation::{InReplyTo, Thread};
use matrix_sdk::ruma::events::room::message::{
    FormattedBody, MessageType, Relation, ReplyMetadata, RoomMessageEvent, RoomMessageEventContent,
};
use matrix_sdk::ruma::events::{Mentions, OriginalMessageLikeEvent};
use matrix_sdk::Room;
use ruma_common::{EventId, OwnedEventId, OwnedUserId};

use crate::client::SessionContext;
use crate::error::{Error, Result};
use crate::media::MediaManager;

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
                crate::messages::download_file!(
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
                crate::messages::download_file!(
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
                crate::messages::download_file!(
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
pub(crate) use download_file;
pub(crate) use generate_message_content;

pub async fn message_from_event(
    media_manager: &MediaManager,
    room: &Room,
    event: &OriginalMessageLikeEvent<RoomMessageEventContent>,
) -> Message {
    let mentioned_user_ids = matrix_mentions_to_proto_mentions(&event.content.mentions);
    let room_mentioned = event
        .content
        .mentions
        .as_ref()
        .map(|m| m.room)
        .unwrap_or(false);

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
        related_message_id: get_related_message_id(event),
        is_pinned: false,
        is_encrypted: false,
        reactions: Vec::new(),
        mentioned_user_ids,
        room_mentioned,
        thread_id: get_thread_id(event),
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

fn get_thread_id(event: &OriginalMessageLikeEvent<RoomMessageEventContent>) -> Option<String> {
    let Some(relation) = &event.content.relates_to else {
        return None;
    };

    let Relation::Thread(thread) = relation else {
        return None;
    };

    Some(thread.event_id.to_string())
}

pub fn proto_mentions_to_matrix_mentions(
    mentioned_user_ids: Vec<OwnedUserId>,
    room_mentioned: bool,
) -> Result<Mentions> {
    let mut mentions = Mentions::with_user_ids(mentioned_user_ids);
    mentions.room = room_mentioned;
    Ok(mentions)
}

pub struct MessageBuilder<'a> {
    media_manager: &'a MediaManager,
    content: message_send_request::Content,

    related_message_id: Option<OwnedEventId>,
    mentioned_user_ids: Vec<OwnedUserId>,
    room_mentioned: bool,
    thread_id: Option<OwnedEventId>,
}

impl<'a> MessageBuilder<'a> {
    pub fn new(ctx: &'a SessionContext, content: message_send_request::Content) -> Self {
        Self {
            media_manager: &ctx.media_manager,
            content,

            related_message_id: None,
            mentioned_user_ids: Vec::new(),
            room_mentioned: false,
            thread_id: None,
        }
    }

    pub fn related_message_id(mut self, id: impl Into<OwnedEventId>) -> Self {
        self.related_message_id = Some(id.into());
        self
    }

    pub fn mentioned_user_ids(mut self, ids: Vec<OwnedUserId>) -> Self {
        self.mentioned_user_ids = ids;
        self
    }

    pub fn room_mentioned(mut self, mentioned: bool) -> Self {
        self.room_mentioned = mentioned;
        self
    }

    pub fn thread_id(mut self, thread_id: OwnedEventId) -> Self {
        self.thread_id = Some(thread_id);
        self
    }

    pub async fn send(mut self, room: &Room) -> Result<String> {
        match std::mem::take(&mut self.content) {
            message_send_request::Content::Text(c) => self.send_text(room, c).await,
            message_send_request::Content::File(c) => self.send_file(room, c).await,
        }
    }

    async fn send_text(self, room: &Room, content: MessageContentText) -> Result<String> {
        let mut event = RoomMessageEventContent::text_markdown(content.content);

        if let MessageType::Text(text) = &mut event.msgtype {
            if text.formatted.is_none() {
                text.formatted = Some(FormattedBody::html(text.body.clone()));
            }
        }

        if let Some(thread_id) = &self.thread_id {
            event = self.add_thread_metadata(event, thread_id).await?;
        } else if let Some(related_message_id) = &self.related_message_id {
            event = self.add_reply_metadata(room, event, related_message_id).await?;
        }

        if !self.mentioned_user_ids.is_empty() || self.room_mentioned {
            let mentions =
                proto_mentions_to_matrix_mentions(self.mentioned_user_ids, self.room_mentioned)?;
            event = event.add_mentions(mentions);
        }

        let re = room.send(event).await?;

        Ok(re.response.event_id.to_string())
    }

    async fn send_file(self, room: &Room, content: MessageContentFile) -> Result<String> {
        let message_id = self
            .media_manager
            .send_room_attachment(
                room,
                content.file_path,
                content.file_name,
                self.related_message_id,
            )
            .await?;

        Ok(message_id)
    }

    async fn add_thread_metadata(
        &self,
        event: RoomMessageEventContent,
        thread_id: &EventId,
    ) -> Result<RoomMessageEventContent> {
        todo!()
    }

    async fn add_reply_metadata(
        &self,
        room: &Room,
        event: RoomMessageEventContent,
        related_message_id: &EventId,
    ) -> Result<RoomMessageEventContent> {
        let metadata = self.get_main_reply_metadata(room, related_message_id).await?;

        let event = event.make_reply_to(
            metadata.metadata(),
            matrix_sdk::ruma::events::room::message::ForwardThread::Yes,
            matrix_sdk::ruma::events::room::message::AddMentions::Yes,
        );

        Ok(event)
    }

    async fn get_main_reply_metadata(
        &self,
        room: &Room,
        related_message_id: &EventId,
    ) -> Result<CustomReplyMetadata> {
        let sender_id = get_event_sender_id(room, related_message_id).await?;
        let metadata = CustomReplyMetadata::new(related_message_id.to_owned(), sender_id);
        Ok(metadata)
    }

    async fn get_thread_reply_metadata(
        &self,
        room: &Room,
        thread_id: &EventId,
    ) -> Result<CustomReplyMetadata> {
        let related_message = self
            .related_message_id
            .clone()
            .unwrap_or(thread_id.to_owned());

        let sender_id = get_event_sender_id(room, &related_message).await?;

        let mut thread = Thread::without_fallback(thread_id.to_owned());

        if let Some(reply) = &self.related_message_id {
            thread.in_reply_to = Some(InReplyTo::new(reply.to_owned()));
        }

        let metadata = CustomReplyMetadata::new(related_message, sender_id).thread(thread);

        Ok(metadata)
    }
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
    thread: Option<Thread>,
}

impl CustomReplyMetadata {
    pub fn new(event_id: OwnedEventId, sender_id: OwnedUserId) -> Self {
        Self {
            event_id,
            sender_id,
            thread: None,
        }
    }

    pub fn thread(mut self, thread: Thread) -> Self {
        self.thread = Some(thread);
        self
    }

    pub fn metadata(&self) -> ReplyMetadata<'_> {
        ReplyMetadata::new(&self.event_id, &self.sender_id, self.thread.as_ref())
    }
}

async fn get_event_sender_id(room: &Room, event_id: &EventId) -> Result<OwnedUserId> {
    let event = room
        .event(event_id, None)
        .await
        .map_err(|_| Error::MessageNotFound)?;

    sender_id_from_timeline_event(&event)
}

fn sender_id_from_timeline_event(event: &TimelineEvent) -> Result<OwnedUserId> {
    match &event.kind {
        TimelineEventKind::PlainText { event } => {
            let event = event
                .deserialize()
                .map_err(|_| Error::internal("Error deserializing related message"))?;

            Ok(event.sender().to_owned())
        }
        TimelineEventKind::Decrypted(event) => {
            let event = event
                .event
                .deserialize()
                .map_err(|_| Error::internal("Error deserializing related message"))?;

            Ok(event.sender().to_owned())
        }
        _ => Err(Error::internal(
            "Related event is not plaintext or decrypted",
        )),
    }
}
