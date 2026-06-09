use futures_util::StreamExt;
use gouda_core::{RequestContext, Result};
use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::*;
use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::api::client::filter::RoomEventFilter;
use matrix_sdk::ruma::events::room::message::{ReplyMetadata, RoomMessageEventContent};
use matrix_sdk::ruma::events::Mentions;
use matrix_sdk::{Client, Room};
use ruma_common::{EventId, OwnedEventId, OwnedUserId, RoomId, UserId};
use tokio::sync::mpsc;

use crate::client::InitializedData;
use crate::media::MediaManager;
use crate::memory_cache::{self, cache_room_messages_response, MemoryCache};
use crate::proto_cache::ProtoCache;
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
                messages::download_audio!(
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
                messages::download_file!(
                    file,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            matrix_sdk::ruma::events::room::message::MessageType::Image(image) => {
                messages::download_image!(
                    image,
                    $media_manager,
                    $room,
                    $event_id,
                    $dest_proto_message
                )
            }
            matrix_sdk::ruma::events::room::message::MessageType::Location(location) => {
                messages::convert_location!(location, $dest_proto_message)
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
                messages::download_video!(
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

pub fn message_content_to_message_change_event_content(
    content: message::Content,
) -> message_change_event::Content {
    match content {
        message::Content::Text(c) => message_change_event::Content::Text(c),
        message::Content::Image(c) => message_change_event::Content::Image(c),
        message::Content::File(c) => message_change_event::Content::File(c),
        message::Content::MembershipChange(c) => message_change_event::Content::MembershipChange(c),
        message::Content::AudioFile(c) => message_change_event::Content::AudioFile(c),
        message::Content::VideoFile(c) => message_change_event::Content::VideoFile(c),
    }
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

pub struct RoomMessagesManager {
    ctx: RequestContext,
    client: Client,
    room: Room,

    media_manager: MediaManager,
    memory_cache: MemoryCache,
    proto_cache: ProtoCache,
}

impl RoomMessagesManager {
    pub fn from_initialized_data(
        ctx: RequestContext,
        initialized_data: &InitializedData,
        room: Room,
    ) -> Self {
        Self {
            ctx,
            client: initialized_data.client.clone(),
            room,

            media_manager: initialized_data.media_manager.clone(),
            memory_cache: initialized_data.memory_cache.clone(),
            proto_cache: initialized_data.proto_cache.clone(),
        }
    }

    /// Gets the messages of a room and sends them as a multipart response to the application.
    /// If the messages have been cached before, they are retrieved from the cache and synced
    /// in the background.
    pub async fn send_and_sync_messages(
        &self,
        order: MessagesOrder,
        limit: u32,
        from_message_id: Option<OwnedEventId>,
    ) -> Result<()> {
        // Caching is currently only implemented if we start from the newest messages in the room.
        if let Some(from_message_id) = from_message_id {
            let result = self
                .fetch_and_send_messages(order, limit, Some(from_message_id))
                .await;

            return result;
        };

        let room_id = self.room.room_id().as_str();

        let Some(messages) = self.proto_cache.cached_messages(room_id).await else {
            let result = self.fetch_and_send_messages(order, limit, None).await;

            return result;
        };

        // TODO: Return the cached messages with the requested limit, fetch all other in the background
        //  and compare both lists.

        todo!()
    }

    async fn fetch_and_send_messages(
        &self,
        order: MessagesOrder,
        limit: u32,
        from_message_id: Option<OwnedEventId>,
    ) -> Result<()> {
        let key_change_rx = setup_room_key_listener(self.room.room_id(), &self.client).await?;

        // Set limit of first fetch a little higher than requested limit
        let mut fetch_limit = initial_fetch_limit(limit);

        let mut skip_first = true;
        let from_id = match from_message_id {
            Some(val) => val,
            None => {
                let (_, id) = fetch_messages_from_sdk(
                    &self.memory_cache,
                    order,
                    &self.room,
                    None,
                    fetch_limit,
                )
                .await?;

                // Reduce limit for subsequent fetches
                fetch_limit = subsequent_fetch_limit(limit);

                // The first message is part of the response when no from_id has been specified
                skip_first = false;

                id.ok_or(errors::create_unknown("no messages in room"))?
            }
        };

        let cached_room = memory_cache::get_or_create_room(&self.memory_cache, self.room.room_id())
            .map_err(errors::convert_cache_error)?;

        loop {
            let next_batch = memory_cache::check_cached_enough(
                &cached_room.clone(),
                from_id.clone(),
                limit,
                order,
                skip_first,
            )
            .map_err(errors::convert_cache_error)?;

            match next_batch {
                None => break,
                Some(val) => {
                    log::debug!("Attempting to fetch further messages from sdk");
                    let (fetched, _) = fetch_messages_from_sdk(
                        &self.memory_cache,
                        order,
                        &self.room,
                        Some(val),
                        fetch_limit,
                    )
                    .await?;

                    // Reduce limit for subsequent fetches
                    fetch_limit = subsequent_fetch_limit(limit);

                    if fetched == 0 {
                        break;
                    }
                }
            }
        }

        let room_client =
            memory_cache::MatrixRoomClient::new(&self.room, self.media_manager.clone());

        // Fetch events from sdk and assemble response
        let seq = memory_cache::send_and_get_sequence_chunk(
            &cached_room.clone(),
            from_id.clone(),
            limit,
            order,
            skip_first,
            &room_client,
            &self.memory_cache,
            &self.ctx,
        )
        .await
        .map_err(|err| gouda_proto::chat::Error {
            r#type: 0,
            error_string: Some(err.to_string()),
        })?;

        if !seq.is_complete {
            log::warn!("Sequence chunk was incomplete");
        }

        let ctx = self.ctx.clone();
        let room_id = self.room.room_id().to_owned();
        let room_client = room_client.clone();
        let cache = self.memory_cache.clone();

        tokio::spawn(async move {
            let result = memory_cache::retry_decryption(
                seq.messages,
                &room_id,
                &room_client,
                &cache,
                key_change_rx,
                &ctx,
            )
            .await;

            if let Err(err) = result {
                ctx.send_error(errors::convert_cache_error(err)).await;
            }
        });

        Ok(())
    }
}

async fn fetch_messages_from_sdk(
    cache: &MemoryCache,
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

    cache_room_messages_response(cache, &messages, room.room_id().to_owned(), chronological)
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

async fn setup_room_key_listener(room_id: &RoomId, client: &Client) -> Result<mpsc::Receiver<()>> {
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

const fn initial_fetch_limit(limit: u32) -> u32 {
    ((limit as f32) * 1.2).ceil() as u32
}

const fn subsequent_fetch_limit(limit: u32) -> u32 {
    ((limit as f32) * 0.1).ceil() as u32
}
