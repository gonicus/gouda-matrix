use std::collections::HashMap;
use std::sync::Arc;

use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{message, Message, MessageContentMembershipChange, Reaction};
use matrix_sdk::deserialized_responses::{
    DecryptedRoomEvent, TimelineEvent, TimelineEventKind, UnableToDecryptInfo,
};
use matrix_sdk::ruma::events::reaction::ReactionEvent;
use matrix_sdk::ruma::events::room::member::RoomMemberEvent;
use matrix_sdk::ruma::events::room::message::{RoomMessageEvent, RoomMessageEventContent};
use matrix_sdk::ruma::events::room::redaction::RoomRedactionEvent;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEvent, AnyStateEvent, AnySyncTimelineEvent, AnyTimelineEvent,
    OriginalMessageLikeEvent,
};
use matrix_sdk::Room;
use ruma_common::api::Direction;
use ruma_common::serde::Raw;
use ruma_common::{EventId, OwnedEventId};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

use crate::media::MediaManager;
use crate::{messages, user};

/// The capacity of the channel for receiving retreived and assembled messages.
const MESSAGES_CHANNEL_CAPACITY: usize = 10;
/// How many events are fetched with each room.messages request.
const ROOM_EVENTS_CHUNK_SIZE: u32 = 10;

#[derive(Debug, thiserror::Error)]
pub enum MessageCacheError {
    #[error("receiver of the messages dropped")]
    MessageReceiverDropped,

    #[error("matrix sdk error: {0}")]
    MatrixError(#[from] matrix_sdk::Error),
}

pub type Result<T> = std::result::Result<T, MessageCacheError>;

pub struct QueryOptions {
    /// How many assembled messages should be returned.
    pub limit: u32,
    /// The ID of the message from where to begin fetching messages.
    pub from_message_id: Option<OwnedEventId>,
}

#[derive(Clone)]
pub struct MessageCache {
    inner: Arc<Mutex<MessageCacheInner>>,
}

impl MessageCache {
    pub fn new(media_manager: MediaManager) -> Self {
        let inner = MessageCacheInner::new(media_manager);

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn cache_response_content(&self, content: &ResponseContent) {
        // TODO: Cache response content and update data accordingly
    }

    pub async fn fetch_messages(
        &self,
        room: Room,
        options: QueryOptions,
    ) -> Result<ReceiverStream<Result<Message>>> {
        self.inner.lock().await.fetch_messages(room, options).await
    }
}

struct MessageCacheInner {
    media_manager: MediaManager,
    cached_rooms: Mutex<HashMap<String, Arc<Mutex<CachedRoom>>>>,
}

impl MessageCacheInner {
    pub fn new(media_manager: MediaManager) -> Self {
        Self {
            media_manager,
            cached_rooms: Mutex::new(HashMap::new()),
        }
    }

    pub async fn fetch_messages(
        &mut self,
        room: Room,
        options: QueryOptions,
    ) -> Result<ReceiverStream<Result<Message>>> {
        let from_token = if let Some(message_id) = options.from_message_id {
            resolve_event_for_pagination(&room, &message_id).await?
        } else {
            None
        };

        let room = self.get_or_create_room(room).await;
        let media_manager = self.media_manager.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(MESSAGES_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut room = room.lock().await;

            MessageFetcher::new(
                media_manager.clone(),
                &mut room,
                tx,
                options.limit,
                from_token,
            )
            .run()
            .await;
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn get_or_create_room(&mut self, room: Room) -> Arc<Mutex<CachedRoom>> {
        let mut guard = self.cached_rooms.lock().await;

        let room = guard
            .entry(room.room_id().to_string())
            .or_insert_with(|| self.create_new_room(room))
            .clone();

        room
    }

    fn create_new_room(&self, room: Room) -> Arc<Mutex<CachedRoom>> {
        Arc::new(Mutex::new(CachedRoom::new(room)))
    }
}

struct CachedRoom {
    pub room: Room,
    pub messages: HashMap<String, CachedMessage>,
}

impl CachedRoom {
    pub fn new(room: Room) -> Self {
        Self {
            room,
            messages: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct CachedReplacement {
    /// The timestamp when the replacement event was created.
    pub timestamp: u64,
    /// The new content replacing the original or any other related
    /// replacement event.
    pub new_content: message::Content,
}

#[derive(Debug)]
struct CachedReaction {
    /// The ID of the user who reacted.
    pub user: String,
    /// The emoji the user reacted with.
    pub emoji: String,
}

#[derive(Debug, Default)]
struct CachedMessage {
    /// The parent aka original event of the message.
    /// If none, we have not yet reached the original message event.
    pub original: Option<Message>,
    /// The reactions to this message.
    pub reactions: HashMap<String, CachedReaction>,
    /// The replacement events to this message.
    pub replacements: HashMap<String, CachedReplacement>,
}

impl CachedMessage {
    pub fn build_from_original(&mut self, mut original: Message) -> Message {
        self.original = Some(original.clone());

        self.apply_reactions(&mut original);
        self.apply_replacements(&mut original);

        original
    }

    fn apply_reactions(&self, msg: &mut Message) {
        for reaction in self.reactions.values() {
            let reaction = Reaction {
                message_id: msg.message_id.clone(),
                room_id: msg.room_id.clone(),
                reaction: reaction.emoji.clone(),
                user_id: Some(reaction.user.clone()),
            };

            msg.reactions.push(reaction);
        }
    }

    fn apply_replacements(&mut self, msg: &mut Message) {
        let mut replacements: Vec<&CachedReplacement> = self.replacements.values().collect();
        replacements.sort_by_key(|f| f.timestamp);

        if let Some(replacement) = replacements.last() {
            msg.content = Some(replacement.new_content.clone());
        }
    }
}

struct MessageFetcher<'a> {
    /// The media manager to use to download message attachements.
    media_manager: MediaManager,
    /// The room to use to fetch messages.
    cache: &'a mut CachedRoom,

    /// Where to send finished messages.
    sender: Sender<Result<Message>>,
    /// How many messages should be fetched.
    message_limit: u32,

    /// The pagination token for the next chunk.
    from_token: Option<String>,
    /// How many events are fetched with each request.
    chunk_size: js_int::UInt,

    /// The number of chat messages we have build and send to the message receiver.
    retrieved_messages: u32,
}

impl<'a> MessageFetcher<'a> {
    pub fn new(
        media_manager: MediaManager,
        cache: &'a mut CachedRoom,
        sender: Sender<Result<Message>>,
        limit: u32,
        from_token: Option<String>,
    ) -> Self {
        Self {
            media_manager,
            cache,

            sender,
            message_limit: limit,

            from_token,
            chunk_size: ROOM_EVENTS_CHUNK_SIZE.into(),

            retrieved_messages: 0,
        }
    }

    pub async fn run(mut self) {
        if self.message_limit == 0 {
            return;
        }

        let result = self.fetch_until_completion().await;

        if let Err(err) = result {
            log::error!("Received error when fetching messages: {err}");

            if let Err(err) = self.sender.send(Err(err)).await {
                log::error!("Error sending original error to message receiver: {err}");
            }
        }
    }

    async fn fetch_until_completion(&mut self) -> Result<()> {
        while self.retrieved_messages < self.message_limit {
            log::debug!("Fetching next event chunk");

            let options = self.build_messages_options();

            let matrix_sdk::room::Messages { end, chunk, .. } =
                self.cache.room.messages(options).await?;

            log::debug!("Processing chunk");

            self.process_event_chunk(chunk).await?;

            if let Some(next_token) = end {
                self.from_token = Some(next_token);
            } else {
                log::debug!("No more events left to fetch");
                break;
            }
        }

        if self.retrieved_messages != self.message_limit {
            log::warn!("Did not receive enough messages to reach requested limit");
        } else {
            log::debug!("Successfully fetched requested number of messages");
        }

        Ok(())
    }

    fn build_messages_options(&self) -> matrix_sdk::room::MessagesOptions {
        let mut options = matrix_sdk::room::MessagesOptions::new(Direction::Backward);
        options.from = self.from_token.clone();
        options.limit = self.chunk_size;
        options
    }

    async fn process_event_chunk(&mut self, chunk: Vec<TimelineEvent>) -> Result<()> {
        for event in chunk {
            match event.kind {
                TimelineEventKind::Decrypted(event) => self.process_decrypted_event(event).await?,
                TimelineEventKind::UnableToDecrypt { event, utd_info } => {
                    self.process_unable_to_decrypt_event(event, utd_info)
                        .await?
                }
                TimelineEventKind::PlainText { event } => {
                    self.process_plain_text_event(event).await?
                }
            }

            if self.retrieved_messages == self.message_limit {
                log::debug!("Reached the number of requested messages, aborting chunk processing");
                break;
            }
        }

        Ok(())
    }

    async fn process_decrypted_event(&mut self, event: DecryptedRoomEvent) -> Result<()> {
        let deserialized = match event.event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::error!("Unable to deserialize event {event:?}: {err}");
                // We return ok so we don't abord the fetching process
                return Ok(());
            }
        };

        self.process_any_timeline_event(deserialized).await
    }

    async fn process_unable_to_decrypt_event(
        &mut self,
        event: Raw<AnySyncTimelineEvent>,
        utd_info: UnableToDecryptInfo,
    ) -> Result<()> {
        log::error!("Unable to decrypt event {event:?}: {utd_info:?}");

        let deserialized = match event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::error!("Unable to deserialize decrypted event {event:?}: {err}");
                // We return ok so we don't abord the fetching process
                return Ok(());
            }
        };

        // TODO: Add the decryption failure to some vector for retrying decryption later.

        let message = Message {
            room_id: self.cache.room.room_id().to_string(),
            message_id: deserialized.event_id().to_string(),
            timestamp: deserialized.origin_server_ts().0.into(),
            sender_id: deserialized.sender().to_string(),
            is_encrypted: true,
            ..Default::default()
        };

        self.send_finished_message(message).await?;

        Ok(())
    }

    async fn process_plain_text_event(&mut self, event: Raw<AnySyncTimelineEvent>) -> Result<()> {
        let deserialized = match event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::error!("Unable to deserialize event {event:?}: {err}");
                // We return ok so we don't abord the fetching process
                return Ok(());
            }
        };

        let full_event = deserialized.into_full_event(self.cache.room.room_id().to_owned());

        self.process_any_timeline_event(full_event).await
    }

    async fn process_any_timeline_event(&mut self, event: AnyTimelineEvent) -> Result<()> {
        match event {
            AnyTimelineEvent::MessageLike(event) => {
                self.process_any_message_like_event(event).await
            }
            AnyTimelineEvent::State(event) => self.process_any_state_event(event).await,
        }
    }

    async fn process_any_message_like_event(&mut self, event: AnyMessageLikeEvent) -> Result<()> {
        match event {
            AnyMessageLikeEvent::RoomMessage(event) => self.process_room_message(event).await,
            AnyMessageLikeEvent::RoomRedaction(event) => self.process_room_redaction(event),
            AnyMessageLikeEvent::Reaction(event) => self.process_reaction_event(event),
            _ => Ok(()),
        }
    }

    async fn process_room_message(&mut self, event: RoomMessageEvent) -> Result<()> {
        use matrix_sdk::ruma::events::room::message::Relation;

        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to care about that.
            return Ok(());
        };

        // Replacement events are stashed until we reach the original event.
        if let Some(Relation::Replacement(relation)) = original.content.relates_to.clone() {
            let new_content = messages::generate_message_content!(
                self.media_manager,
                &self.cache.room,
                relation.event_id,
                relation.new_content.msgtype,
                message
            );

            let Some(new_content) = new_content else {
                log::debug!("Ignoring an unsupported RoomMessageEvent content type");
                return Ok(());
            };

            let replacement = CachedReplacement {
                timestamp: event.origin_server_ts().0.into(),
                new_content,
            };

            self.stash_replacement(
                relation.event_id.to_string(),
                original.event_id.to_string(),
                replacement,
            );

            return Ok(());
        }

        self.build_and_send_message_event(original).await
    }

    fn process_room_redaction(&self, event: RoomRedactionEvent) -> Result<()> {
        // TODO: Process the redaction event and remove the appropriate event from the cache
        Ok(())
    }

    fn process_reaction_event(&mut self, event: ReactionEvent) -> Result<()> {
        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to care about that.
            return Ok(());
        };

        let reaction = CachedReaction {
            user: original.sender.to_string(),
            emoji: original.content.relates_to.key.clone(),
        };

        let related_message = original.content.relates_to.event_id.to_string();

        self.stash_reaction(related_message, event.event_id().to_string(), reaction);

        Ok(())
    }

    async fn process_any_state_event(&mut self, event: AnyStateEvent) -> Result<()> {
        match event {
            AnyStateEvent::RoomMember(event) => self.process_room_member_event(event).await,
            _ => Ok(()),
        }
    }

    async fn process_room_member_event(&mut self, event: RoomMemberEvent) -> Result<()> {
        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to carte about that.
            return Ok(());
        };

        let Some(change) = user::convert_membership_change(&original.membership_change()) else {
            // Not a relevant membership change
            return Ok(());
        };

        let content = MessageContentMembershipChange {
            change: change.into(),
            affected_user_id: original.state_key.to_string(),
        };

        let message = Message {
            room_id: self.cache.room.room_id().to_string(),
            message_id: event.event_id().to_string(),
            sender_id: event.sender().to_string(),
            timestamp: event.origin_server_ts().0.into(),
            content: Some(message::Content::MembershipChange(content)),
            ..Default::default()
        };

        self.build_and_send_message(message).await
    }

    fn stash_replacement(
        &mut self,
        original_message_id: String,
        replacement_id: String,
        replacement: CachedReplacement,
    ) {
        let message = self.cache.messages.entry(original_message_id).or_default();
        message.replacements.insert(replacement_id, replacement);
    }

    fn stash_reaction(
        &mut self,
        original_message_id: String,
        reaction_id: String,
        reaction: CachedReaction,
    ) {
        let message = self.cache.messages.entry(original_message_id).or_default();
        message.reactions.insert(reaction_id, reaction);
    }

    async fn build_and_send_message_event(
        &mut self,
        event: &OriginalMessageLikeEvent<RoomMessageEventContent>,
    ) -> Result<()> {
        let msg = messages::message_from_event(&self.media_manager, &self.cache.room, event).await;
        self.build_and_send_message(msg).await
    }

    async fn build_and_send_message(&mut self, message: Message) -> Result<()> {
        let cached_message = self
            .cache
            .messages
            .entry(message.message_id.clone())
            .or_default();

        let message = cached_message.build_from_original(message);

        self.send_finished_message(message).await
    }

    async fn send_finished_message(&mut self, message: Message) -> Result<()> {
        self.sender
            .send(Ok(message))
            .await
            .map_err(|_| MessageCacheError::MessageReceiverDropped)?;

        self.retrieved_messages += 1;

        Ok(())
    }
}

async fn resolve_event_for_pagination(room: &Room, event_id: &EventId) -> Result<Option<String>> {
    let response = room
        .event_with_context(event_id, true, js_int::uint!(0), None)
        .await?;

    Ok(response.prev_batch_token)
}
