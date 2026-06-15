use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use gouda_core::RequestContext;
use gouda_proto::chat::builder::MessageChangeEventBuilder;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{
    message, EventOrigin, Message, MessageContentMembershipChange, MessageRemoveEvent, Reaction,
};
use matrix_sdk::deserialized_responses::{
    DecryptedRoomEvent, TimelineEvent, TimelineEventKind, UnableToDecryptInfo,
};
use matrix_sdk::ruma::events::reaction::ReactionEvent;
use matrix_sdk::ruma::events::room::encrypted::OriginalSyncRoomEncryptedEvent;
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
use tokio_stream::wrappers::ReceiverStream;

use crate::media::MediaManager;
use crate::{messages, user};

/// The capacity of the channel for receiving retreived and assembled messages.
const MESSAGES_CHANNEL_CAPACITY: usize = 10;

/// How many events to fetch at least with each chunk.
const ROOM_EVENTS_CHUNK_SIZE_MIN: u32 = 10;
/// How many events to fetch at most with each chunk.
const ROOM_EVENTS_CHUNK_SIZE_MAX: u32 = 100;

const _: () = assert!(ROOM_EVENTS_CHUNK_SIZE_MIN <= ROOM_EVENTS_CHUNK_SIZE_MAX);

#[derive(Debug, thiserror::Error)]
pub enum MessageCacheError {
    #[error("receiver of the messages dropped")]
    MessageReceiverDropped,

    #[error("cache lock poisoined")]
    CachePoisoined,

    #[error("matrix sdk error: {0}")]
    MatrixError(#[from] matrix_sdk::Error),
}

impl<T> From<std::sync::PoisonError<T>> for MessageCacheError {
    fn from(_value: std::sync::PoisonError<T>) -> Self {
        Self::CachePoisoined
    }
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
    inner: Arc<MessageCacheInner>,
}

impl MessageCache {
    pub fn new(ctx: RequestContext, media_manager: MediaManager) -> Self {
        let inner = MessageCacheInner::new(ctx, media_manager);

        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn fetch_messages(
        &self,
        room: Room,
        options: QueryOptions,
    ) -> Result<ReceiverStream<Result<Message>>> {
        self.inner.fetch_messages(room, options).await
    }

    pub async fn retry_encrypted_events(
        &self,
        room_id: impl Into<String>,
        events: Option<BTreeSet<OwnedEventId>>,
    ) {
        let result = self
            .inner
            .retry_encrypted_events(room_id.into(), events)
            .await;

        if let Err(err) = result {
            log::error!("Error retrying decryption of events: {err}");
        }
    }
}

struct MessageCacheInner {
    ctx: RequestContext,
    media_manager: MediaManager,
    cached_rooms: Mutex<HashMap<String, Arc<CachedRoom>>>,
}

impl MessageCacheInner {
    pub fn new(ctx: RequestContext, media_manager: MediaManager) -> Self {
        Self {
            ctx,
            media_manager,
            cached_rooms: Mutex::new(HashMap::new()),
        }
    }

    pub async fn fetch_messages(
        &self,
        room: Room,
        options: QueryOptions,
    ) -> Result<ReceiverStream<Result<Message>>> {
        let from_token = if let Some(message_id) = options.from_message_id {
            resolve_event_for_pagination(&room, &message_id).await?
        } else {
            None
        };

        let room = self.get_or_create_room(room)?;

        let (tx, rx) = tokio::sync::mpsc::channel(MESSAGES_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            MessageFetcher::new(room, tx, options.limit, from_token)
                .run()
                .await;
        });

        Ok(ReceiverStream::new(rx))
    }

    pub async fn retry_encrypted_events(
        &self,
        room_id: String,
        events: Option<BTreeSet<OwnedEventId>>,
    ) -> Result<()> {
        let guard = self.cached_rooms.lock()?;

        let Some(room) = guard.get(&room_id).cloned() else {
            return Ok(());
        };

        tokio::spawn(async move {
            if let Err(err) = room.retry_decryption(events).await {
                log::error!("Error retrying decryption of room events: {err}");
            }
        });

        Ok(())
    }

    fn get_or_create_room(&self, room: Room) -> Result<Arc<CachedRoom>> {
        let mut guard = self.cached_rooms.lock()?;

        let room = guard
            .entry(room.room_id().to_string())
            .or_insert_with(|| self.build_room(room))
            .clone();

        Ok(room)
    }

    fn build_room(&self, room: Room) -> Arc<CachedRoom> {
        Arc::new(CachedRoom::new(
            self.ctx.clone(),
            self.media_manager.clone(),
            room,
        ))
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

/// An event we where not able to decrypt.
#[derive(Debug, Clone)]
struct CachedEncryptedEvent {
    pub event: Raw<OriginalSyncRoomEncryptedEvent>,
}

struct CachedRoom {
    /// The context to use to send update events to the application.
    ctx: RequestContext,
    /// The media manager to use to download message attachements.
    media_manager: MediaManager,
    /// The room we work with.
    room: Room,

    /// The messages we have cached.
    pub messages: Mutex<HashMap<String, CachedMessage>>,
    /// Events that we could not decrypt and that were sent to the application
    /// as an encrypted message.
    pub encrypted_events: Mutex<HashMap<String, CachedEncryptedEvent>>,
}

impl CachedRoom {
    pub fn new(ctx: RequestContext, media_manager: MediaManager, room: Room) -> Self {
        Self {
            ctx,
            media_manager,
            room,

            messages: Mutex::new(HashMap::new()),
            encrypted_events: Mutex::new(HashMap::new()),
        }
    }

    /// Retries the decryption of the specified events.
    /// If none, all encrypted events are retried.
    pub async fn retry_decryption(&self, events: Option<BTreeSet<OwnedEventId>>) -> Result<()> {
        log::debug!(
            "Retrying decryption of events from room {:?}. Request events: {events:?}",
            self.room.room_id()
        );

        for event in self.get_events_for_redecryption(events)? {
            self.retry_cached_encrypted_event(event).await?;
        }

        Ok(())
    }

    /// Processes the given timeline event of the room.
    /// Returns the message that has been fully assembled with the relations if a message
    /// could be built using this event.
    /// Only returns an error when the cache lock is poisoined.
    pub async fn process_timeline_event(&self, event: TimelineEvent) -> Result<Option<Message>> {
        match event.kind {
            TimelineEventKind::Decrypted(event) => self.process_decrypted_event(event).await,
            TimelineEventKind::UnableToDecrypt { event, utd_info } => {
                self.process_unable_to_decrypt_event(event, utd_info)
            }
            TimelineEventKind::PlainText { event } => self.process_plain_text_event(event).await,
        }
    }

    /// Returns the message that has been fully assembled with the relations if a message
    /// could be built using this event.
    /// Only returns an error when the cache lock is poisoined.
    pub async fn process_decrypted_event(
        &self,
        event: DecryptedRoomEvent,
    ) -> Result<Option<Message>> {
        let deserialized = match event.event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::warn!("Unable to deserialize event {event:?}: {err}");
                return Ok(None);
            }
        };

        self.process_any_timeline_event(deserialized).await
    }

    fn process_unable_to_decrypt_event(
        &self,
        event: Raw<AnySyncTimelineEvent>,
        utd_info: UnableToDecryptInfo,
    ) -> Result<Option<Message>> {
        log::error!("Unable to decrypt event {event:?}: {utd_info:?}");

        let deserialized = match event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::warn!("Unable to deserialize decrypted event {event:?}: {err}");
                return Ok(None);
            }
        };

        let json_str = event.json().get().to_string();

        let encrypted_raw: Raw<OriginalSyncRoomEncryptedEvent> =
            match serde_json::from_str(&json_str) {
                Ok(raw) => raw,
                Err(e) => {
                    log::warn!("Failed to parse encrypted event: {e}");
                    return Ok(None);
                }
            };

        let event_id = deserialized.event_id().to_string();

        let message = Message {
            message_id: event_id.clone(),
            room_id: self.room.room_id().to_string(),
            timestamp: deserialized.origin_server_ts().0.into(),
            sender_id: deserialized.sender().to_string(),
            is_encrypted: true,
            ..Default::default()
        };

        let cached_object = CachedEncryptedEvent {
            event: encrypted_raw,
        };

        self.cache_encrypted_event(event_id, cached_object)?;

        Ok(Some(message))
    }

    async fn process_plain_text_event(
        &self,
        event: Raw<AnySyncTimelineEvent>,
    ) -> Result<Option<Message>> {
        let deserialized = match event.deserialize() {
            Ok(event) => event,
            Err(err) => {
                log::warn!("Unable to deserialize event {event:?}: {err}");
                return Ok(None);
            }
        };

        let full_event = deserialized.into_full_event(self.room.room_id().to_owned());

        self.process_any_timeline_event(full_event).await
    }

    async fn process_any_timeline_event(&self, event: AnyTimelineEvent) -> Result<Option<Message>> {
        match event {
            AnyTimelineEvent::MessageLike(event) => {
                self.process_any_message_like_event(event).await
            }
            AnyTimelineEvent::State(event) => self.process_any_state_event(event),
        }
    }

    async fn process_any_message_like_event(
        &self,
        event: AnyMessageLikeEvent,
    ) -> Result<Option<Message>> {
        match event {
            AnyMessageLikeEvent::RoomMessage(event) => self.process_room_message(event).await,
            AnyMessageLikeEvent::RoomRedaction(event) => self.process_room_redaction(event),
            AnyMessageLikeEvent::Reaction(event) => self.process_reaction_event(event),
            _ => Ok(None),
        }
    }

    async fn process_room_message(&self, event: RoomMessageEvent) -> Result<Option<Message>> {
        use matrix_sdk::ruma::events::room::message::Relation;

        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to care about that.
            return Ok(None);
        };

        // Replacement events are stashed until we reach the original event.
        if let Some(Relation::Replacement(relation)) = original.content.relates_to.clone() {
            let new_content = messages::generate_message_content!(
                self.media_manager,
                &self.room,
                relation.event_id,
                relation.new_content.msgtype,
                message
            );

            let Some(new_content) = new_content else {
                log::debug!("Ignoring an unsupported RoomMessageEvent content type");
                return Ok(None);
            };

            let replacement = CachedReplacement {
                timestamp: event.origin_server_ts().0.into(),
                new_content,
            };

            self.cache_replacement(
                relation.event_id.to_string(),
                original.event_id.to_string(),
                replacement,
            )?;

            return Ok(None);
        }

        self.build_from_message_event(original)
            .await
            .map(|m| Some(m))
    }

    fn process_room_redaction(&self, _event: RoomRedactionEvent) -> Result<Option<Message>> {
        // TODO: Process the redaction event and remove the appropriate event from the cache
        //   This is relevant when the same messages are requested multiple times.
        Ok(None)
    }

    fn process_reaction_event(&self, event: ReactionEvent) -> Result<Option<Message>> {
        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to care about that.
            return Ok(None);
        };

        let reaction = CachedReaction {
            user: original.sender.to_string(),
            emoji: original.content.relates_to.key.clone(),
        };

        let related_message = original.content.relates_to.event_id.to_string();

        self.cache_reaction(related_message, event.event_id().to_string(), reaction)?;

        Ok(None)
    }

    fn process_any_state_event(&self, event: AnyStateEvent) -> Result<Option<Message>> {
        match event {
            AnyStateEvent::RoomMember(event) => self.process_room_member_event(event),
            _ => Ok(None),
        }
    }

    fn process_room_member_event(&self, event: RoomMemberEvent) -> Result<Option<Message>> {
        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to carte about that.
            return Ok(None);
        };

        let Some(change) = user::convert_membership_change(&original.membership_change()) else {
            // Not a relevant membership change
            return Ok(None);
        };

        let content = MessageContentMembershipChange {
            change: change.into(),
            affected_user_id: original.state_key.to_string(),
        };

        let message = Message {
            room_id: self.room.room_id().to_string(),
            message_id: event.event_id().to_string(),
            sender_id: event.sender().to_string(),
            timestamp: event.origin_server_ts().0.into(),
            content: Some(message::Content::MembershipChange(content)),
            ..Default::default()
        };

        self.build_from_message(message).map(|m| Some(m))
    }

    fn get_events_for_redecryption(
        &self,
        events: Option<BTreeSet<OwnedEventId>>,
    ) -> Result<Vec<CachedEncryptedEvent>> {
        let guard = self.encrypted_events.lock()?;

        let events = if let Some(ref ids) = events {
            guard
                .iter()
                .filter(|(key, _)| ids.contains(key.as_str()))
                .map(|(_, value)| value)
                .cloned()
                .collect()
        } else {
            guard.values().cloned().collect()
        };

        Ok(events)
    }

    /// Retries to decrypt and process the given event.
    async fn retry_cached_encrypted_event(&self, event: CachedEncryptedEvent) -> Result<()> {
        log::debug!("Retrying decryption of event: {event:?}");

        let event = match self.room.decrypt_event(&event.event, None).await {
            Ok(event) => event,
            Err(err) => {
                log::warn!("Error retrying decryption for event {event:?}: {err}");
                return Ok(());
            }
        };

        if matches!(event.kind, TimelineEventKind::UnableToDecrypt { .. }) {
            log::warn!("Unknown error retrying decryption for event {event:?}");
            return Ok(());
        }

        let Some(event_id) = event.event_id() else {
            return Ok(());
        };

        let message = self.process_timeline_event(event).await?;

        if let Some(message) = message {
            self.process_successfull_redecryption(message).await?;
        } else {
            self.redact_encrypted_message(event_id.to_string()).await;
        }

        Ok(())
    }

    /// Sends a message update event to the application with the now encrypted content.
    async fn process_successfull_redecryption(&self, message: Message) -> Result<()> {
        let Some(content) = message.content else {
            return Ok(());
        };

        let event = MessageChangeEventBuilder::new(message.room_id, message.message_id)
            .change_content(content.into())
            .change_is_encrypted(false)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::MessageChangeEvent(event))
            .await;

        Ok(())
    }

    /// Sends a remove event to the application to redact a previously encrypted message,
    /// which after decryption is no longer relevant for the application.
    async fn redact_encrypted_message(&self, message_id: String) {
        let proto = MessageRemoveEvent {
            message_id,
            room_id: self.room.room_id().to_string(),
            origin: EventOrigin::BackendOrigin.into(),
            ..Default::default()
        };

        self.ctx
            .send_event(ResponseContent::MessageRemoveEvent(proto))
            .await;
    }

    /// Converts the given event to the original message object and
    /// builds the final message with the cached relations.
    /// Only returns an error when the cache lock is posoined or the message receiver dropped.
    async fn build_from_message_event(
        &self,
        event: &OriginalMessageLikeEvent<RoomMessageEventContent>,
    ) -> Result<Message> {
        let msg = messages::message_from_event(&self.media_manager, &self.room, event).await;
        self.build_from_message(msg)
    }

    /// Caches the given original message and assembles the final message object
    /// with the cached related events. Sends assembled message to the message receiver.
    /// Only returns an error when the cache lock is posoined or the message receiver dropped.
    fn build_from_message(&self, message: Message) -> Result<Message> {
        let message = self.cache_and_build_message(message)?;
        Ok(message)
    }

    /// Caches the given replacement.
    /// Only returns an error when the cache lock is posoined.
    fn cache_replacement(
        &self,
        original_message_id: String,
        replacement_id: String,
        replacement: CachedReplacement,
    ) -> Result<()> {
        self.remove_encrypted_event(&replacement_id)?;

        let mut guard = self.messages.lock()?;
        let message = guard.entry(original_message_id).or_default();
        message.replacements.insert(replacement_id, replacement);

        Ok(())
    }

    /// Caches the given reaction.
    /// Only returns an error when the cache lock is posoined.
    fn cache_reaction(
        &self,
        original_message_id: String,
        reaction_id: String,
        reaction: CachedReaction,
    ) -> Result<()> {
        self.remove_encrypted_event(&reaction_id)?;

        let mut guard = self.messages.lock()?;
        let message = guard.entry(original_message_id).or_default();
        message.reactions.insert(reaction_id, reaction);

        Ok(())
    }

    /// Caches the given original message and builds the final message
    /// with the cached related events.
    /// Only returns an error when the cache lock is posoined.
    fn cache_and_build_message(&self, original: Message) -> Result<Message> {
        self.remove_encrypted_event(&original.message_id)?;

        let mut guard = self.messages.lock()?;

        let cached_message = guard.entry(original.message_id.clone()).or_default();

        Ok(cached_message.build_from_original(original))
    }

    /// Caches the given encrypted event.
    /// Only returns an error when the cache lock is poisoined.
    fn cache_encrypted_event(&self, event_id: String, event: CachedEncryptedEvent) -> Result<()> {
        let mut guard = self.encrypted_events.lock()?;
        guard.insert(event_id, event);
        Ok(())
    }

    /// Removes a tracked encrypted event, if it exists.
    /// Only returns an error when the cache lock is poisoined.
    fn remove_encrypted_event(&self, event_id: &String) -> Result<()> {
        let mut guard = self.encrypted_events.lock()?;
        guard.remove(event_id);
        Ok(())
    }
}

struct MessageFetcher {
    /// The room to use to fetch messages.
    cache: Arc<CachedRoom>,

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

impl MessageFetcher {
    pub fn new(
        cache: Arc<CachedRoom>,
        sender: Sender<Result<Message>>,
        limit: u32,
        from_token: Option<String>,
    ) -> Self {
        Self {
            cache,

            sender,
            message_limit: limit,

            from_token,
            chunk_size: calc_chunk_size(limit),

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
        log::debug!(
            "Fetching events in chunks of {} to retrieve {} messages",
            self.chunk_size,
            self.message_limit
        );

        while self.retrieved_messages < self.message_limit {
            log::debug!("Fetching next chunk of events");

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
            if let Some(message) = self.cache.process_timeline_event(event).await? {
                self.send_finished_message(message).await?;
            }

            if self.retrieved_messages == self.message_limit {
                log::debug!("Reached the number of requested messages, aborting chunk processing");
                break;
            }
        }

        Ok(())
    }

    /// Sends the message to the message receiver.
    /// Only returns an error when the message receiver dropped.
    async fn send_finished_message(&mut self, message: Message) -> Result<()> {
        self.sender
            .send(Ok(message))
            .await
            .map_err(|_| MessageCacheError::MessageReceiverDropped)?;

        self.retrieved_messages += 1;

        Ok(())
    }
}

/// Retrieves the correct pagination token to start pagination from the specified event.
async fn resolve_event_for_pagination(room: &Room, event_id: &EventId) -> Result<Option<String>> {
    let response = room
        .event_with_context(event_id, true, js_int::uint!(0), None)
        .await?;

    Ok(response.prev_batch_token)
}

//// Calculates the chunk size of events to retrieve.
fn calc_chunk_size(limit: u32) -> js_int::UInt {
    let estimated_events = (limit as f64) * 1.5;
    let mut chunk_size = (estimated_events * 0.2).floor() as u32;

    if chunk_size < ROOM_EVENTS_CHUNK_SIZE_MIN {
        chunk_size = ROOM_EVENTS_CHUNK_SIZE_MIN
    } else if chunk_size > ROOM_EVENTS_CHUNK_SIZE_MAX {
        chunk_size = ROOM_EVENTS_CHUNK_SIZE_MAX
    }

    chunk_size.into()
}
