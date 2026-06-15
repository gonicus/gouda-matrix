use std::collections::HashMap;
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
use matrix_sdk::ruma::events::reaction::{OriginalSyncReactionEvent, ReactionEvent};
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

/// The capacity of the channel for receiving retrieved and assembled messages.
const MESSAGES_CHANNEL_CAPACITY: usize = 10;

/// How many events to fetch at least with each chunk.
const ROOM_EVENTS_CHUNK_SIZE_MIN: u32 = 10;
/// How many events to fetch at most with each chunk.
const ROOM_EVENTS_CHUNK_SIZE_MAX: u32 = 100;

const _: () = assert!(ROOM_EVENTS_CHUNK_SIZE_MIN <= ROOM_EVENTS_CHUNK_SIZE_MAX);

#[derive(Debug, thiserror::Error)]
pub enum MemoryCacheError {
    #[error("receiver of the messages dropped")]
    MessageReceiverDropped,

    #[error("cache lock poisoined")]
    CachePoisoined,

    #[error("matrix sdk error: {0}")]
    MatrixError(#[from] matrix_sdk::Error),
}

impl<T> From<std::sync::PoisonError<T>> for MemoryCacheError {
    fn from(_value: std::sync::PoisonError<T>) -> Self {
        Self::CachePoisoined
    }
}

pub type Result<T> = std::result::Result<T, MemoryCacheError>;

pub struct QueryOptions {
    /// How many assembled messages should be returned.
    pub limit: u32,
    /// The ID of the message from where to begin fetching messages.
    pub from_message_id: Option<OwnedEventId>,
}

#[derive(Clone)]
pub struct MemoryCache {
    inner: Arc<MemoryCacheInner>,
}

impl MemoryCache {
    pub fn new(ctx: RequestContext, media_manager: MediaManager) -> Self {
        let inner = MemoryCacheInner::new(ctx, media_manager);

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Fetches the assembled messages from the room with the given options.
    pub async fn fetch_messages(
        &self,
        room: Room,
        options: QueryOptions,
    ) -> Result<ReceiverStream<Result<Message>>> {
        self.inner.fetch_messages(room, options).await
    }

    /// Retries to decrypt previously unencryptable events.
    ///
    /// # Arguments
    ///
    /// * `room_id`: The ID of the room the events belong to.
    /// * `session_id`: The ID of the session used to encrypt the events.
    pub async fn retry_encrypted_events(&self, room_id: impl Into<String>, session_id: String) {
        let result = self
            .inner
            .retry_encrypted_events(room_id.into(), session_id)
            .await;

        if let Err(err) = result {
            log::error!("Error retrying decryption of events: {err}");
        }
    }

    pub fn cache_reaction(&self, room: Room, event: OriginalSyncReactionEvent) {
        if let Err(err) = self.inner.cache_reaction(room, event) {
            log::error!("Error caching reaction: {err}");
        }
    }

    pub fn remove_reaction_by_id(
        &self,
        room_id: impl AsRef<str>,
        reaction_id: impl AsRef<str>,
    ) -> Option<ReactionMetadata> {
        let result = self
            .inner
            .remove_reaction_by_id(room_id.as_ref(), reaction_id.as_ref());

        result.unwrap_or_else(|err| {
            log::error!("Error removing reaction by id: {err}");
            None
        })
    }

    pub fn remove_reaction_by_emoji(
        &self,
        room_id: impl AsRef<str>,
        message_id: impl AsRef<str>,
        user: impl AsRef<str>,
        emoji: impl AsRef<str>,
    ) -> Option<ReactionMetadata> {
        let result = self.inner.remove_reaction_by_emoji(
            message_id.as_ref(),
            room_id.as_ref(),
            user.as_ref(),
            emoji.as_ref(),
        );

        result.unwrap_or_else(|err| {
            log::error!("Error removing reaction by id: {err}");
            None
        })
    }
}

struct MemoryCacheInner {
    ctx: RequestContext,
    media_manager: MediaManager,
    cached_rooms: Mutex<HashMap<String, Arc<CachedRoom>>>,
}

impl MemoryCacheInner {
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

    pub async fn retry_encrypted_events(&self, room_id: String, session_id: String) -> Result<()> {
        let guard = self.cached_rooms.lock()?;

        let Some(room) = guard.get(&room_id).cloned() else {
            return Ok(());
        };

        tokio::spawn(async move {
            if let Err(err) = room.retry_decryption(session_id).await {
                log::error!("Error retrying decryption of room events: {err}");
            }
        });

        Ok(())
    }

    pub fn cache_reaction(&self, room: Room, event: OriginalSyncReactionEvent) -> Result<()> {
        let cached_room = self.get_or_create_room(room)?;

        let message_id = event.content.relates_to.event_id.to_string();
        let reaction_id = event.event_id.to_string();

        let reaction = CachedReaction {
            user_id: event.sender.to_string(),
            emoji: event.content.relates_to.key,
        };

        log::debug!("Caching reaction {reaction:?} for message {message_id}");

        cached_room.cache_reaction(message_id, reaction_id, reaction)
    }

    pub fn remove_reaction_by_id(
        &self,
        room_id: &str,
        reaction_id: &str,
    ) -> Result<Option<ReactionMetadata>> {
        let Some(cached_room) = self.get_room(room_id)? else {
            return Ok(None);
        };

        cached_room.remove_reaction_by_id(reaction_id)
    }

    pub fn remove_reaction_by_emoji(
        &self,
        room_id: &str,
        message_id: &str,
        user_id: &str,
        emoji: &str,
    ) -> Result<Option<ReactionMetadata>> {
        let Some(cached_room) = self.get_room(room_id)? else {
            return Ok(None);
        };

        cached_room.remove_reaction_by_emoji(message_id, user_id, emoji)
    }

    fn get_room(&self, room_id: &str) -> Result<Option<Arc<CachedRoom>>> {
        Ok(self.cached_rooms.lock()?.get(room_id).cloned())
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

/// Contains metadata to a reaction.
#[derive(Debug, Clone)]
pub struct ReactionMetadata {
    pub reaction_id: String,
    pub user_id: String,
    pub emoji: String,
    pub message_id: String,
    pub room_id: String,
}

impl ReactionMetadata {
    pub(self) fn new(
        cached: CachedReaction,
        reaction_id: String,
        message_id: String,
        room_id: String,
    ) -> Self {
        let CachedReaction { user_id, emoji } = cached;

        Self {
            reaction_id,
            user_id,
            emoji,
            message_id,
            room_id,
        }
    }
}

#[derive(Debug, Clone)]
struct CachedReplacement {
    /// The timestamp when the replacement event was created.
    pub timestamp: u64,
    /// The new content replacing the original or any other related
    /// replacement event.
    pub new_content: message::Content,
}

#[derive(Debug, Clone)]
struct CachedReaction {
    /// The ID of the user who reacted.
    pub user_id: String,
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
                user_id: Some(reaction.user_id.clone()),
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
    /// The original event we where not able to decrypt.
    pub event: Raw<OriginalSyncRoomEncryptedEvent>,
    /// The ID of the session used to encrypt the message, if it used the
    /// `m.megolm.v1.aes-sha2` algorithm.
    pub session_id: Option<String>,
}

/// An action that was executed when processing an event.
#[derive(Debug)]
enum CachedRoomAction {
    /// A message could be build from the event.
    Message(Message),
    /// A reaction could be build from the event.
    Reaction(ReactionMetadata),
}

struct CachedRoom {
    /// The context to use to send update events to the application.
    ctx: RequestContext,
    /// The media manager to use to download message attachments.
    media_manager: MediaManager,
    /// The room we work with.
    room: Room,

    /// The messages we have cached.
    pub messages: Mutex<HashMap<String, CachedMessage>>,
    /// Events that we could not decrypt and that were sent to the application
    /// as an encrypted message.
    pub encrypted_events: Mutex<HashMap<String, CachedEncryptedEvent>>,

    /// Maps a reaction ID to a message ID.
    /// (reaction_id, message_id)
    reaction_id_to_message: Mutex<HashMap<String, String>>,
}

impl CachedRoom {
    pub fn new(ctx: RequestContext, media_manager: MediaManager, room: Room) -> Self {
        Self {
            ctx,
            media_manager,
            room,

            messages: Mutex::new(HashMap::new()),
            encrypted_events: Mutex::new(HashMap::new()),

            reaction_id_to_message: Mutex::new(HashMap::new()),
        }
    }

    /// Retries the decryption of the specified events.
    /// If none, all encrypted events are retried.
    pub async fn retry_decryption(&self, session_id: String) -> Result<()> {
        log::debug!(
            "Retrying decryption of events from room {:?} for session_id: {session_id}",
            self.room.room_id()
        );

        for event in self.get_events_for_redecryption(session_id)? {
            self.retry_cached_encrypted_event(event).await?;
        }

        Ok(())
    }

    /// Processes the given timeline event of the room.
    /// Returns the message that has been fully assembled with the relations if a message
    /// could be built using this event.
    /// Only returns an error when the cache lock is poisoined.
    pub async fn process_timeline_event(
        &self,
        event: TimelineEvent,
    ) -> Result<Option<CachedRoomAction>> {
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
    ) -> Result<Option<CachedRoomAction>> {
        log::debug!("Processing decrypted event");

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
    ) -> Result<Option<CachedRoomAction>> {
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
            session_id: utd_info.session_id,
        };

        self.cache_encrypted_event(event_id, cached_object)?;

        Ok(Some(CachedRoomAction::Message(message)))
    }

    async fn process_plain_text_event(
        &self,
        event: Raw<AnySyncTimelineEvent>,
    ) -> Result<Option<CachedRoomAction>> {
        log::debug!("Processing plain text event");

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

    async fn process_any_timeline_event(
        &self,
        event: AnyTimelineEvent,
    ) -> Result<Option<CachedRoomAction>> {
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
    ) -> Result<Option<CachedRoomAction>> {
        match event {
            AnyMessageLikeEvent::RoomMessage(event) => self.process_room_message(event).await,
            AnyMessageLikeEvent::RoomRedaction(event) => self.process_room_redaction(event),
            AnyMessageLikeEvent::Reaction(event) => self.process_reaction_event(event),
            _ => Ok(None),
        }
    }

    async fn process_room_message(
        &self,
        event: RoomMessageEvent,
    ) -> Result<Option<CachedRoomAction>> {
        use matrix_sdk::ruma::events::room::message::Relation;

        log::debug!("Processing RoomMessageEvent");

        let Some(original) = event.as_original() else {
            log::debug!("Event is redacted, nothing to do");
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
            .map(|msg| Some(CachedRoomAction::Message(msg)))
    }

    fn process_room_redaction(
        &self,
        _event: RoomRedactionEvent,
    ) -> Result<Option<CachedRoomAction>> {
        // TODO: Process the redaction event and remove the appropriate event from the cache
        //   This is only relevant when the same messages are requested multiple times.
        Ok(None)
    }

    fn process_reaction_event(&self, event: ReactionEvent) -> Result<Option<CachedRoomAction>> {
        log::debug!("Processing ReactionEvent");

        let Some(original) = event.as_original() else {
            log::debug!("Event is redacted, nothing to do");
            return Ok(None);
        };

        let reaction = CachedReaction {
            user_id: original.sender.to_string(),
            emoji: original.content.relates_to.key.clone(),
        };

        let reaction_id = event.event_id().to_string();
        let message_id = original.content.relates_to.event_id.to_string();

        log::debug!("Caching reaction: {reaction:?}");

        self.cache_reaction(message_id.clone(), reaction_id.clone(), reaction.clone())?;

        let metadata = ReactionMetadata::new(
            reaction,
            reaction_id,
            message_id,
            self.room.room_id().to_string(),
        );

        log::debug!("Returning reaction metadata: {metadata:?}");

        Ok(Some(CachedRoomAction::Reaction(metadata)))
    }

    fn process_any_state_event(&self, event: AnyStateEvent) -> Result<Option<CachedRoomAction>> {
        match event {
            AnyStateEvent::RoomMember(event) => self.process_room_member_event(event),
            _ => Ok(None),
        }
    }

    fn process_room_member_event(
        &self,
        event: RoomMemberEvent,
    ) -> Result<Option<CachedRoomAction>> {
        log::debug!("Processing RoomMemberEvent");

        let Some(original) = event.as_original() else {
            log::debug!("Event is redacted, nothing to do");
            return Ok(None);
        };

        let Some(change) = user::convert_membership_change(&original.membership_change()) else {
            log::debug!("Event does not contain a relevant membership change, nothing to do");
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

        log::debug!("Build original membership change message: {message:?}");

        self.build_from_message(message)
            .map(|msg| Some(CachedRoomAction::Message(msg)))
    }

    fn get_events_for_redecryption(&self, session_id: String) -> Result<Vec<CachedEncryptedEvent>> {
        let guard = self.encrypted_events.lock()?;

        let events = guard
            .iter()
            .filter(|p| p.1.session_id.as_ref() == Some(&session_id))
            .map(|p| p.1.clone())
            .collect();

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
            log::warn!("Successfully decrypted event but event does not have an ID");
            return Ok(());
        };

        log::debug!("Processing successfully decrypted event: {event:?}");

        let action = self.process_timeline_event(event).await?;

        log::debug!("Received action after processing event: {action:?}");

        if let Some(action) = action {
            self.process_successful_redecryption(action).await?;
        } else {
            self.redact_encrypted_message(event_id.to_string()).await;
        }

        Ok(())
    }

    /// Processes the successful redecryption of a event.
    /// This sends updates events to the application.
    async fn process_successful_redecryption(&self, action: CachedRoomAction) -> Result<()> {
        match action {
            CachedRoomAction::Message(message) => {
                self.process_successful_redecrypted_message(message).await
            }
            CachedRoomAction::Reaction(reaction) => {
                self.process_successful_redecrypted_reaction(reaction).await
            }
        }
    }

    /// Sends a message update event to the application with the now encrypted content.
    async fn process_successful_redecrypted_message(&self, message: Message) -> Result<()> {
        log::debug!("Processing successful redecryption of message: {message:?}");

        let Some(content) = message.content else {
            log::debug!("Redacting previously send message as decrypted message has no content");
            self.redact_encrypted_message(message.message_id).await;
            return Ok(());
        };

        log::debug!("Sending MessageChangeEvent to application");

        let event = MessageChangeEventBuilder::new(message.room_id, message.message_id)
            .change_content(content.into())
            .change_is_encrypted(false)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::MessageChangeEvent(event))
            .await;

        Ok(())
    }

    /// Redacts the previously send encrypted message and sends a
    /// reaction created event afterwards.
    async fn process_successful_redecrypted_reaction(
        &self,
        reaction: ReactionMetadata,
    ) -> Result<()> {
        log::debug!("Processing successful redecryption of reaction: {reaction:?}");

        let ReactionMetadata {
            reaction_id,
            user_id,
            emoji,
            message_id,
            room_id,
        } = reaction;

        self.redact_encrypted_message(reaction_id).await;

        let proto = Reaction {
            room_id,
            message_id,
            reaction: emoji,
            user_id: Some(user_id),
        };

        self.ctx
            .send_event(ResponseContent::ReactionCreatedEvent(proto))
            .await;

        Ok(())
    }

    /// Sends a remove event to the application to redact a previously encrypted message,
    /// which after decryption is no longer relevant for the application.
    async fn redact_encrypted_message(&self, message_id: String) {
        log::debug!(
            "Sending MessageRemoveEvent to redact previously encrypted message: {message_id}"
        );

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
    /// Only returns an error when the cache lock is poisoined.
    pub fn cache_reaction(
        &self,
        message_id: String,
        reaction_id: String,
        reaction: CachedReaction,
    ) -> Result<()> {
        self.remove_encrypted_event(&reaction_id)?;

        let mut guard = self.messages.lock()?;
        let message = guard.entry(message_id.clone()).or_default();
        message.reactions.insert(reaction_id.clone(), reaction);

        let mut guard = self.reaction_id_to_message.lock()?;
        guard.insert(reaction_id, message_id);

        Ok(())
    }

    /// Removes the given reaction by id.
    /// Only returns an error when the cache lock is poisoined.
    pub fn remove_reaction_by_id(&self, reaction_id: &str) -> Result<Option<ReactionMetadata>> {
        let Some(message_id) = self.reaction_id_to_message_id(reaction_id)? else {
            return Ok(None);
        };

        let mut guard = self.messages.lock()?;

        let Some(message) = guard.get_mut(&message_id) else {
            return Ok(None);
        };

        let Some(removed) = message.reactions.remove(reaction_id) else {
            return Ok(None);
        };

        let metadata = ReactionMetadata::new(
            removed,
            reaction_id.to_string(),
            message_id,
            self.room.room_id().to_string(),
        );

        Ok(Some(metadata))
    }

    /// Removes the given reaction by user and emoji.
    /// Only returns an error when the cache lock is poisoined.
    pub fn remove_reaction_by_emoji(
        &self,
        message_id: &str,
        user_id: &str,
        emoji: &str,
    ) -> Result<Option<ReactionMetadata>> {
        let mut guard = self.messages.lock()?;

        let Some(message) = guard.get_mut(message_id) else {
            return Ok(None);
        };

        let removed: Vec<(String, CachedReaction)> = message
            .reactions
            .extract_if(|_, p| p.user_id == user_id && p.emoji == emoji)
            .collect();

        let Some(removed) = removed.into_iter().next() else {
            return Ok(None);
        };

        let metadata = ReactionMetadata::new(
            removed.1,
            removed.0,
            message_id.to_owned(),
            self.room.room_id().to_string(),
        );

        Ok(Some(metadata))
    }

    /// Gets the message_id a  reaction belongs to.
    fn reaction_id_to_message_id(&self, reaction_id: &str) -> Result<Option<String>> {
        Ok(self
            .reaction_id_to_message
            .lock()?
            .get(reaction_id)
            .cloned())
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
            let action = self.cache.process_timeline_event(event).await?;

            // We only need to act on assembled messages, as the reactions
            // are already included.
            if let Some(CachedRoomAction::Message(message)) = action {
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
            .map_err(|_| MemoryCacheError::MessageReceiverDropped)?;

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

/// Calculates the chunk size of events to retrieve.
fn calc_chunk_size(limit: u32) -> js_int::UInt {
    let estimated_events = (limit as f64) * 1.5;
    let chunk_size = (estimated_events * 0.2).floor() as u32;
    let chunk_size = chunk_size.clamp(ROOM_EVENTS_CHUNK_SIZE_MIN, ROOM_EVENTS_CHUNK_SIZE_MAX);
    chunk_size.into()
}
