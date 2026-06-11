use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gouda_proto::chat::{message, Message, MessagesOrder, Reaction};
use matrix_sdk::deserialized_responses::{
    DecryptedRoomEvent, TimelineEvent, TimelineEventKind, UnableToDecryptInfo,
};
use matrix_sdk::ruma::events::message::{MessageEvent, MessageEventContentWithoutRelation};
use matrix_sdk::ruma::events::reaction::{ReactionEvent, ReactionEventContent};
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::member::RoomMemberEvent;
use matrix_sdk::ruma::events::room::message::{
    RoomMessageEvent, RoomMessageEventContentWithoutRelation,
};
use matrix_sdk::ruma::events::room::redaction::RoomRedactionEvent;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEvent, AnyStateEvent, AnySyncTimelineEvent, AnyTimelineEvent,
    BundledMessageLikeRelations,
};
use ruma_common::api::Direction;
use ruma_common::serde::Raw;
use ruma_common::{OwnedEventId, OwnedRoomId, RoomId};
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;

use crate::messages;

const MESSAGES_CHANNEL_CAPACITY: usize = 32;
const ROOM_EVENTS_CHUNK_SIZE: u32 = 10;

#[derive(Debug, thiserror::Error)]
pub enum MatrixMessageBridgeError {
    #[error("decryption error")]
    UnableToDecrypt(UnableToDecryptInfo),

    #[error("receiver of the messages dropped")]
    ReceiverDropped,

    #[error("matrix sdk error: {0}")]
    MatrixError(#[from] matrix_sdk::Error),
}

pub type Result<T> = std::result::Result<T, MatrixMessageBridgeError>;

pub struct QueryOptions {
    pub limit: u32,
    pub from_message_id: Option<OwnedEventId>,
    pub order: MessagesOrder,
}

/// Fetches and builds messages from the matrix server.
/// This struct does not handle caching and acts as a low level bridge
/// between matrix and our own API.
pub struct MatrixMessageBridge {
    room: Arc<dyn RoomAbstraction>,
}

impl MatrixMessageBridge {
    pub fn from_matrix_room(room: matrix_sdk::Room) -> Self {
        Self {
            room: Arc::new(room),
        }
    }

    pub fn fetch_messages(&self, options: QueryOptions) -> ReceiverStream<Result<Message>> {
        let (tx, rx) = tokio::sync::mpsc::channel(MESSAGES_CHANNEL_CAPACITY);

        let room = self.room.clone();

        if options.from_message_id.is_some() {
            todo!("Implement retrieval of the correct pagination token");
        }

        let direction = match options.order {
            MessagesOrder::Backward => Direction::Backward,
            MessagesOrder::Forward => Direction::Forward,
        };

        if direction == Direction::Forward {
            // This requires more work in the MessageFetcher....
            todo!("Forward direction is currently not implemented");
        }

        tokio::spawn(async move {
            let fetcher = MessageFetcher::new(room, tx, options.limit, None, direction);
            fetcher.fetch_messages().await;
        });

        ReceiverStream::new(rx)
    }
}

#[async_trait]
trait RoomAbstraction: Send + Sync {
    fn get_room_id(&self) -> &RoomId;

    async fn fetch_events(
        &self,
        options: matrix_sdk::room::MessagesOptions,
    ) -> matrix_sdk::Result<matrix_sdk::room::Messages>;
}

#[async_trait]
impl RoomAbstraction for matrix_sdk::Room {
    fn get_room_id(&self) -> &RoomId {
        self.room_id()
    }

    async fn fetch_events(
        &self,
        options: matrix_sdk::room::MessagesOptions,
    ) -> matrix_sdk::Result<matrix_sdk::room::Messages> {
        log::debug!("Fetching events from matrix server with: {options:?}");
        self.messages(options).await
    }
}

#[derive(Debug)]
enum MessageRelation {
    Replacement(ReplacementRelation),
    Reaction(ReactionRelation),
}

#[derive(Debug)]
struct ReplacementRelation {
    /// TODO: This is just for testing and should be the correct generated content of the message.
    pub new_content: String,
}

#[derive(Debug)]
struct ReactionRelation {
    /// The user that reacted.
    pub user: String,
    /// The emoji the user reacted with.
    pub key: String,
}

struct MessageFetcher {
    /// From where to fetch events.
    room: Arc<dyn RoomAbstraction>,
    /// Where to send finished messages.
    sender: Sender<Result<Message>>,
    /// How many messages should be fetched.
    message_limit: u32,

    /// The pagination token for the next chunk.
    from_token: Option<String>,
    /// The direction for every request.
    direction: Direction,
    /// How many events are fetched with each request.
    chunk_size: js_int::UInt,

    /// Stashed child events that have not yet been assembled into a complete message.
    /// The events are grouped by the parent event they are referencing.
    /// The order of the events is indeterminate.
    event_stash: HashMap<String, Vec<MessageRelation>>,
    /// The number of chat messages we have build and send to the message receiver.
    retrieved_messages: u32,
}

impl MessageFetcher {
    pub fn new(
        room: Arc<dyn RoomAbstraction>,
        sender: Sender<Result<Message>>,
        limit: u32,
        from_token: Option<String>,
        direction: Direction,
    ) -> Self {
        Self {
            room,
            sender,
            message_limit: limit,

            from_token,
            direction,
            chunk_size: ROOM_EVENTS_CHUNK_SIZE.into(),

            event_stash: HashMap::new(),
            retrieved_messages: 0,
        }
    }

    pub async fn fetch_messages(mut self) {
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
                self.room.fetch_events(options).await?;

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
        let mut options = matrix_sdk::room::MessagesOptions::new(self.direction);
        options.from = self.from_token.clone();
        options.limit = self.chunk_size;
        options
    }

    async fn process_event_chunk(&mut self, chunk: Vec<TimelineEvent>) -> Result<()> {
        // TODO:
        // - Add the retrieved events to some hash map and group them by parent event
        // - Check if we have retrieved a parent event, if so assemble the final message
        //   with all child events
        // - Set the token for the next chunk

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
            room_id: self.room.get_room_id().to_string(),
            message_id: deserialized.event_id().to_string(),
            timestamp: deserialized.origin_server_ts().0.into(),
            sender_id: deserialized.sender().to_string(),
            is_encrypted: true,
            ..Default::default()
        };

        self.send_finished_message(message).await?;

        Err(MatrixMessageBridgeError::UnableToDecrypt(utd_info))
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

        let full_event = deserialized.into_full_event(self.room.get_room_id().to_owned());

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
        if let Some(Relation::Replacement(relation)) = &original.content.relates_to {
            let replacement = ReplacementRelation {
                new_content: String::from("New content!"),
            };

            self.stash_replacement(relation.event_id.to_string(), replacement);

            return Ok(());
        }

        self.build_and_send_message(event).await
    }

    fn process_room_redaction(&self, event: RoomRedactionEvent) -> Result<()> {
        // We can probably just ignore the redaction events.
        // Maybe cleanup the stash of the redacted event?
        Ok(())
    }

    fn process_reaction_event(&mut self, event: ReactionEvent) -> Result<()> {
        let Some(original) = event.as_original() else {
            // Redacted event, we don't need to care about that.
            return Ok(());
        };

        let reaction = ReactionRelation {
            key: original.content.relates_to.key.clone(),
            user: original.sender.to_string(),
        };

        let related_message = original.content.relates_to.event_id.to_string();

        self.stash_reaction(related_message, reaction);

        Ok(())
    }

    async fn process_any_state_event(&self, event: AnyStateEvent) -> Result<()> {
        match event {
            AnyStateEvent::RoomMember(event) => self.process_room_member_event(event).await,
            _ => Ok(()),
        }
    }

    async fn process_room_member_event(&self, event: RoomMemberEvent) -> Result<()> {
        // TODO: Implement room member event.
        Ok(())
    }

    fn stash_replacement(&mut self, original_message_id: String, new: ReplacementRelation) {
        let related_events = self.event_stash.entry(original_message_id).or_default();
        related_events.push(MessageRelation::Replacement(new));
    }

    fn stash_reaction(&mut self, original_message_id: String, reaction: ReactionRelation) {
        let related_events = self.event_stash.entry(original_message_id).or_default();
        related_events.push(MessageRelation::Reaction(reaction));
    }

    async fn build_and_send_message(&mut self, event: RoomMessageEvent) -> Result<()> {
        let message_id = event.event_id().to_string();

        let message = if let Some(related) = self.event_stash.remove(&message_id) {
            MessageBuilder::relations(event, related)
        } else {
            MessageBuilder::single(event)
        };

        self.send_finished_message(message).await
    }

    async fn send_finished_message(&mut self, message: Message) -> Result<()> {
        self.sender
            .send(Ok(message))
            .await
            .map_err(|_| MatrixMessageBridgeError::ReceiverDropped)?;

        self.retrieved_messages += 1;

        Ok(())
    }
}

struct MessageBuilder {}

impl MessageBuilder {
    /// Builds a message from a single RoomMessageEvent.
    pub fn single(event: RoomMessageEvent) -> Message {
        println!("BUILDING_SINGLE_MESSAGE: {event:?}");
        // TODO
        Message::default()
    }

    /// Builds a message from the original RoomMessageEvent and
    /// all of its related events.
    pub fn relations(original: RoomMessageEvent, related: Vec<MessageRelation>) -> Message {
        println!("BUILDING_MESSAGE_WITH_RELATIONS: {original:?}, RELATED: {related:?}");
        // TODO
        Message::default()
    }
}
