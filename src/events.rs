use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use gouda_core::RequestContext;
use gouda_proto::chat::builder::RoomChangeEventBuilder;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::room_left_event::RoomLeaveReason;
use gouda_proto::chat::{Reaction as ChatReaction, *};
use matrix_sdk::deserialized_responses::TimelineEventKind;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::fully_read::FullyReadEvent;
use matrix_sdk::ruma::events::presence::PresenceEvent;
use matrix_sdk::ruma::events::reaction::OriginalSyncReactionEvent;
use matrix_sdk::ruma::events::receipt::SyncReceiptEvent;
use matrix_sdk::ruma::events::relation::Replacement;
use matrix_sdk::ruma::events::room::avatar::OriginalSyncRoomAvatarEvent;
use matrix_sdk::ruma::events::room::join_rules::OriginalSyncRoomJoinRulesEvent;
use matrix_sdk::ruma::events::room::member::{
    Change, MembershipChange, MembershipState, OriginalSyncRoomMemberEvent, StrippedRoomMemberEvent,
};
use matrix_sdk::ruma::events::room::message::{
    OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContentWithoutRelation,
};
use matrix_sdk::ruma::events::room::name::OriginalSyncRoomNameEvent;
use matrix_sdk::ruma::events::room::pinned_events::OriginalSyncRoomPinnedEventsEvent;
use matrix_sdk::ruma::events::room::redaction::OriginalSyncRoomRedactionEvent;
use matrix_sdk::ruma::events::tag::{TagEvent, TagName};
use matrix_sdk::ruma::events::{
    AnyEphemeralRoomEventContent, AnyMessageLikeEvent, AnySyncTimelineEvent, AnyTimelineEvent,
};
use matrix_sdk::sync::JoinedRoomUpdate;
use matrix_sdk::{Client, Room, RoomState};
use ruma_common::serde::Raw;
use ruma_common::{MilliSecondsSinceUnixEpoch, MxcUri, OwnedRoomId, OwnedUserId, RoomId};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::bridge::{IntoChat, TryIntoChat};
use crate::media::MediaManager;
use crate::memory_cache::{MemoryCache, ReactionMetadata};
use crate::rooms::RoomsManager;
use crate::{messages, rooms, unwrap_or_log_return};

/// After how many seconds does an event count as historical?
const HISTORICAL_EVENT_TIMEOUT: u64 = 5;

/// How many room change events should be queued per room?
const MAX_QUEUED_ROOM_CHANGES: usize = 15;

macro_rules! impl_room_event_handler {
    ($event:ident, $handler_name:ident, $processor_name:ident) => {
        async fn $handler_name(event: $event, room: Room, event_manager: Ctx<EventManager>) {
            event_manager.$processor_name(room, event);
        }
    };
}

macro_rules! impl_event_handler {
    ($event:ident, $handler_name:ident, $processor_name:ident) => {
        async fn $handler_name(event: $event, event_manager: Ctx<EventManager>) {
            event_manager.$processor_name(event);
        }
    };
}

macro_rules! skip_historical_event {
    ($event:ident) => {
        if is_historical_event($event.origin_server_ts) {
            log::debug!("Ignoring event as it is older than {HISTORICAL_EVENT_TIMEOUT} seconds");
            return;
        }
    };
}

/// Checks if the event at the given timestamp counts as historical.
/// This is to prevent us from responding to old state events,
/// as the chat is not designed for such use.
/// This is currently only used for room state events.
fn is_historical_event(origin_server_ts: MilliSecondsSinceUnixEpoch) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    now.saturating_sub(origin_server_ts.as_secs().into()) > HISTORICAL_EVENT_TIMEOUT
}

/// Manages incoming events from matrix.
/// The `EventManager` is designed to be cloned. All state is held in a separate
/// `EventExecutor` object which is accessed using an unbounded channel.
#[derive(Clone)]
pub struct EventManager {
    /// Used to retrieve media based on events, for example if a room
    /// avatar changes.
    media_manager: MediaManager,
    /// Sender to send requested actions to the event executor.
    action_sender: UnboundedSender<Action>,
}

impl EventManager {
    pub fn new(
        client: Client,
        ctx: RequestContext,
        memory_cache: MemoryCache,
        media_manager: MediaManager,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let manager = Self {
            media_manager: media_manager.clone(),
            action_sender: tx,
        };

        EventExecutor::new(client, ctx, rx, memory_cache, media_manager).run();

        manager
    }

    /// Setup all event handlers and begin tracking and processing of incoming events.
    pub fn setup_event_handlers(&self, client: &Client) {
        client.add_event_handler_context(self.clone());
        client.add_event_handler_context(self.media_manager.clone());

        client.add_event_handler(room_redaction_event_handler);
        client.add_event_handler(room_name_event_handler);
        client.add_event_handler(room_member_event_handler);
        client.add_event_handler(stripped_room_member_event_handler);
        client.add_event_handler(room_join_rules_event_handler);
        client.add_event_handler(room_avatar_event_handler);
        client.add_event_handler(room_message_event_handler);
        client.add_event_handler(room_pinned_events_event_handler);
        client.add_event_handler(reaction_event_handler);
        client.add_event_handler(presence_event_handler);
        client.add_event_handler(tag_event_handler);
        client.add_event_handler(sync_receipt_event_handler);
        client.add_event_handler(fully_read_event_handler);

        self.clone()
            .subscribe_to_event_cache_generic_updates(client.clone());
        self.clone().subscribe_to_room_updates(client);
    }

    fn subscribe_to_room_updates(self, client: &Client) {
        let mut stream = client.subscribe_to_all_room_updates();

        log::debug!("Subscribing to room updates");

        tokio::spawn(async move {
            while let Ok(updates) = stream.recv().await {
                for (room_id, update) in updates.joined {
                    self.process_joined_room_update(room_id, update);
                }
            }

            log::warn!("Stream of the room updates closed");
        });
    }

    fn subscribe_to_event_cache_generic_updates(self, client: Client) {
        let mut stream = client.event_cache().subscribe_to_room_generic_updates();
        log::debug!("Subscribing to event cache generic updates");

        tokio::spawn(async move {
            while let Ok(update) = stream.recv().await {
                log::debug!("Received event cache generic room update: {update:?}");

                let _ = self.action_sender.send(Action::EventCacheGenericUpdate {
                    room_id: update.room_id,
                });
            }

            log::warn!("Stream of the event cache generic updates closed");
        });
    }

    pub fn process_response(&self, content: &ResponseContent) {
        log::debug!("Processing response: {content:?}");

        match content {
            ResponseContent::RoomListResponse(re) => self.process_room_list_response(re),
            ResponseContent::RoomCreatedEvent(room) => {
                let action = Action::RoomDiscorved {
                    room_id: room.room_id.clone(),
                };

                let _ = self.action_sender.send(action);
            }
            _ => (),
        }
    }

    pub fn process_room_list_response(&self, response: &RoomListResponse) {
        for room in &response.room_list {
            let action = Action::RoomDiscorved {
                room_id: room.room_id.clone(),
            };

            let _ = self.action_sender.send(action);
        }
    }

    pub fn process_room_redaction_event(&self, room: Room, event: OriginalSyncRoomRedactionEvent) {
        log::debug!("Received OriginalSyncRoomRedactionEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::RoomRedactionEvent { room, event });
    }

    pub fn process_room_name_event(&self, room: Room, event: OriginalSyncRoomNameEvent) {
        log::debug!("Received OriginalSyncRoomNameEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self
            .action_sender
            .send(Action::RoomNameEvent { room, event });
    }

    pub fn process_room_member_event(&self, room: Room, event: OriginalSyncRoomMemberEvent) {
        log::debug!("Received OriginalSyncRoomMemberEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self
            .action_sender
            .send(Action::RoomMemberEvent { room, event });
    }

    pub fn process_stripped_room_member_event(&self, room: Room, event: StrippedRoomMemberEvent) {
        log::debug!("Received StrippedRoomMemberEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::StrippedRoomMemberEvent { room, event });
    }

    pub fn process_room_join_rules_event(&self, room: Room, event: OriginalSyncRoomJoinRulesEvent) {
        log::debug!("Received OriginalSyncRoomJoinRulesEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self
            .action_sender
            .send(Action::RoomJoinRulesEvent { room, event });
    }

    pub fn process_room_avatar_event(&self, room: Room, event: OriginalSyncRoomAvatarEvent) {
        log::debug!("Received OriginalSyncRoomAvatarEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self
            .action_sender
            .send(Action::RoomAvatarEvent { room, event });
    }

    pub fn process_room_message_event(&self, room: Room, event: OriginalSyncRoomMessageEvent) {
        log::debug!("Received OriginalSyncRoomMessageEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::RoomMessageEvent { room, event });
    }

    pub fn process_room_pinned_events_event(&self, room: Room, event: OriginalSyncRoomPinnedEventsEvent) {
        log::debug!("Received RoomPinnedEventsEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::RoomPinnedEventsEvent { room, event });
    }

    pub fn process_reaction_event(&self, room: Room, event: OriginalSyncReactionEvent) {
        log::debug!("Received OriginalSyncReactionEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::ReactionEvent { room, event });
    }

    pub fn process_presence_event(&self, event: PresenceEvent) {
        log::debug!("Received PresenceEvent: {event:?}");
        let _ = self.action_sender.send(Action::PresenceEvent(event));
    }

    pub fn process_tag_event(&self, room: Room, event: TagEvent) {
        log::debug!("Received TagEvent: {event:?}");
        let _ = self.action_sender.send(Action::TagEvent { room, event });
    }

    pub fn process_sync_receipt_event(&self, room: Room, event: SyncReceiptEvent) {
        log::debug!("Received SyncReceiptEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::SyncReceiptEvent { room, event });
    }

    pub fn process_fully_read_event(&self, room: Room, event: FullyReadEvent) {
        log::debug!("Received FullyReadEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Action::FullyReadEvent { room, event });
    }

    pub fn process_joined_room_update(&self, room_id: OwnedRoomId, update: JoinedRoomUpdate) {
        log::debug!("Received JoinedRoomUpdate for room: {room_id:?}: {update:?}");
        let _ = self
            .action_sender
            .send(Action::JoinedRoomUpdate { room_id, update });
    }

}

enum Action {
    RoomDiscorved {
        room_id: String,
    },
    RoomRedactionEvent {
        room: Room,
        event: OriginalSyncRoomRedactionEvent,
    },
    RoomNameEvent {
        room: Room,
        event: OriginalSyncRoomNameEvent,
    },
    RoomMemberEvent {
        room: Room,
        event: OriginalSyncRoomMemberEvent,
    },
    StrippedRoomMemberEvent {
        room: Room,
        event: StrippedRoomMemberEvent,
    },
    RoomJoinRulesEvent {
        room: Room,
        event: OriginalSyncRoomJoinRulesEvent,
    },
    RoomAvatarEvent {
        room: Room,
        event: OriginalSyncRoomAvatarEvent,
    },
    RoomMessageEvent {
        room: Room,
        event: OriginalSyncRoomMessageEvent,
    },
    ReactionEvent {
        room: Room,
        event: OriginalSyncReactionEvent,
    },
    PresenceEvent(PresenceEvent),
    TagEvent {
        room: Room,
        event: TagEvent,
    },
    SyncReceiptEvent {
        room: Room,
        event: SyncReceiptEvent,
    },
    FullyReadEvent {
        room: Room,
        event: FullyReadEvent,
    },
    JoinedRoomUpdate {
        room_id: OwnedRoomId,
        update: JoinedRoomUpdate,
    },
    EventCacheGenericUpdate {
        room_id: OwnedRoomId,
    },
    RoomPinnedEventsEvent {
        room: Room,
        event: OriginalSyncRoomPinnedEventsEvent,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct UserChange {
    pub displayname: Option<String>,
    pub avatar_uri: Option<String>,
    pub status: Option<UserStatus>,
}

impl UserChange {
    pub fn new() -> Self {
        Self {
            displayname: None,
            avatar_uri: None,
            status: None,
        }
    }

    pub fn new_profile_change(
        displayname_change: Option<Change<Option<&str>>>,
        avatar_uri_change: Option<Change<Option<&MxcUri>>>,
    ) -> Self {
        let mut obj = Self::new();

        if let Some(change) = displayname_change {
            obj = obj.displayname(change);
        }

        if let Some(change) = avatar_uri_change {
            obj = obj.avatar_uri(change);
        }

        obj
    }

    pub fn displayname(mut self, change: Change<Option<&str>>) -> Self {
        self.displayname = Some(change.new.map(str::to_string).unwrap_or_default());
        self
    }

    pub fn avatar_uri(mut self, change: Change<Option<&MxcUri>>) -> Self {
        self.avatar_uri = Some(change.new.map(MxcUri::to_string).unwrap_or_default());
        self
    }

    pub fn status(mut self, status: UserStatus) -> Self {
        self.status = Some(status);
        self
    }
}

struct EventExecutor {
    client: Client,
    ctx: RequestContext,
    recv: UnboundedReceiver<Action>,

    memory_cache: MemoryCache,
    media_manager: MediaManager,

    user_changes: HashMap<String, UserChange>,
    room_changes: HashMap<String, VecDeque<RoomChangeEvent>>,
}

impl EventExecutor {
    pub fn new(
        client: Client,
        ctx: RequestContext,
        recv: UnboundedReceiver<Action>,
        memory_cache: MemoryCache,
        media_manager: MediaManager,
    ) -> Self {
        Self {
            client,
            ctx,
            recv,
            memory_cache,
            media_manager,

            user_changes: HashMap::new(),
            room_changes: HashMap::new(),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            while let Some(action) = self.recv.recv().await {
                self.exec_event(action).await;
            }
        });
    }

    async fn exec_event(&mut self, event: Action) {
        match event {
            Action::RoomDiscorved { room_id } => self.exec_queued_room_changes(room_id).await,
            Action::RoomRedactionEvent { room, event } => {
                self.exec_room_redaction_event(room, event).await
            }
            Action::RoomNameEvent { room, event } => self.exec_room_name_event(room, event).await,
            Action::RoomMemberEvent { room, event } => {
                self.exec_room_member_event(room, event).await
            }
            Action::StrippedRoomMemberEvent { room, event } => {
                self.exec_stripped_room_member_event(room, event).await
            }
            Action::RoomJoinRulesEvent { room, event } => {
                self.exec_room_join_rules_event(room, event).await
            }
            Action::RoomAvatarEvent { room, event } => {
                self.exec_room_avatar_event(room, event).await
            }
            Action::RoomMessageEvent { room, event } => {
                self.exec_room_message_event(room, event).await
            }
            Action::RoomPinnedEventsEvent { room, event } => {
                self.exec_room_pinned_events_event(room, event).await
            }
            Action::ReactionEvent { room, event } => self.exec_reaction_event(room, event).await,
            Action::PresenceEvent(event) => self.exec_presence_event(event).await,
            Action::TagEvent { room, event } => self.exec_tag_event(room, event).await,
            Action::SyncReceiptEvent { room, event } => {
                self.exec_sync_receipt_event(room, event).await
            }
            Action::FullyReadEvent { room, event } => self.exec_fully_read_event(room, event).await,
            Action::JoinedRoomUpdate { room_id, update } => {
                self.exec_joined_room_update(room_id, update).await
            }
            Action::EventCacheGenericUpdate { room_id } => {
                self.exec_event_cache_generic_update(room_id).await
            }
        }
    }

    fn track_user_change(&mut self, user_id: impl Into<String>, change: UserChange) {
        let user_id = user_id.into();
        log::debug!("Tracking change for user {user_id}: {change:?}");
        self.user_changes.insert(user_id, change);
    }

    fn is_new_user_change(&self, user_id: impl AsRef<str>, change: &UserChange) -> bool {
        if let Some(old) = self.user_changes.get(user_id.as_ref()) {
            old != change
        } else {
            true
        }
    }

    fn track_room_change(&mut self, change: RoomChangeEvent) {
        let room_id = change.room_id.clone();
        log::debug!("Tracking change for room {room_id}: {change:?}");

        let entry = self.room_changes.entry(room_id).or_default();

        if entry.len() >= MAX_QUEUED_ROOM_CHANGES {
            entry.pop_front();
        }

        entry.push_back(change);
    }

    fn is_new_room_change(&self, change: &RoomChangeEvent) -> bool {
        let Some(queue) = self.room_changes.get(&change.room_id) else {
            return true;
        };

        let Some(old) = queue.back() else {
            return true;
        };

        old != change
    }
}

/// Implements the actual event execution methods.
impl EventExecutor {
    async fn exec_queued_room_changes(&mut self, room_id: String) {
        log::debug!("Executing queued room changes for room: {room_id}");

        let Some(changes) = self.room_changes.get_mut(&room_id) else {
            log::debug!("No changes for room queued");
            return;
        };

        for change in std::mem::take(changes) {
            log::debug!("Executing queued room change: {change:?}");
            self.ctx
                .send_event(ResponseContent::RoomChangeEvent(change))
                .await;
        }
    }

    async fn exec_room_redaction_event(
        &mut self,
        room: Room,
        event: OriginalSyncRoomRedactionEvent,
    ) {
        let Some(redact_id) = event.redacts else {
            log::error!("Event redact id is not set");
            return;
        };

        let event = unwrap_or_log_return!(
            room.event(&redact_id, None).await,
            "Error retrieving redacted event"
        );

        match event.kind {
            TimelineEventKind::Decrypted(decrypted) => {
                self.redact_any_timeline_event(room, decrypted.event).await;
            }
            TimelineEventKind::PlainText { event } => {
                self.redact_any_sync_timeline_event(room, event).await;
            }
            _ => {
                log::warn!("Event is not decrypted or plain text");
            }
        };
    }

    async fn redact_any_timeline_event(
        &mut self,
        room: Room,
        redacted_event: Raw<AnyTimelineEvent>,
    ) {
        let redacted_event =
            unwrap_or_log_return!(redacted_event.deserialize(), "Error deserializing event");

        let AnyTimelineEvent::MessageLike(event) = redacted_event else {
            log::debug!("Ignoring event as it is not message like");
            return;
        };

        self.redact_any_message_like_event(room, event).await;
    }

    async fn redact_any_sync_timeline_event(
        &mut self,
        room: Room,
        redacted_event: Raw<AnySyncTimelineEvent>,
    ) {
        let redacted_event =
            unwrap_or_log_return!(redacted_event.deserialize(), "Error deserializing event");

        let AnySyncTimelineEvent::MessageLike(event) = redacted_event else {
            log::debug!("Ignoring event as it is not message like");
            return;
        };

        let event = event.into_full_event(room.room_id().to_owned());

        self.redact_any_message_like_event(room, event).await;
    }

    async fn redact_any_message_like_event(&mut self, room: Room, event: AnyMessageLikeEvent) {
        match event {
            AnyMessageLikeEvent::Reaction(event) => {
                self.redact_reaction(room, event.event_id().as_str()).await;
            }
            AnyMessageLikeEvent::RoomEncrypted(event) => {
                self.redact_room_message(room, event.event_id().to_string())
                    .await;
            }
            AnyMessageLikeEvent::RoomMessage(event) => {
                self.redact_room_message(room, event.event_id().to_string())
                    .await;
            }
            _ => {
                log::debug!("Ignoring event as it is not implemented: {event:?}");
            }
        }
    }

    async fn redact_room_message(&self, room: Room, event_id: String) {
        // TODO: Support reason

        let proto = MessageRemoveEvent {
            room_id: room.room_id().to_string(),
            message_id: event_id,
            reason: None,
            origin: EventOrigin::UserOrigin.into(),
        };

        self.ctx
            .send_event(ResponseContent::MessageRemoveEvent(proto))
            .await;
    }

    async fn redact_reaction(&mut self, room: Room, event_id: &str) {
        let reaction = self
            .memory_cache
            .remove_reaction_by_id(room.room_id().as_str(), event_id);

        let Some(reaction) = reaction else {
            return;
        };

        let ReactionMetadata {
            room_id,
            message_id,
            user_id,
            emoji,
            ..
        } = reaction;

        let proto = ChatReaction {
            room_id,
            message_id,
            reaction: emoji,
            user_id: Some(user_id),
        };

        self.ctx
            .send_event(ResponseContent::ReactionRemovedEvent(proto))
            .await;
    }

    async fn exec_room_name_event(&self, room: Room, event: OriginalSyncRoomNameEvent) {
        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_display_name(event.content.name.clone())
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn exec_room_member_event(&mut self, room: Room, event: OriginalSyncRoomMemberEvent) {
        // Check if our user's membership has changed and if we need to handle this
        // change differently than a membership change for other users.
        if Some(event.state_key.to_string()) == self.client.user_id().map(|f| f.to_string()) {
            self.send_room_created_event_if_needed(&room).await;

            if self.process_own_membership_change(&room, &event).await {
                return;
            }
        }

        if let MembershipChange::ProfileChanged {
            displayname_change,
            avatar_url_change,
        } = event.membership_change()
        {
            log::debug!("The users profile has changed, sending an UserChangeEvent");
            self.process_profile_change(&event, displayname_change, avatar_url_change)
                .await;
            return;
        }

        log::debug!("General room member change, sending a RoomChangeEvent");

        let members = unwrap_or_log_return!(
            rooms::get_room_members(&room).await,
            "Error retrieving room members"
        );

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_user_id_list(members)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;

        let Ok(membership_change) = event.membership_change().try_into_chat() else {
            log::warn!("Unknown membership change: {:?}", event.membership_change());
            return;
        };

        let message_content = MessageContentMembershipChange {
            change: membership_change.into(),
            affected_user_id: event.state_key.to_string(),
        };

        let message = Message {
            room_id: room.room_id().to_owned().to_string(),
            message_id: event.event_id.to_string(),
            sender_id: event.sender.to_string(),
            timestamp: event.origin_server_ts.0.into(),
            content: Some(message::Content::MembershipChange(message_content)),
            ..Default::default()
        };

        self.ctx
            .send_event(ResponseContent::MessageReceivedEvent(message))
            .await;
    }

    async fn process_own_membership_change(
        &self,
        room: &Room,
        event: &OriginalSyncRoomMemberEvent,
    ) -> bool {
        log::debug!("The users own membership has changed");

        match event.membership_change() {
            MembershipChange::Left => {
                self.process_own_leave_change(room, event, RoomLeaveReason::User)
                    .await;
            }
            MembershipChange::Kicked => {
                self.process_own_leave_change(room, event, RoomLeaveReason::Kicked)
                    .await;
            }
            MembershipChange::Banned | MembershipChange::KickedAndBanned => {
                self.process_own_leave_change(room, event, RoomLeaveReason::Banned)
                    .await;
            }
            _ => {
                log::debug!("Treating it like any other membership change");
                return false;
            }
        }

        true
    }

    async fn process_own_leave_change(
        &self,
        room: &Room,
        event: &OriginalSyncRoomMemberEvent,
        reason: RoomLeaveReason,
    ) {
        log::debug!("Our own user left the room, sending a RoomLeftEvent instead");

        let proto = RoomLeftEvent {
            room_id: room.room_id().to_string(),
            reason: reason.into(),
            message: event.content.reason.clone(),
        };

        self.ctx
            .send_event(ResponseContent::RoomLeftEvent(proto))
            .await;
    }

    async fn exec_stripped_room_member_event(
        &mut self,
        room: Room,
        event: StrippedRoomMemberEvent,
    ) {
        if Some(event.state_key.to_string()) != self.client.user_id().map(|f| f.to_string()) {
            log::debug!("Our user did not trigger this event, nothing to do");
            return;
        }

        if event.content.membership != MembershipState::Invite {
            log::debug!("Our user is not in invited state, nothing to do");
            return;
        }

        let proto = InvitedEvent {
            room_id: room.room_id().to_string(),
            invitation_text: event.content.reason.clone(),
            room_display_name: room.display_name().await.ok().map(|n| n.to_string()),
        };

        self.ctx
            .send_event(ResponseContent::InvitedEvent(proto))
            .await;
    }

    async fn process_profile_change(
        &mut self,
        event: &OriginalSyncRoomMemberEvent,
        displayname_change: Option<Change<Option<&str>>>,
        avatar_url_change: Option<Change<Option<&MxcUri>>>,
    ) {
        let user_id = event.sender.to_string();
        let change = UserChange::new_profile_change(displayname_change, avatar_url_change);

        if !self.is_new_user_change(&user_id, &change) {
            log::debug!(
                "User profile change for {user_id} has already been processed before, nothing to do"
            );
            return;
        }

        let mut builder = builder::UserChangeEventBuilder::new(user_id.clone());

        if let Some(displayname) = &change.displayname {
            builder = builder.change_display_name(displayname.clone());
        }

        if change.avatar_uri.is_some() {
            let path = self
                .media_manager
                .get_user_avatar_path(event.sender.clone())
                .await;
            builder = builder.change_avatar_path(path.unwrap_or_default());
        }

        self.track_user_change(user_id, change);

        let proto = builder.to_proto();
        self.ctx
            .send_event(ResponseContent::UserChangeEvent(proto))
            .await;
    }

    async fn exec_room_join_rules_event(&self, room: Room, event: OriginalSyncRoomJoinRulesEvent) {
        let join_rule = event.content.join_rule.clone().into_chat();

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_join_rule(join_rule)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn exec_room_avatar_event(&self, room: Room, _event: OriginalSyncRoomAvatarEvent) {
        let avatar_path = self
            .media_manager
            .get_room_avatar_path(&room)
            .await
            .unwrap_or_default();

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_avatar_path(avatar_path)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn exec_room_message_event(&self, room: Room, event: OriginalSyncRoomMessageEvent) {
        if room.state() != RoomState::Joined {
            log::debug!("Room is not in a joined state");
            return;
        }

        if let Some(Relation::Replacement(relation)) = event.content.relates_to {
            self.process_replacement_message(&room, relation).await;
            return;
        }

        self.process_new_message(room, event).await;
    }

    async fn exec_room_pinned_events_event(&self, room: Room, event: OriginalSyncRoomPinnedEventsEvent) {
        let ids = event.content.pinned.iter().map(|i| i.to_string()).collect();

        let proto = RoomChangeEventBuilder::new(room.room_id())
            .change_pinned_messages(ids)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn process_new_message(&self, room: Room, event: OriginalSyncRoomMessageEvent) {
        let event = event.into_full_event(room.room_id().to_owned());
        let message = messages::message_from_event(&self.media_manager, &room, &event).await;

        self.ctx
            .send_event(ResponseContent::MessageReceivedEvent(message))
            .await;
    }

    async fn process_replacement_message(
        &self,
        room: &Room,
        relation: Replacement<RoomMessageEventContentWithoutRelation>,
    ) {
        let original_message_id = relation.event_id.to_string();
        let mentions = messages::matrix_mentions_to_proto_mentions(&relation.new_content.mentions);

        let content = messages::generate_message_content!(
            self.media_manager,
            room,
            relation.event_id,
            relation.new_content.msgtype,
            message_change_event
        );

        // Error when retrieving the messages content.
        // Sending a Message without content is not allowed, so we will return early.
        let Some(content) = content else {
            return;
        };

        let proto = builder::MessageChangeEventBuilder::new(room.room_id(), original_message_id)
            .change_content(content)
            .change_mentioned_user_ids(mentions)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::MessageChangeEvent(proto))
            .await;
    }

    async fn exec_reaction_event(&mut self, room: Room, event: OriginalSyncReactionEvent) {
        let proto = ChatReaction {
            room_id: room.room_id().to_string(),
            message_id: event.content.relates_to.event_id.to_string(),
            reaction: event.content.relates_to.key.to_string(),
            user_id: Some(event.sender.to_string()),
        };

        self.memory_cache.cache_reaction(room, event);

        self.ctx
            .send_event(ResponseContent::ReactionCreatedEvent(proto))
            .await
    }

    async fn exec_presence_event(&mut self, event: PresenceEvent) {
        let user_id = event.sender.to_string();
        let presence = event.content.presence.into_chat();

        let user_status = UserStatus {
            state: presence.into(),
            status_message: event.content.status_msg,
        };

        let change = UserChange::new().status(user_status.clone());

        if !self.is_new_user_change(&user_id, &change) {
            log::debug!(
                "User profile change for {user_id} has already been processed before, nothing to do"
            );
            return;
        }

        self.track_user_change(&user_id, change);

        let proto = builder::UserChangeEventBuilder::new(user_id)
            .change_status(user_status)
            .to_proto();

        self.ctx
            .send_event(ResponseContent::UserChangeEvent(proto))
            .await;
    }

    async fn exec_tag_event(&mut self, room: Room, event: TagEvent) {
        let is_favourite = event.content.tags.get(&TagName::Favorite);

        let proto = RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_is_favourite(is_favourite.is_some())
            .to_proto();

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn exec_sync_receipt_event(&self, room: Room, event: SyncReceiptEvent) {
        log::debug!(
            "Processing SyncReceiptEvent {event:?} inside room {:?}",
            room.room_id(),
        )
    }

    async fn exec_fully_read_event(&mut self, room: Room, _event: FullyReadEvent) {
        self.update_room_unread_count(&room).await;
    }

    async fn exec_joined_room_update(&mut self, room_id: OwnedRoomId, update: JoinedRoomUpdate) {
        self.update_room_unread_count_by_id(&room_id).await;

        let Some(list) = get_user_typing_list(&update) else {
            return;
        };

        let proto = RoomChangeEventBuilder::new(room_id.to_string())
            .change_typing_user_id_list(list)
            .to_proto();

        if !self.is_new_room_change(&proto) {
            log::debug!(
                "Room change for {room_id} has already been processed before, nothing to do"
            );
            return;
        }

        self.track_room_change(proto.clone());

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn exec_event_cache_generic_update(&mut self, room_id: OwnedRoomId) {
        self.update_room_unread_count_by_id(&room_id).await;
    }

    async fn update_room_unread_count_by_id(&mut self, room_id: &RoomId) {
        let Some(room) = self.client.get_room(room_id) else {
            log::warn!("Unable to find matrix room to update unread count");
            return;
        };

        self.update_room_unread_count(&room).await;
    }

    async fn update_room_unread_count(&mut self, room: &Room) {
        let room_id = room.room_id();
        let new = u32::try_from(room.num_unread_messages()).unwrap_or(u32::MAX);

        log::debug!("Received new unread count of room: {new}");

        let proto = RoomChangeEventBuilder::new(room_id.to_string())
            .change_unread_count(new)
            .to_proto();

        if !self.is_new_room_change(&proto) {
            log::debug!(
                "Room change for {room_id} has already been processed before, nothing to do"
            );
            return;
        }

        self.track_room_change(proto.clone());

        self.ctx
            .send_event(ResponseContent::RoomChangeEvent(proto))
            .await;
    }

    async fn send_room_created_event_if_needed(&self, room: &Room) {
        let Ok(is_known) = self.memory_cache.is_known_room(room.room_id()) else {
            log::error!("Error checking if room {} is known", room.room_id());
            return;
        };

        if is_known {
            log::debug!("Room {} is already known", room.room_id());
            return;
        }

        log::debug!(
            "Sending RoomCreatedEvent for {} as room is not known",
            room.room_id()
        );

        let assemble_result = RoomsManager::new(self.client.clone(), self.media_manager.clone())
            .assemble_chat_room(room)
            .await
            .inspect_err(|err| log::error!("Error assembling chat room: {err}"));

        let Ok(proto) = assemble_result else {
            return;
        };

        self.ctx
            .send_event(ResponseContent::RoomCreatedEvent(proto))
            .await;
    }
}

fn get_user_typing_list(update: &JoinedRoomUpdate) -> Option<Vec<String>> {
    let mut result = None;

    for event in &update.ephemeral {
        let Ok(event) = event.deserialize() else {
            continue;
        };

        match event.content() {
            AnyEphemeralRoomEventContent::Typing(event) => {
                result = Some(event.user_ids.iter().map(OwnedUserId::to_string).collect());
            }
            AnyEphemeralRoomEventContent::Receipt(_) => {
                continue;
            }
            _ => {
                continue;
            }
        }
    }

    result
}

impl_room_event_handler!(
    OriginalSyncRoomRedactionEvent,
    room_redaction_event_handler,
    process_room_redaction_event
);

impl_room_event_handler!(
    OriginalSyncRoomNameEvent,
    room_name_event_handler,
    process_room_name_event
);

impl_room_event_handler!(
    OriginalSyncRoomMemberEvent,
    room_member_event_handler,
    process_room_member_event
);

impl_room_event_handler!(
    StrippedRoomMemberEvent,
    stripped_room_member_event_handler,
    process_stripped_room_member_event
);

impl_room_event_handler!(
    OriginalSyncRoomJoinRulesEvent,
    room_join_rules_event_handler,
    process_room_join_rules_event
);

impl_room_event_handler!(
    OriginalSyncRoomAvatarEvent,
    room_avatar_event_handler,
    process_room_avatar_event
);

impl_room_event_handler!(
    OriginalSyncRoomMessageEvent,
    room_message_event_handler,
    process_room_message_event
);

impl_room_event_handler!(
    OriginalSyncRoomPinnedEventsEvent,
    room_pinned_events_event_handler,
    process_room_pinned_events_event
);

impl_room_event_handler!(
    OriginalSyncReactionEvent,
    reaction_event_handler,
    process_reaction_event
);

impl_event_handler!(
    PresenceEvent,
    presence_event_handler,
    process_presence_event
);

impl_room_event_handler!(TagEvent, tag_event_handler, process_tag_event);

impl_room_event_handler!(
    SyncReceiptEvent,
    sync_receipt_event_handler,
    process_sync_receipt_event
);

impl_room_event_handler!(
    FullyReadEvent,
    fully_read_event_handler,
    process_fully_read_event
);
