use std::time::{SystemTime, UNIX_EPOCH};

use matrix_sdk::deserialized_responses::TimelineEventKind;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::reaction::OriginalSyncReactionEvent;
use matrix_sdk::ruma::events::room::avatar::OriginalSyncRoomAvatarEvent;
use matrix_sdk::ruma::events::room::join_rules::OriginalSyncRoomJoinRulesEvent;
use matrix_sdk::ruma::events::room::member::{MembershipChange, OriginalSyncRoomMemberEvent};
use matrix_sdk::ruma::events::room::message::{
    MessageType, OriginalSyncRoomMessageEvent, Relation,
};
use matrix_sdk::ruma::events::room::name::OriginalSyncRoomNameEvent;
use matrix_sdk::ruma::events::room::redaction::OriginalSyncRoomRedactionEvent;
use matrix_sdk::ruma::events::{
    AnyMessageLikeEvent, AnySyncMessageLikeEvent, AnySyncTimelineEvent, AnyTimelineEvent,
};
use matrix_sdk::{Client, Room, RoomState};
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::room_left_event::RoomLeaveReason;
use mrhc_proto::chat::{Reaction as ChatReaction, *};
use ruma_common::serde::Raw;
use ruma_common::MilliSecondsSinceUnixEpoch;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::media::MediaManager;
use crate::{rooms, unwrap_or_log_return};

// After how many seconds does an event count as historical?
const HISTORICAL_EVENT_TIMEOUT: u64 = 5;

macro_rules! impl_event_handler {
    ($event:ident, $handler_name:ident, $processor_name:ident) => {
        async fn $handler_name(event: $event, room: Room, event_manager: Ctx<EventManager>) {
            event_manager.$processor_name(room, event);
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
/// This manager is designed to be cloned and contains only a
/// channel to the shared event executor.
#[derive(Clone)]
pub struct EventManager {
    media_manager: MediaManager,
    /// Sender to send requested actions to the event executor.
    action_sender: UnboundedSender<Event>,
}

impl EventManager {
    pub fn new(client: Client, ctx: ClientContext, media_manager: MediaManager) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        EventExecutor::new(client, ctx, rx, media_manager.clone()).run();

        Self {
            media_manager,
            action_sender: tx,
        }
    }

    /// Setup all event handlers and begin tracking and processing of incoming events.
    pub fn setup_event_handlers(&self, client: &Client) {
        client.add_event_handler_context(self.clone());
        client.add_event_handler_context(self.media_manager.clone());

        client.add_event_handler(room_redaction_event_handler);
        client.add_event_handler(room_name_event_handler);
        client.add_event_handler(room_member_event_handler);
        client.add_event_handler(room_join_rules_event_handler);
        client.add_event_handler(room_avatar_event_handler);
        client.add_event_handler(room_message_event_handler);
        client.add_event_handler(reaction_event_handler);
    }

    pub fn process_room_redaction_event(&self, room: Room, event: OriginalSyncRoomRedactionEvent) {
        log::debug!("Received OriginalSyncRoomRedactionEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Event::RoomRedaction { room, event });
    }

    pub fn process_room_name_event(&self, room: Room, event: OriginalSyncRoomNameEvent) {
        log::debug!("Received OriginalSyncRoomNameEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self.action_sender.send(Event::RoomName { room, event });
    }

    pub fn process_room_member_event(&self, room: Room, event: OriginalSyncRoomMemberEvent) {
        log::debug!("Received OriginalSyncRoomMemberEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self.action_sender.send(Event::RoomMember { room, event });
    }

    pub fn process_room_join_rules_event(&self, room: Room, event: OriginalSyncRoomJoinRulesEvent) {
        log::debug!("Received OriginalSyncRoomJoinRulesEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self
            .action_sender
            .send(Event::RoomJoinRules { room, event });
    }

    pub fn process_room_avatar_event(&self, room: Room, event: OriginalSyncRoomAvatarEvent) {
        log::debug!("Received OriginalSyncRoomAvatarEvent: {event:?}");
        skip_historical_event!(event);
        let _ = self.action_sender.send(Event::RoomAvatar { room, event });
    }

    pub fn process_room_message_event(&self, room: Room, event: OriginalSyncRoomMessageEvent) {
        log::debug!("Received OriginalSyncRoomMessageEvent: {event:?}");
        let _ = self.action_sender.send(Event::RoomMessage { room, event });
    }

    pub fn process_reaction_event(&self, room: Room, event: OriginalSyncReactionEvent) {
        log::debug!("Received OriginalSyncReactionEvent: {event:?}");
        let _ = self.action_sender.send(Event::Reaction { room, event });
    }
}

enum Event {
    RoomRedaction {
        room: Room,
        event: OriginalSyncRoomRedactionEvent,
    },
    RoomName {
        room: Room,
        event: OriginalSyncRoomNameEvent,
    },
    RoomMember {
        room: Room,
        event: OriginalSyncRoomMemberEvent,
    },
    RoomJoinRules {
        room: Room,
        event: OriginalSyncRoomJoinRulesEvent,
    },
    RoomAvatar {
        room: Room,
        event: OriginalSyncRoomAvatarEvent,
    },
    RoomMessage {
        room: Room,
        event: OriginalSyncRoomMessageEvent,
    },
    Reaction {
        room: Room,
        event: OriginalSyncReactionEvent,
    },
}

#[derive(Debug)]
pub struct Reaction {
    pub event_id: String,
    pub room_id: String,
    pub message_id: String,
    pub user_id: String,
    pub emoji: String,
}

struct EventExecutor {
    client: Client,
    ctx: ClientContext,
    recv: UnboundedReceiver<Event>,
    media_manager: MediaManager,

    reactions: Vec<Reaction>,
}

impl EventExecutor {
    pub fn new(
        client: Client,
        ctx: ClientContext,
        recv: UnboundedReceiver<Event>,
        media_manager: MediaManager,
    ) -> Self {
        Self {
            client,
            ctx,
            recv,
            media_manager,

            reactions: Vec::new(),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            while let Some(action) = self.recv.recv().await {
                self.exec_event(action).await;
            }
        });
    }

    async fn exec_event(&mut self, event: Event) {
        match event {
            Event::RoomRedaction { room, event } => {
                self.exec_room_redaction_event(room, event).await
            }
            Event::RoomName { room, event } => self.exec_room_name_event(room, event).await,
            Event::RoomMember { room, event } => self.exec_room_member_event(room, event).await,
            Event::RoomJoinRules { room, event } => {
                self.exec_room_join_rules_event(room, event).await
            }
            Event::RoomAvatar { room, event } => self.exec_room_avatar_event(room, event).await,
            Event::RoomMessage { room, event } => self.exec_room_message_event(room, event).await,
            Event::Reaction { room, event } => self.exec_reaction_event(room, event).await,
        }
    }

    fn track_reaction(&mut self, reaction: Reaction) {
        log::debug!("Tracking reaction: {reaction:?}");
        self.reactions.push(reaction);
    }

    fn untrack_reaction(&mut self, id: &str) -> Option<Reaction> {
        log::debug!("Untracking reaction: {id}");

        let Some(pos) = self.reactions.iter().position(|p| p.event_id == id) else {
            log::warn!("Unable to find reaction in tracked reactions");
            return None;
        };

        Some(self.reactions.remove(pos))
    }
}

/// Implements the actual event execution methods.
impl EventExecutor {
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

    async fn redact_any_timeline_event(&mut self, room: Room, event: Raw<AnyTimelineEvent>) {
        let redacted_event =
            unwrap_or_log_return!(event.deserialize(), "Error deserializing event");

        let AnyTimelineEvent::MessageLike(event) = redacted_event else {
            log::debug!("Ignoring event as it is not message like");
            return;
        };

        match event {
            AnyMessageLikeEvent::Reaction(event) => {
                self.redact_reaction(event.event_id().as_str()).await;
            }
            AnyMessageLikeEvent::RoomEncrypted(event) => {
                // TODO: This doesn't necessarily have to be a text message event.
                self.redact_room_message(room, event.event_id().to_string())
                    .await;
            }
            _ => {
                log::debug!("Ignoring event as it is not implemented: {event:?}");
            }
        }
    }

    async fn redact_any_sync_timeline_event(
        &mut self,
        room: Room,
        event: Raw<AnySyncTimelineEvent>,
    ) {
        let redacted_event =
            unwrap_or_log_return!(event.deserialize(), "Error deserializing event");

        let AnySyncTimelineEvent::MessageLike(event) = redacted_event else {
            log::debug!("Ignoring event as it is not message like");
            return;
        };

        match event {
            AnySyncMessageLikeEvent::Reaction(event) => {
                self.redact_reaction(event.event_id().as_str()).await;
            }
            AnySyncMessageLikeEvent::RoomEncrypted(event) => {
                // TODO: This doesn't necessarily have to be a text message event.
                self.redact_room_message(room, event.event_id().to_string())
                    .await;
            }
            _ => {
                log::debug!("Ignoring event as it is not implemented: {event:?}");
            }
        }
    }

    async fn redact_room_message(&self, room: Room, event_id: String) {
        let proto = MessageRemoveEvent {
            room_id: room.room_id().to_string(),
            message_id: event_id,
        };

        self.ctx
            .send_event(ResponseContent::MessageRemoveEvent(proto));
    }

    async fn redact_reaction(&mut self, event_id: &str) {
        let Some(reaction) = self.untrack_reaction(event_id) else {
            return;
        };

        let Reaction {
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
            .send_event(ResponseContent::ReactionRemovedEvent(proto));
    }

    async fn exec_room_name_event(&self, room: Room, event: OriginalSyncRoomNameEvent) {
        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_display_name(event.content.name.clone())
            .into_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
    }

    async fn exec_room_member_event(&self, room: Room, event: OriginalSyncRoomMemberEvent) {
        // Check if our user's membership changed
        if Some(event.state_key.to_string()) == self.client.user_id().map(|f| f.to_string())
            && self.process_membership_change(&room, event).await
        {
            return;
        }

        let members = unwrap_or_log_return!(
            rooms::get_members(&room).await,
            "Error retrieving room members"
        );

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_user_id_list(members)
            .into_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
    }

    async fn process_membership_change(
        &self,
        room: &Room,
        event: OriginalSyncRoomMemberEvent,
    ) -> bool {
        let reason = match event.membership_change() {
            MembershipChange::Left => RoomLeaveReason::User,
            MembershipChange::Kicked => RoomLeaveReason::Kicked,
            MembershipChange::Banned | MembershipChange::KickedAndBanned => RoomLeaveReason::Banned,
            _ => return false,
        };

        let proto = RoomLeftEvent {
            room_id: room.room_id().to_string(),
            reason: reason.into(),
            message: event.content.reason.clone(),
        };

        self.ctx.send_event(ResponseContent::RoomLeftEvent(proto));

        true
    }

    async fn exec_room_join_rules_event(&self, room: Room, event: OriginalSyncRoomJoinRulesEvent) {
        let join_rule = rooms::convert_join_rule(event.content.join_rule.clone());

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_join_rule(join_rule)
            .into_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
    }

    async fn exec_room_avatar_event(&self, room: Room, _event: OriginalSyncRoomAvatarEvent) {
        let avatar_path = self
            .media_manager
            .get_room_avatar_path(&room)
            .await
            .unwrap_or_default();

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_avatar_path(avatar_path)
            .into_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
    }

    async fn exec_room_message_event(&self, room: Room, event: OriginalSyncRoomMessageEvent) {
        if room.state() != RoomState::Joined {
            log::debug!("Room is not in a joined state");
            return;
        }

        let MessageType::Text(text_content) = event.content.msgtype else {
            return;
        };

        if let Some(Relation::Replacement(relation)) = event.content.relates_to {
            let content = text_content.body.strip_prefix("* ");

            let proto = MessageChangeEvent {
                message_id: relation.event_id.to_string(),
                content: Some(content.unwrap_or(text_content.body.as_str()).to_string()),
                is_encrypted: None,
                is_pinned: None,
            };

            self.ctx
                .send_event(ResponseContent::MessageChangeEvent(proto));

            return;
        }

        let proto = MessageReceivedEvent {
            message_content: Some(Message {
                message_id: Some(event.event_id.to_string()),
                room_id: room.room_id().to_string(),
                sender_id: event.sender.to_string(),
                timestamp: event.origin_server_ts.get().into(),
                mime_type: "text/plain".to_owned(),
                content: text_content.body,
                related_message_id: None,
                is_pinned: false,
                is_encrypted: false,
                reactions: Vec::new(),
            }),
        };

        self.ctx
            .send_event(ResponseContent::MessageReceivedEvent(proto));
    }

    async fn exec_reaction_event(&mut self, room: Room, event: OriginalSyncReactionEvent) {
        let reaction = Reaction {
            event_id: event.event_id.to_string(),
            room_id: room.room_id().to_string(),
            message_id: event.content.relates_to.event_id.to_string(),
            user_id: event.sender.to_string(),
            emoji: event.content.relates_to.key,
        };

        let proto = ChatReaction {
            room_id: reaction.room_id.clone(),
            message_id: reaction.message_id.clone(),
            reaction: reaction.emoji.clone(),
            user_id: Some(reaction.user_id.clone()),
        };

        self.track_reaction(reaction);

        self.ctx
            .send_event(ResponseContent::ReactionCreatedEvent(proto));
    }
}

impl_event_handler!(
    OriginalSyncRoomRedactionEvent,
    room_redaction_event_handler,
    process_room_redaction_event
);

impl_event_handler!(
    OriginalSyncRoomNameEvent,
    room_name_event_handler,
    process_room_name_event
);

impl_event_handler!(
    OriginalSyncRoomMemberEvent,
    room_member_event_handler,
    process_room_member_event
);

impl_event_handler!(
    OriginalSyncRoomJoinRulesEvent,
    room_join_rules_event_handler,
    process_room_join_rules_event
);

impl_event_handler!(
    OriginalSyncRoomAvatarEvent,
    room_avatar_event_handler,
    process_room_avatar_event
);

impl_event_handler!(
    OriginalSyncRoomMessageEvent,
    room_message_event_handler,
    process_room_message_event
);

impl_event_handler!(
    OriginalSyncReactionEvent,
    reaction_event_handler,
    process_reaction_event
);
