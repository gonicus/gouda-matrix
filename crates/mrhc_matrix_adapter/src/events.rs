use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use matrix_sdk::deserialized_responses::TimelineEventKind;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::presence::PresenceEvent;
use matrix_sdk::ruma::events::reaction::OriginalSyncReactionEvent;
use matrix_sdk::ruma::events::room::avatar::OriginalSyncRoomAvatarEvent;
use matrix_sdk::ruma::events::room::join_rules::OriginalSyncRoomJoinRulesEvent;
use matrix_sdk::ruma::events::room::member::{
    Change, MembershipChange, MembershipState, OriginalSyncRoomMemberEvent, StrippedRoomMemberEvent,
};
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
use ruma_common::{MilliSecondsSinceUnixEpoch, MxcUri};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::media::MediaManager;
use crate::{rooms, unwrap_or_log_return, user};

// After how many seconds does an event count as historical?
const HISTORICAL_EVENT_TIMEOUT: u64 = 5;

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
        client.add_event_handler(stripped_room_member_event_handler);
        client.add_event_handler(room_join_rules_event_handler);
        client.add_event_handler(room_avatar_event_handler);
        client.add_event_handler(room_message_event_handler);
        client.add_event_handler(reaction_event_handler);
        client.add_event_handler(presence_event_handler);
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

    pub fn process_stripped_room_member_event(&self, room: Room, event: StrippedRoomMemberEvent) {
        log::debug!("Received StrippedRoomMemberEvent: {event:?}");
        let _ = self
            .action_sender
            .send(Event::StrippedRoomMember { room, event });
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

    pub fn process_presence_event(&self, event: PresenceEvent) {
        log::debug!("Received PresenceEvent: {event:?}");
        let _ = self.action_sender.send(Event::Presence(event));
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
    StrippedRoomMember {
        room: Room,
        event: StrippedRoomMemberEvent,
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
    Presence(PresenceEvent),
}

#[derive(Debug)]
struct Reaction {
    pub event_id: String,
    pub room_id: String,
    pub message_id: String,
    pub user_id: String,
    pub emoji: String,
}

#[derive(Debug, PartialEq, Eq)]
struct UserChange {
    pub displayname: Option<String>,
    pub avatar_uri: Option<String>,
    pub presence_state: Option<PresenceState>,
}

impl UserChange {
    pub fn new() -> Self {
        Self {
            displayname: None,
            avatar_uri: None,
            presence_state: None,
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

    pub fn presence_state(mut self, presence_state: PresenceState) -> Self {
        self.presence_state = Some(presence_state);
        self
    }
}

struct EventExecutor {
    client: Client,
    ctx: ClientContext,
    recv: UnboundedReceiver<Event>,
    media_manager: MediaManager,

    reactions: Vec<Reaction>,
    user_changes: HashMap<String, UserChange>,
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
            user_changes: HashMap::new(),
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
            Event::StrippedRoomMember { room, event } => {
                self.exec_stripped_room_member_event(room, event).await
            }
            Event::RoomJoinRules { room, event } => {
                self.exec_room_join_rules_event(room, event).await
            }
            Event::RoomAvatar { room, event } => self.exec_room_avatar_event(room, event).await,
            Event::RoomMessage { room, event } => self.exec_room_message_event(room, event).await,
            Event::Reaction { room, event } => self.exec_reaction_event(room, event).await,
            Event::Presence(event) => self.exec_presence_event(event).await,
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
            .to_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
    }

    async fn exec_room_member_event(&mut self, room: Room, event: OriginalSyncRoomMemberEvent) {
        // Check if our user's membership has changed and if we need to handle this
        // change differently than a membership change for other users.
        if Some(event.state_key.to_string()) == self.client.user_id().map(|f| f.to_string())
            && self.process_own_membership_change(&room, &event).await
        {
            return;
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
            rooms::get_members(&room).await,
            "Error retrieving room members"
        );

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_user_id_list(members)
            .to_proto();

        self.ctx.send_event(ResponseContent::RoomChangeEvent(proto));
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

        self.ctx.send_event(ResponseContent::RoomLeftEvent(proto));
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

        self.ctx.send_event(ResponseContent::InvitedEvent(proto));
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
        self.ctx.send_event(ResponseContent::UserChangeEvent(proto));
    }

    async fn exec_room_join_rules_event(&self, room: Room, event: OriginalSyncRoomJoinRulesEvent) {
        let join_rule = rooms::convert_join_rule(event.content.join_rule.clone());

        let proto = builder::RoomChangeEventBuilder::new(room.room_id().to_string())
            .change_join_rule(join_rule)
            .to_proto();

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
            .to_proto();

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

    async fn exec_presence_event(&mut self, event: PresenceEvent) {
        let user_id = event.sender.to_string();
        let presence = user::convert_presence_state(event.content.presence);
        let change = UserChange::new().presence_state(presence);

        if !self.is_new_user_change(&user_id, &change) {
            log::debug!(
                "User profile change for {user_id} has already been processed before, nothing to do"
            );
            return;
        }

        self.track_user_change(&user_id, change);

        let proto = builder::UserChangeEventBuilder::new(user_id)
            .change_presence_state(presence)
            .to_proto();

        self.ctx.send_event(ResponseContent::UserChangeEvent(proto));
    }
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
    OriginalSyncReactionEvent,
    reaction_event_handler,
    process_reaction_event
);

impl_event_handler!(
    PresenceEvent,
    presence_event_handler,
    process_presence_event
);
