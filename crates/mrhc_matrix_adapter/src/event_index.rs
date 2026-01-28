use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::OriginalMessageLikeEvent;
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::Reaction as ChatReaction;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct ReactionData {
    pub event_id: String,
    pub room_id: String,
    pub message_id: String,
    pub user_id: String,
    pub emoji: String,
}

impl From<OriginalMessageLikeEvent<ReactionEventContent>> for ReactionData {
    fn from(value: OriginalMessageLikeEvent<ReactionEventContent>) -> Self {
        Self {
            event_id: value.event_id.to_string(),
            room_id: value.room_id.to_string(),
            message_id: value.content.relates_to.event_id.to_string(),
            user_id: value.sender.to_string(),
            emoji: value.content.relates_to.key,
        }
    }
}

/// Contains cached events with data we might need after the event has been
/// redacted by the Matrix server.
#[derive(Clone)]
pub struct EventIndex {
    action_sender: UnboundedSender<Action>,
}

impl EventIndex {
    pub fn new(ctx: ClientContext) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        EventIndexData::new(ctx, rx).run();

        Self { action_sender: tx }
    }

    pub fn add_reaction(&self, event: ReactionData) {
        let _ = self.action_sender.send(Action::AddReaction(event));
    }

    pub fn redact_reaction(&self, event_id: String) {
        let _ = self.action_sender.send(Action::RedactReaction(event_id));
    }
}

enum Action {
    AddReaction(ReactionData),
    RedactReaction(String),
}

struct EventIndexData {
    ctx: ClientContext,
    recv: UnboundedReceiver<Action>,

    reactions: Vec<ReactionData>,
}

impl EventIndexData {
    pub fn new(ctx: ClientContext, recv: UnboundedReceiver<Action>) -> Self {
        Self {
            ctx,
            recv,
            reactions: Vec::new(),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            while let Some(action) = self.recv.recv().await {
                self.exec_action(action);
            }
        });
    }

    fn exec_action(&mut self, action: Action) {
        match action {
            Action::AddReaction(reaction) => self.add_reaction(reaction),
            Action::RedactReaction(id) => self.redact_reaction(&id),
        }
    }

    fn add_reaction(&mut self, reaction: ReactionData) {
        log::debug!("Adding reaction: {reaction:?}");

        let proto = ChatReaction {
            room_id: reaction.room_id.clone(),
            message_id: reaction.message_id.clone(),
            reaction: reaction.emoji.clone(),
            user_id: Some(reaction.user_id.clone()),
        };

        self.reactions.push(reaction);

        self.ctx
            .send_event(ResponseContent::ReactionCreatedEvent(proto));
    }

    fn redact_reaction(&mut self, id: &str) {
        log::debug!("Redacting reaction: {id}");

        let Some(pos) = self.reactions.iter().position(|p| p.event_id == id) else {
            log::warn!(
                "Unable to redact reaction with id '{id}': Unable to find reaction in event index"
            );

            return;
        };

        let reaction = self.reactions.remove(pos);

        let ReactionData {
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
}
