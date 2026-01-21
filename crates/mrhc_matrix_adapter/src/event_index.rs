use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::ReactionEvent;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct Reaction {
    pub event_id: String,
    pub message_id: String,
    pub user_id: String,
    pub emoji: String,
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

    pub fn add_reaction(&self, event: Reaction) {
        let _ = self.action_sender.send(Action::AddReaction(event));
    }

    pub fn redact_reaction(&self, event_id: String) {
        let _ = self.action_sender.send(Action::RedactReaction(event_id));
    }
}

enum Action {
    AddReaction(Reaction),
    RedactReaction(String),
}

struct EventIndexData {
    ctx: ClientContext,
    recv: UnboundedReceiver<Action>,

    reactions: Vec<Reaction>,
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

    fn add_reaction(&mut self, reaction: Reaction) {
        log::info!("Adding reaction: {reaction:?}");

        let proto = ReactionEvent {
            message_id: reaction.message_id.clone(),
            emoji: reaction.emoji.clone(),
            user_id: reaction.user_id.clone(),
            removed: false,
        };

        self.reactions.push(reaction);

        self.ctx.send_event(ResponseContent::ReactionEvent(proto));
    }

    fn redact_reaction(&mut self, id: &str) {
        log::info!("Redacting reaction: {id}");

        let Some(pos) = self.reactions.iter().position(|p| p.event_id == id) else {
            log::warn!(
                "Unable to redact reaction with id '{id}': Unable to find reaction in event index"
            );

            return;
        };

        let reaction = self.reactions.remove(pos);

        let proto = ReactionEvent {
            message_id: reaction.message_id,
            emoji: reaction.emoji,
            user_id: reaction.user_id,
            removed: true,
        };

        self.ctx.send_event(ResponseContent::ReactionEvent(proto));
    }
}
