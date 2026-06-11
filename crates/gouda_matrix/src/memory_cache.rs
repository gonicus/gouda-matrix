use std::sync::{Arc, RwLock};

use matrix_sdk::ruma::OwnedEventId;

use crate::debug_assert_or_log;

#[derive(Default, Clone)]
pub struct MemoryCache {
    reactions: Arc<RwLock<Vec<CachedReaction>>>,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn track_reaction(&self, reaction: CachedReaction) {
        log::debug!("Tracking reaction: {reaction:?}");

        let Ok(mut writer) = self.reactions.write() else {
            debug_assert_or_log!(false, "Reaction lock poisoned");
            return;
        };

        writer.push(reaction);
    }

    pub fn untrack_reaction_by_id(&self, id: &str) -> Option<CachedReaction> {
        log::debug!("Untracking reaction: {id}");

        let Ok(mut writer) = self.reactions.write() else {
            debug_assert_or_log!(false, "Reaction lock poisoned");
            return None;
        };

        let Some(pos) = writer.iter().position(|p| p.event_id == id) else {
            log::warn!("Unable to find reaction in tracked reactions");
            return None;
        };

        Some(writer.remove(pos))
    }

    pub fn untrack_reaction_by_emoji(
        &self,
        room_id: impl AsRef<str>,
        message_id: impl AsRef<str>,
        user_id: impl AsRef<str>,
        emoji: impl AsRef<str>,
    ) -> Option<CachedReaction> {
        let room_id = room_id.as_ref();
        let message_id = message_id.as_ref();
        let user_id = user_id.as_ref();
        let emoji = emoji.as_ref();

        log::debug!(
            "Untracking reaction: room_id: {room_id}, message_id: {message_id}, \
            user_id: {user_id}, emoji: {emoji}"
        );

        let Ok(mut writer) = self.reactions.write() else {
            debug_assert_or_log!(false, "Reaction lock poisoned");
            return None;
        };

        let pos = writer.iter().position(|r| {
            r.room_id == room_id
                && r.message_id == message_id
                && r.user_id == user_id
                && r.emoji == emoji
        });

        let Some(pos) = pos else {
            log::warn!("Unable to find reaction in tracked reactions");
            return None;
        };

        Some(writer.remove(pos))
    }
}

#[derive(Debug, Clone)]
pub struct CachedReaction {
    pub event_id: OwnedEventId,
    pub room_id: String,
    pub message_id: String,
    pub user_id: String,
    pub emoji: String,
}

impl From<CachedReaction> for gouda_proto::chat::Reaction {
    fn from(reaction_event: CachedReaction) -> Self {
        Self {
            room_id: reaction_event.room_id,
            message_id: reaction_event.message_id,
            reaction: reaction_event.emoji,
            user_id: Some(reaction_event.user_id),
        }
    }
}
