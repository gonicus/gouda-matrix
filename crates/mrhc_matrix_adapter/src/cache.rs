use core::time::Duration;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};

use js_int::UInt;
use matrix_sdk::room::{IncludeRelations, Relations as MatrixRelations, RelationsOptions};
use matrix_sdk::ruma::api::Direction;
use matrix_sdk::ruma::events::relation::RelationType as MatrixRelationType;
use matrix_sdk::ruma::events::room::encrypted::{
    OriginalSyncRoomEncryptedEvent, Relation as EncryptionRelation,
};
use matrix_sdk::ruma::events::room::member::{
    MembershipChange as MatrixMembershipChange, RoomMemberEventContent,
};
use matrix_sdk::ruma::events::room::message::{MessageType, Relation, RoomMessageEventContent};
use matrix_sdk::ruma::events::room::redaction::SyncRoomRedactionEvent;
use matrix_sdk::ruma::events::{
    AnySyncMessageLikeEvent, AnySyncStateEvent, AnySyncTimelineEvent, Mentions,
    OriginalSyncMessageLikeEvent, StateEventType, SyncMessageLikeEvent, SyncStateEvent,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId};
use matrix_sdk_common::deserialized_responses::{TimelineEvent, TimelineEventKind};
use mrhc_core::{ClientContext, MultipartResponse};
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{
    builder, message, EventOrigin, Message, MessageContentMembershipChange, MessageRemoveEvent,
    MessagesOrder, Reaction,
};
use ruma_common::EventId;
use tokio::sync::mpsc;

use crate::media::MediaManager;
use crate::user::convert_membership_change;
use crate::{debug_assert_or_log, messages};

#[derive(Debug, Clone, thiserror::Error)]
pub enum CacheError {
    #[error("cache has been poisoned and may be invalid")]
    CachePoisoned,

    #[error("attempt to access a dropped message")]
    DeallocatedMessageAccess,

    #[error("failed to deserialize event for caching")]
    DeserializationFailed,

    #[error("failed to fetch event from SDK")]
    EventFetchFailed,

    #[error("id is not a valid matrix event id")]
    InvalidEventId,

    #[error("attempt to access an unknown message")]
    UncachedMessageAccess,

    #[error("attempt to access an unknown message sequence")]
    UncachedSequenceAccess,

    #[error("attempt to access an unknown room")]
    UncachedRoomAccess,

    #[error("an unexpected error occurred")]
    Unexpected,

    #[error("{message}: {source}")]
    Context {
        message: String,
        source: Box<CacheError>,
    },
}

type Result<T> = std::result::Result<T, CacheError>;

pub type CachedData = HashMap<OwnedRoomId, Arc<RwLock<CachedChronoRoom>>>;

#[derive(Default, Clone)]
pub struct Cache {
    cached_data: Arc<RwLock<CachedData>>,
    reactions: Arc<RwLock<Vec<CachedReaction>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cached_data_read_lock(&self) -> Result<RwLockReadGuard<'_, CachedData>> {
        self.cached_data.read().map_err(|_| {
            log::error!("Cached data poisoned");
            CacheError::CachePoisoned
        })
    }

    pub fn cached_data_write_lock(&self) -> Result<RwLockWriteGuard<'_, CachedData>> {
        self.cached_data.write().map_err(|_| {
            log::error!("Cached data poisoned");
            CacheError::CachePoisoned
        })
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

impl From<CachedReaction> for mrhc_proto::chat::Reaction {
    fn from(reaction_event: CachedReaction) -> Self {
        Self {
            room_id: reaction_event.room_id,
            message_id: reaction_event.message_id,
            reaction: reaction_event.emoji,
            user_id: Some(reaction_event.user_id),
        }
    }
}

#[derive(Default, Debug)]
pub struct CachedChronoRoom {
    pub unresolved_relations: Vec<UnresolvedRelation>,
    pub chrono_sequences: Vec<CachedChronoSequence>,
}

#[derive(Default)]
pub struct CachedChronoSequence {
    pub messages: HashMap<OwnedEventId, Arc<RwLock<Option<CachedMessage>>>>,
    msg_begin: Arc<RwLock<Option<CachedMessage>>>,
    msg_end: Arc<RwLock<Option<CachedMessage>>>,
}

#[derive(Default, Clone)]
pub struct CachedMessage {
    pub id: String,
    pub r#type: EventType,
    pub timestamp: u64,
    pub rel_type: Option<RelationType>,
    pub rel_to: Arc<RwLock<Option<CachedMessage>>>,
    pub rel_by: Vec<Weak<RwLock<Option<CachedMessage>>>>,
    pub before: Arc<RwLock<Option<CachedMessage>>>,
    pub after: Weak<RwLock<Option<CachedMessage>>>,
    pub next_token: Option<String>,
    pub prev_token: Option<String>,
}

#[derive(Debug)]
pub struct UnlinkedMessage {
    pub id: String,
    pub r#type: EventType,
    pub timestamp: u64,
    pub rel_type: Option<RelationType>,
    pub rel_to: Option<OwnedEventId>,
    pub next_token: Option<String>,
    pub prev_token: Option<String>,
}

impl Default for UnlinkedMessage {
    fn default() -> Self {
        UnlinkedMessage {
            id: String::new(),
            r#type: EventType::Unknown,
            timestamp: 0,
            rel_type: None,
            rel_to: None,
            next_token: None,
            prev_token: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct UnresolvedRelation {
    pub related_id: String,
    pub relator_id: String,
}

#[derive(Copy, Clone, Debug)]
pub enum SyncSource {
    InitialSync,
    ContinuousSync,
    RoomMessages,
    _EventContext,
}

pub struct SequenceChunkResult {
    pub messages: Option<Vec<Message>>,
    pub is_complete: bool,
}

impl CachedChronoRoom {
    pub fn new() -> Self {
        let sequences = Vec::new();

        Self {
            unresolved_relations: Vec::new(),
            chrono_sequences: sequences,
        }
    }

    fn add_batch(
        &mut self,
        messages: Vec<UnlinkedMessage>,
        neighboring_batch: Option<&str>,
        source: SyncSource,
    ) -> Result<()> {
        let connects_to_sequence: bool;
        let mut i_seq = 0usize;

        match source {
            SyncSource::InitialSync => connects_to_sequence = false,
            SyncSource::_EventContext => connects_to_sequence = false,
            SyncSource::ContinuousSync => {
                if self.chrono_sequences.is_empty() {
                    connects_to_sequence = false;
                } else {
                    connects_to_sequence = true;
                    i_seq = self.chrono_sequences.len() - 1;
                }
            }
            SyncSource::RoomMessages => {
                if let Some(token) = neighboring_batch {
                    if let Some(i) = self.find_sequence(token)? {
                        connects_to_sequence = true;
                        i_seq = i;
                    } else {
                        connects_to_sequence = false;
                    }
                } else {
                    connects_to_sequence = false;
                }
            }
        }

        let mut unresolved: Vec<UnresolvedRelation>;

        if connects_to_sequence {
            log::trace!("Message batch connects to existing CachedChronoSequence");
            unresolved = self
                .chrono_sequences
                .get_mut(i_seq)
                .ok_or(CacheError::UncachedSequenceAccess)?
                .add_batch(messages)?;
        } else {
            log::trace!("Creating new CachedChronoSequence for message batch");
            let mut sequence = CachedChronoSequence::new()?;
            unresolved = sequence.add_batch(messages)?;

            self.insert_sequence(sequence)?;
        }

        self.unresolved_relations.append(&mut unresolved);

        // 2. check if unconnected sequences have common messages and merge them
        self.merge_islands()?;

        self.link_unresolved_relations();

        Ok(())
    }

    fn merge_islands(&mut self) -> Result<()> {
        let mut merge_sequences = None;

        for (pos1, seq1) in self.chrono_sequences.iter().enumerate() {
            for (pos2, seq2) in self.chrono_sequences.iter().enumerate() {
                if pos2 <= pos1 {
                    continue;
                }
                if !seq1
                    .messages
                    .keys()
                    .any(|key| seq2.messages.contains_key(key))
                {
                    continue;
                }

                log::trace!("Merging sequence islands at positions {pos1} and {pos2}");
                merge_sequences = Some(vec![pos1, pos2]);
            }
        }

        if let Some(pair) = merge_sequences {
            let seq = self.merge_sequences(&pair)?;

            self.chrono_sequences
                .remove(*pair.get(1).ok_or(CacheError::Unexpected)?);
            self.chrono_sequences
                .remove(*pair.first().ok_or(CacheError::Unexpected)?);

            self.chrono_sequences
                .insert(*pair.first().ok_or(CacheError::Unexpected)?, seq)
        }

        Ok(())
    }

    fn insert_sequence(&mut self, sequence: CachedChronoSequence) -> Result<()> {
        let first_ts = sequence.get_first_timestamp()?;

        for (pos, seq) in self.chrono_sequences.iter().enumerate() {
            if first_ts < seq.get_first_timestamp()? {
                self.chrono_sequences.insert(pos, sequence);
                return Ok(());
            }
        }

        self.chrono_sequences.push(sequence);

        Ok(())
    }

    fn find_sequence(&self, token: &str) -> Result<Option<usize>> {
        for (pos, seq) in self.chrono_sequences.iter().enumerate() {
            for msg in seq.message_iterator() {
                if let Some(msg_token) = &msg
                    .read()
                    .map_err(|_| CacheError::CachePoisoned)?
                    .as_ref()
                    .ok_or(CacheError::UncachedMessageAccess)?
                    .next_token
                {
                    if msg_token == token {
                        return Ok(Some(pos));
                    }
                }

                if let Some(msg_token) = &msg
                    .read()
                    .map_err(|_| CacheError::CachePoisoned)?
                    .as_ref()
                    .ok_or(CacheError::UncachedMessageAccess)?
                    .prev_token
                {
                    if msg_token == token {
                        return Ok(Some(pos));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Merges arbitrary CachedChronoSequences (identified by their vector indices) into one
    /// while draining the original CachedChronoSequence::messages HashMaps. The double-linking is recreated
    /// and the ordering is determined anew based on CachedMessage::timestamp. CachedMessages with the same id
    /// (key) are collapsed into a single entity.
    /// Note, that CachedMessage::rel_to and CachedMessage::rel_by are not modified here, neither are other fields.
    fn merge_sequences(&mut self, sequences_pos: &Vec<usize>) -> Result<CachedChronoSequence> {
        let mut new_seq = CachedChronoSequence::new()?;

        for pos in sequences_pos {
            if let Some(seq) = self.chrono_sequences.get_mut(*pos) {
                for (id, msg) in seq.messages.drain() {
                    new_seq.insert_chronological(msg, &id)?;
                }
            }
        }

        Ok(new_seq)
    }

    fn get_message(&self, id: &OwnedEventId) -> Option<Arc<RwLock<Option<CachedMessage>>>> {
        for seq in &self.chrono_sequences {
            match seq.messages.get(id) {
                Some(msg) => return Some(msg.clone()),
                None => continue,
            }
        }

        None
    }

    fn link_unresolved_relations(&mut self) {
        let mut unresolved = std::mem::take(&mut self.unresolved_relations);

        unresolved.retain(|rel| {
            for seq in &self.chrono_sequences {
                let related_id = match OwnedEventId::try_from(rel.related_id.clone()) {
                    Ok(val) => val,
                    Err(_) => {
                        log::warn!("Encountered invalid event id (id={})", rel.related_id);
                        continue;
                    }
                };

                let relator_id = match OwnedEventId::try_from(rel.relator_id.clone()) {
                    Ok(val) => val,
                    Err(_) => {
                        log::warn!("Encountered invalid event id (id={})", rel.relator_id);
                        continue;
                    }
                };

                if seq.messages.contains_key(&related_id) {
                    match self.link_relation(&relator_id, &related_id) {
                        Ok(_) => return false,
                        Err(_) => return true,
                    }
                }
            }
            true
        });

        self.unresolved_relations = unresolved;
    }

    fn link_relation(&mut self, id: &OwnedEventId, rel_id: &OwnedEventId) -> Result<()> {
        let message = self
            .get_message(id)
            .ok_or(CacheError::UncachedMessageAccess)?
            .clone();

        let mut message_w = message.write().map_err(|_| CacheError::CachePoisoned)?;

        let rel_message = self
            .get_message(rel_id)
            .ok_or(CacheError::UncachedMessageAccess)?
            .clone();

        let mut rel_message_w = rel_message.write().map_err(|_| CacheError::CachePoisoned)?;

        message_w
            .as_mut()
            .ok_or(CacheError::UncachedMessageAccess)?
            .rel_to = rel_message.clone();

        rel_message_w
            .as_mut()
            .ok_or(CacheError::UncachedMessageAccess)?
            .rel_by
            .push(Arc::downgrade(&message.clone()));

        Ok(())
    }
}

impl CachedChronoSequence {
    fn new() -> Result<Self> {
        let begin_sentinel = CachedMessage::default();
        let end_sentinel = CachedMessage::default();

        let begin = Arc::new(RwLock::new(Some(begin_sentinel)));
        let end = Arc::new(RwLock::new(Some(end_sentinel)));

        {
            let mut begin_w = begin.write().map_err(|_| CacheError::Unexpected)?;
            begin_w.as_mut().ok_or(CacheError::Unexpected)?.after = Arc::downgrade(&end.clone());
            begin_w.as_mut().ok_or(CacheError::Unexpected)?.id =
                SequenceSentinel::Begin.to_string();
        }

        {
            let mut end_w = end.write().map_err(|_| CacheError::Unexpected)?;
            end_w.as_mut().ok_or(CacheError::Unexpected)?.before = begin.clone();
            end_w.as_mut().ok_or(CacheError::Unexpected)?.id = SequenceSentinel::End.to_string();
        }

        let messages = HashMap::new();

        Ok(Self {
            messages,
            msg_begin: begin,
            msg_end: end,
        })
    }

    /// Implements the critical transformation from sync-timeline to chronological timeline.
    /// Messages provided to add_batch may be obtained from a call to /sync, /rooms/{roomId}/messages
    /// or /room/{roomId}/context/{eventId}. Sync tokens obtained from these APIs are expected to
    /// be attached to the first and last message of each timeline-batch.
    fn add_batch(&mut self, mut messages: Vec<UnlinkedMessage>) -> Result<Vec<UnresolvedRelation>> {
        // Remove all unlinked messages which are already cached
        messages.retain(|msg| {
            !self
                .messages
                .contains_key(&match OwnedEventId::try_from(msg.id.clone()) {
                    Ok(val) => val,
                    Err(_) => return false,
                })
        });

        log::trace!("Adding batch of unlinked messages to CachedChronoSequence:\n{messages:#?}");

        // Separate unlinked messages in three batches:
        //   1. before the oldest cached event
        //   2. after the latest cached event
        //   3. in between
        messages.sort_unstable_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let earliest_ts = self.get_first_timestamp()?;
        let latest_ts = self.get_last_timestamp()?;

        let (prior, rest): (_, Vec<UnlinkedMessage>) = messages
            .into_iter()
            .partition(|x| x.timestamp < earliest_ts);

        let (concurrent, after): (_, Vec<UnlinkedMessage>) =
            rest.into_iter().partition(|x| x.timestamp < latest_ts);

        let mut unresolved_rel = vec![];

        // Append batch 1 from the oldest cached message (begin.after) to begin in reverse order
        for ul_msg in prior.into_iter().rev() {
            let (msg, unresolved) = CachedMessage::from_unlinked(&ul_msg);

            if let Some(rel) = unresolved {
                unresolved_rel.push(rel);
            }

            self.insert_begin(
                Arc::new(RwLock::new(Some(msg))),
                &OwnedEventId::try_from(ul_msg.id).map_err(|_| CacheError::InvalidEventId)?,
            )
            .map_err(|e| CacheError::Context {
                message: "failed to append message to room history".to_string(),
                source: Box::new(e),
            })?;
        }

        // Append batch 2 from the last cached message (end.before) to end in normal order
        for ul_msg in after {
            let (msg, unresolved) = CachedMessage::from_unlinked(&ul_msg);

            if let Some(rel) = unresolved {
                unresolved_rel.push(rel);
            }

            self.insert_end(
                Arc::new(RwLock::new(Some(msg))),
                &OwnedEventId::try_from(ul_msg.id).map_err(|_| CacheError::InvalidEventId)?,
            )
            .map_err(|e| CacheError::Context {
                message: "failed to append message to room history".to_string(),
                source: Box::new(e),
            })?;
        }
        // For each message in batch 3 cycle through all cached events in normal order to find the right
        // place for insertion
        for ul_msg in concurrent {
            let (msg, unresolved) = CachedMessage::from_unlinked(&ul_msg);

            if let Some(rel) = unresolved {
                unresolved_rel.push(rel);
            }

            self.insert_chronological(
                Arc::new(RwLock::new(Some(msg))),
                &OwnedEventId::try_from(ul_msg.id).map_err(|_| CacheError::InvalidEventId)?,
            )
            .map_err(|e| CacheError::Context {
                message: "failed to insert message into room history".to_string(),
                source: Box::new(e),
            })?;
        }

        log::trace!("Cached chrono sequence is now:\n{self:#?}");

        Ok(unresolved_rel)
    }

    fn get_first_timestamp(&self) -> Result<u64> {
        Ok(self.
            msg_begin.
            read().
            map_err(|_| CacheError::CachePoisoned)?.
            as_ref().
            ok_or(CacheError::UncachedMessageAccess)?.
            after.
            upgrade().
            ok_or(CacheError::DeallocatedMessageAccess)?. // Theres the catch
            read().
            map_err(|_| CacheError::CachePoisoned)?.
            as_ref().
            ok_or(CacheError::UncachedMessageAccess)?.
            timestamp)
    }

    fn get_last_timestamp(&self) -> Result<u64> {
        Ok(self
            .msg_end
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .before
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .timestamp)
    }

    fn insert_before(
        &mut self,
        message: Arc<RwLock<Option<CachedMessage>>>,
        id: &OwnedEventId,
        before: Arc<RwLock<Option<CachedMessage>>>,
    ) -> Result<()> {
        let msg_after = before;

        let msg_before = msg_after
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .before
            .clone();

        self.relink(&message, &msg_before, &Arc::downgrade(&msg_after))?;

        self.insert_messages_map(id, message);

        Ok(())
    }

    fn insert_end(
        &mut self,
        message: Arc<RwLock<Option<CachedMessage>>>,
        id: &OwnedEventId,
    ) -> Result<()> {
        let msg_after = self.msg_end.clone();

        let msg_before = msg_after
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .before
            .clone();

        self.relink(&message, &msg_before, &Arc::downgrade(&msg_after))?;

        self.insert_messages_map(id, message);

        Ok(())
    }

    fn insert_begin(
        &mut self,
        message: Arc<RwLock<Option<CachedMessage>>>,
        id: &OwnedEventId,
    ) -> Result<()> {
        let msg_before = self.msg_begin.clone();

        let msg_after = msg_before
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .after
            .clone();

        self.relink(&message, &msg_before, &msg_after)?;

        self.insert_messages_map(id, message);

        Ok(())
    }

    fn insert_chronological(
        &mut self,
        message: Arc<RwLock<Option<CachedMessage>>>,
        id: &OwnedEventId,
    ) -> Result<()> {
        if self.messages.is_empty() {
            self.insert_end(message, id)?;

            log::trace!("Inserting event {id} as the first element in the list");

            return Ok(());
        }

        if self.messages.contains_key(id) {
            return Ok(());
        }

        let message_time = message
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .timestamp;

        let mut insert_before_msg = None;

        for c_msg in self.message_iterator() {
            let t1 = c_msg
                .read()
                .map_err(|_| CacheError::CachePoisoned)?
                .as_ref()
                .ok_or(CacheError::UncachedMessageAccess)?
                .timestamp;

            if message_time < t1 {
                insert_before_msg = Some(c_msg.clone());
                break;
            }
        }

        match insert_before_msg {
            Some(c_msg) => self.insert_before(message, id, c_msg)?,
            None => self.insert_end(message, id)?,
        }

        Ok(())
    }

    fn relink(
        &mut self,
        message: &Arc<RwLock<Option<CachedMessage>>>,
        before: &Arc<RwLock<Option<CachedMessage>>>,
        after: &Weak<RwLock<Option<CachedMessage>>>,
    ) -> Result<()> {
        {
            let mut message_w = message.write().map_err(|_| CacheError::CachePoisoned)?;
            message_w
                .as_mut()
                .ok_or(CacheError::UncachedMessageAccess)?
                .after = after.clone();
            message_w
                .as_mut()
                .ok_or(CacheError::UncachedMessageAccess)?
                .before = before.clone();
        }

        {
            let arc_after = after
                .upgrade()
                .ok_or(CacheError::DeallocatedMessageAccess)?;
            let mut msg_after_w = arc_after.write().map_err(|_| CacheError::CachePoisoned)?;
            msg_after_w
                .as_mut()
                .ok_or(CacheError::UncachedMessageAccess)?
                .before = message.clone();

            let mut msg_before_w = before.write().map_err(|_| CacheError::CachePoisoned)?;
            msg_before_w
                .as_mut()
                .ok_or(CacheError::UncachedMessageAccess)?
                .after = Arc::downgrade(&message.clone());
        }

        Ok(())
    }

    fn insert_messages_map(&mut self, id: &OwnedEventId, val: Arc<RwLock<Option<CachedMessage>>>) {
        self.messages.insert(id.clone(), val);
    }

    fn message_iterator(&self) -> MessageIterator {
        MessageIterator {
            current: Some(self.msg_begin.clone()),
        }
    }
}

impl CachedMessage {
    fn is_last(&self) -> Result<bool> {
        if self
            .after
            .upgrade()
            .ok_or(CacheError::DeallocatedMessageAccess)?
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .id
            == SequenceSentinel::End.to_string()
        {
            return Ok(true);
        }

        Ok(false)
    }

    fn is_first(&self) -> Result<bool> {
        if self
            .before
            .read()
            .map_err(|_| CacheError::CachePoisoned)?
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .id
            == SequenceSentinel::Begin.to_string()
        {
            return Ok(true);
        }

        Ok(false)
    }
}

#[derive(Debug, Copy, Clone)]
enum SequenceSentinel {
    Begin,
    End,
}

impl fmt::Display for SequenceSentinel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SequenceSentinel::Begin => write!(f, "begin"),
            SequenceSentinel::End => write!(f, "end"),
        }
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum EventType {
    Encrypted,
    Message,
    Reaction,
    Redacted,
    Redaction,
    State,
    #[default]
    Unknown,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum RelationType {
    Reaction,
    Redaction,
    Replacement,
    Thread,
}

impl UnlinkedMessage {
    fn from_any_sync_timeline_event(
        tl_evt: &AnySyncTimelineEvent,
        next_token: Option<&str>,
        prev_token: Option<&str>,
    ) -> UnlinkedMessage {
        let nt = next_token.map(|s| s.to_string());
        let pt = prev_token.map(|s| s.to_string());

        let mut ul_msg = UnlinkedMessage {
            id: tl_evt.event_id().to_string(),
            timestamp: tl_evt.origin_server_ts().get().into(),
            next_token: nt,
            prev_token: pt,
            ..Default::default()
        };

        // fill type of the message and possible relation information
        match &tl_evt {
            AnySyncTimelineEvent::MessageLike(event) => {
                match event {
                    AnySyncMessageLikeEvent::RoomMessage(SyncMessageLikeEvent::Original(orig)) => {
                        ul_msg.r#type = EventType::Message;

                        if let Some(rel) = &orig.content.relates_to {
                            match rel {
                                // for now a reply is not relevant for caching and offline preprocessing
                                Relation::Reply { in_reply_to: _ } => {}
                                Relation::Replacement(repl) => {
                                    ul_msg.rel_to = Some(repl.event_id.clone());
                                    ul_msg.rel_type = Some(RelationType::Replacement);
                                }
                                Relation::Thread(thread) => {
                                    ul_msg.rel_to = Some(thread.event_id.clone());
                                    ul_msg.rel_type = Some(RelationType::Thread);
                                }
                                &_ => {
                                    log::warn!(
                                        "Ignoring custom relation in event {} on cache assembly",
                                        tl_evt.event_id()
                                    );
                                }
                            }
                        }
                    }
                    AnySyncMessageLikeEvent::Reaction(SyncMessageLikeEvent::Original(orig)) => {
                        ul_msg.r#type = EventType::Reaction;
                        ul_msg.rel_to = Some(orig.content.relates_to.event_id.clone());
                        ul_msg.rel_type = Some(RelationType::Reaction);
                    }
                    AnySyncMessageLikeEvent::RoomRedaction(SyncRoomRedactionEvent::Original(
                        orig,
                    )) => {
                        ul_msg.r#type = EventType::Redaction;
                        ul_msg.rel_to = orig.redacts.clone();
                        ul_msg.rel_type = Some(RelationType::Redaction);
                    }
                    AnySyncMessageLikeEvent::RoomEncrypted(SyncMessageLikeEvent::Original(
                        orig,
                    )) => {
                        ul_msg.r#type = EventType::Encrypted;

                        if let Some(rel) = &orig.content.relates_to {
                            match rel {
                                EncryptionRelation::Reply { in_reply_to: _ } => {}
                                EncryptionRelation::Replacement(repl) => {
                                    ul_msg.rel_to = Some(repl.event_id.clone());
                                    ul_msg.rel_type = Some(RelationType::Replacement);
                                }
                                EncryptionRelation::Thread(thread) => {
                                    ul_msg.rel_to = Some(thread.event_id.clone());
                                    ul_msg.rel_type = Some(RelationType::Thread);
                                }
                                &_ => {
                                    log::warn!(
                                        "Ignoring custom relation in event {} on cache assembly",
                                        tl_evt.event_id()
                                    );
                                }
                            }
                        }
                    }
                    _ => {
                        // Add further event types here
                    }
                }

                if event.is_redacted() {
                    ul_msg.r#type = EventType::Redacted;
                }
            }
            AnySyncTimelineEvent::State(event) => match event.is_redacted() {
                true => ul_msg.r#type = EventType::Redacted,
                false => ul_msg.r#type = EventType::State,
            },
        }

        ul_msg
    }

    fn from_unrecoverable_timeline(evt: &TimelineEvent) -> UnlinkedMessage {
        let id = evt
            .event_id()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let ts = evt
            .timestamp()
            .unwrap_or(MilliSecondsSinceUnixEpoch(UInt::MIN));

        UnlinkedMessage {
            id,
            r#type: EventType::Unknown,
            timestamp: ts.get().into(),
            ..Default::default()
        }
    }
}

impl CachedMessage {
    fn from_unlinked(ul_msg: &UnlinkedMessage) -> (Self, Option<UnresolvedRelation>) {
        let msg = CachedMessage {
            id: ul_msg.id.clone(),
            r#type: ul_msg.r#type,
            timestamp: ul_msg.timestamp,
            rel_type: ul_msg.rel_type,
            next_token: ul_msg.next_token.clone(),
            prev_token: ul_msg.prev_token.clone(),
            ..Default::default()
        };

        let rel = ul_msg.rel_to.as_ref().map(|id| UnresolvedRelation {
            related_id: id.to_string(),
            relator_id: ul_msg.id.to_string(),
        });

        (msg, rel)
    }
}

struct MessageIterator {
    current: Option<Arc<RwLock<Option<CachedMessage>>>>,
}

impl Iterator for MessageIterator {
    type Item = Arc<RwLock<Option<CachedMessage>>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().and_then(|node| {
            // return None if the lock is poisoned
            let node_ref = node.clone();
            let node_ref_r = node_ref.read().ok()?;

            // return None if the current node is None - should not happen normally
            let node_ref_inner = node_ref_r.as_ref()?;

            let after_ref = node_ref_inner.after.clone();
            // return None if the forward reference has been invalidated
            let arc_after = after_ref.upgrade()?;

            // return None if we are at `end`
            if node_ref_inner.id == SequenceSentinel::End.to_string() {
                return None;
            };

            // update current for next iteration
            self.current = Some(arc_after.clone());

            // fast forward to the next node if we are at `begin`
            if node_ref_inner.id == SequenceSentinel::Begin.to_string() {
                self.next()
            } else {
                Some(node_ref.clone())
            }
        })
    }
}

pub fn get_or_create_room(
    cache: &Cache,
    room_id: &OwnedRoomId,
) -> Result<Arc<RwLock<CachedChronoRoom>>> {
    let mut cache_w = cache.cached_data_write_lock()?;

    let cached_room = match cache_w.get_mut(room_id) {
        Some(val) => {
            log::debug!("Found cached room with id {}", room_id);
            val.clone()
        }
        None => {
            log::debug!(
                "Did not find cached room with id {} - creating a new empty room cache",
                room_id
            );
            cache_w.insert(
                room_id.clone(),
                Arc::new(RwLock::new(CachedChronoRoom::new())),
            );
            cache_w
                .get(&room_id.clone())
                .ok_or(CacheError::UncachedRoomAccess)?
                .clone()
        }
    };

    Ok(cached_room)
}

pub fn cache_sync_response(
    cache: &Cache,
    response: &matrix_sdk::sync::SyncResponse,
    source: SyncSource,
) -> Result<()> {
    let next_token = &response.next_batch;

    // 1. go through all rooms in the response
    for (room_id, room_update) in &response.rooms.joined {
        // 2. check if row with key=roomId exists in cache, if not create a new CachedChronoRoom
        {
            let mut cache_w = cache.cached_data_write_lock()?;
            cache_w
                .entry(room_id.clone())
                .or_insert_with(|| Arc::new(RwLock::new(CachedChronoRoom::new())));
        }

        // 3. go through all messages in the response for that room and create UnlinkedMessage's
        let messages = unlinked_from_timeline(
            &room_update.timeline.events,
            Some(next_token),
            room_update.timeline.prev_batch.as_deref(),
            true,
        )?;

        // 4. call CachedChronoRoom::add_batch(messages, None, SyncSource::InitialSync)
        let room = {
            let cache_r = cache.cached_data_read_lock()?;

            cache_r
                .get(room_id)
                .ok_or(CacheError::UncachedRoomAccess)?
                .clone()
        };

        room.write()
            .map_err(|_| CacheError::CachePoisoned)?
            .add_batch(messages, None, source)?;
    }

    Ok(())
}

/// Takes the response of a room-messages request to the matrix_sdk
/// and appends it to a `CachedChronoRoom` specified by `room_id`
/// Will create a new `CachedChronoRoom` if necessary
pub fn cache_room_messages_response(
    cache: &Cache,
    response: &matrix_sdk::room::Messages,
    room_id: OwnedRoomId,
    chronological: bool,
) -> Result<()> {
    {
        let mut cache_w = cache.cached_data_write_lock()?;
        cache_w
            .entry(room_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(CachedChronoRoom::new())));
    }

    let room = {
        let cache_r = cache.cached_data_read_lock()?;

        cache_r
            .get(&room_id)
            .ok_or(CacheError::UncachedRoomAccess)?
            .clone()
    };

    cache_room_messages_response_to_room(room, response, chronological)
}

/// Takes the response of a room-messages request to the matrix_sdk
/// and appends it to a `CachedChronoRoom` specified by `room` and `room_id`.
fn cache_room_messages_response_to_room(
    room: Arc<RwLock<CachedChronoRoom>>,
    response: &matrix_sdk::room::Messages,
    chronological: bool,
) -> Result<()> {
    log::trace!("Received messages response:\n{response:#?}");

    let neighboring_batch: Option<String>;
    let (next_token, prev_token): (Option<String>, Option<String>);
    if chronological {
        prev_token = Some(response.start.clone());
        next_token = response.end.clone();
        neighboring_batch = prev_token.clone();
    } else {
        prev_token = response.end.clone();
        next_token = Some(response.start.clone());
        neighboring_batch = next_token.clone();
    }

    let messages = unlinked_from_timeline(
        &response.chunk,
        next_token.as_deref(),
        prev_token.as_deref(),
        chronological,
    )?;

    room.write()
        .map_err(|_| CacheError::CachePoisoned)?
        .add_batch(
            messages,
            neighboring_batch.as_deref(),
            SyncSource::RoomMessages,
        )?;

    Ok(())
}

fn unlinked_from_timeline(
    events: &Vec<TimelineEvent>,
    next_token: Option<&str>,
    prev_token: Option<&str>,
    chronological: bool,
) -> Result<Vec<UnlinkedMessage>> {
    let mut messages = vec![];

    for evt in events {
        let tl_evt = match try_deserialize_evt(evt) {
            Some(ev) => ev,
            None => {
                // fallback for unrecoverable events
                messages.push(UnlinkedMessage::from_unrecoverable_timeline(evt));
                continue;
            }
        };

        // create an UnlinkedMessage for each message and push them to a Vec<UnlinkedMessage>
        let ul_msg = UnlinkedMessage::from_any_sync_timeline_event(&tl_evt, None, None);
        messages.push(ul_msg);
    }

    if let Some(last) = messages.last_mut() {
        if chronological {
            last.next_token = next_token.map(|s| s.to_string());
        } else {
            last.prev_token = prev_token.map(|s| s.to_string());
        }
    }

    if let Some(first) = messages.first_mut() {
        if chronological {
            first.prev_token = prev_token.map(|s| s.to_string());
        } else {
            first.next_token = next_token.map(|s| s.to_string());
        }
    }

    Ok(messages)
}

fn try_deserialize_evt(evt: &TimelineEvent) -> Option<AnySyncTimelineEvent> {
    match &evt.kind {
        TimelineEventKind::Decrypted(decrypted) => match decrypted.event.deserialize() {
            Ok(ev) => Some(ev.into()),
            Err(_) => {
                handle_timeline_error(evt, CacheError::DeserializationFailed);
                None
            }
        },
        TimelineEventKind::UnableToDecrypt { event, .. }
        | TimelineEventKind::PlainText { event } => match event.deserialize() {
            Ok(ev) => Some(ev),
            Err(_) => {
                handle_timeline_error(evt, CacheError::DeserializationFailed);
                None
            }
        },
    }
}

fn handle_timeline_error(evt: &TimelineEvent, err: CacheError) {
    let id = evt
        .event_id()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    log::error!("Failed to process event {}: {:?}", id, err);
}

fn read_event_id(msg: &Arc<RwLock<Option<CachedMessage>>>) -> Result<OwnedEventId> {
    let guard = msg.read().map_err(|_| CacheError::CachePoisoned)?;
    let cached = guard.as_ref().ok_or(CacheError::UncachedMessageAccess)?;

    OwnedEventId::try_from(cached.id.clone()).map_err(|_| CacheError::InvalidEventId)
}

fn advance_message(
    msg: &Arc<RwLock<Option<CachedMessage>>>,
    order: MessagesOrder,
    sync_token: &mut Option<String>,
    check_edge: bool,
) -> Result<Option<Arc<RwLock<Option<CachedMessage>>>>> {
    let guard = msg.read().map_err(|_| CacheError::CachePoisoned)?;
    let cached = guard.as_ref().ok_or(CacheError::UncachedMessageAccess)?;

    match order {
        MessagesOrder::Backward => {
            if let Some(token) = &cached.prev_token {
                *sync_token = Some(token.clone());
            }
            if check_edge && cached.is_first()? {
                return Ok(None);
            }
            Ok(Some(cached.before.clone()))
        }
        MessagesOrder::Forward => {
            if let Some(token) = &cached.next_token {
                *sync_token = Some(token.clone());
            }
            if check_edge && cached.is_last()? {
                return Ok(None);
            }
            Ok(Some(
                cached
                    .after
                    .upgrade()
                    .ok_or(CacheError::DeallocatedMessageAccess)?
                    .clone(),
            ))
        }
    }
}

/// Retrieves a set of `limit` mrhc_proto::chat::Message objects
/// starting from the message with id `from_id` in the given order from the chronological
/// cache (or from the first or last known ID if omitted -- depending on the `order`).
/// Redacted events are counted as 1, replacement events are counted as 0. Only the
/// new_content of the latest replacement is considered as the Message.content, while the id of
/// of the original event is used as the `message_id`.
/// None is returned if the required chunk is not fully contained in the cache. In this case,
/// the second return value returns the sync token needed to query the next batch from the server.
pub async fn send_and_get_sequence_chunk<T: RoomClient>(
    cached_room: &Arc<RwLock<CachedChronoRoom>>,
    from_id: OwnedEventId,
    limit: u32,
    order: MessagesOrder,
    skip_first: bool,
    room_client: &T,
    cache: &Cache,
    ctx: &ClientContext,
) -> Result<SequenceChunkResult> {
    let msg_opt = {
        let room_cache_r = cached_room.read().map_err(|_| CacheError::CachePoisoned)?;
        room_cache_r.get_message(&from_id.clone()).clone()
    };

    let mut msg = match msg_opt {
        Some(m) => m,
        None => {
            log::warn!("Requested id {from_id} is not cached TODO: implement context request");
            return Err(CacheError::UncachedMessageAccess);
        }
    };

    log::trace!("Found requested id {from_id} in cache");

    let mut sync_token = None;
    let limit = usize::try_from(limit).map_err(|_| CacheError::Unexpected)?;
    let mut result = Vec::new();

    let list_stream = ctx.begin_multipart_response();

    // iterate over msg until result has enough events
    while result.len() < limit {
        let current_id = read_event_id(&msg)?;

        // Skip the starting message when the caller provided an explicit from_id
        if !skip_first || current_id != from_id {
            try_send_and_append_message(
                &mut result,
                cached_room,
                &current_id,
                room_client,
                cache,
                &list_stream,
            )
            .await;
        }

        let need_more = result.len() < limit;
        match advance_message(&msg, order, &mut sync_token, need_more)? {
            Some(next_msg) => msg = next_msg,
            None => {
                return Ok(SequenceChunkResult {
                    messages: Some(result),
                    is_complete: false,
                });
            }
        }
    }

    Ok(SequenceChunkResult {
        messages: Some(result),
        is_complete: true,
    })
}

async fn try_send_and_append_message<T: RoomClient>(
    result: &mut Vec<Message>,
    cached_room: &Arc<RwLock<CachedChronoRoom>>,
    current_id: &OwnedEventId,
    room_client: &T,
    cache: &Cache,
    list_stream: &MultipartResponse,
) {
    let proto =
        assemble_proto_message(cached_room.clone(), current_id.clone(), room_client, cache).await;

    let Ok(proto_msg) = proto else {
        log::warn!("Failed to assemble proto message for eventy {current_id}");
        return;
    };

    if let Some(proto_msg) = proto_msg {
        result.push(proto_msg.clone());
        list_stream.send_item(ResponseContent::MessageReceivedEvent(proto_msg));
    }
}

pub fn check_cached_enough(
    cached_room: &Arc<RwLock<CachedChronoRoom>>,
    from_id: OwnedEventId,
    limit: u32,
    order: MessagesOrder,
    skip_first: bool,
) -> Result<Option<String>> {
    let msg_opt = {
        let room_cache_r = cached_room.read().map_err(|_| CacheError::CachePoisoned)?;
        room_cache_r.get_message(&from_id.clone()).clone()
    };

    let mut msg = match msg_opt {
        Some(m) => m,
        None => {
            log::warn!("Requested id {from_id} is not cached TODO: implement context request");
            return Err(CacheError::UncachedMessageAccess);
        }
    };

    log::trace!("Found requested id {from_id} in cache");

    let mut sync_token = None;
    let limit = usize::try_from(limit).map_err(|_| CacheError::Unexpected)?;
    let mut n_cached_messages = 0;

    // iterate over msg until result has enough events
    while n_cached_messages < limit {
        let current_id = read_event_id(&msg)?;

        // Skip the starting message when the caller provided an explicit from_id
        if (!skip_first || current_id != from_id)
            && check_message_assembly(cached_room.clone(), current_id)?
        {
            n_cached_messages += 1;
        }

        let need_more = n_cached_messages < limit;

        match advance_message(&msg, order, &mut sync_token, need_more)? {
            Some(next_msg) => msg = next_msg,
            None => {
                // either a sync token is returned to fetch more messages or the room has
                // less messages than requested
                return Ok(sync_token);
            }
        }
    }

    // enough messages are now cached
    Ok(None)
}

async fn assemble_proto_message<T: RoomClient>(
    cached_room: Arc<RwLock<CachedChronoRoom>>,
    id: OwnedEventId,
    room_client: &T,
    cache: &Cache,
) -> Result<Option<Message>> {
    let cached_msg = get_cached_msg(cached_room.clone(), &id)?;

    // let (tl_evt, is_encrypted) = room_client.fetch_and_deserialize(&id).await?;
    let tl_evt = room_client.fetch(&id).await?;
    let (evt, is_encrypted) = deserialize(&tl_evt)?;

    let mut result = Message {
        message_id: id.to_string(),
        room_id: room_client.room_id(),
        sender_id: evt.sender().to_string(),
        timestamp: evt.origin_server_ts().get().into(),
        is_pinned: false,
        is_encrypted,
        ..Default::default()
    };

    // Return on redacted events. Its feasible to just drop them as most messengers do.
    // But we might want to display a placeholder along with the optional redaction
    // reason, using the cached EventType::Redaction, at some point
    if is_marked_redacted(cached_msg.clone())? {
        return Ok(None);
    }

    // Return on replacement, redaction and reaction events -- they are handled in conjunction
    // with their parent event. Threads are normal displayable messages and thus excluded from this distinction
    if is_relation(cached_msg.clone())? && !is_thread(cached_msg.clone())? {
        return Ok(None);
    }

    // Assemble content if event is State
    if is_state(cached_msg.clone())? {
        result.content = get_state_content(&tl_evt)?;
    }

    // Assmble content if event is MessageLike
    if let Some(content) = get_latest_content(cached_msg.clone(), &tl_evt, room_client).await? {
        result.content = Some(room_client.get_message_like_content(&id, content).await?);
    };

    // Return if don't yet support or do not want to display a message with that content
    if result.content.is_none() {
        return Ok(None);
    }

    result.related_message_id = get_replied_to_id(&tl_evt).await?.map(|s| s.to_string());
    result.mentioned_user_ids = messages::convert_mentions(&get_mentions(&tl_evt)?);
    result.reactions = collect_reactions_to_event(id, room_client, cache).await?;

    Ok(Some(result))
}

async fn collect_reactions_to_event<T: RoomClient>(
    id: OwnedEventId,
    room_client: &T,
    cache: &Cache,
) -> Result<Vec<Reaction>> {
    // TODO: This will be expensive to do for every single event. Its ok to do so in
    // a first approach. For efficiency reasons we should proceed as follows:
    // 1. check if the message belongs to the latest sequence of the room.
    //   If so, we can fully deduce the reactions from the chat cache - we
    //   also want to cache the reaction key then.
    // 2. if not, we must assume that we have uncached reactions. In this case
    //   we need to call the Room::relations() method
    let reactions = room_client.query_reactions_to_event(id).await?;
    let result = reactions_from_matrix_relations(reactions, room_client.room_id(), cache)?;

    Ok(result)
}

fn reactions_from_matrix_relations(
    relations: MatrixRelations,
    room_id: String,
    cache: &Cache,
) -> Result<Vec<Reaction>> {
    let mut reactions = vec![];
    for rel in relations.chunk {
        let tl_evt = match try_deserialize_evt(&rel) {
            Some(ev) => ev,
            None => continue,
        };

        // Check if it's a reaction event
        if let AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::Reaction(
            SyncMessageLikeEvent::Original(orig),
        )) = tl_evt
        {
            // extract relation content
            let reaction_content = &orig.content;
            let related_event_id = reaction_content.relates_to.event_id.to_string();
            let reaction_key = reaction_content.relates_to.key.clone();

            // Create the Reaction event for tracking
            let reaction_event = CachedReaction {
                event_id: orig.event_id,
                room_id: room_id.clone(),
                message_id: related_event_id,
                user_id: orig.sender.to_string(),
                emoji: reaction_key,
            };

            cache.track_reaction(reaction_event.clone());

            // Create the Reaction protobuf message
            let reaction = reaction_event.into();

            reactions.push(reaction);
        }
    }

    Ok(reactions)
}

fn check_message_assembly(
    cached_room: Arc<RwLock<CachedChronoRoom>>,
    id: OwnedEventId,
) -> Result<bool> {
    let cached_msg = get_cached_msg(cached_room.clone(), &id)?;

    if is_marked_redacted(cached_msg.clone())? {
        return Ok(false);
    }
    if is_relation(cached_msg.clone())? && !is_thread(cached_msg.clone())? {
        return Ok(false);
    }

    Ok(true)
}

fn get_cached_msg(
    cached_room: Arc<RwLock<CachedChronoRoom>>,
    id: &OwnedEventId,
) -> Result<Arc<RwLock<Option<CachedMessage>>>> {
    let room_r = cached_room.read().map_err(|_| CacheError::CachePoisoned)?;
    let cached_msg = room_r
        .get_message(id)
        .ok_or(CacheError::UncachedMessageAccess)?;
    Ok(cached_msg)
}

fn is_relation(cached_msg: Arc<RwLock<Option<CachedMessage>>>) -> Result<bool> {
    let msg_r = cached_msg.read().map_err(|_| CacheError::CachePoisoned)?;
    if msg_r
        .as_ref()
        .ok_or(CacheError::UncachedMessageAccess)?
        .rel_type
        .is_some()
    {
        return Ok(true);
    }
    Ok(false)
}

fn is_state(cached_msg: Arc<RwLock<Option<CachedMessage>>>) -> Result<bool> {
    let msg_r = cached_msg.read().map_err(|_| CacheError::CachePoisoned)?;
    let evt_type = msg_r
        .as_ref()
        .ok_or(CacheError::UncachedMessageAccess)?
        .r#type;

    Ok(evt_type == EventType::State)
}

fn is_thread(cached_msg: Arc<RwLock<Option<CachedMessage>>>) -> Result<bool> {
    let msg_r = cached_msg.read().map_err(|_| CacheError::CachePoisoned)?;
    let rel_type = msg_r
        .as_ref()
        .ok_or(CacheError::UncachedMessageAccess)?
        .rel_type;

    Ok(rel_type == Some(RelationType::Thread))
}

fn is_marked_redacted(cached_msg: Arc<RwLock<Option<CachedMessage>>>) -> Result<bool> {
    let msg_r = cached_msg.read().map_err(|_| CacheError::CachePoisoned)?;
    let evt_type = msg_r
        .as_ref()
        .ok_or(CacheError::UncachedMessageAccess)?
        .r#type;

    Ok(evt_type == EventType::Redacted)
}

async fn get_latest_content<T: RoomClient>(
    msg: Arc<RwLock<Option<CachedMessage>>>,
    tl_evt: &TimelineEvent,
    room_client: &T,
) -> Result<Option<MessageType>> {
    let replacement_relations = get_relations(msg.clone(), &RelationType::Replacement)?;

    let mut replacements = BTreeMap::new();

    // build time-ordered map of replacement events
    for rel in replacement_relations {
        let rel_arc = rel.upgrade().ok_or(CacheError::DeallocatedMessageAccess)?;

        let rel_r = rel_arc.read().map_err(|_| CacheError::CachePoisoned)?;

        let ts = rel_r
            .as_ref()
            .ok_or(CacheError::UncachedMessageAccess)?
            .timestamp;

        replacements.insert(ts, rel_arc.clone());
    }

    if replacements.is_empty() {
        let content = get_message_type(tl_evt)?;

        return Ok(content);
    }

    if let Some(content) =
        get_latest_accessible_replacement_content(replacements, room_client).await?
    {
        return Ok(Some(content));
    }

    Ok(None)
}

fn get_message_type(tl_evt: &TimelineEvent) -> Result<Option<MessageType>> {
    let (tl_evt, encrypted) = deserialize(tl_evt)?;

    if encrypted {
        return Ok(None);
    };

    let Some(orig_evt) = get_original_message_like_from_decrypted_any_sync_timeline(tl_evt) else {
        return Ok(None);
    };

    Ok(Some(orig_evt.content.msgtype))
}

fn get_original_message_like_from_decrypted_any_sync_timeline(
    tl_evt: AnySyncTimelineEvent,
) -> Option<OriginalSyncMessageLikeEvent<RoomMessageEventContent>> {
    // event is a state events
    let AnySyncTimelineEvent::MessageLike(msg_evt) = tl_evt else {
        return None;
    };

    match msg_evt {
        AnySyncMessageLikeEvent::RoomMessage(event) => {
            let orig_evt = event.as_original()?;
            Some(orig_evt.clone())
        }
        // support other MessageLike events
        _ => None,
    }
}

fn get_state_content(tl_evt: &TimelineEvent) -> Result<Option<message::Content>> {
    let (tl_evt, encrypted) = deserialize(tl_evt)?;

    if encrypted {
        log::warn!("Encountered encrypted state event");
        return Ok(None);
    };

    let Some(state_type) = get_state_type(&tl_evt)? else {
        return Ok(None);
    };

    match state_type {
        StateEventType::PolicyRuleRoom => Ok(None),
        StateEventType::PolicyRuleServer => Ok(None),
        StateEventType::PolicyRuleUser => Ok(None),
        StateEventType::RoomAliases => Ok(None),
        StateEventType::RoomAvatar => Ok(None),
        StateEventType::RoomCanonicalAlias => Ok(None),
        StateEventType::RoomCreate => Ok(None),
        StateEventType::RoomEncryption => Ok(None),
        StateEventType::RoomGuestAccess => Ok(None),
        StateEventType::RoomHistoryVisibility => Ok(None),
        StateEventType::RoomJoinRules => Ok(None),
        StateEventType::RoomMember => get_membership_change_content(tl_evt),
        StateEventType::RoomName => Ok(None),
        StateEventType::RoomPinnedEvents => Ok(None),
        StateEventType::RoomPowerLevels => Ok(None),
        StateEventType::RoomServerAcl => Ok(None),
        StateEventType::RoomThirdPartyInvite => Ok(None),
        StateEventType::RoomTombstone => Ok(None),
        StateEventType::RoomTopic => Ok(None),
        StateEventType::SpaceChild => Ok(None),
        StateEventType::SpaceParent => Ok(None),
        _ => Ok(None), // covers _Custom variants
    }
}

fn get_membership_change_content(tl_evt: AnySyncTimelineEvent) -> Result<Option<message::Content>> {
    let AnySyncTimelineEvent::State(state_evt) = tl_evt else {
        return Err(CacheError::Unexpected);
    };

    let AnySyncStateEvent::RoomMember(member_evt) = state_evt else {
        return Err(CacheError::Unexpected);
    };

    let Some(change) = get_membership_change(&member_evt) else {
        return Err(CacheError::Unexpected);
    };

    let Some(membership_change) = convert_membership_change(&change) else {
        return Ok(None);
    };

    let membership_change_content = MessageContentMembershipChange {
        change: membership_change.into(),
        affected_user_id: member_evt.state_key().to_string(),
    };

    Ok(Some(message::Content::MembershipChange(
        membership_change_content,
    )))
}

fn get_membership_change(
    member_evt: &SyncStateEvent<RoomMemberEventContent>,
) -> Option<MatrixMembershipChange<'_>> {
    let orig = member_evt.as_original()?;

    Some(orig.membership_change())
}

fn get_state_type(tl_evt: &AnySyncTimelineEvent) -> Result<Option<StateEventType>> {
    let AnySyncTimelineEvent::State(state_evt) = tl_evt else {
        return Ok(None);
    };

    Ok(Some(state_evt.event_type()))
}

async fn get_replied_to_id(tl_evt: &TimelineEvent) -> Result<Option<OwnedEventId>> {
    let (tl_evt, encrypted) = deserialize(tl_evt)?;

    // cannot read relations of encrypted
    if encrypted {
        return Ok(None);
    };

    let Some(orig_evt) = get_original_message_like_from_decrypted_any_sync_timeline(tl_evt) else {
        return Ok(None);
    };

    if let Some(Relation::Reply {
        in_reply_to: in_repl,
    }) = &orig_evt.content.relates_to
    {
        return Ok(Some(in_repl.event_id.clone()));
    }

    Ok(None)
}

fn get_mentions(tl_evt: &TimelineEvent) -> Result<Option<Mentions>> {
    let (tl_evt, encrypted) = deserialize(tl_evt)?;

    // Cannot read mentions of encrypted events
    if encrypted {
        return Ok(None);
    }

    let Some(orig_evt) = get_original_message_like_from_decrypted_any_sync_timeline(tl_evt) else {
        return Ok(None);
    };

    Ok(orig_evt.content.mentions.clone())
}

async fn get_latest_accessible_replacement_content<T: RoomClient>(
    mut replacements: BTreeMap<u64, Arc<RwLock<Option<CachedMessage>>>>,
    room_client: &T,
) -> Result<Option<MessageType>> {
    while let Some((_, latest_replacement)) = replacements.last_key_value() {
        let repl_id = OwnedEventId::try_from(
            latest_replacement
                .read()
                .map_err(|_| CacheError::CachePoisoned)?
                .as_ref()
                .ok_or(CacheError::UncachedMessageAccess)?
                .id
                .clone(),
        )
        .map_err(|_| CacheError::InvalidEventId)?;

        let repl_tl_evt = room_client.fetch(&repl_id).await?;
        let (repl_evt, is_encrypted) = deserialize(&repl_tl_evt)?;

        if is_encrypted {
            log::warn!("Replacement of event {repl_id} could not be decrypted.");

            replacements.pop_last();
            continue;
        }

        let AnySyncTimelineEvent::MessageLike(repl_msg_evt) = repl_evt else {
            log::warn!(
                "Encountered a State event that appears to replace another event: {repl_id}"
            );

            replacements.pop_last();
            continue;
        };

        let AnySyncMessageLikeEvent::RoomMessage(repl_event) = repl_msg_evt else {
            log::warn!(
                "Encountered other type than RoomMessage for AnySyncMessageLikeEvent \
                when compiling replacements: {repl_id}"
            );
            replacements.pop_last();
            continue;
        };

        let Some(repl_orig_evt) = repl_event.as_original() else {
            log::warn!("Replacement event has been redacted: {repl_id}");
            replacements.pop_last();
            continue;
        };

        let Some(Relation::Replacement(replacement)) = &repl_orig_evt.content.relates_to else {
            log::warn!("Replacement event does not have m.new_content: {repl_id}");
            replacements.pop_last();
            continue;
        };

        return Ok(Some(replacement.new_content.msgtype.clone()));
    }

    Ok(None)
}

fn get_relations(
    msg: Arc<RwLock<Option<CachedMessage>>>,
    rel_type: &RelationType,
) -> Result<Vec<Weak<RwLock<Option<CachedMessage>>>>> {
    let cached_msg_r = msg.read().map_err(|_| CacheError::CachePoisoned)?;

    let rels = cached_msg_r
        .as_ref()
        .ok_or(CacheError::UncachedMessageAccess)?
        .rel_by
        .iter()
        .filter_map(|weak| {
            let arc = weak.upgrade()?;
            let rel_r = arc.read().ok()?;
            let rel = rel_r.as_ref()?;

            (rel.rel_type.as_ref() == Some(rel_type)).then(|| weak.clone())
        })
        .collect();

    Ok(rels)
}

pub async fn retry_decryption<T: RoomClient>(
    messages_opt: Option<Vec<Message>>,
    room_id: &OwnedRoomId,
    room_client: &T,
    cache: &Cache,
    mut key_change_rx: mpsc::Receiver<()>,
    ctx: &ClientContext,
) -> Result<()> {
    let Some(mut messages) = messages_opt else {
        return Ok(());
    };

    if !messages.iter().any(|msg| msg.is_encrypted) {
        return Ok(());
    }

    let mut pending_decryption = Vec::new();

    for msg in messages.drain(..) {
        if msg.is_encrypted {
            pending_decryption.push(msg)
        }
    }

    let cached_room = {
        let cache_r = cache.cached_data_read_lock()?;

        cache_r
            .get(room_id)
            .ok_or(CacheError::UncachedRoomAccess)?
            .clone()
    };

    log::debug!("Retrying to decrypt {} messages", pending_decryption.len());

    // Wait for key change notification with timeout
    let retry_result = tokio::time::timeout(
        Duration::from_secs(30),
        wait_for_keys_and_retry(
            &mut pending_decryption,
            &mut key_change_rx,
            room_client,
            cached_room.clone(),
            ctx,
            cache,
        ),
    )
    .await;

    match retry_result {
        Ok(Ok(decrypted_count)) => {
            log::debug!("Successfully decrypted {decrypted_count} events after key import");
        }
        Ok(Err(e)) => {
            log::error!("Error during retry: {e}");
        }
        Err(_) => {
            log::warn!(
                "Timeout waiting for key imports. {} events still encrypted.",
                pending_decryption.len()
            );
        }
    }

    Ok(())
}

async fn wait_for_keys_and_retry<T: RoomClient>(
    pending: &mut Vec<Message>,
    key_rx: &mut mpsc::Receiver<()>,
    room_client: &T,
    room: Arc<RwLock<CachedChronoRoom>>,
    ctx: &ClientContext,
    cache: &Cache,
) -> Result<usize> {
    let mut decrypted_count = 0;

    while !pending.is_empty() {
        if key_rx.recv().await.is_none() {
            break;
        }

        log::info!(
            "Room key downloaded, retrying {} pending events...",
            pending.len()
        );

        // delay for key processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Retry all pending events
        let mut still_pending = Vec::new();

        for event in pending.drain(..) {
            let id =
                OwnedEventId::try_from(event.message_id).map_err(|_| CacheError::InvalidEventId)?;

            let tl_event = room_client.fetch(&id).await?;
            let (_, is_encrypted) = deserialize(&tl_event)?;

            if !is_encrypted {
                continue;
            }

            let Some(_) = room_client.try_fresh_decrypt(&tl_event).await else {
                log::debug!("Event {id} could not be decrypted on retry");
                continue;
            };

            log::info!("Successfully decrypted event {id} on retry!");

            match assemble_proto_message(room.clone(), id.clone(), room_client, cache).await? {
                Some(msg) => {
                    if msg.is_encrypted {
                        log::warn!("Event {id} still encrypted");
                        still_pending.push(msg)
                    } else {
                        log::info!("Decrypted event {id} and sending MessageChangeEvent");

                        let Some(content) = msg.content else {
                            return Err(CacheError::Unexpected);
                        };

                        let content =
                            messages::message_content_to_message_change_event_content(content);

                        // Send a MessageChangeEvent with the new, decrypted content to the client
                        let response = builder::MessageChangeEventBuilder::new(
                            room_client.room_id(),
                            id.to_string(),
                        )
                        .change_is_encrypted(false)
                        .change_content(content)
                        .to_proto();

                        ctx.send_event(ResponseContent::MessageChangeEvent(response));
                        decrypted_count += 1;
                    }
                }
                None => {
                    let response = MessageRemoveEvent {
                        message_id: id.to_string(),
                        room_id: room_client.room_id(),
                        reason: None,
                        origin: EventOrigin::BackendOrigin.into(),
                    };
                    ctx.send_event(ResponseContent::MessageRemoveEvent(response));
                }
            }
        }

        *pending = still_pending;

        if pending.is_empty() {
            break;
        }
    }

    Ok(decrypted_count)
}

fn deserialize(tl_event: &TimelineEvent) -> Result<(AnySyncTimelineEvent, bool)> {
    let mut is_encrypted = false;

    let tl_evt = match &tl_event.kind {
        TimelineEventKind::Decrypted(decrypted_event) => decrypted_event
            .event
            .deserialize()
            .map_err(|_| CacheError::DeserializationFailed)?
            .into(),
        TimelineEventKind::PlainText { event } => event
            .deserialize()
            .map_err(|_| CacheError::DeserializationFailed)?,
        TimelineEventKind::UnableToDecrypt {
            event,
            utd_info: info,
        } => {
            log::warn!(
                "Received undecryptable event from SDK - \
                cannot derive full content: eventId={}, session_id={:#?}, reason={:#?}",
                event
                    .get_field("event_id")
                    .ok()
                    .unwrap_or(Some("unknown"))
                    .unwrap_or("unknown"),
                info.session_id,
                info.reason
            );

            is_encrypted = true;
            event
                .deserialize()
                .map_err(|_| CacheError::DeserializationFailed)?
        }
    };

    // if the event is redacted, we don't count it as encrypted,
    // as all encrypted content is deleted on redaction
    match &tl_evt {
        AnySyncTimelineEvent::MessageLike(evt) => {
            if evt.is_redacted() {
                is_encrypted = false;
            }
        }
        AnySyncTimelineEvent::State(evt) => {
            if evt.is_redacted() {
                is_encrypted = false;
            }
        }
    }

    Ok((tl_evt, is_encrypted))
}

struct DebugMessageStrongRef(pub Arc<RwLock<Option<CachedMessage>>>);
struct DebugMessageWeakRef(pub Weak<RwLock<Option<CachedMessage>>>);
struct DebugMessageWeakVec(pub Vec<Weak<RwLock<Option<CachedMessage>>>>);

impl fmt::Debug for DebugMessageStrongRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self.0.read() {
            Ok(msg_opt_r) => match &*msg_opt_r {
                Some(msg) => write!(f, "{}", msg.id),
                None => write!(f, "None"),
            },
            Err(_) => write!(f, "<poisoned>"),
        }
    }
}

impl fmt::Debug for DebugMessageWeakRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self.0.upgrade() {
            Some(val) => match val.read() {
                Ok(msg_opt_r) => match &*msg_opt_r {
                    Some(msg) => write!(f, "{}", msg.id),
                    None => write!(f, "None"),
                },
                Err(_) => write!(f, "<poisoned>"),
            },
            None => write!(f, "<dropped>"),
        }
    }
}

impl fmt::Debug for DebugMessageWeakVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let mut list = f.debug_list();

        for w in &self.0 {
            list.entry(&DebugMessageWeakRef(w.clone()));
        }

        list.finish()
    }
}

impl fmt::Debug for CachedChronoSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("CachedChronoSequence")
            .field("messages", &self.messages)
            .finish()
    }
}

impl fmt::Debug for CachedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("CachedMessage")
            .field("id", &self.id)
            .field("type", &self.r#type)
            .field("timestamp", &self.timestamp)
            .field("rel_type", &self.rel_type)
            .field("rel_to", &DebugMessageStrongRef(self.rel_to.clone()))
            .field("rel_by", &DebugMessageWeakVec(self.rel_by.clone()))
            .field("before", &DebugMessageStrongRef(self.before.clone()))
            .field("after", &DebugMessageWeakRef(self.after.clone()))
            .field("next_token", &self.next_token)
            .field("prev_token", &self.prev_token)
            .finish()
    }
}

pub trait RoomClient {
    async fn fetch(&self, id: &OwnedEventId) -> Result<TimelineEvent>;

    async fn try_fresh_decrypt(&self, tl_event: &TimelineEvent) -> Option<AnySyncTimelineEvent>;

    async fn query_reactions_to_event(&self, id: OwnedEventId) -> Result<MatrixRelations>;

    async fn get_message_like_content(
        &self,
        message_id: &EventId,
        message_type: MessageType,
    ) -> Result<message::Content>;

    fn room_id(&self) -> String;
}

#[derive(Clone)]
pub struct MatrixRoomClient {
    room_client: matrix_sdk::Room,
    media_manager: MediaManager,
}

impl MatrixRoomClient {
    pub fn new(room_client: &matrix_sdk::Room, media_manager: MediaManager) -> MatrixRoomClient {
        MatrixRoomClient {
            room_client: room_client.clone(),
            media_manager,
        }
    }
}

impl RoomClient for MatrixRoomClient {
    async fn fetch(&self, id: &OwnedEventId) -> Result<TimelineEvent> {
        // This method is expensive and eats up most of the runtime
        let sdk_event =
            self.room_client
                .event(id, None)
                .await
                .map_err(|err| CacheError::Context {
                    message: format!("original error: {}", err),
                    source: Box::new(CacheError::EventFetchFailed),
                })?;

        Ok(sdk_event)
    }

    async fn try_fresh_decrypt(&self, tl_event: &TimelineEvent) -> Option<AnySyncTimelineEvent> {
        let TimelineEventKind::UnableToDecrypt {
            event: raw_encrypted,
            utd_info: _,
        } = &tl_event.kind
        else {
            log::warn!("Attempted to retry decryption on an unencrypted event");
            return None;
        };

        let json_str = raw_encrypted.json().get();

        let encrypted_raw: Raw<OriginalSyncRoomEncryptedEvent> =
            match serde_json::from_str(json_str) {
                Ok(raw) => raw,
                Err(e) => {
                    log::warn!("Failed to parse as encrypted event: {e}");
                    return None;
                }
            };

        match self.room_client.decrypt_event(&encrypted_raw, None).await {
            Ok(timeline_event) => match timeline_event.kind {
                TimelineEventKind::Decrypted(decrypted) => match decrypted.event.deserialize() {
                    Ok(ev) => Some(ev.into()),
                    Err(e) => {
                        log::warn!("Failed to deserialize freshly decrypted event: {e}");
                        None
                    }
                },
                TimelineEventKind::UnableToDecrypt { .. } => {
                    log::warn!("Fresh decrypt still returned UTD");
                    None
                }
                TimelineEventKind::PlainText { .. } => {
                    log::warn!("Fresh decrypt returned PlainText (unexpected)");
                    None
                }
            },
            Err(e) => {
                log::warn!("Fresh decryption attempt failed: {e}");
                None
            }
        }
    }

    async fn query_reactions_to_event(&self, id: OwnedEventId) -> Result<MatrixRelations> {
        let opts = RelationsOptions {
            dir: Direction::Forward,
            limit: Some(UInt::from(20u32)), // should be enough for now - TODO: replace with iteration
            include_relations: IncludeRelations::RelationsOfType(MatrixRelationType::Annotation),
            recurse: false,
            ..Default::default()
        };

        let relations = self
            .room_client
            .relations(id, opts)
            .await
            .map_err(|_| CacheError::EventFetchFailed)?;

        Ok(relations)
    }

    async fn get_message_like_content(
        &self,
        message_id: &EventId,
        message_type: MessageType,
    ) -> Result<message::Content> {
        messages::generate_message_content!(
            self.media_manager,
            self.room_client,
            message_id,
            message_type,
            message
        )
        .ok_or(CacheError::Unexpected)
    }

    fn room_id(&self) -> String {
        self.room_client.room_id().to_string()
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use mrhc_core::OutputTask;
    use mrhc_proto::chat::MessageContentText;
    use serde_json::json;
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    struct MockRoomClient {
        room_id_result: String,
        fetch_room_messages_at_edge_result: Result<Option<OwnedEventId>>,
        fetch_result: Result<TimelineEvent>,
        try_fresh_decrypt_result: Option<AnySyncTimelineEvent>,
        query_reactions_to_event_result: Result<MatrixRelations>,
    }

    impl MockRoomClient {
        fn new() -> MockRoomClient {
            let fetch_room_messages_at_edge_result = Ok(None);
            let event_json = json!({
                "type": "m.room.message",
                "event_id": "$test_event:example.org",
                "sender": "@gonicus:example.org",
                "origin_server_ts": 1234,
                "content": {
                    "msgtype": "m.text",
                    "body": "testing"
                }
            });

            let matrix_relations = MatrixRelations {
                chunk: vec![],
                prev_batch_token: None,
                next_batch_token: None,
                recursion_depth: None,
            };

            let raw = Raw::from_json_string(serde_json::to_string(&event_json).unwrap()).unwrap();
            let event = TimelineEvent::from_plaintext(raw);
            let fetch_result: Result<TimelineEvent> = Ok(event);

            let event: AnySyncTimelineEvent = serde_json::from_value(event_json.clone()).unwrap();
            let try_fresh_decrypt_result = Some(event);
            let query_reactions_to_event_result = Ok(matrix_relations);

            MockRoomClient {
                room_id_result: "!000000000000000000:example.org".to_string(),
                fetch_room_messages_at_edge_result: fetch_room_messages_at_edge_result,
                fetch_result: fetch_result,
                try_fresh_decrypt_result: try_fresh_decrypt_result,
                query_reactions_to_event_result: query_reactions_to_event_result,
            }
        }

        fn get_clone_query_reactions_to_event_result(&self) -> Result<MatrixRelations> {
            match &self.query_reactions_to_event_result {
                Ok(self_rel) => {
                    let rel = MatrixRelations {
                        chunk: self_rel.chunk.clone(),
                        prev_batch_token: self_rel.prev_batch_token.clone(),
                        next_batch_token: self_rel.next_batch_token.clone(),
                        recursion_depth: self_rel.recursion_depth.clone(),
                    };

                    Ok(rel)
                }
                Err(err) => Err(err.clone()),
            }
        }
    }

    impl RoomClient for MockRoomClient {
        async fn fetch(&self, _id: &OwnedEventId) -> Result<TimelineEvent> {
            self.fetch_result.clone()
        }

        async fn try_fresh_decrypt(&self, _: &TimelineEvent) -> Option<AnySyncTimelineEvent> {
            self.try_fresh_decrypt_result.clone()
        }

        async fn query_reactions_to_event(&self, _: OwnedEventId) -> Result<MatrixRelations> {
            self.get_clone_query_reactions_to_event_result()
        }

        async fn get_message_like_content(
            &self,
            _message_id: &EventId,
            _message_type: MessageType,
        ) -> Result<message::Content> {
            Ok(message::Content::Text(MessageContentText {
                content: "Hello World".to_owned(),
            }))
        }

        fn room_id(&self) -> String {
            self.room_id_result.clone()
        }
    }

    impl Cache {
        pub fn reactions(&self) -> &RwLock<Vec<CachedReaction>> {
            &self.reactions
        }
    }

    struct SetupData {
        prefilled_room: CachedChronoRoom,
        msgs0: Vec<UnlinkedMessage>, // prefilled
        msgs1: Vec<UnlinkedMessage>, // connecting (after prefilled)
        msgs2: Vec<UnlinkedMessage>, // connecting (before prefilled)
        msgs3: Vec<UnlinkedMessage>, // overlapping
        msgs4: Vec<UnlinkedMessage>, // non-connecting
        msgs5: Vec<UnlinkedMessage>, // prefilled - but with shuffled_order
        msg_after: UnlinkedMessage,  // after prefilled
        room_client: MockRoomClient,
        output_recv: UnboundedReceiver<OutputTask>,
        ctx: ClientContext,
        cache: Cache,
    }

    fn new_setup() -> SetupData {
        let prefilled_room = make_cached_chrono_room(4, 3);
        let mut result_room4 = make_cached_chrono_room(4, 3);
        append_sequence(&mut result_room4, 8, 2);

        let msgs0 = vec![
            UnlinkedMessage {
                id: test_id(4).to_string(),
                r#type: EventType::Message,
                timestamp: (4).try_into().unwrap(),
                prev_token: Some("a".to_string()),
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(5).to_string(),
                r#type: EventType::Message,
                timestamp: (5).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(6).to_string(),
                r#type: EventType::Message,
                timestamp: (6).try_into().unwrap(),
                prev_token: None,
                next_token: Some("b".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msgs1 = vec![
            UnlinkedMessage {
                id: test_id(7).to_string(),
                r#type: EventType::Message,
                timestamp: (7).try_into().unwrap(),
                prev_token: Some("b".to_string()),
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(8).to_string(),
                r#type: EventType::Message,
                timestamp: (8).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(9).to_string(),
                r#type: EventType::Message,
                timestamp: (9).try_into().unwrap(),
                prev_token: None,
                next_token: Some("c".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msgs2 = vec![
            UnlinkedMessage {
                id: test_id(1).to_string(),
                r#type: EventType::Message,
                timestamp: (1).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(2).to_string(),
                r#type: EventType::Message,
                timestamp: (2).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(3).to_string(),
                r#type: EventType::Message,
                timestamp: (3).try_into().unwrap(),
                prev_token: None,
                next_token: Some("a".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msgs3 = vec![
            UnlinkedMessage {
                id: test_id(5).to_string(),
                r#type: EventType::Message,
                timestamp: (5).try_into().unwrap(),
                prev_token: Some("a.1".to_string()),
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(6).to_string(),
                r#type: EventType::Message,
                timestamp: (6).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(7).to_string(),
                r#type: EventType::Message,
                timestamp: (7).try_into().unwrap(),
                prev_token: None,
                next_token: Some("b.1".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msgs4 = vec![
            UnlinkedMessage {
                id: test_id(8).to_string(),
                r#type: EventType::Message,
                timestamp: (8).try_into().unwrap(),
                prev_token: Some("b.1".to_string()),
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(9).to_string(),
                r#type: EventType::Message,
                timestamp: (9).try_into().unwrap(),
                prev_token: None,
                next_token: Some("c".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msgs5 = vec![
            UnlinkedMessage {
                id: test_id(4).to_string(),
                r#type: EventType::Message,
                timestamp: (6).try_into().unwrap(),
                prev_token: Some("a".to_string()),
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(5).to_string(),
                r#type: EventType::Message,
                timestamp: (4).try_into().unwrap(),
                prev_token: None,
                next_token: None,
                rel_type: None,
                rel_to: None,
            },
            UnlinkedMessage {
                id: test_id(6).to_string(),
                r#type: EventType::Message,
                timestamp: (5).try_into().unwrap(),
                prev_token: None,
                next_token: Some("b".to_string()),
                rel_type: None,
                rel_to: None,
            },
        ];

        let msg_after = UnlinkedMessage {
            id: test_id(7).to_string(),
            r#type: EventType::Message,
            timestamp: (7).try_into().unwrap(),
            prev_token: None,
            next_token: Some("b1".to_string()),
            rel_type: None,
            rel_to: None,
        };

        let prev_token = msgs0[0].prev_token.clone();
        let next_token = msgs0[2].next_token.clone();
        let last_id = OwnedEventId::try_from(test_id(6)).unwrap();
        prefilled_room.chrono_sequences[0].messages[&last_id]
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .next_token = next_token.clone();
        let first_id = OwnedEventId::try_from(test_id(4)).unwrap();
        prefilled_room.chrono_sequences[0].messages[&first_id]
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .prev_token = prev_token.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        SetupData {
            prefilled_room: prefilled_room,
            msgs0: msgs0,
            msgs1: msgs1,
            msgs2: msgs2,
            msgs3: msgs3,
            msgs4: msgs4,
            msgs5: msgs5,
            msg_after: msg_after,
            room_client: MockRoomClient::new(),
            output_recv: rx,
            ctx: ClientContext::new(0, tx),
            cache: Cache::new(),
        }
    }

    fn make_sequential_cached_messages(
        start: usize,
        count: usize,
    ) -> (CachedMessage, Vec<CachedMessage>, CachedMessage) {
        let begin = CachedMessage {
            id: SequenceSentinel::Begin.to_string(),
            ..Default::default()
        };

        let end = CachedMessage {
            id: SequenceSentinel::End.to_string(),
            ..Default::default()
        };

        let result = (0..count)
            .map(|i| {
                let n = start + i;
                make_cached_message(test_id(n), n.try_into().unwrap())
            })
            .collect();

        (begin, result, end)
    }

    fn make_chrono_sequence(start: usize, count: usize) -> CachedChronoSequence {
        let (begin, msgs, end) = make_sequential_cached_messages(start, count);

        let begin_arc = Arc::new(RwLock::new(Some(begin)));
        let end_arc = Arc::new(RwLock::new(Some(end)));

        let mut messages = HashMap::new();

        for i in 0..count {
            messages.insert(
                OwnedEventId::try_from(msgs[i].id.clone()).unwrap(),
                Arc::new(RwLock::new(Some(msgs[i].clone()))),
            );
        }

        // link
        for i in (0..count).rev() {
            let mut m_w = messages[&OwnedEventId::try_from(test_id(start + i)).unwrap()]
                .write()
                .unwrap();
            let m = m_w.as_mut().unwrap();

            if i == 0 {
                m.before = begin_arc.clone();
            } else {
                m.before =
                    messages[&OwnedEventId::try_from(test_id(start + i - 1)).unwrap()].clone();
            }

            if i == count - 1 {
                m.after = Arc::downgrade(&end_arc.clone());
            } else {
                m.after = Arc::downgrade(
                    &messages[&OwnedEventId::try_from(test_id(start + i + 1)).unwrap()].clone(),
                );
            }
        }

        begin_arc.write().unwrap().as_mut().unwrap().after =
            Arc::downgrade(&messages[&OwnedEventId::try_from(test_id(start)).unwrap()].clone());
        end_arc.write().unwrap().as_mut().unwrap().before =
            messages[&OwnedEventId::try_from(test_id(start + count - 1)).unwrap()].clone();

        CachedChronoSequence {
            messages,
            msg_begin: begin_arc.clone(),
            msg_end: end_arc.clone(),
        }
    }

    fn make_cached_chrono_room(start: usize, count: usize) -> CachedChronoRoom {
        let chrono_sequence = make_chrono_sequence(start, count);

        CachedChronoRoom {
            unresolved_relations: vec![],
            chrono_sequences: vec![chrono_sequence],
        }
    }

    fn append_sequence(room: &mut CachedChronoRoom, start: usize, count: usize) {
        let chrono_sequence = make_chrono_sequence(start, count);

        room.chrono_sequences.push(chrono_sequence);
    }

    fn set_relation(
        room: &mut CachedChronoRoom,
        relation_type: RelationType,
        child: usize,
        parent: usize,
    ) {
        let sequence = room
            .chrono_sequences
            .iter_mut()
            .find(|seq| {
                seq.messages
                    .contains_key(&OwnedEventId::try_from(test_id(parent)).unwrap())
            })
            .expect("Parent message not found in any sequence");

        let parent_msg =
            sequence.messages[&OwnedEventId::try_from(test_id(parent)).unwrap()].clone();
        let child_msg = sequence.messages[&OwnedEventId::try_from(test_id(child)).unwrap()].clone();

        child_msg.write().unwrap().as_mut().unwrap().rel_to = parent_msg.clone();
        child_msg.write().unwrap().as_mut().unwrap().rel_type = Some(relation_type);

        parent_msg.write().unwrap().as_mut().unwrap().rel_by = vec![Arc::downgrade(&child_msg)];
    }

    fn make_cached_message(id: impl Into<String>, timestamp: u64) -> CachedMessage {
        CachedMessage {
            id: id.into(),
            r#type: EventType::Message,
            timestamp,
            ..Default::default()
        }
    }

    fn test_id(n: usize) -> OwnedEventId {
        OwnedEventId::try_from(format!("$event{:0>38}", n)).unwrap()
    }

    fn b_after_a(sequence: &CachedChronoSequence, a: usize, b: usize) -> bool {
        if !(sequence.messages[&OwnedEventId::try_from(test_id(b)).unwrap()]
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .before
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .id
            == OwnedEventId::try_from(test_id(a)).unwrap())
        {
            return false;
        }

        if !(sequence.messages[&OwnedEventId::try_from(test_id(a)).unwrap()]
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .after
            .upgrade()
            .unwrap()
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .id
            == OwnedEventId::try_from(test_id(b)).unwrap())
        {
            return false;
        }

        return true;
    }

    fn set_b_replaces_a(sequence: &CachedChronoSequence, a: usize, b: usize) {
        let replaced_msg = sequence.messages[&OwnedEventId::try_from(test_id(a)).unwrap()].clone();
        let replacing_msg = sequence.messages[&OwnedEventId::try_from(test_id(b)).unwrap()].clone();
        replacing_msg.write().unwrap().as_mut().unwrap().rel_to = replaced_msg.clone();
        replacing_msg.write().unwrap().as_mut().unwrap().rel_type = Some(RelationType::Replacement);
        replaced_msg.write().unwrap().as_mut().unwrap().rel_by =
            vec![Arc::downgrade(&replaced_msg)];
    }

    fn set_b_redacts_a(sequence: &CachedChronoSequence, a: usize, b: usize) {
        let redacted_msg = sequence.messages[&OwnedEventId::try_from(test_id(a)).unwrap()].clone();
        let redacting_msg = sequence.messages[&OwnedEventId::try_from(test_id(b)).unwrap()].clone();
        redacting_msg.write().unwrap().as_mut().unwrap().rel_to = redacted_msg.clone();
        redacting_msg.write().unwrap().as_mut().unwrap().rel_type = Some(RelationType::Redaction);
        redacted_msg.write().unwrap().as_mut().unwrap().r#type = EventType::Redacted;
    }

    #[tokio::test]
    async fn test_add_batch_through_initial_sync_on_empty_room() {
        // Arrange
        let setup = new_setup();
        let mut room = CachedChronoRoom::new();
        // Act
        room.add_batch(setup.msgs0, None, SyncSource::InitialSync)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 3);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
    }

    #[tokio::test]
    async fn test_add_batch_through_continuous_sync_on_empty_room() {
        // Arrange
        let setup = new_setup();
        let mut room = CachedChronoRoom::new();
        // Act
        room.add_batch(vec![setup.msg_after], None, SyncSource::ContinuousSync)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 1);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(7)).unwrap()));
    }

    #[tokio::test]
    async fn test_add_batch_through_event_context_on_empty_room() {
        // Arrange
        let setup = new_setup();
        let mut room = CachedChronoRoom::new();
        // Act
        room.add_batch(setup.msgs0, None, SyncSource::_EventContext)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 3);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
    }

    #[tokio::test]
    async fn test_add_batch_through_room_messages_on_empty_room() {
        // Arrange
        let setup = new_setup();
        let mut room = CachedChronoRoom::new();
        // Act
        room.add_batch(setup.msgs0, None, SyncSource::RoomMessages)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 3);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
    }

    #[tokio::test]
    async fn test_add_batch_through_continuous_sync_on_prefilled_room() {
        // Arrange
        let setup = new_setup();
        let mut room = setup.prefilled_room;
        // Act
        room.add_batch(vec![setup.msg_after], None, SyncSource::ContinuousSync)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 4);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(7)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
        assert!(b_after_a(&room.chrono_sequences[0], 6, 7));
        assert!(b_after_a(&room.chrono_sequences[0], 6, 7));
    }

    #[tokio::test]
    async fn test_add_batch_through_initial_sync_on_empty_room_with_shuffled_order() {
        // Arrange
        let setup = new_setup();
        let mut room = CachedChronoRoom::new();
        // Act
        room.add_batch(setup.msgs5, None, SyncSource::InitialSync)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 3);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
        assert!(b_after_a(&room.chrono_sequences[0], 6, 4));
    }

    #[tokio::test]
    async fn test_add_batch_through_room_messages_appending_to_prefilled_room() {
        // Arrange
        let setup = new_setup();
        let mut room = setup.prefilled_room;
        let token = setup.msgs0[2].next_token.clone();
        // Act
        room.add_batch(setup.msgs1, token.as_deref(), SyncSource::RoomMessages)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 6);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(7)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(8)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(9)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
        assert!(b_after_a(&room.chrono_sequences[0], 6, 7));
        assert!(b_after_a(&room.chrono_sequences[0], 7, 8));
        assert!(b_after_a(&room.chrono_sequences[0], 8, 9));
    }

    #[tokio::test]
    async fn test_add_batch_through_room_messages_prepending_to_prefilled_room() {
        // Arrange
        let setup = new_setup();
        let mut room = setup.prefilled_room;
        let token = setup.msgs0[0].prev_token.clone();
        // Act
        room.add_batch(setup.msgs2, token.as_deref(), SyncSource::RoomMessages)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 6);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(1)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(2)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(3)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 1, 2));
        assert!(b_after_a(&room.chrono_sequences[0], 2, 3));
        assert!(b_after_a(&room.chrono_sequences[0], 3, 4));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
    }

    #[tokio::test]
    async fn test_add_batch_through_event_context_unconnected_to_prefilled_room() {
        // Arrange
        let setup = new_setup();
        let mut room = setup.prefilled_room;
        // Act
        room.add_batch(setup.msgs4, None, SyncSource::_EventContext)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 2);
        assert_eq!(room.chrono_sequences[0].messages.len(), 3);
        assert_eq!(room.chrono_sequences[1].messages.len(), 2);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(room.chrono_sequences[1]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(8)).unwrap()));
        assert!(room.chrono_sequences[1]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(9)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
        assert!(b_after_a(&room.chrono_sequences[1], 8, 9));
    }

    #[tokio::test]
    async fn test_add_batch_through_event_context_overlapping_to_prefilled_room() {
        // Arrange
        let setup = new_setup();
        let mut room = setup.prefilled_room;
        // Act
        room.add_batch(setup.msgs3, None, SyncSource::_EventContext)
            .unwrap();

        // Assert
        assert_eq!(room.chrono_sequences.len(), 1);
        assert_eq!(room.chrono_sequences[0].messages.len(), 4);
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(4)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(5)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(6)).unwrap()));
        assert!(room.chrono_sequences[0]
            .messages
            .contains_key(&OwnedEventId::try_from(test_id(7)).unwrap()));
        assert!(b_after_a(&room.chrono_sequences[0], 4, 5));
        assert!(b_after_a(&room.chrono_sequences[0], 5, 6));
        assert!(b_after_a(&room.chrono_sequences[0], 6, 7));
    }

    #[tokio::test]
    async fn test_mock_works() {
        // Arrange
        let setup = new_setup();
        // Act
        let id = setup.room_client.room_id();

        // Assert
        assert_eq!(id, "!000000000000000000:example.org".to_string())
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_forward_complete() {
        // Arrange
        let setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(4),
            2,
            MessagesOrder::Forward,
            true,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 2);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(5));
        assert_eq!(messages[1].message_id, test_id(6));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_forward_complete_from_begin() {
        // Arrange
        let mut setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        setup.room_client.fetch_room_messages_at_edge_result =
            Ok(Some(OwnedEventId::try_from(test_id(4)).unwrap()));
        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(4),
            3,
            MessagesOrder::Forward,
            false,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 3);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(4));
        assert_eq!(messages[1].message_id, test_id(5));
        assert_eq!(messages[2].message_id, test_id(6));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_forward_incomplete() {
        // Arrange
        let setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(4),
            3,
            MessagesOrder::Forward,
            true,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 2);
        assert!(!result.is_complete);
        assert_eq!(messages[0].message_id, test_id(5));
        assert_eq!(messages[1].message_id, test_id(6));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_backward_complete() {
        // Arrange
        let setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(6),
            2,
            MessagesOrder::Backward,
            true,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 2);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(5));
        assert_eq!(messages[1].message_id, test_id(4));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_backward_incomplete() {
        // Arrange
        let setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(6),
            3,
            MessagesOrder::Backward,
            true,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 2);
        assert!(!result.is_complete);
        assert_eq!(messages[0].message_id, test_id(5));
        assert_eq!(messages[1].message_id, test_id(4));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_backward_complete_from_end() {
        // Arrange
        let mut setup = new_setup();
        let room_arc = Arc::new(RwLock::new(setup.prefilled_room));
        setup.room_client.fetch_room_messages_at_edge_result =
            Ok(Some(OwnedEventId::try_from(test_id(6)).unwrap()));

        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(6),
            3,
            MessagesOrder::Backward,
            false,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 3);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(6));
        assert_eq!(messages[1].message_id, test_id(5));
        assert_eq!(messages[2].message_id, test_id(4));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_backward_complete_from_end_with_replacements() {
        // Arrange
        let mut setup = new_setup();
        let mut room = make_cached_chrono_room(4, 4);
        set_b_replaces_a(&mut room.chrono_sequences[0], 4, 6);
        let room_arc = Arc::new(RwLock::new(room));
        setup.room_client.fetch_room_messages_at_edge_result =
            Ok(Some(OwnedEventId::try_from(test_id(7)).unwrap()));

        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(7),
            3,
            MessagesOrder::Backward,
            false,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 3);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(7));
        assert_eq!(messages[1].message_id, test_id(5));
        assert_eq!(messages[2].message_id, test_id(4));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_backward_complete_from_end_with_redaction() {
        // Arrange
        let mut setup = new_setup();
        let mut room = make_cached_chrono_room(4, 5);
        set_b_redacts_a(&mut room.chrono_sequences[0], 6, 7);
        let room_arc = Arc::new(RwLock::new(room));
        setup.room_client.fetch_room_messages_at_edge_result =
            Ok(Some(OwnedEventId::try_from(test_id(8)).unwrap()));

        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(8),
            3,
            MessagesOrder::Backward,
            false,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 3);
        assert!(result.is_complete);
        assert_eq!(messages[0].message_id, test_id(8));
        assert_eq!(messages[1].message_id, test_id(5));
        assert_eq!(messages[2].message_id, test_id(4));
    }

    #[tokio::test]
    async fn test_get_sequence_chunk_with_reactions() {
        // Arrange
        let mut setup = new_setup();
        let mut room = make_cached_chrono_room(1, 3);

        // no effect yet - only after reactions are compiled from cache where possible
        set_relation(&mut room, RelationType::Reaction, 2, 1);

        let room_arc = Arc::new(RwLock::new(room));

        // Create a reaction event for test_id(2)
        let event_json = json!({
            "type": "m.reaction",
            "event_id": "$reaction000000000000000000000000000000000001:example.org",
            "sender": "@user2:example.org",
            "origin_server_ts": 1500,
            "content": {
                "m.relates_to": {
                    "event_id": "$event000000000000000000000000000000000000001:example.org",
                    "key": "😵‍💫",
                    "rel_type": "m.annotation"
                }
            },
            "unsigned": {
                "age": 1000
            }
        });

        let matrix_relations = MatrixRelations {
            chunk: vec![TimelineEvent::from_plaintext(
                Raw::from_json_string(serde_json::to_string(&event_json).unwrap()).unwrap(),
            )],
            prev_batch_token: None,
            next_batch_token: None,
            recursion_depth: None,
        };

        setup.room_client.query_reactions_to_event_result = Ok(matrix_relations);

        // Act
        let result = send_and_get_sequence_chunk(
            &room_arc.clone(),
            test_id(1),
            1,
            MessagesOrder::Forward,
            false,
            &setup.room_client,
            &setup.cache,
            &setup.ctx,
        )
        .await
        .unwrap();

        // Assert
        assert!(result.messages.is_some());
        let messages = result.messages.unwrap();
        assert_eq!(messages.len(), 1);
        assert!(result.is_complete);

        // Check that the message has reactions
        assert!(
            !messages[0].reactions.is_empty(),
            "Expected message to have reactions but got none. Message ID: {}",
            messages[0].message_id
        );
        assert_eq!(
            messages[0].reactions[0].message_id,
            "$event000000000000000000000000000000000000001:example.org"
        );
        assert_eq!(messages[0].reactions[0].reaction, "😵‍💫");
        assert_eq!(
            messages[0].reactions[0].user_id,
            Some("@user2:example.org".to_string())
        );

        let tracked_reactions = setup.cache.reactions().read().unwrap();
        assert_eq!(tracked_reactions.len(), 1)
    }
}
