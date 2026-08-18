use std::time::{SystemTime, UNIX_EPOCH};

use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::ruma::events::{AnySyncTimelineEvent, AnyTimelineEvent};
use matrix_sdk::Room;
use ruma_common::serde::Raw;
use ruma_common::OwnedRoomId;

use crate::error::{Error, Result};

/// Gets the current unix timestamp in seconds.
pub fn get_unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn timeline_event_to_any_timeline_event(
    room: &Room,
    event: &TimelineEvent,
) -> Result<AnyTimelineEvent> {
    match &event.kind {
        TimelineEventKind::Decrypted(decrypted) => {
            any_timeline_event_from_decrypted(&decrypted.event)
        }
        TimelineEventKind::PlainText { event } => {
            any_timeline_event_from_plain_text(room.room_id().to_owned(), event)
        }
        TimelineEventKind::UnableToDecrypt { .. } => {
            Err(Error::internal("Unable to decrypt event"))
        }
    }
}

fn any_timeline_event_from_decrypted(event: &Raw<AnyTimelineEvent>) -> Result<AnyTimelineEvent> {
    event
        .deserialize()
        .map_err(|_| Error::internal("Unable to deserialize event"))
}

fn any_timeline_event_from_plain_text(
    room_id: OwnedRoomId,
    event: &Raw<AnySyncTimelineEvent>,
) -> Result<AnyTimelineEvent> {
    let deserialized = event
        .deserialize()
        .map_err(|_| Error::internal("Unable to deserialize event"))?;

    Ok(deserialized.into_full_event(room_id))
}

#[derive(Debug, Clone)]
pub struct ComparisonResult<T> {
    pub new: Vec<T>,
    /// (old, new)
    pub updated: Vec<(T, T)>,
    pub deleted: Vec<T>,
}

impl<T> ComparisonResult<T> {
    pub fn new() -> Self {
        Self {
            new: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
        }
    }
}

// We may need this in the future.
#[allow(unused)]
pub fn compare_lists_partial_eq<T>(
    old: &[T],
    new: &[T],
    same_item: impl Fn(&T, &T) -> bool,
) -> ComparisonResult<T>
where
    T: PartialEq + Clone,
{
    compare_lists(old, new, same_item, |a, b| a == b)
}

pub fn compare_lists<T>(
    old: &[T],
    new: &[T],
    same_item: impl Fn(&T, &T) -> bool,
    data_matches: impl Fn(&T, &T) -> bool,
) -> ComparisonResult<T>
where
    T: Clone,
{
    let mut result = ComparisonResult::new();
    let mut matched_old_indices = Vec::new();

    for new_item in new {
        let mut found_match = false;

        for (old_index, old_item) in old.iter().enumerate() {
            if same_item(old_item, new_item) {
                found_match = true;
                matched_old_indices.push(old_index);

                if !data_matches(old_item, new_item) {
                    result.updated.push((old_item.clone(), new_item.clone()));
                }

                break;
            }
        }

        if !found_match {
            result.new.push(new_item.clone());
        }
    }

    for (index, old_item) in old.iter().enumerate() {
        if !matched_old_indices.contains(&index) {
            result.deleted.push(old_item.clone());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Item {
        pub id: i32,
        pub value: String,
    }

    impl Item {
        pub fn new(id: i32, value: impl Into<String>) -> Self {
            Self {
                id,
                value: value.into(),
            }
        }
    }

    #[test]
    fn test_compare_lists_identical_lists() {
        let old = vec![1, 2, 3];
        let new = vec![1, 2, 3];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, Vec::<i32>::new());
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, Vec::<i32>::new());
    }

    #[test]
    fn test_compare_lists_all_deleted() {
        let old = vec![1, 2, 3];
        let new = vec![4, 5];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, vec![4, 5]);
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, vec![1, 2, 3]);
    }

    #[test]
    fn test_compare_lists_updated_items() {
        let old = vec![Item::new(1, "alice"), Item::new(2, "bob")];
        let new = vec![Item::new(1, "alice_updated"), Item::new(2, "bob")];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a.id == b.id);

        assert_eq!(result.new, vec![]);
        assert_eq!(
            result.updated,
            vec![(Item::new(1, "alice"), Item::new(1, "alice_updated"))]
        );
        assert_eq!(result.deleted, vec![]);
    }

    #[test]
    fn test_compare_lists_mixed_changes() {
        let old = vec![1, 2, 3, 4];
        let new = vec![2, 4, 5, 6];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, vec![5, 6]);
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, vec![1, 3]);
    }

    #[test]
    fn test_compare_lists_empty_old() {
        let old = vec![];
        let new = vec![1, 2, 3];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, vec![1, 2, 3]);
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, Vec::<i32>::new());
    }

    #[test]
    fn test_compare_lists_empty_new() {
        let old = vec![1, 2, 3];
        let new = vec![];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, Vec::<i32>::new());
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, vec![1, 2, 3]);
    }

    #[test]
    fn test_compare_lists_both_empty() {
        let old = Vec::<i32>::new();
        let new = Vec::<i32>::new();

        let result = compare_lists_partial_eq(&old, &new, |a, b| a == b);

        assert_eq!(result.new, Vec::<i32>::new());
        assert_eq!(result.updated, Vec::<(i32, i32)>::new());
        assert_eq!(result.deleted, Vec::<i32>::new());
    }

    #[test]
    fn test_compare_lists_updated_deleted_new() {
        let old = vec![Item::new(1, "alice"), Item::new(2, "bob")];
        let new = vec![Item::new(1, "alice_updated"), Item::new(3, "max")];

        let result = compare_lists_partial_eq(&old, &new, |a, b| a.id == b.id);

        assert_eq!(result.new, vec![Item::new(3, "max")]);
        assert_eq!(
            result.updated,
            vec![(Item::new(1, "alice"), Item::new(1, "alice_updated"))]
        );
        assert_eq!(result.deleted, vec![Item::new(2, "bob")]);
    }
}
