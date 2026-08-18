use gouda_proto::chat::{self, MessageContentPoll};
use matrix_sdk::deserialized_responses::TimelineEvent;
use matrix_sdk::ruma::events::poll::unstable_end::UnstablePollEndEventContent;
use matrix_sdk::ruma::events::poll::unstable_response::UnstablePollResponseEventContent;
use matrix_sdk::ruma::events::poll::unstable_start::{
    UnstablePollStartContentBlock, UnstablePollStartEventContent,
};
use matrix_sdk::ruma::events::{AnyMessageLikeEvent, AnyTimelineEvent};
use matrix_sdk::Room;
use ruma_common::{EventId, OwnedUserId};

use crate::bridge::{IntoChat, TryIntoChat};
use crate::error::{Error, Result};
use crate::utils;

pub async fn assemble_poll(room: Room, poll_id: &EventId) -> Result<chat::MessageContentPoll> {
    let (event, mut relations) = room
        .load_or_fetch_event_with_relations(poll_id, None, None)
        .await?;

    let poll_content = deserialize_poll_start_content(&room, &event)?;
    let mut content = assemble_poll_start(poll_content.poll_start())?;

    relations.sort_by_key(|e| e.timestamp);

    for relation in relations {
        let Some(sender) = relation.sender() else {
            continue;
        };

        if let Ok(event) = deserialize_poll_start_content(&room, &relation) {
            replace_content(&mut content, event.poll_start())?;
        }

        if let Ok(event) = deserialize_poll_response_content(&room, &relation) {
            add_answer(&mut content, sender, event.poll_response.answers);
        }

        if let Ok(_) = deserialize_poll_end_content(&room, &relation) {
            content.completed = true;
        }
    }

    Ok(content)
}

pub fn assemble_poll_start(
    content: &UnstablePollStartContentBlock,
) -> Result<chat::MessageContentPoll> {
    let Ok(r#type) = content.kind.clone().try_into_chat() else {
        return Err(Error::InvalidPollType);
    };

    let options = content
        .answers
        .iter()
        .map(|f| f.clone().into_chat())
        .collect();

    let content = MessageContentPoll {
        r#type: r#type.into(),
        completed: false,
        max_selections: content.max_selections.try_into().unwrap_or(u32::MAX),
        question: content.question.text.clone(),
        options,
    };

    Ok(content)
}

fn replace_content(old: &mut MessageContentPoll, new: &UnstablePollStartContentBlock) -> Result<()> {
    old.max_selections = new.max_selections.try_into().unwrap_or(u32::MAX);

    old.options = new
        .answers
        .iter()
        .map(|f| f.clone().into_chat())
        .collect();

    old.question = new.question.text.clone();

    old.r#type = new.kind.clone().try_into_chat()?.into();

    Ok(())
}

fn add_answer(content: &mut MessageContentPoll, sender: OwnedUserId, answers: Vec<String>) {
    clear_user_answers(content, sender.as_str());

    let sender_id = sender.to_string();

    for answer in answers {
        let Some(option) = content.options.iter_mut().find(|p| p.id == answer) else {
            continue;
        };

        if !option.voted_user_ids.contains(&sender_id) {
            option.voted_user_ids.push(sender_id.clone());
        }
    }
}

fn clear_user_answers(content: &mut MessageContentPoll, user_id: &str) {
    for option in &mut content.options {
        option.voted_user_ids.retain(|u| u != user_id);
    }
}

fn deserialize_poll_start_content(
    room: &Room,
    event: &TimelineEvent,
) -> Result<UnstablePollStartEventContent> {
    let event = utils::timeline_event_to_any_timeline_event(&room, event)?;
    let message_like = any_timeline_event_to_message_like(event)?;

    let AnyMessageLikeEvent::UnstablePollStart(poll_start) = message_like else {
        return Err(Error::internal(
            "Event with the given ID is not a poll start event",
        ));
    };

    let Some(original) = poll_start.as_original() else {
        return Err(Error::internal("Event with the given ID is redacted"));
    };

    Ok(original.content.clone())
}

fn deserialize_poll_response_content(
    room: &Room,
    event: &TimelineEvent,
) -> Result<UnstablePollResponseEventContent> {
    let event = utils::timeline_event_to_any_timeline_event(&room, event)?;
    let message_like = any_timeline_event_to_message_like(event)?;

    let AnyMessageLikeEvent::UnstablePollResponse(event) = message_like else {
        return Err(Error::internal(
            "Event with the given ID is not a poll start event",
        ));
    };

    let Some(original) = event.as_original() else {
        return Err(Error::internal("Event with the given ID is redacted"));
    };

    Ok(original.content.clone())
}

fn deserialize_poll_end_content(
    room: &Room,
    event: &TimelineEvent,
) -> Result<UnstablePollEndEventContent> {
    let event = utils::timeline_event_to_any_timeline_event(&room, event)?;
    let message_like = any_timeline_event_to_message_like(event)?;

    let AnyMessageLikeEvent::UnstablePollEnd(event) = message_like else {
        return Err(Error::internal(
            "Event with the given ID is not a poll end event",
        ));
    };

    let Some(original) = event.as_original() else {
        return Err(Error::internal("Event with the given ID is redacted"));
    };

    Ok(original.content.clone())
}

fn any_timeline_event_to_message_like(event: AnyTimelineEvent) -> Result<AnyMessageLikeEvent> {
    let AnyTimelineEvent::MessageLike(message_like) = event else {
        return Err(Error::internal("Event is not message like"));
    };

    Ok(message_like)
}
