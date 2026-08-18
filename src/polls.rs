use gouda_proto::chat;
use matrix_sdk::Room;
use ruma_common::EventId;

use crate::error::Result;

pub async fn assemble_poll(room: Room, poll_id: &EventId) -> Result<chat::MessageContentPoll> {
    let (event, relations) = room
        .load_or_fetch_event_with_relations(poll_id, None, None)
        .await?;

    todo!()
}
