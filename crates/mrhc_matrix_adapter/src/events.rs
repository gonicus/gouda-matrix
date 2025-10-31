use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent;

use mrhc_core::ClientContext;

pub async fn event_handler(ev: SyncRoomMessageEvent, _ctx: Ctx<ClientContext>) {
    log::info!("Received event: {ev:?}");

    // ctx.clone()
    //     .send_event(ResponseContent::StatusUpdate(StatusUpdate { code: 1 }));
}
