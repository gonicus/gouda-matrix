use matrix_sdk::config::SyncSettings;
use matrix_sdk::sync::SyncResponse;
use matrix_sdk::Client;

use mrhc_core::{ClientContext, Result};

use crate::errors;
use crate::events::event_handler;

/// Performs a single synchronization on the client, blocking the current thread until
/// the synchronization is complete.
pub async fn initial_sync(client: &Client, sync_settings: SyncSettings) -> Result<SyncResponse> {
    log::info!("Starting initial sync of the client");

    let result = client
        .sync_once(sync_settings)
        .await
        .map_err(errors::convert_matrix_sdk_error);

    log::info!("Initial sync finished");

    result
}

/// Starts an infinitely long sync of the client in a separate tokio task,
/// making this function non blocking.
pub fn start_background_sync(mut ctx: ClientContext, client: Client, sync_settings: SyncSettings) {
    // TODO: Check if there is already another sync in progress

    client.add_event_handler_context(ctx.clone());
    client.add_event_handler(event_handler);

    tokio::spawn(async move {
        let result = client.sync(sync_settings).await;

        // TOKO: check if it makes sense to restart the sync.
        if let Err(err) = result {
            ctx.send_error(errors::convert_matrix_sdk_error(err));
        }
    });
}
