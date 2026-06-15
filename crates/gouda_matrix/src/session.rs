use std::path::PathBuf;

use gouda_core::{RequestContext, Result};
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{builder, CapabilityEvent, VerificationStatusEvent};
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::stream::StreamExt;
use matrix_sdk::Client;
use matrix_sdk_crypto::store::types::RoomKeyInfo;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::client::InitializedData;
use crate::memory_cache::MemoryCache;
use crate::{crypto, errors, user};

/// The full session to persist.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// The Matrix user session.
    pub user_session: MatrixSession,

    #[serde(skip)]
    file: PathBuf,
    #[serde(skip)]
    passphrase: String,
}

impl Session {
    /// Creates a new session from a Matrix client.
    /// This should only be used if the session is new and has not yet been saved.
    pub fn new(client: &Client, file: PathBuf, passphrase: String) -> Result<Self> {
        let user_session = client.matrix_auth().session();

        let Some(user_session) = user_session else {
            return Err(errors::create_unknown(
                "InternalError: Client is not logged in",
            ));
        };

        Ok(Self {
            user_session,
            file,
            passphrase,
        })
    }

    /// Reads the session from the given path.
    pub async fn read_from_file(file: PathBuf, passphrase: String) -> Result<Self> {
        let decrypted = crypto::decrypt_file(&file, &passphrase)
            .await
            .map_err(|err| {
                log::error!("Error loading and decrypting session file: {err}");
                errors::create_unknown("Error loading and decrypting session file")
            })?;

        let mut session: Session = serde_json::from_slice(&decrypted)
            .map_err(|_| errors::create_unknown("Error deserialing session"))?;

        session.file = file;
        session.passphrase = passphrase;

        Ok(session)
    }

    pub async fn save(&self) -> Result<()> {
        log::debug!("Persisting session");

        let serialized = serde_json::to_vec(&self)
            .map_err(|_| errors::create_unknown("Error serializing session"))?;

        crypto::encrypt_to_file(&self.file, &self.passphrase, serialized)
            .await
            .map_err(|err| {
                log::error!("Error encrypting and writing to session file: {err}");
                errors::create_unknown("Error writing session to file")
            })?;

        log::debug!("Session persisted in: {}", self.file.to_string_lossy());

        Ok(())
    }

    /// Performs the initial synchronization followed by an infinite
    /// background synchronization.
    /// This method blocks until the initial sync is finished.
    pub async fn sync(
        mut self,
        mut ctx: RequestContext,
        initialized_data: InitializedData,
    ) -> Result<()> {
        subscribe_to_room_keys(
            initialized_data.client.clone(),
            initialized_data.memory_cache.clone(),
        );

        self.initial_sync(&mut ctx, &initialized_data).await?;

        self.exec_initial_actions(&ctx, &initialized_data).await;

        initialized_data
            .event_manager
            .setup_event_handlers(&initialized_data.client);

        self.start_background_sync(ctx, initialized_data).await?;

        Ok(())
    }

    /// Executes all actions required after the initial sync.
    async fn exec_initial_actions(&self, ctx: &RequestContext, initialized_data: &InitializedData) {
        let InitializedData {
            client,
            proto_cache,
            ..
        } = initialized_data;

        let Some(user_id) = client.user_id() else {
            log::error!("Unable to retrieve user id after initial sync");
            return;
        };

        if let Some(status) = proto_cache.user_status().await {
            let proto = builder::UserChangeEventBuilder::new(user_id.to_string())
                .change_status(status)
                .to_proto();

            ctx.send_event(ResponseContent::UserChangeEvent(proto))
                .await;
        }
    }

    /// Performs a single synchronization on the client, blocking the current thread until
    /// the synchronization is complete.
    /// The session is automatically persisted once a new sync token is received.
    async fn initial_sync(
        &mut self,
        ctx: &mut RequestContext,
        initialized_data: &InitializedData,
    ) -> Result<()> {
        log::info!("Starting initial sync");

        self.sync_once(initialized_data)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        log::info!("Initial sync finished");
        log::debug!("Checking verification status");

        send_capabilities_event(ctx).await;
        send_verification_status_event(ctx, &initialized_data.client).await?;

        Ok(())
    }

    /// Starts an endless synchronization loop in a separate tokio task,
    /// thus making this function non blocking.
    async fn start_background_sync(
        mut self,
        ctx: RequestContext,
        initialized_data: InitializedData,
    ) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            loop {
                let result = self.sync_once(&initialized_data).await;

                if let Err(err) = self.handle_sync_result(result, &initialized_data).await {
                    log::error!(
                        "Received an unrecoverable error during sync, stopping background sync"
                    );
                    ctx.send_error(err).await;

                    break;
                }
            }
        });

        Ok(handle)
    }

    async fn sync_once(&self, initialized_data: &InitializedData) -> matrix_sdk::Result<()> {
        let mut sync_settings = SyncSettings::new();

        if let Some(token) = &initialized_data.proto_cache.sync_token().await {
            sync_settings = sync_settings.token(token);
        }

        if let Some(user_status) = &initialized_data.proto_cache.user_status().await {
            let presence_state = user_status.state.try_into().unwrap_or_default();
            let matrix_presence = user::chat_presence_state_to_matrix(presence_state)
                .unwrap_or(ruma_common::presence::PresenceState::Online);
            sync_settings = sync_settings.set_presence(matrix_presence);
        }

        let response = initialized_data.client.sync_once(sync_settings).await?;

        initialized_data
            .proto_cache
            .set_sync_token(response.next_batch.clone())
            .await;

        Ok(())
    }

    async fn handle_sync_result(
        &mut self,
        result: matrix_sdk::Result<()>,
        initialized_data: &InitializedData,
    ) -> Result<()> {
        use ruma_common::api::error::ErrorKind;

        let Err(err) = result else {
            return Ok(());
        };

        log::warn!("Error during sync: {err}");

        let Some(error_kind) = err.client_api_error_kind() else {
            return Err(errors::convert_matrix_sdk_error(err));
        };

        if let ErrorKind::UnknownToken(error_data) = error_kind {
            if !error_data.soft_logout {
                return Err(errors::convert_matrix_sdk_error(err));
            }

            return self.refresh_access_token(initialized_data).await;
        }

        Err(errors::convert_matrix_sdk_error(err))
    }

    async fn refresh_access_token(&mut self, initialized_data: &InitializedData) -> Result<()> {
        log::info!("Refreshing access token");

        initialized_data
            .client
            .refresh_access_token()
            .await
            .map_err(errors::convert_refresh_token_error)?;

        self.user_session = initialized_data
            .client
            .matrix_auth()
            .session()
            .ok_or(errors::create_unknown("msg"))?;

        if let Err(err) = self.save().await {
            log::error!("Error when saving session: {err}");
        }

        log::info!("Successfully refreshed access token");

        Ok(())
    }
}

fn subscribe_to_room_keys(client: Client, memory_cache: MemoryCache) {
    log::debug!("Subscribing to room keys stream");

    tokio::spawn(async move {
        let Some(mut stream) = client.encryption().room_keys_received_stream().await else {
            log::error!("Unable to subscribe to room keys stream");
            return;
        };

        while let Some(result) = stream.next().await {
            match result {
                Ok(keys) => handle_room_keys(&memory_cache, keys).await,
                Err(err) => {
                    log::error!("Received error on room keys stream {err}");
                }
            }
        }
    });
}

async fn handle_room_keys(memory_cache: &MemoryCache, keys: Vec<RoomKeyInfo>) {
    for info in keys {
        log::debug!("Received new room keys: {info:?}");

        memory_cache
            .retry_encrypted_events(info.room_id, info.session_id)
            .await;
    }
}

async fn send_capabilities_event(ctx: &mut RequestContext) {
    let re = CapabilityEvent {
        direct_rooms: false,
        group_rooms: true,
        sub_threads: true,
        user_search: true,
        invitations: true,
        spaces: false,
        client_verification: true,
        user_presence: true,
        mime_types: vec!["text/plain".to_owned()],
    };

    ctx.send_event(ResponseContent::CapabilityEvent(re)).await;
}

async fn send_verification_status_event(ctx: &mut RequestContext, client: &Client) -> Result<()> {
    let result = client
        .encryption()
        .get_own_device()
        .await
        .map_err(|err| errors::create_unknown(format!("Error retrieving own device: {err}")))?;

    let Some(this_device) = result else {
        return Err(errors::create_unknown(
            "Client is not logged in, but verification status has been requested",
        ));
    };

    let is_cross_signing_available = client
        .encryption()
        .has_devices_to_verify_against()
        .await
        .unwrap_or(false);

    ctx.send_event(ResponseContent::VerificationStatusEvent(
        VerificationStatusEvent {
            is_verified: this_device.is_verified_with_cross_signing(),
            is_recovery_key_verification_available: true,
            is_cross_signing_available,
        },
    ))
    .await;

    Ok(())
}
