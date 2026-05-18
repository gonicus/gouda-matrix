use std::path::PathBuf;

use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::Client;
use mrhc_core::{ClientContext, Result};
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{builder, CapabilityEvent, VerificationStatusEvent};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::client::InitializedData;
use crate::{crypto, errors, memory_cache, user};

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
        mut ctx: ClientContext,
        initialized_data: InitializedData,
    ) -> Result<()> {
        self.initial_sync(&mut ctx, &initialized_data).await?;

        self.exec_initial_actions(&ctx, &initialized_data).await;

        initialized_data
            .event_manager
            .setup_event_handlers(&initialized_data.client);

        self.start_background_sync(initialized_data).await?;

        Ok(())
    }

    /// Executes all actions required after the initial sync.
    async fn exec_initial_actions(&self, ctx: &ClientContext, initialized_data: &InitializedData) {
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
        ctx: &mut ClientContext,
        initialized_data: &InitializedData,
    ) -> Result<()> {
        log::info!("Starting initial sync");

        self.sync_once(initialized_data, memory_cache::SyncSource::InitialSync)
            .await?;

        log::info!("Initial sync finished");
        log::debug!("Checking verification status");

        send_capabilities_event(ctx).await;
        send_verification_status_event(ctx, &initialized_data.client).await?;

        Ok(())
    }

    /// Starts an endless synchronization loop in a separate tokio task,
    /// thus making this function non blocking.
    async fn start_background_sync(
        self,
        initialized_data: InitializedData,
    ) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            loop {
                let result = self
                    .sync_once(&initialized_data, memory_cache::SyncSource::ContinuousSync)
                    .await;

                if let Err(err) = result {
                    log::error!("Error during sync: {err}");
                }
            }
        });

        Ok(handle)
    }

    async fn sync_once(
        &self,
        initialized_data: &InitializedData,
        sync_source: memory_cache::SyncSource,
    ) -> Result<()> {
        let mut sync_settings = SyncSettings::new();

        if let Some(token) = &initialized_data.proto_cache.sync_token().await {
            sync_settings = sync_settings.token(token);
        }

        if let Some(user_status) = &initialized_data.proto_cache.user_status().await {
            let presence = user::chat_presence_state_to_matrix(
                user_status.state.try_into().unwrap_or_default(),
            )
            .unwrap_or(ruma_common::presence::PresenceState::Online);
            sync_settings = sync_settings.set_presence(presence);
        }

        let response = initialized_data
            .client
            .sync_once(sync_settings)
            .await
            .map_err(errors::create_unknown)?;

        initialized_data
            .proto_cache
            .set_sync_token(response.next_batch.clone())
            .await;

        memory_cache::cache_sync_response(&initialized_data.memory_cache, &response, sync_source)
            .map_err(errors::create_unknown)?;

        self.save().await?;

        Ok(())
    }
}

async fn send_capabilities_event(ctx: &mut ClientContext) {
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

async fn send_verification_status_event(ctx: &mut ClientContext, client: &Client) -> Result<()> {
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
