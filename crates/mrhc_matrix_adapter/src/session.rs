use std::path::PathBuf;

use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::{Client, LoopCtrl};
use mrhc_core::{ClientContext, Result};
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{CapabilityEvent, VerificationStatusEvent};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;

use crate::client::InitializedData;
use crate::{cache, crypto, errors};

/// The full session to persist.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// The Matrix user session.
    pub user_session: MatrixSession,
    /// The latest sync token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_token: Option<String>,

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
            sync_token: None,
            file,
            passphrase,
        })
    }

    /// Reads the session from the given path.
    pub async fn read_from_file(file: PathBuf, passphrase: String) -> Result<Self> {
        let mut reader = tokio::fs::File::open(&file)
            .await
            .map_err(|_| errors::create_unknown("Error opening session file"))?;

        let mut salt = [0u8; 16];
        reader
            .read_exact(&mut salt)
            .await
            .map_err(|_| errors::create_unknown("Error reading salt"))?;

        let key = crypto::derive_key(&passphrase, &salt)?;
        let decrypted = crypto::decrypt(reader, &key).await?;

        let mut session: Session = serde_json::from_slice(&decrypted)
            .map_err(|_| errors::create_unknown("Error deserialing session"))?;

        session.file = file;
        session.passphrase = passphrase;

        Ok(session)
    }

    pub async fn save(&self) -> Result<()> {
        log::debug!("Persisting session");

        let serialized = serde_json::to_string(&self)
            .map_err(|_| errors::create_unknown("Error serializing session"))?;

        let (salt, key) = crypto::derive_new_key(&self.passphrase)?;

        let mut encrypted = crypto::encrypt(serialized.as_bytes().to_vec(), &key)?;

        let mut result = salt.to_vec();
        result.append(&mut encrypted);

        tokio::fs::write(&self.file, result)
            .await
            .map_err(|_| errors::create_unknown("Error writing session to file"))?;

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
        self.start_background_sync(ctx, initialized_data)?;
        Ok(())
    }

    /// Performs a single synchronization on the client, blocking the current thread until
    /// the synchronization is complete.
    /// The session is automatically persisted once a new sync token is received.
    /// Note that the token specified in `sync_settings` will be overwritten.
    /// This method should be called every time the client is being logged in.
    async fn initial_sync(
        &mut self,
        ctx: &mut ClientContext,
        initialized_data: &InitializedData,
    ) -> Result<()> {
        log::info!("Starting initial sync");

        let mut sync_settings = SyncSettings::new();

        if let Some(token) = &self.sync_token {
            sync_settings = sync_settings.token(token);
        }

        let response = initialized_data
            .client
            .sync_once(sync_settings)
            .await
            .map_err(errors::convert_matrix_sdk_error)?;

        self.sync_token = Some(response.next_batch.clone());

        cache::cache_sync_response(
            &initialized_data.cache,
            &response,
            cache::SyncSource::InitialSync,
        )
        .map_err(errors::create_unknown)?;

        log::info!("Initial sync finished");
        log::debug!("Checking verification status");

        self.save().await?;

        send_capabilities_event(ctx);
        send_verification_status_event(ctx, &initialized_data.client).await?;

        Ok(())
    }

    /// Starts an indefinite sync loop in a separate tokio task,
    /// making this function non blocking.
    fn start_background_sync(
        self,
        ctx: ClientContext,
        initialized_data: InitializedData,
    ) -> Result<JoinHandle<()>> {
        let mut sync_settings = SyncSettings::new();

        if let Some(token) = &self.sync_token {
            sync_settings = sync_settings.token(token);
        }

        let client = initialized_data.client.clone();
        let event_manager = initialized_data.event_manager.clone();

        event_manager.setup_event_handlers(&client);

        let handle = tokio::spawn(async move {
            let result = client
                .sync_with_result_callback(sync_settings, |sync_result| {
                    let mut session = self.clone();
                    let cache = initialized_data.cache.clone();

                    async move {
                        let response = sync_result?;
                        session.sync_token = Some(response.next_batch.clone());

                        if let Err(err) = cache::cache_sync_response(
                            &cache,
                            &response,
                            cache::SyncSource::ContinuousSync,
                        ) {
                            log::warn!("Failed to cache sync response: {err}");
                        }

                        match session.save().await {
                            Ok(_) => Ok(LoopCtrl::Continue),
                            // TODO: Better error type
                            Err(_) => Err(matrix_sdk::Error::BadCryptoStoreState),
                        }
                    }
                })
                .await;

            // TODO: Check if it makes sense to restart the sync.
            if let Err(err) = result {
                ctx.send_error(errors::convert_matrix_sdk_error(err));
            }
        });

        Ok(handle)
    }
}

fn send_capabilities_event(ctx: &mut ClientContext) {
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

    ctx.send_event(ResponseContent::CapabilityEvent(re));
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
    ));

    Ok(())
}
