use std::path::PathBuf;

use gouda_core::RequestContext;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{builder, CapabilityEvent, VerificationStatusEvent};
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::stream::StreamExt;
use matrix_sdk::Client;
use matrix_sdk_crypto::store::types::RoomKeyInfo;
use ruma_common::api::error::UnknownTokenErrorData;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::client::SessionContext;
use crate::error::{Error, Result};
use crate::memory_cache::MemoryCache;
use crate::notifications::NotificationManager;
use crate::{crypto, user};

const SYNC_RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
            return Err(Error::NotLoggedIn);
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
                Error::internal("Error loading and decrypting session file")
            })?;

        let mut session: Session = serde_json::from_slice(&decrypted)
            .map_err(|_| Error::internal("Error deserialing session"))?;

        session.file = file;
        session.passphrase = passphrase;

        Ok(session)
    }

    pub async fn save(&self) -> Result<()> {
        log::debug!("Persisting session");

        let serialized =
            serde_json::to_vec(&self).map_err(|_| Error::internal("Error serializing session"))?;

        crypto::encrypt_to_file(&self.file, &self.passphrase, serialized)
            .await
            .map_err(|err| {
                log::error!("Error encrypting and writing to session file: {err}");
                Error::internal("Error writing session to file")
            })?;

        log::debug!("Session persisted in: {}", self.file.to_string_lossy());

        Ok(())
    }

    // TODO: Refactor entire session stuff

    /// Performs the initial synchronization followed by an infinite
    /// background synchronization.
    /// This method blocks until the initial sync is finished.
    pub async fn sync(
        mut self,
        mut ctx: RequestContext,
        session_context: SessionContext,
    ) -> Result<()> {
        subscribe_to_room_keys(
            session_context.client.clone(),
            session_context.memory_cache.clone(),
        );

        NotificationManager::from_session(ctx.clone(), &session_context)
            .subscribe_to_changes()
            .await;

        self.initial_sync(&mut ctx, &session_context).await?;
        self.exec_initial_actions(&ctx, &session_context).await;

        session_context
            .event_manager
            .setup_event_handlers(&session_context.client);

        self.start_background_sync(ctx, session_context).await?;

        Ok(())
    }

    /// Executes all actions required after the initial sync.
    async fn exec_initial_actions(&self, ctx: &RequestContext, session_context: &SessionContext) {
        let SessionContext {
            client,
            proto_cache,
            ..
        } = session_context;

        let Some(user_id) = client.user_id() else {
            log::error!("Unable to retrieve user id after initial sync");
            return;
        };

        if let Some(status) = proto_cache.user_status() {
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
        session_context: &SessionContext,
    ) -> Result<()> {
        log::info!("Starting initial sync");

        self.sync_once(session_context).await?;

        log::info!("Initial sync finished");
        log::debug!("Checking verification status");

        send_capabilities_event(ctx).await;
        send_verification_status_event(ctx, &session_context.client).await?;

        Ok(())
    }

    /// Starts an endless synchronization loop in a separate tokio task,
    /// thus making this function non blocking.
    async fn start_background_sync(
        mut self,
        ctx: RequestContext,
        session_context: SessionContext,
    ) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            loop {
                let result = self.sync_once(&session_context).await;

                let result = self
                    .process_sync_result(&session_context.client, result)
                    .await;

                if let Err(err) = result {
                    log::error!("Received an unrecoverable error during sync: {err}");
                    ctx.send_error(err.into()).await;
                    break;
                }
            }
        });

        Ok(handle)
    }

    async fn sync_once(&self, session_context: &SessionContext) -> matrix_sdk::Result<()> {
        let mut sync_settings = SyncSettings::new();

        let SessionContext {
            client,
            proto_cache,
            ..
        } = session_context;

        if let Some(token) = proto_cache.sync_token() {
            sync_settings = sync_settings.token(token);
        }

        if let Some(user_status) = proto_cache.user_status() {
            let presence_state = user_status.state.try_into().unwrap_or_default();
            let matrix_presence = user::chat_presence_state_to_matrix(presence_state)
                .unwrap_or(ruma_common::presence::PresenceState::Online);
            sync_settings = sync_settings.set_presence(matrix_presence);
        }

        let response = client.sync_once(sync_settings).await?;

        proto_cache.set_sync_token(response.next_batch.clone());

        Ok(())
    }

    /// Processes the result of a single sync.
    /// Only returns an error when the sync result is an unrecoverable error, for example
    /// if the auth token is no longer valid.
    /// In case of a network error, this function will block for a specific
    /// timeout and return Ok, so the sync is being retried.
    async fn process_sync_result(
        &mut self,
        client: &Client,
        result: matrix_sdk::Result<()>,
    ) -> Result<()> {
        let Err(err) = result else {
            return Ok(());
        };

        log::warn!("Error during sync: {err}");

        if self.is_connection_error(&err) {
            return self.handle_connection_error().await;
        }

        if let Some(data) = self.is_token_error(&err) {
            return self.handle_token_error(client, data).await;
        }

        Err(err.into())
    }

    fn is_connection_error(&self, err: &matrix_sdk::Error) -> bool {
        if let matrix_sdk::Error::Http(http_err) = &err {
            if let matrix_sdk::HttpError::Reqwest(e) = &**http_err {
                return e.is_request() || e.is_connect() || e.is_timeout();
            }
        }

        false
    }

    async fn handle_connection_error(&self) -> Result<()> {
        log::info!(
            "Received a connection error during sync, waiting for {} seconds to retry",
            SYNC_RETRY_TIMEOUT.as_secs()
        );

        tokio::time::sleep(SYNC_RETRY_TIMEOUT).await;

        Ok(())
    }

    fn is_token_error<'a>(&self, err: &'a matrix_sdk::Error) -> Option<&'a UnknownTokenErrorData> {
        use ruma_common::api::error::ErrorKind;

        let kind = err.client_api_error_kind()?;

        if let ErrorKind::UnknownToken(data) = kind {
            return Some(data);
        }

        None
    }

    async fn handle_token_error(
        &mut self,
        client: &Client,
        data: &UnknownTokenErrorData,
    ) -> Result<()> {
        log::info!("Received an unknown token error during sync");

        if !data.soft_logout {
            log::info!("Session has been logged out, stopping sync");
            return Err(Error::LoggedOut);
        }

        return self.refresh_access_token(client).await;
    }

    async fn refresh_access_token(&mut self, client: &Client) -> Result<()> {
        log::info!("Refreshing access token");

        client.refresh_access_token().await?;

        self.user_session = client
            .matrix_auth()
            .session()
            .ok_or(Error::internal("Unable to retrieve matrix auth session"))?;

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
        let event_cache = client.event_cache();
        let mut stream = event_cache.subscribe_to_decryption_reports();

        while let Some(result) = stream.next().await {
            match result {
                Ok(report) => println!("REPORT: {report:?}"),
                Err(err) => {
                    log::error!("Received error subscribing to redecryption reports: {err}");
                }
            }
        }

        log::error!("Stream stopped");
    });

    // tokio::spawn(async move {
    //     let Some(mut stream) = client.encryption().room_keys_received_stream().await else {
    //         log::error!("Unable to subscribe to room keys stream");
    //         return;
    //     };

    //     while let Some(result) = stream.next().await {
    //         match result {
    //             Ok(keys) => handle_room_keys(&memory_cache, keys).await,
    //             Err(err) => {
    //                 log::error!("Received error on room keys stream {err}");
    //             }
    //         }
    //     }
    // });
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
        .map_err(|err| Error::internal(format!("Error retrieving own device: {err}")))?;

    let Some(this_device) = result else {
        return Err(Error::NotLoggedIn);
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
