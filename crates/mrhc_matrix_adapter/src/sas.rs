use matrix_sdk::crypto::{EmojiShortAuthString, SasState};
use matrix_sdk::encryption::verification::SasVerification;
use matrix_sdk::stream::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;

use mrhc_core::ClientContext;
use mrhc_proto::chat::cross_signing_method_selected_event::VerificationCode;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;

use super::verification::{self, VerificationAction};
use crate::utils;
use crate::verification::send_verification_end_event_err;

/// Manages a SAS verification process until its completion.
/// Actions to be performed (e.g. Cancel, Confirm) can be sent via
/// the sender of the `action_rx`.
pub struct SasVerificationManager<'a> {
    ctx: ClientContext,
    verification: SasVerification,
    flow_id: String,
    action_rx: &'a mut UnboundedReceiver<VerificationAction>,
}

impl<'a> SasVerificationManager<'a> {
    pub fn new(
        ctx: ClientContext,
        verification: SasVerification,
        flow_id: String,
        action_rx: &'a mut UnboundedReceiver<VerificationAction>,
    ) -> Self {
        Self {
            ctx,
            verification,
            flow_id,
            action_rx,
        }
    }

    /// Consumes `Self` and waits for changes to the SAS verification flow
    /// or for requested actions to be executed.
    /// This method blocks until the verification flow is complete.
    /// Either successfully, cancelled, or with an error.
    pub async fn run(mut self) {
        let mut stream = self.verification.changes();

        loop {
            tokio::select! {
                state = stream.next() => {
                    if let Some(state) = state {
                        log::debug!("SAS verification proceeded into next state {:?}", state);
                        if self.process_state_change(state).await {
                            break;
                        }
                    } else {
                        log::debug!("No more changes left in sas verification");
                        break;
                    }
                }
                action = self.action_rx.recv() => {
                    if self.process_action(action).await {
                        break;
                    }
                }
            }
        }
    }

    /// Processes a state change of the SAS verification flow.
    ///
    /// Returns `true` if the flow should be stopped. This is the case if it has finished,
    /// an error has occurred, or the process has been aborted.
    async fn process_state_change(&mut self, state: SasState) -> bool {
        match state {
            SasState::Created { .. } => false,
            SasState::Started { .. } => false,
            SasState::Accepted { .. } => false,
            SasState::KeysExchanged { emojis, decimals } => {
                self.present_sas(emojis, decimals);
                false
            }
            SasState::Confirmed => false,
            SasState::Done { .. } => {
                verification::send_verification_end_event(
                    &mut self.ctx,
                    self.flow_id.to_owned(),
                    true,
                );
                true
            }
            SasState::Cancelled(_) => {
                verification::send_verification_end_event(
                    &mut self.ctx,
                    self.flow_id.to_owned(),
                    false,
                );
                true
            }
        }
    }

    /// Processes an action received from the `Self::action_rx`.
    ///
    /// Returns `true` if the flow should be stopped. This is the case if it has finished,
    /// an error has occurred, or the process has been aborted.
    async fn process_action(&mut self, action: Option<VerificationAction>) -> bool {
        let Some(action) = action else {
            log::info!("Action sender of the verification request dropped");
            return true;
        };

        match action {
            VerificationAction::Cancel => {
                self.cancel().await;
                true
            }
            VerificationAction::Confirm => {
                if let Err(err) = self.verification.confirm().await {
                    log::error!("Error confirming SAS verification: {err}");
                    send_verification_end_event_err(&mut self.ctx, self.flow_id.to_owned(), err);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Sends the corresponding `CrossSigningMethodSelectedEvent` to the application when
    /// the participants have exchanged keys and agreed on a short authentication string.
    /// If emojis are specified, the emojis (`VerificationCode::Symbols`) will be used.
    /// Otherwise, the decimal representation (`VerificationCode::StringCode`) is used.
    fn present_sas(&mut self, emojis: Option<EmojiShortAuthString>, decimals: (u16, u16, u16)) {
        let method = if emojis.is_some() {
            CrossSigningMethod::SasSymbol
        } else {
            CrossSigningMethod::SasString
        };

        let verification_code = if let Some(emojis) = emojis {
            VerificationCode::Symbols(utils::sas_emojis_to_chat_symbols(emojis.emojis))
        } else {
            VerificationCode::StringCode(format!("{} {} {}", decimals.0, decimals.1, decimals.2))
        };

        let re = CrossSigningMethodSelectedEvent {
            verification_flow_id: self.flow_id.to_owned(),
            selected_method: method.into(),
            verification_code: Some(verification_code),
        };

        self.ctx
            .send_event(ResponseContent::CrossSigningMethodSelectedEvent(re));
    }

    /// Cancels the verification flow and sends the corresponding `VerificationEndEvent`
    /// to the application.
    async fn cancel(&mut self) {
        log::info!("Cancelling SAS verification");

        if let Err(err) = self.verification.cancel().await {
            log::error!("Error canceling SAS verification: {err}");
            verification::send_verification_end_event_err(
                &mut self.ctx,
                self.flow_id.to_owned(),
                err,
            );
        } else {
            verification::send_verification_end_event(
                &mut self.ctx,
                self.flow_id.to_owned(),
                false,
            );
        }
    }
}
