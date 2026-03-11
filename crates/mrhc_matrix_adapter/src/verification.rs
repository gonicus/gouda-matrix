use matrix_sdk::encryption::verification::{
    Verification, VerificationRequest, VerificationRequestState,
};
use matrix_sdk::ruma::events::key::verification::VerificationMethod;
use matrix_sdk::stream::StreamExt;
use mrhc_core::ClientContext;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::*;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::sas::SasVerificationManager;

pub enum VerificationAction {
    /// Cancel the verification request.
    Cancel,
    /// Confirm the verification, for example that the emojis on both devices match.
    Confirm,
    /// Selects the specified verification method.
    /// This method should be selected by the user in the UI.
    SelectMethod(CrossSigningMethod),
}

pub struct VerificationManager {
    /// The ID of the flow this manager is controlling.
    flow_id: String,
    /// Sender to send a verification action to the executing task.
    action_tx: UnboundedSender<VerificationAction>,
    /// The handle to the tokio task executing the verification.
    handle: JoinHandle<()>,
}

impl VerificationManager {
    pub fn from_verification_request(ctx: ClientContext, request: VerificationRequest) -> Self {
        let flow_id = request.flow_id().to_owned();

        let (action_tx, action_rx) = mpsc::unbounded_channel();

        let processor = VerificationProcessor::new(ctx.clone(), action_rx, request);
        let handle = processor.run();

        Self {
            flow_id,
            action_tx,
            handle,
        }
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    pub fn is_active(&self) -> bool {
        !self.handle.is_finished()
    }

    pub fn cancel(self) {
        let _ = self.action_tx.send(VerificationAction::Cancel);
    }

    pub fn confirm(&self) {
        let _ = self.action_tx.send(VerificationAction::Confirm);
    }

    pub fn select_method(&self, method: CrossSigningMethod) {
        let _ = self
            .action_tx
            .send(VerificationAction::SelectMethod(method));
    }
}

/// Processes and manages an ongoing verification flow.
/// Actions to be performed (e.g. Cancel, Confirm) can be sent via
/// the sender of `action_rx`.
struct VerificationProcessor {
    ctx: ClientContext,
    action_rx: UnboundedReceiver<VerificationAction>,
    verification: VerificationRequest,
    flow_id: String,
}

impl VerificationProcessor {
    pub fn new(
        ctx: ClientContext,
        action_rx: UnboundedReceiver<VerificationAction>,
        request: VerificationRequest,
    ) -> Self {
        let flow_id = request.flow_id().to_owned();

        Self {
            ctx,
            action_rx,
            verification: request,
            flow_id,
        }
    }

    /// Consumes `Self` and waits for changes to the SAS verification flow
    /// or for requested actions to be executed.
    /// This method blocks until the verification flow is complete.
    /// Either successfully, cancelled, or with an error.
    pub fn run(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.wait_for_changes().await;
        })
    }

    /// Waits for changes to the verification stream itself or for actions to be executed.
    /// This method handles the verification state changes as well as the execution of actions.
    async fn wait_for_changes(&mut self) {
        let mut stream = self.verification.changes();

        loop {
            tokio::select! {
                state = stream.next() => {
                    if let Some(state) = state {
                        if self.process_state_change(state).await {
                            break;
                        }
                    } else {
                        log::debug!("No more changes left in verification request");
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

    /// Processes a state change of the verification flow.
    ///
    /// Returns `true` if the flow should be stopped. This is the case if it has finished,
    /// an error has occurred, or the process has been aborted.
    async fn process_state_change(&mut self, change: VerificationRequestState) -> bool {
        match change {
            VerificationRequestState::Created { .. } => {
                log::debug!("Verification request transitioned into Created state");
                false
            }
            VerificationRequestState::Requested { .. } => {
                log::debug!("Verification request transitioned into Requested state");
                false
            }
            VerificationRequestState::Ready {
                their_methods,
                our_methods,
                ..
            } => {
                log::debug!("Verification request transitioned into Ready state");

                let available_methods = get_available_methods(&our_methods, &their_methods);

                self.ctx.send_event(ResponseContent::CrossSigningStartEvent(
                    CrossSigningStartEvent {
                        verification_flow_id: self.flow_id.clone(),
                        available_methods,
                    },
                ));

                false
            }
            VerificationRequestState::Transitioned { verification } => {
                log::debug!("Verification request transitioned into Transitioned state");

                let Verification::SasV1(sas) = verification else {
                    log::error!(
                        "Verification request transitioned into an unsupported verification flow",
                    );

                    self.cancel_err(
                        "Verification request transitioned into an unsupported verification flow",
                    )
                    .await;

                    return true;
                };

                if let Err(err) = sas.accept().await {
                    self.cancel_err(err).await;
                    return true;
                }

                SasVerificationManager::new(
                    self.ctx.clone(),
                    sas,
                    self.flow_id.clone(),
                    &mut self.action_rx,
                )
                .run()
                .await;

                true
            }
            VerificationRequestState::Done => {
                log::debug!("Verification request transitioned into Done state");
                true
            }
            VerificationRequestState::Cancelled(_) => {
                log::debug!("Verification request transitioned into Cancelled state");
                send_verification_end_event(&mut self.ctx, self.flow_id.clone(), false);
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
            log::debug!("Action sender of the verification request dropped");
            return true;
        };

        match action {
            VerificationAction::Cancel => {
                self.cancel().await;
                true
            }
            VerificationAction::SelectMethod(method) => match method {
                CrossSigningMethod::SasString | CrossSigningMethod::SasSymbol => {
                    let sas_request = match self.verification.start_sas().await {
                        Ok(r) => r,
                        Err(err) => {
                            self.cancel_err(err).await;
                            return true;
                        }
                    };

                    let Some(sas_request) = sas_request else {
                        self.cancel_err("Error starting SAS request").await;
                        return true;
                    };

                    SasVerificationManager::new(
                        self.ctx.clone(),
                        sas_request,
                        self.flow_id.clone(),
                        &mut self.action_rx,
                    )
                    .run()
                    .await;

                    true
                }
            },
            _ => false,
        }
    }

    async fn cancel(&mut self) {
        log::debug!("Cancelling verification flow");

        if let Err(err) = self.verification.cancel().await {
            log::error!("Error canceling verification flow {err}");
            send_verification_end_event_err(&mut self.ctx, self.flow_id.clone(), err);
        } else {
            send_verification_end_event(&mut self.ctx, self.flow_id.clone(), false);
        }
    }

    async fn cancel_err<E: std::fmt::Display>(&mut self, err: E) {
        log::debug!("Cancelling verification flow");

        if let Err(err) = self.verification.cancel().await {
            log::error!("Error canceling verification flow {err}");
        }

        send_verification_end_event_err(&mut self.ctx, self.flow_id.clone(), err);
    }
}

/// Retrieves the available methods both devices support and converts them to
/// an integer usable in the chat interface.
fn get_available_methods(
    our_methods: &Vec<VerificationMethod>,
    their_methods: &Vec<VerificationMethod>,
) -> Vec<i32> {
    let mut matching_methods = Vec::new();

    for ours in our_methods {
        for theirs in their_methods {
            if ours == theirs {
                matching_methods.push(ours);
                break;
            }
        }
    }

    cross_signing_methods_from_matrix(matching_methods)
}

/// Converts the cross signing verification methods to matrix verification methods.
pub fn cross_signing_methods_to_matrix(methods: Vec<i32>) -> Vec<VerificationMethod> {
    let mut result = Vec::new();

    for method in methods {
        let Ok(method) = CrossSigningMethod::try_from(method) else {
            continue;
        };

        match method {
            CrossSigningMethod::SasString | CrossSigningMethod::SasSymbol => {
                if !result.iter().any(|m| m == &VerificationMethod::SasV1) {
                    result.push(VerificationMethod::SasV1);
                }
            }
        }
    }

    result
}

pub fn send_verification_end_event(ctx: &mut ClientContext, flow_id: String, success: bool) {
    let event = VerificationEndEvent {
        verification_flow_id: Some(flow_id),
        result: Some(verification_end_event::Result::Successful(success)),
    };

    ctx.send_event(ResponseContent::VerificationEndEvent(event));
}

pub fn send_verification_end_event_err<E: std::fmt::Display>(
    ctx: &mut ClientContext,
    flow_id: String,
    error: E,
) {
    let event = VerificationEndEvent {
        verification_flow_id: Some(flow_id),
        result: Some(verification_end_event::Result::Error(error.to_string())),
    };

    ctx.send_event(ResponseContent::VerificationEndEvent(event));
}

/// Converts verification methods received from the matrix-sdk to verification methods of
/// the chat interface.
fn cross_signing_methods_from_matrix(methods: Vec<&VerificationMethod>) -> Vec<i32> {
    let mut result = Vec::new();

    for method in methods {
        if matches!(method, VerificationMethod::SasV1) {
            result.push(CrossSigningMethod::SasString.into());
            result.push(CrossSigningMethod::SasSymbol.into());
        }
    }

    result
}
