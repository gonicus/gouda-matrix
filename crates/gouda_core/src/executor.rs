use std::sync::Arc;

use gouda_proto::chat::request_container::Content as RequestContent;
use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{RequestContainer, ResponseContainer};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::error::{RunnerError, RunnerResult};
use crate::output::OutputTask;
use crate::{Client, RequestContext, Result};

/// A task for the executor.
#[derive(Debug, PartialEq)]
pub enum ExecutorTask {
    /// Exits the executor, resulting in the `Executor::run` method being stopped.
    Exit,
    /// Executes some request send to this client.
    Request(Box<RequestContainer>),
    /// Execute some response to be send to the application.
    /// This calls the `on_response` event handler of the client and sends
    /// the response to the output processor afterwards.
    Response(Box<ResponseContainer>),
}

/// The executor is responsible for receiving decoded messages from the input and executing
/// the corresponding tasks using the client. The resulting data
/// is then send to the output processor.
pub struct Executor {
    /// The client to be used to execute incoming tasks.
    client: Arc<dyn Client>,
    /// Receiver for tasks to be executed.
    task_receiver: Receiver<ExecutorTask>,
    /// Sender for new executor tasks. Used to be cloned when creating
    /// new client contexts on new requests.
    task_sender: Sender<ExecutorTask>,
    /// Where to send the resulting output tasks.
    output_sender: Sender<OutputTask>,
}

impl Executor {
    pub fn new(
        client: Arc<dyn Client>,
        task_receiver: Receiver<ExecutorTask>,
        task_sender: Sender<ExecutorTask>,
        output_sender: Sender<OutputTask>,
    ) -> Self {
        Self {
            client,
            task_receiver,
            task_sender,
            output_sender,
        }
    }

    /// Spawns an asynchronous tokio task and starts the executor to wait for events to execute.
    /// This method is executed until an `ExecutorTask::Exit` is received.
    pub fn run(
        mut self,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<RunnerResult<Self>> {
        tokio::spawn(async move {
            log::debug!("Waiting for tasks...");

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("Executor was cancelled");
                        break;
                    }
                    task = self.task_receiver.recv() => {
                        let Some(task) = task else {
                            log::warn!("InputProcessor channel has been closed");
                            break;
                        };

                        log::debug!("Received task: {task:?}");

                        if matches!(task, ExecutorTask::Exit) {
                            log::info!("Exiting as an exit event was received");
                            break;
                        }

                        if let Err(err) = self.process_task(task).await {
                            log::error!("Error processing task: {err}");
                            return Err(err);
                        }
                    }
                }
            }

            Ok(self)
        })
    }

    async fn process_task(&mut self, task: ExecutorTask) -> RunnerResult<()> {
        match task {
            // ExecutorTask::Exit is handled by the `Self::run` method
            ExecutorTask::Exit => Ok(()),
            ExecutorTask::Request(container) => {
                let tag = container.tag;

                let Some(content) = container.content else {
                    return Err(RunnerError::InvalidData);
                };

                let processor = RequestProcessor::new(
                    self.client.clone(),
                    self.task_sender.clone(),
                    self.output_sender.clone(),
                );

                tokio::spawn(async move {
                    if let Err(err) = processor.exec_request(tag, content).await {
                        log::error!("Error processing request: {err}");
                    }
                });

                Ok(())
            }
            ExecutorTask::Response(container) => {
                send_response(self.client.clone(), &self.output_sender, *container).await
            }
        }
    }
}

struct RequestProcessor {
    client: Arc<dyn Client>,
    task_sender: Sender<ExecutorTask>,
    output_sender: Sender<OutputTask>,
}

impl RequestProcessor {
    pub fn new(
        client: Arc<dyn Client>,
        task_sender: Sender<ExecutorTask>,
        output_sender: Sender<OutputTask>,
    ) -> Self {
        Self {
            client,
            task_sender,
            output_sender,
        }
    }

    pub async fn exec_request(self, tag: u64, request: RequestContent) -> RunnerResult<()> {
        let ctx = RequestContext::new(tag, self.task_sender.clone());

        match request {
            RequestContent::InitializationRequest(request) => {
                let result = self.client.initialize(ctx, request).await;
                self.send_result(0, result.map(ResponseContent::StatusUpdate))
                    .await?;
            }
            RequestContent::LoginFlowsRequest(_) => {
                let result = self.client.get_login_flows(ctx).await;
                self.send_result(tag, result.map(ResponseContent::LoginFlowsResponse))
                    .await?;
            }
            RequestContent::IdentityProvidersRequest(_) => {
                let result = self.client.get_identity_providers(ctx).await;
                self.send_result(tag, result.map(ResponseContent::IdentityProvidersResponse))
                    .await?;
            }
            RequestContent::LoginUsernamePasswordRequest(request) => {
                let result = self.client.login_username_password(ctx, request).await;
                self.send_result(0, result.map(ResponseContent::StatusUpdate))
                    .await?;
            }
            RequestContent::LoginSsoRequest(request) => {
                let result = self.client.login_sso(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::LoginSsoResponse))
                    .await?;
            }
            RequestContent::RecoveryKeyVerificationRequest(request) => {
                let result = self.client.recovery_key_verification(ctx, request).await;
                self.send_result(0, result.map(ResponseContent::VerificationEndEvent))
                    .await?;
            }
            RequestContent::CrossSigningStartRequest(request) => {
                let result = self.client.cross_signing_start(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::CrossSigningStartResponse))
                    .await?;
            }
            RequestContent::CrossSigningMethodSelectedRequest(request) => {
                let result = self.client.cross_signing_select_method(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::CrossSigningConfirmRequest(request) => {
                let result = self.client.cross_signing_confirm(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::VerificationAbortRequest(request) => {
                let result = self.client.abort_verification(ctx, request).await;
                self.send_result(0, result.map(ResponseContent::VerificationEndEvent))
                    .await?;
            }
            RequestContent::GlobalSettingsRequest(request) => {
                let result = self.client.get_global_settings(ctx, request).await;
                self.send_result(0, result.map(ResponseContent::GlobalSettingsEvent))
                    .await?;
            }
            RequestContent::UserRequest(request) => {
                let result = self.client.get_user(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::UserResponse))
                    .await?;
            }
            RequestContent::UserSearchRequest(request) => {
                let result = self.client.search_users(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::UserSearchResponse))
                    .await?;
            }
            RequestContent::UserStatusSetOwnRequest(request) => {
                let result = self.client.set_status(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::PublicRoomListRequest(request) => {
                let result = self.client.get_public_rooms(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::PublicRoomListResponse))
                    .await?;
            }
            RequestContent::InvitationRequest(request) => {
                let result = self.client.invite(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomChangeEvent))
                    .await?;
            }
            RequestContent::InvitedReply(request) => {
                let result = self.client.invitation_reply(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::RoomListRequest(request) => {
                let result = self.client.get_rooms(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomListResponse))
                    .await?;
            }
            RequestContent::RoomCreateGroupRequest(request) => {
                let result = self.client.create_group_room(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomCreatedEvent))
                    .await?;
            }
            RequestContent::RoomCreateDirectRequest(request) => {
                let result = self.client.create_direct_room(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomCreatedEvent))
                    .await?;
            }
            RequestContent::RoomChangeRequest(request) => {
                let result = self.client.change_room(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomChangeEvent))
                    .await?;
            }
            RequestContent::RoomLeaveRequest(request) => {
                let result = self.client.leave_room(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomLeftEvent))
                    .await?;
            }
            RequestContent::RoomJoinRequest(request) => {
                let result = self.client.join_room(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomCreatedEvent))
                    .await?;
            }
            RequestContent::RoomKnockRequest(request) => {
                let result = self.client.knock_room(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::RoomMessagesRequest(request) => {
                let result = self.client.get_room_messages(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::RoomMarkAsReadRequest(request) => {
                let result = self.client.mark_as_read(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::RoomChangeEvent))
                    .await?;
            }
            RequestContent::RoomTypingRequest(request) => {
                let result = self.client.activate_typing_notice(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::MessageSendRequest(request) => {
                let result = self.client.send_message(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::MessageSendResponse))
                    .await?;
            }
            RequestContent::MessageRemoveRequest(request) => {
                let result = self.client.remove_message(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::MessageChangeRequest(request) => {
                let result = self.client.change_message(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::CreateReactionRequest(request) => {
                let result = self.client.create_reaction(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::RemoveReactionRequest(request) => {
                let result = self.client.remove_reaction(ctx, request).await;
                if let Err(err) = result {
                    self.send_result(tag, Err(err)).await?;
                }
            }
            RequestContent::MessageRequest(request) => {
                let result = self.client.get_message(ctx, request).await;
                self.send_result(tag, result.map(ResponseContent::MessageReceivedEvent))
                    .await?;
            }
        }

        Ok(())
    }

    async fn send_result(self, tag: u64, content: Result<ResponseContent>) -> RunnerResult<()> {
        let content = match content {
            Ok(c) => Some(c),
            Err(err) => Some(ResponseContent::Error(err)),
        };

        let container = ResponseContainer { tag, content };

        send_response(self.client, &self.output_sender, container).await
    }
}

async fn send_response(
    client: Arc<dyn Client>,
    sender: &Sender<OutputTask>,
    response: ResponseContainer,
) -> RunnerResult<()> {
    log::debug!("Preparing response: {response:?}");

    let response_handler = response.clone();

    tokio::spawn(async move {
        log::debug!("Calling response event handler on the client");
        client.on_response(response_handler).await;
    });

    log::debug!("Sending response to output processor");

    sender
        .send(OutputTask::Response(Box::new(response)))
        .await
        .map_err(|_| RunnerError::InternalChannelClosed)
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use gouda_proto::chat::error::ErrorType;
    use gouda_proto::chat::request_container::Content as RequestContent;
    use gouda_proto::chat::response_container::Content as ResponseContent;
    use gouda_proto::chat::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{Arc, Executor, ExecutorTask, OutputTask};
    use crate::test_utils::ClientMock;
    use crate::RunnerError;

    fn create_executor_task(tag: u64, content: RequestContent) -> ExecutorTask {
        ExecutorTask::Request(Box::new(RequestContainer {
            tag,
            content: Some(content),
        }))
    }

    fn create_output_task(tag: u64, content: ResponseContent) -> OutputTask {
        OutputTask::Response(Box::new(ResponseContainer {
            tag,
            content: Some(content),
        }))
    }

    #[tokio::test]
    async fn test_executor_run() {
        // Arrange
        let client = ClientMock::new();
        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());
        let response =
            ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse::default());

        // Act
        executor_tx
            .try_send(create_executor_task(12, request.clone()))
            .unwrap();

        executor_tx
            .try_send(create_executor_task(13, request))
            .unwrap();

        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(2);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(12, response.clone())
        );
        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(13, response)
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_executor_run_cancellation() {
        // Arrange
        let client = ClientMock::new();
        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        let token = CancellationToken::new();

        // Act
        token.cancel();

        executor.run(token.clone()).await.unwrap().unwrap();

        // Assert
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_executor_run_empty_request_content() {
        // Arrange
        let client = ClientMock::new();
        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, _) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        let request = RequestContainer {
            content: None,
            tag: 42,
        };

        // Act
        executor_tx
            .try_send(ExecutorTask::Request(Box::new(request)))
            .unwrap();

        let result = executor.run(CancellationToken::new()).await.unwrap();

        // Assert
        assert!(matches!(result, Err(RunnerError::InvalidData)));
    }

    #[tokio::test]
    async fn test_executor_client_on_response() {
        // Arrange
        let client = ClientMock::new();
        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());
        let response =
            ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse::default());
        let response_container = ResponseContainer {
            content: Some(response.clone()),
            tag: 12,
        };

        // Act
        executor_tx
            .try_send(create_executor_task(12, request.clone()))
            .unwrap();

        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(1);
        client.assert_received_response(response_container);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(12, response.clone())
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_executor_request_context() {
        // Arrange
        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());

        let client = ClientMock::default();

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(
                2,
                ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse::default())
            )
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_initialization_request() {
        // Arrange
        let request = RequestContent::InitializationRequest(InitializationRequest::default());
        let response = StatusUpdate {
            code: status_update::StatusCode::Connected as i32,
        };

        let client = ClientMock::new().initialize_response(Ok(response));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_initialize_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::StatusUpdate(response)),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::StatusUpdate(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_initialization_request_err() {
        // Arrange
        let request = RequestContent::InitializationRequest(InitializationRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().initialize_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_initialize_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_flows_request() {
        // Arrange
        let request = RequestContent::LoginFlowsRequest(LoginFlowsRequest::default());
        let response = LoginFlowsResponse {
            login_flows: vec![
                login_flows_response::LoginFlow::UsernamePassword as i32,
                login_flows_response::LoginFlow::Sso as i32,
            ],
        };

        let client = ClientMock::new().get_login_flows_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_login_flows_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::LoginFlowsResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::LoginFlowsResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_flows_request_err() {
        // Arrange
        let request = RequestContent::LoginFlowsRequest(LoginFlowsRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_login_flows_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_login_flows_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_identity_providers_request() {
        // Arrange
        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());
        let response = IdentityProvidersResponse {
            identity_providers: vec!["idp1.example.com".to_owned(), "idp2.example.com".to_owned()],
        };

        let client = ClientMock::new().get_identity_providers_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::IdentityProvidersResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::IdentityProvidersResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_identity_providers_request_err() {
        // Arrange
        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_identity_providers_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_username_password_request() {
        // Arrange
        let request =
            RequestContent::LoginUsernamePasswordRequest(LoginUsernamePasswordRequest::default());
        let response = StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        };

        let client = ClientMock::new().login_username_password_response(Ok(response));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_username_password_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::StatusUpdate(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_username_password_request_err() {
        // Arrange
        let request =
            RequestContent::LoginUsernamePasswordRequest(LoginUsernamePasswordRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().login_username_password_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_username_password_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_sso_request() {
        // Arrange
        let request = RequestContent::LoginSsoRequest(LoginSsoRequest::default());
        let response = LoginSsoResponse {
            login_url: "https://some.backend".to_owned(),
        };

        let client = ClientMock::new().login_sso_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_sso_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::LoginSsoResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::LoginSsoResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_sso_request_err() {
        // Arrange
        let request = RequestContent::LoginSsoRequest(LoginSsoRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().login_sso_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_sso_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_recovery_key_verification_request() {
        // Arrange
        let request = RequestContent::RecoveryKeyVerificationRequest(
            RecoveryKeyVerificationRequest::default(),
        );
        let response = VerificationEndEvent {
            verification_flow_id: None,
            result: Some(verification_end_event::Result::Successful(true)),
        };

        let client = ClientMock::new().recovery_key_verification_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_recovery_key_verification_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::VerificationEndEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::VerificationEndEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_recovery_key_verification_request_err() {
        // Arrange
        let request = RequestContent::RecoveryKeyVerificationRequest(
            RecoveryKeyVerificationRequest::default(),
        );
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().recovery_key_verification_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_recovery_key_verification_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_start_request() {
        // Arrange
        let request = RequestContent::CrossSigningStartRequest(CrossSigningStartRequest::default());
        let response = CrossSigningStartResponse {
            verification_flow_id: "flow-1".to_owned(),
        };

        let client = ClientMock::new().cross_signing_start_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_start_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::CrossSigningStartResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::CrossSigningStartResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_start_request_err() {
        // Arrange
        let request = RequestContent::CrossSigningStartRequest(CrossSigningStartRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().cross_signing_start_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_start_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_method_selected_request() {
        // Arrange
        let request = RequestContent::CrossSigningMethodSelectedRequest(
            CrossSigningMethodSelectedRequest::default(),
        );

        let client = ClientMock::new().cross_signing_select_method_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_select_method_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_method_selected_request_err() {
        // Arrange
        let request = RequestContent::CrossSigningMethodSelectedRequest(
            CrossSigningMethodSelectedRequest::default(),
        );
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().cross_signing_select_method_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_select_method_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_confirm_request() {
        // Arrange
        let request =
            RequestContent::CrossSigningConfirmRequest(CrossSigningConfirmRequest::default());

        let client = ClientMock::new().cross_signing_confirm_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_confirm_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_confirm_request_err() {
        // Arrange
        let request =
            RequestContent::CrossSigningConfirmRequest(CrossSigningConfirmRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().cross_signing_confirm_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_confirm_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_verification_abort_request() {
        // Arrange
        let request = RequestContent::VerificationAbortRequest(VerificationAbortRequest::default());
        let response = VerificationEndEvent {
            verification_flow_id: Some("some-flow-123".to_owned()),
            result: None,
        };

        let client = ClientMock::new().abort_verification_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_abort_verification_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::VerificationEndEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::VerificationEndEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_verification_abort_request_err() {
        // Arrange
        let request = RequestContent::VerificationAbortRequest(VerificationAbortRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().abort_verification_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_abort_verification_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_global_settings_request() {
        // Arrange
        let request = RequestContent::GlobalSettingsRequest(GlobalSettingsRequest::default());
        let response = GlobalSettings {
            notification_setting: NotificationSetting::AllMessages.into(),
        };

        let client = ClientMock::new().get_global_settings_response(Ok(response));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_global_settings_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::GlobalSettingsEvent(response)),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::GlobalSettingsEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_global_settings_request_err() {
        // Arrange
        let request = RequestContent::GlobalSettingsRequest(GlobalSettingsRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_global_settings_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_global_settings_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_request() {
        // Arrange
        let request = RequestContent::UserRequest(UserRequest::default());
        let response = User {
            user_id: "user_0".to_owned(),
            display_name: Some("Test User 1".to_owned()),
            status: None,
            avatar_path: None,
        };

        let client = ClientMock::new().get_user_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_user_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::UserResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::UserResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_request_err() {
        // Arrange
        let request = RequestContent::UserRequest(UserRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_user_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_user_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_search_request() {
        // Arrange
        let request = RequestContent::UserSearchRequest(UserSearchRequest::default());
        let response = UserSearchResponse {
            user_list: vec![
                User {
                    user_id: "user_0".to_owned(),
                    display_name: Some("Test User 1".to_owned()),
                    status: None,
                    avatar_path: None,
                },
                User {
                    user_id: "user_1".to_owned(),
                    display_name: Some("Test User 2".to_owned()),
                    status: None,
                    avatar_path: None,
                },
            ],
        };

        let client = ClientMock::new().search_users_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_search_users_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::UserSearchResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::UserSearchResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_search_request_err() {
        // Arrange
        let request = RequestContent::UserSearchRequest(UserSearchRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().search_users_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_search_users_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_status_set_own_request() {
        // Arrange
        let request = RequestContent::UserStatusSetOwnRequest(UserStatus::default());

        let client = ClientMock::new().set_status_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_set_status_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_status_set_own_request_err() {
        // Arrange
        let request = RequestContent::UserStatusSetOwnRequest(UserStatus::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().set_status_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_set_status_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_public_room_list_request() {
        // Arrange
        let request = RequestContent::PublicRoomListRequest(PublicRoomListRequest::default());
        let response = PublicRoomListResponse {
            room_list: vec![PublicRoom {
                room_id: "some-public-room-1".to_owned(),
                display_name: Some("Some public room!".to_owned()),
                num_joined_members: 20,
                topic: None,
                join_rule: RoomJoinRule::Invite.into(),
            }],
            next_batch: None,
        };

        let client = ClientMock::new().get_public_rooms_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_public_rooms_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::PublicRoomListResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::PublicRoomListResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_public_room_list_request_err() {
        // Arrange
        let request = RequestContent::PublicRoomListRequest(PublicRoomListRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_public_rooms_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_public_rooms_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_invitation_request() {
        // Arrange
        let request = RequestContent::InvitationRequest(InvitationRequest::default());
        let response = RoomChangeEvent {
            room_id: "new-room".to_owned(),
            has_typing_user_id_list_changed: true,
            has_user_id_list_changed: false,
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            typing_user_id_list: Vec::new(),
            display_name: None,
            unread_count: Some(0),
            join_rule: None,
            is_direct: None,
            permissions: None,
            avatar_path: None,
            is_favorite: None,
            room_settings: None,
        };

        let client = ClientMock::new().invite_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_invite_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomChangeEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomChangeEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_invitation_request_err() {
        // Arrange
        let request = RequestContent::InvitationRequest(InvitationRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().invite_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_invite_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_invite_reply() {
        // Arrange
        let request = RequestContent::InvitedReply(InvitedReply::default());

        let client = ClientMock::new().invitation_reply_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_invitation_reply_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_invite_reply_err() {
        // Arrange
        let request = RequestContent::InvitedReply(InvitedReply::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().invitation_reply_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_invitation_reply_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_room_list_request() {
        // Arrange
        let request = RequestContent::RoomListRequest(RoomListRequest::default());
        let response = RoomListResponse {
            room_list: vec![
                Room {
                    room_id: "room-1".to_owned(),
                    display_name: Some("Test Room 1".to_owned()),
                    user_id_list: HashMap::from([
                        ("user-1".to_owned(), UserRoomState::Joined as i32),
                        ("user-2".to_owned(), UserRoomState::Knocked as i32),
                        ("user-3".to_owned(), UserRoomState::Banned as i32),
                    ]),
                    space_id: Vec::new(),
                    unread_count: 0,
                    is_direct: false,
                    join_rule: RoomJoinRule::Invite.into(),
                    permissions: None,
                    latest_message_timestamp: None,
                    avatar_path: None,
                    is_favorite: false,
                    room_settings: None,
                },
                Room {
                    room_id: "room-2".to_owned(),
                    display_name: Some("Test Room 2".to_owned()),
                    user_id_list: HashMap::from([
                        ("user-1".to_owned(), UserRoomState::Joined as i32),
                        ("user-4".to_owned(), UserRoomState::Joined as i32),
                    ]),
                    space_id: Vec::new(),
                    unread_count: 0,
                    is_direct: false,
                    join_rule: RoomJoinRule::Invite.into(),
                    permissions: None,
                    latest_message_timestamp: None,
                    avatar_path: None,
                    is_favorite: false,
                    room_settings: None,
                },
            ],
        };

        let client = ClientMock::new().get_rooms_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_rooms_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomListResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomListResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_list_request_err() {
        // Arrange
        let request = RequestContent::RoomListRequest(RoomListRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_rooms_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_rooms_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_create_group_request() {
        // Arrange
        let request = RequestContent::RoomCreateGroupRequest(RoomCreateGroupRequest::default());
        let response = Room {
            room_id: "new-room".to_owned(),
            display_name: Some("Test Room".to_owned()),
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            space_id: Vec::new(),
            unread_count: 0,
            is_direct: false,
            join_rule: RoomJoinRule::Invite.into(),
            permissions: None,
            latest_message_timestamp: None,
            avatar_path: None,
            is_favorite: false,
            room_settings: None,
        };

        let client = ClientMock::new().create_group_room_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_group_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomCreatedEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomCreatedEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_create_group_request_err() {
        // Arrange
        let request = RequestContent::RoomCreateGroupRequest(RoomCreateGroupRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().create_group_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_group_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_create_direct_request() {
        // Arrange
        let request = RequestContent::RoomCreateDirectRequest(RoomCreateDirectRequest::default());
        let response = Room {
            room_id: "new-room".to_owned(),
            display_name: Some("Test Room".to_owned()),
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            space_id: Vec::new(),
            unread_count: 0,
            is_direct: false,
            join_rule: RoomJoinRule::Invite.into(),
            permissions: None,
            latest_message_timestamp: None,
            avatar_path: None,
            is_favorite: false,
            room_settings: None,
        };

        let client = ClientMock::new().create_direct_room_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_direct_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomCreatedEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomCreatedEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_create_direct_request_err() {
        // Arrange
        let request = RequestContent::RoomCreateDirectRequest(RoomCreateDirectRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().create_direct_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_direct_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_change_request() {
        // Arrange
        let request = RequestContent::RoomChangeRequest(RoomChangeRequest::default());
        let response = RoomChangeEvent {
            room_id: "new-room".to_owned(),
            has_typing_user_id_list_changed: true,
            has_user_id_list_changed: false,
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            typing_user_id_list: Vec::new(),
            display_name: None,
            unread_count: Some(0),
            join_rule: None,
            is_direct: None,
            permissions: None,
            avatar_path: None,
            is_favorite: None,
            room_settings: None,
        };

        let client = ClientMock::new().change_room_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_change_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomChangeEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomChangeEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_change_request_err() {
        // Arrange
        let request = RequestContent::RoomChangeRequest(RoomChangeRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().change_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_change_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_room_leave_request() {
        // Arrange
        let request = RequestContent::RoomLeaveRequest(RoomLeaveRequest::default());
        let response = RoomLeftEvent {
            room_id: "some-room".to_owned(),
            reason: room_left_event::RoomLeaveReason::User.into(),
            message: None,
        };

        let client = ClientMock::new().leave_room_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_leave_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomLeftEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomLeftEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_leave_request_err() {
        // Arrange
        let request = RequestContent::RoomLeaveRequest(RoomLeaveRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().leave_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_leave_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_room_join_request() {
        // Arrange
        let request = RequestContent::RoomJoinRequest(RoomJoinRequest::default());
        let response = Room {
            room_id: "new-room".to_owned(),
            display_name: Some("Test Room".to_owned()),
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            space_id: Vec::new(),
            unread_count: 0,
            is_direct: false,
            join_rule: RoomJoinRule::Invite.into(),
            permissions: None,
            latest_message_timestamp: None,
            avatar_path: None,
            is_favorite: false,
            room_settings: None,
        };

        let client = ClientMock::new().join_room_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_join_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomCreatedEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomCreatedEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_join_request_err() {
        // Arrange
        let request = RequestContent::RoomJoinRequest(RoomJoinRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().join_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_join_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_room_knock_request() {
        // Arrange
        let request = RequestContent::RoomKnockRequest(RoomKnockRequest::default());

        let client = ClientMock::new().knock_room_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_knock_room_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_knock_request_err() {
        // Arrange
        let request = RequestContent::RoomKnockRequest(RoomKnockRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().knock_room_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_knock_room_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_room_messages_request() {
        // Arrange
        let request = RequestContent::RoomMessagesRequest(RoomMessagesRequest::default());

        let client = ClientMock::new().get_room_messages_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_room_messages_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_messages_request_err() {
        // Arrange
        let request = RequestContent::RoomMessagesRequest(RoomMessagesRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_room_messages_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_room_messages_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_mark_as_read_request() {
        // Arrange
        let request = RequestContent::RoomMarkAsReadRequest(RoomMarkAsReadRequest::default());
        let response = RoomChangeEvent {
            room_id: "new-room".to_owned(),
            has_typing_user_id_list_changed: true,
            has_user_id_list_changed: false,
            user_id_list: HashMap::from([
                ("user-1".to_owned(), UserRoomState::Joined as i32),
                ("user-4".to_owned(), UserRoomState::Joined as i32),
            ]),
            typing_user_id_list: Vec::new(),
            display_name: None,
            unread_count: Some(0),
            join_rule: None,
            is_direct: None,
            permissions: None,
            avatar_path: None,
            is_favorite: None,
            room_settings: None,
        };

        let client = ClientMock::new().mark_as_read_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_mark_as_read_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::RoomChangeEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomChangeEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_mark_as_read_request_err() {
        // Arrange
        let request = RequestContent::RoomMarkAsReadRequest(RoomMarkAsReadRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().mark_as_read_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_mark_as_read_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_typing_request() {
        // Arrange
        let request = RequestContent::RoomTypingRequest(RoomTypingRequest::default());

        let client = ClientMock::new().activate_typing_notice_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_activate_typing_notice_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_typing_request_err() {
        // Arrange
        let request = RequestContent::RoomTypingRequest(RoomTypingRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().activate_typing_notice_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_activate_typing_notice_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_send_request() {
        // Arrange
        let request = RequestContent::MessageSendRequest(MessageSendRequest::default());
        let response = MessageSendResponse {
            message_id: "some-message-123".to_owned(),
        };

        let client = ClientMock::new().send_message_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_send_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::MessageSendResponse(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::MessageSendResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_send_request_err() {
        // Arrange
        let request = RequestContent::MessageSendRequest(MessageSendRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().send_message_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_send_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_remove_request() {
        // Arrange
        let request = RequestContent::MessageRemoveRequest(MessageRemoveRequest::default());

        let client = ClientMock::new().remove_message_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_remove_message_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_remove_request_err() {
        // Arrange
        let request = RequestContent::MessageRemoveRequest(MessageRemoveRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().remove_message_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_remove_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_message_change_request() {
        // Arrange
        let request = RequestContent::MessageChangeRequest(MessageChangeRequest::default());

        let client = ClientMock::new().change_message_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_change_message_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_change_request_err() {
        // Arrange
        let request = RequestContent::MessageChangeRequest(MessageChangeRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().change_message_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_change_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_create_reation_request() {
        // Arrange
        let request = RequestContent::CreateReactionRequest(Reaction::default());

        let client = ClientMock::new().create_reaction_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_reaction_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_create_reaction_request_err() {
        // Arrange
        let request = RequestContent::CreateReactionRequest(Reaction::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().create_reaction_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_create_reaction_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_remove_reation_request() {
        // Arrange
        let request = RequestContent::RemoveReactionRequest(Reaction::default());

        let client = ClientMock::new().remove_reaction_response(Ok(()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_remove_reaction_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_remove_reaction_request_err() {
        // Arrange
        let request = RequestContent::RemoveReactionRequest(Reaction::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().remove_reaction_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_remove_reaction_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty());
    }

    #[tokio::test]
    async fn test_message_request() {
        // Arrange
        let request = RequestContent::MessageRequest(MessageRequest::default());
        let response = Message {
            message_id: "some-message-123".to_owned(),
            ..Default::default()
        };

        let client = ClientMock::new().get_message_response(Ok(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::MessageReceivedEvent(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::MessageReceivedEvent(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_message_request_err() {
        // Arrange
        let request = RequestContent::MessageRequest(MessageRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock::new().get_message_response(Err(response.clone()));

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);

        let executor = Executor::new(
            Arc::new(client),
            executor_rx,
            executor_tx.clone(),
            output_tx,
        );

        // Act
        executor_tx
            .try_send(create_executor_task(2, request))
            .unwrap();
        executor_tx.try_send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_message_called_n(1);
        client.assert_received_response(ResponseContainer {
            tag: 2,
            content: Some(ResponseContent::Error(response.clone())),
        });

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }
}
