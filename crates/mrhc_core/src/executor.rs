use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{RequestContainer, ResponseContainer};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::output_processor::OutputTask;
use crate::{Client, ClientContext, Result};

#[derive(Debug, PartialEq)]
pub enum ExecutorTask {
    /// Exits the executor, resulting in the `Executor::run` method being stopped.
    Exit,
    /// Executes some request send to this client.
    Request(Box<RequestContainer>),
}

/// The executor is responsible for receiving decoded messages from the input and executing the corresponding tasks
/// using the client. The resulting data is then send to the output processor.
pub struct Executor {
    /// The client to be used to execute incoming tasks.
    client: Box<dyn Client>,
    /// Receiver for tasks to be executed.
    task_receiver: UnboundedReceiver<ExecutorTask>,
    /// Where to send the resulting output tasks.
    output_sender: UnboundedSender<OutputTask>,
}

impl Executor {
    pub fn new(
        client: Box<dyn Client>,
        task_receiver: UnboundedReceiver<ExecutorTask>,
        output_sender: UnboundedSender<OutputTask>,
    ) -> Self {
        Self {
            client,
            task_receiver,
            output_sender,
        }
    }

    /// Spawns an asynchronous tokio task and starts the executor to wait for events to execute.
    /// This method is executed until an `ExecutorTask::Exit` is received.
    pub fn run(mut self) -> tokio::task::JoinHandle<Self> {
        tokio::spawn(async move {
            log::debug!("Waiting for tasks...");

            while let Some(task) = self.task_receiver.recv().await {
                log::debug!("Received task: {task:?}");

                if matches!(task, ExecutorTask::Exit) {
                    log::info!("Exiting as an exit event was received");
                    break;
                }

                self.process_task(task).await;
            }

            self
        })
    }

    async fn process_task(&mut self, task: ExecutorTask) {
        match task {
            // ExecutorTask::Exit is handled by the `Self::run` method
            ExecutorTask::Exit => (),
            ExecutorTask::Request(container) => {
                let content = container
                    .content
                    .expect("Received client container without content");

                self.process_request(container.tag, content).await;
            }
        }
    }

    async fn process_request(&mut self, tag: u64, content: RequestContent) {
        let ctx = ClientContext::new(self.output_sender.clone());

        match content {
            RequestContent::InitializationRequest(request) => {
                let result = self.client.initialize(ctx, request).await;
                self.send_response(0, result.map(ResponseContent::StatusUpdate));
            }
            RequestContent::LoginFlowsRequest(_) => {
                let result = self.client.get_login_flows(ctx).await;
                self.send_response(tag, result.map(ResponseContent::LoginFlowsResponse));
            }
            RequestContent::UsernamePasswordLoginRequest(request) => {
                let result = self.client.login_username_password(ctx, request).await;
                self.send_response(0, result.map(ResponseContent::StatusUpdate));
            }
            RequestContent::SsoLoginRequest(request) => {
                let result = self.client.login_sso(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::SsoLoginResponse));
            }
            RequestContent::IdentityProvidersRequest(_) => {
                let result = self.client.get_identity_providers(ctx).await;
                self.send_response(tag, result.map(ResponseContent::IdentityProvidersResponse));
            }
            RequestContent::RoomListRequest(request) => {
                let result = self.client.get_rooms(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::RoomListResponse));
            }
            RequestContent::SendMessageRequest(request) => {
                let result = self.client.send_message(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::SendMessageResponse));
            }
            RequestContent::UserListRequest(_) => {
                let result = self.client.get_users(ctx).await;
                self.send_response(tag, result.map(ResponseContent::UserListResponse));
            }
            RequestContent::UserSearchRequest(request) => {
                let result = self.client.search_users(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::UserSearchResponse));
            }
            RequestContent::RecoveryKeyVerificationRequest(request) => {
                let result = self.client.recovery_key_verification(ctx, request).await;
                self.send_response(0, result.map(ResponseContent::VerificationEndEvent));
            }
            RequestContent::CrossSigningStartRequest(request) => {
                let result = self.client.cross_signing_start(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::CrossSigningStartResponse));
            }
            RequestContent::CrossSigningMethodSelectedRequest(request) => {
                let result = self.client.cross_signing_select_method(ctx, request).await;
                if let Err(err) = result {
                    self.send_response(tag, Err(err));
                }
            }
            RequestContent::CrossSigningAcceptRequest(request) => {
                let result = self.client.cross_signing_confirm(ctx, request).await;
                if let Err(err) = result {
                    self.send_response(tag, Err(err));
                }
            }
            RequestContent::VerificationAbortRequest(request) => {
                let result = self.client.abort_verification(ctx, request).await;
                self.send_response(0, result.map(ResponseContent::VerificationEndEvent));
            }
            _ => todo!("Request: {content:?} is currently not implemented"),
        }
    }

    fn send_response(&self, tag: u64, content: Result<ResponseContent>) {
        let content = match content {
            Ok(c) => Some(c),
            Err(err) => Some(ResponseContent::Error(err)),
        };

        let container = ResponseContainer { tag, content };

        self.output_sender
            .send(OutputTask::Response(Box::new(container)))
            .expect("Error sending message to output processor");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mrhc_proto::chat::error::ErrorType;
    use mrhc_proto::chat::request_container::Content as RequestContent;
    use mrhc_proto::chat::response_container::Content as ResponseContent;
    use mrhc_proto::chat::*;
    use tokio::sync::mpsc;

    use super::*;
    use crate::test_utils::ClientMock;

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
        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());
        let response =
            ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse::default());

        // Act
        executor_tx
            .send(create_executor_task(12, request.clone()))
            .unwrap();

        executor_tx.send(create_executor_task(13, request)).unwrap();

        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

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
    async fn test_client_context() {
        // Arrange
        let request = RequestContent::IdentityProvidersRequest(IdentityProvidersRequest::default());

        let client = ClientMock::default();

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        let mut ctx = client.received_ctx.clone().unwrap();

        // Verify that the client context has received the correct output sender.
        // Since we cannot directly compare the receivers of the senders, we send an event and expect it to
        // be received by the correct output receiver.
        ctx.send_error(Error::default());

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(
                2,
                ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse::default())
            )
        );
        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(Error::default()))
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

        let client = ClientMock {
            initialize_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_initialize_called_n(1);

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

        let client = ClientMock {
            initialize_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_initialize_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_flow_request() {
        // Arrange
        let request = RequestContent::LoginFlowsRequest(LoginFlowsRequest::default());
        let response = LoginFlowsResponse {
            login_flows: vec![
                login_flows_response::LoginFlow::UsernamePassword as i32,
                login_flows_response::LoginFlow::Sso as i32,
            ],
        };

        let client = ClientMock {
            get_login_flows_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_login_flows_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::LoginFlowsResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_login_flow_request_err() {
        // Arrange
        let request = RequestContent::LoginFlowsRequest(LoginFlowsRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            get_login_flows_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_login_flows_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_username_password_login_request() {
        // Arrange
        let request =
            RequestContent::UsernamePasswordLoginRequest(UsernamePasswordLoginRequest::default());
        let response = StatusUpdate {
            code: status_update::StatusCode::LoggedIn as i32,
        };

        let client = ClientMock {
            login_username_password_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

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
    async fn test_username_password_login_request_err() {
        // Arrange
        let request =
            RequestContent::UsernamePasswordLoginRequest(UsernamePasswordLoginRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            login_username_password_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

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
    async fn test_sso_login_request() {
        // Arrange
        let request = RequestContent::SsoLoginRequest(SsoLoginRequest::default());
        let response = SsoLoginResponse {
            login_url: "https://some.backend".to_owned(),
        };

        let client = ClientMock {
            login_sso_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_sso_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::SsoLoginResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_sso_login_request_err() {
        // Arrange
        let request = RequestContent::SsoLoginRequest(SsoLoginRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            login_sso_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_login_sso_called_n(1);

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

        let client = ClientMock {
            get_identity_providers_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(1);

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

        let client = ClientMock {
            get_identity_providers_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_identity_providers_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
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
                    is_public: false,
                    unread_count: 0,
                },
                Room {
                    room_id: "room-2".to_owned(),
                    display_name: Some("Test Room 2".to_owned()),
                    user_id_list: HashMap::from([
                        ("user-1".to_owned(), UserRoomState::Joined as i32),
                        ("user-4".to_owned(), UserRoomState::Joined as i32),
                    ]),
                    space_id: Vec::new(),
                    is_public: true,
                    unread_count: 0,
                },
            ],
        };

        let client = ClientMock {
            get_rooms_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_rooms_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::RoomListResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_room_list_request_error() {
        // Arrange
        let request = RequestContent::RoomListRequest(RoomListRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            get_rooms_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_rooms_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_send_message_request() {
        // Arrange
        let request = RequestContent::SendMessageRequest(SendMessageRequest::default());
        let response = SendMessageResponse {
            message_id: "some-message-123".to_owned(),
        };

        let client = ClientMock {
            send_message_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_send_message_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::SendMessageResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_send_message_request_err() {
        // Arrange
        let request = RequestContent::SendMessageRequest(SendMessageRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            send_message_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_send_message_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_list_request() {
        // Arrange
        let request = RequestContent::UserListRequest(UserListRequest::default());
        let response = UserListResponse {
            user_list: vec![
                User {
                    user_id: "user_0".to_owned(),
                    display_name: Some("Test User 1".to_owned()),
                    presence_state: None,
                },
                User {
                    user_id: "user_1".to_owned(),
                    display_name: Some("Test User 2".to_owned()),
                    presence_state: None,
                },
            ],
        };

        let client = ClientMock {
            get_users_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_users_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::UserListResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_user_list_request_err() {
        // Arrange
        let request = RequestContent::UserListRequest(UserListRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            get_users_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_users_called_n(1);

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
                    presence_state: None,
                },
                User {
                    user_id: "user_1".to_owned(),
                    display_name: Some("Test User 2".to_owned()),
                    presence_state: None,
                },
            ],
        };

        let client = ClientMock {
            search_users_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_search_users_called_n(1);

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

        let client = ClientMock {
            search_users_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_search_users_called_n(1);

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

        let client = ClientMock {
            recovery_key_verification_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_recovery_key_verification_called_n(1);

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

        let client = ClientMock {
            recovery_key_verification_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_recovery_key_verification_called_n(1);

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

        let client = ClientMock {
            cross_signing_start_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_start_called_n(1);

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

        let client = ClientMock {
            cross_signing_start_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_start_called_n(1);

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

        let client = ClientMock {
            cross_signing_select_method_response: Ok(()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

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

        let client = ClientMock {
            cross_signing_select_method_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_select_method_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_accept_request() {
        // Arrange
        let request =
            RequestContent::CrossSigningAcceptRequest(CrossSigningAcceptRequest::default());

        let client = ClientMock {
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_confirm_called_n(1);

        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_cross_signing_accept_request_err() {
        // Arrange
        let request =
            RequestContent::CrossSigningAcceptRequest(CrossSigningAcceptRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            cross_signing_confirm_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_cross_signing_confirm_called_n(1);

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

        let client = ClientMock {
            abort_verification_response: Ok(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_abort_verification_called_n(1);

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

        let client = ClientMock {
            abort_verification_response: Err(response.clone()),
            ..Default::default()
        };

        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        let executor = Executor::new(Box::new(client), executor_rx, output_tx);

        // Act
        executor_tx.send(create_executor_task(2, request)).unwrap();
        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_abort_verification_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(response))
        );
        assert!(output_rx.is_empty())
    }
}
