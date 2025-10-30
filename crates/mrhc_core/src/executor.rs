use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{RequestContainer, ResponseContainer};

use crate::output_processor::OutputTask;
use crate::Result;
use crate::{Client, ClientContext};

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
            RequestContent::CapabilityRequest(_) => {
                let result = self.client.get_capabilities(ctx).await;
                self.send_response(tag, result.map(ResponseContent::CapabilityResponse));
            }
            RequestContent::InitializationRequest(request) => {
                let result = self.client.initialize(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::StatusUpdate));
            }
            RequestContent::LoginFlowsRequest(_) => {
                let result = self.client.get_login_flows(ctx).await;
                self.send_response(tag, result.map(ResponseContent::LoginFlowsResponse));
            }
            RequestContent::UsernamePasswordLoginRequest(request) => {
                let result = self.client.login_username_password(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::StatusUpdate));
            }
            RequestContent::SsoLoginRequest(request) => {
                let result = self.client.login_sso(ctx, request).await;
                self.send_response(tag, result.map(ResponseContent::SsoLoginResponse));
            }
            RequestContent::IdentityProvidersRequest(_) => {
                let result = self.client.get_identity_providers(ctx).await;
                self.send_response(tag, result.map(ResponseContent::IdentityProvidersResponse));
            }
            _ => todo!(),
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
    use tokio::sync::mpsc;

    use mrhc_proto::chat::error::ErrorType;
    use mrhc_proto::chat::request_container::Content as RequestContent;
    use mrhc_proto::chat::response_container::Content as ResponseContent;
    use mrhc_proto::chat::*;

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

        let request = RequestContent::CapabilityRequest(CapabilityRequest::default());
        let response = ResponseContent::CapabilityResponse(CapabilityResponse::default());

        // Act
        executor_tx
            .send(create_executor_task(12, request.clone()))
            .unwrap();

        executor_tx.send(create_executor_task(13, request)).unwrap();

        executor_tx.send(ExecutorTask::Exit).unwrap();

        let Executor { client, .. } = executor.run().await.unwrap();

        // Assert
        let client = client.as_any().downcast_ref::<ClientMock>().unwrap();
        client.assert_get_capabilities_called_n(2);

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
        let request = RequestContent::CapabilityRequest(CapabilityRequest::default());

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
        ctx.send_event(ResponseContent::Error(Error::default()));

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(
                2,
                ResponseContent::CapabilityResponse(CapabilityResponse::default())
            )
        );
        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(0, ResponseContent::Error(Error::default()))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_capability_request() {
        // Arrange
        let request = RequestContent::CapabilityRequest(CapabilityRequest::default());
        let response = CapabilityResponse {
            direct_rooms: true,
            group_rooms: false,
            ..Default::default()
        };

        let client = ClientMock {
            get_capabilities_response: Ok(response.clone()),
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
        client.assert_get_capabilities_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::CapabilityResponse(response))
        );
        assert!(output_rx.is_empty())
    }

    #[tokio::test]
    async fn test_capability_request_err() {
        // Arrange
        let request = RequestContent::CapabilityRequest(CapabilityRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            get_capabilities_response: Err(response.clone()),
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
        client.assert_get_capabilities_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, ResponseContent::Error(response))
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
            create_output_task(2, ResponseContent::StatusUpdate(response))
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
            create_output_task(2, ResponseContent::Error(response))
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
            create_output_task(2, ResponseContent::StatusUpdate(response))
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
            create_output_task(2, ResponseContent::Error(response))
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
}
