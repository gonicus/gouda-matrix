use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use mrhc_proto::chat::from_client_container::Content as FromClientContent;
use mrhc_proto::chat::to_client_container::Content as ToClientContent;
use mrhc_proto::chat::{FromClientContainer, ToClientContainer};

use crate::output_processor::OutputTask;
use crate::Client;
use crate::Result;

#[derive(Debug, PartialEq)]
pub enum ExecutorTask {
    /// Exits the executor, resulting in the `Executor::run` method being stopped.
    Exit,
    /// Executes some request send to this client.
    ToClientContainer(Box<ToClientContainer>),
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
            ExecutorTask::ToClientContainer(container) => {
                let content = container
                    .content
                    .expect("Received client container without content");

                self.process_request(container.tag, content).await;
            }
        }
    }

    async fn process_request(&mut self, tag: u64, content: ToClientContent) {
        match content {
            ToClientContent::CapabilityRequest(_) => {
                let capabilities = self.client.get_capabilities().await;
                self.send_response(tag, Ok(FromClientContent::CapabilityResponse(capabilities)));
            }
            ToClientContent::LoginRequest(request) => {
                let result = self.client.login_request(request).await;
                self.send_response(tag, result.map(FromClientContent::LoginResponse));
            }
            _ => todo!(),
        }
    }

    fn send_response(&self, tag: u64, content: Result<FromClientContent>) {
        let content = match content {
            Ok(c) => Some(c),
            Err(err) => Some(FromClientContent::Error(err)),
        };

        let container = FromClientContainer { tag, content };

        self.output_sender
            .send(OutputTask::ProtoMessage(Box::new(container)))
            .expect("Error sending message to output processor");
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use mrhc_proto::chat::error::ErrorType;
    use mrhc_proto::chat::from_client_container::Content as FromClientContent;
    use mrhc_proto::chat::to_client_container::Content as ToClientContent;
    use mrhc_proto::chat::{
        CapabilityRequest, CapabilityResponse, Error, LoginRequest, LoginResponse,
    };

    use super::*;
    use crate::test_utils::ClientMock;

    fn create_executor_task(tag: u64, content: ToClientContent) -> ExecutorTask {
        ExecutorTask::ToClientContainer(Box::new(ToClientContainer {
            tag,
            content: Some(content),
        }))
    }

    fn create_output_task(tag: u64, content: FromClientContent) -> OutputTask {
        OutputTask::ProtoMessage(Box::new(FromClientContainer {
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

        let request = ToClientContent::CapabilityRequest(CapabilityRequest::default());
        let response = FromClientContent::CapabilityResponse(CapabilityResponse::default());

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
    }

    #[tokio::test]
    async fn test_capability_request() {
        // Arrange
        let request = ToClientContent::CapabilityRequest(CapabilityRequest::default());
        let response = CapabilityResponse {
            direct_rooms: true,
            group_rooms: false,
            ..Default::default()
        };

        let client = ClientMock {
            get_capabilities_response: response.clone(),
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
            create_output_task(2, FromClientContent::CapabilityResponse(response))
        );
    }

    #[tokio::test]
    async fn test_login_request() {
        // Arrange
        let request = ToClientContent::LoginRequest(LoginRequest::default());
        let response = LoginResponse {
            login_url: "https://example.org/test/login/url".to_owned(),
        };

        let client = ClientMock {
            login_request_response: Ok(response.clone()),
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
        client.assert_login_request_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, FromClientContent::LoginResponse(response))
        );
    }

    #[tokio::test]
    async fn test_login_request_err() {
        // Arrange
        let request = ToClientContent::LoginRequest(LoginRequest::default());
        let response = Error {
            r#type: ErrorType::Unknown as i32,
            error_string: Some("Test error".to_owned()),
        };

        let client = ClientMock {
            login_request_response: Err(response.clone()),
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
        client.assert_login_request_called_n(1);

        assert_eq!(
            output_rx.recv().await.unwrap(),
            create_output_task(2, FromClientContent::Error(response))
        );
    }
}
