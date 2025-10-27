use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use mrhc_proto::chat::from_client_container::Content as FromClientContent;
use mrhc_proto::chat::to_client_container::Content as ToClientContent;
use mrhc_proto::chat::{FromClientContainer, ToClientContainer};

use crate::output_processor::OutputTask;
use crate::Client;

#[derive(Debug)]
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
    /// This method is executed until the program ends.
    pub fn run(mut self) -> tokio::task::JoinHandle<()> {
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
                self.send_response(tag, FromClientContent::CapabilityResponse(capabilities));
            }
            _ => todo!(),
        }
        todo!();
    }

    fn send_response(&self, tag: u64, content: FromClientContent) {
        let container = FromClientContainer {
            tag,
            content: Some(content),
        };

        self.output_sender
            .send(OutputTask::ProtoMessage(Box::new(container)))
            .expect("Error sending message to output processor");
    }
}
