use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use mrhc_proto::chat::to_client_container::Content as ToClientContent;
use mrhc_proto::chat::ToClientContainer;

use crate::output_processor::OutputTask;

#[derive(Debug)]
pub enum ExecutorTask {
    ToClientContainer(Box<ToClientContainer>),
}

/// The executor is responsible for receiving decoded messages from the input and executing the corresponding tasks
/// using the client. The resulting data is then send to the output processor.
pub struct Executor {
    /// Receiver for tasks to be executed.
    task_receiver: UnboundedReceiver<ExecutorTask>,
    /// Where to send the resulting output tasks.
    output_sender: UnboundedSender<OutputTask>,
}

impl Executor {
    pub fn new(
        task_receiver: UnboundedReceiver<ExecutorTask>,
        output_sender: UnboundedSender<OutputTask>,
    ) -> Self {
        Self {
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

                self.process_task(task).await;
            }
        })
    }

    async fn process_task(&mut self, task: ExecutorTask) {
        match task {
            ExecutorTask::ToClientContainer(container) => {
                let content = container
                    .content
                    .expect("Received client container without content");

                self.process_request(container.tag, content).await;
            }
        }
    }

    async fn process_request(&mut self, tag: u64, content: ToClientContent) {
        todo!();
    }
}
