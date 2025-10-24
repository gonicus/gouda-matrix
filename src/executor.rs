use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::messages::{GreetRequest, GreetResponse};
use crate::output_processor::OutputTask;

#[derive(Debug)]
pub enum ExecutorTask {
    GreetRequest(Box<GreetRequest>),
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
            ExecutorTask::GreetRequest(request) => {
                let response = GreetResponse {
                    result: request.x as i64 + request.y as i64,
                    greeting: format!("Hallo, {} {}", request.prename, request.surname),
                };
                self.output_sender
                    .send(OutputTask::GreetResponse(Box::new(response)))
                    .expect("error sending output event");
            }
        }
    }
}
