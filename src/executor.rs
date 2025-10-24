use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::messages::{GreetRequest, GreetResponse};
use crate::output_processor::OutputTask;

#[derive(Debug)]
pub enum ExecutorTask {
    GreetRequest(Box<GreetRequest>),
}

pub struct Executor {
    task_receiver: UnboundedReceiver<ExecutorTask>,
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
