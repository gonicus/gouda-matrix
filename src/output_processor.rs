use prost::Message;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::messages::GreetResponse;

type Writer = dyn AsyncWrite + Send + Unpin;

#[derive(Debug)]
pub enum OutputTask {
    GreetResponse(Box<GreetResponse>),
}

pub struct OutputProcessor {
    writer: BufWriter<Box<Writer>>,
    task_receiver: UnboundedReceiver<OutputTask>,
}

impl OutputProcessor {
    pub fn new(writer: Box<Writer>, task_receiver: UnboundedReceiver<OutputTask>) -> Self {
        Self {
            writer: BufWriter::new(writer),
            task_receiver,
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

    async fn process_task(&mut self, task: OutputTask) {
        match task {
            OutputTask::GreetResponse(response) => {
                log::debug!("Writing greet response...");

                let serialized = response.encode_to_vec();
                let size = serialized.len().to_le_bytes().to_vec();

                log::debug!("Writing size: {}", serialized.len());

                self.writer
                    .write_all(&size)
                    .await
                    .expect("error writing size");

                log::debug!("Writing response: {serialized:?}");

                self.writer
                    .write_all(&serialized)
                    .await
                    .expect("error writing response");

                log::debug!("Flushing writer");

                self.writer.flush().await.expect("error flushing writer");

                log::debug!("Finished writing response");
            }
        }
    }
}
