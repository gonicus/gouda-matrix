use std::fmt::Debug;

use prost::Message;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::mpsc::UnboundedReceiver;

use mrhc_proto::chat::ResponseContainer;

pub type Writer = dyn AsyncWrite + Send + Unpin;

#[derive(Debug, PartialEq)]
pub enum OutputTask {
    /// Exits the output processor, resulting in the `OutputProcessor::run` method being stopped.
    Exit,
    /// Sends some response or event to the receiving half.
    Response(Box<ResponseContainer>),
}

/// The OutputProcessor is responsible to write data synchronously to the specified output.
/// This prevents multiple processes from writing data at the same time.
/// The output can be any object that implements the `AsyncWrite` trait, as well as `Send` and `Unpin`.
/// This is typically a socket or network stream.
pub struct OutputProcessor {
    /// Where to write the resulting data.
    writer: BufWriter<Box<Writer>>,
    /// Receiver of tasks that should be executed.
    task_receiver: UnboundedReceiver<OutputTask>,
}

impl OutputProcessor {
    pub fn new(writer: Box<Writer>, task_receiver: UnboundedReceiver<OutputTask>) -> Self {
        Self {
            writer: BufWriter::new(writer),
            task_receiver,
        }
    }

    /// Spawns an asynchronous Tokio task and starts the output processor to wait for tasks and write
    /// its data to the `self.writer`.
    /// This method is executed until an `OutputTask::Exit` is received.
    pub fn run(mut self) -> tokio::task::JoinHandle<Self> {
        tokio::spawn(async move {
            log::debug!("Waiting for tasks...");

            while let Some(task) = self.task_receiver.recv().await {
                log::debug!("Received task: {task:?}");

                if matches!(task, OutputTask::Exit) {
                    log::info!("Exiting as an exit event was received");
                    break;
                }

                self.process_task(task).await;
            }

            self
        })
    }

    async fn process_task(&mut self, task: OutputTask) {
        match task {
            // OutputTask::Exit is handled by the `Self::run` method.
            OutputTask::Exit => (),
            OutputTask::Response(response) => {
                log::debug!("Writing proto message...");

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

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use mrhc_proto::chat::response_container::Content as ResponseContent;
    use mrhc_proto::chat::{CapabilityResponse, ResponseContainer};

    use crate::test_utils;

    use super::*;

    fn create_output_task(tag: u64, content: ResponseContent) -> OutputTask {
        OutputTask::Response(Box::new(ResponseContainer {
            tag,
            content: Some(content),
        }))
    }

    #[tokio::test]
    async fn test_output_processor_run() {
        // Arrange
        let (output_tx, output_rx) = mpsc::unbounded_channel();
        let (writer, output) = test_utils::WriterMock::new();

        let output_processor = OutputProcessor::new(Box::new(writer), output_rx);

        let request = ResponseContent::CapabilityResponse(CapabilityResponse::default());

        #[rustfmt::skip]
        let expected_response =  [
            // Size
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Response 1 (tag: 5)
            0x08, 0x05, 0x1A, 0x00,
            // Size
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Response 2 (tag: 6)
            0x08, 0x06, 0x1A, 0x00
        ];

        // Act
        output_tx
            .send(create_output_task(5, request.clone()))
            .unwrap();
        output_tx.send(create_output_task(6, request)).unwrap();
        output_tx.send(OutputTask::Exit).unwrap();

        output_processor.run().await.unwrap();

        // Assert
        let output = output.lock().unwrap();
        let bytes = output.clone().into_inner();
        assert_eq!(expected_response, bytes.as_ref());
    }
}
