use std::fmt::Debug;

use mrhc_proto::chat::ResponseContainer;
use prost::Message;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::UnboundedReceiver;

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
/// The output can be any object that implements the `AsyncWrite` trait, as well
/// as `Send` and `Unpin`. This is typically a socket or network stream.
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

    /// Spawns an asynchronous Tokio task and starts the output processor to
    /// wait for tasks and write its data to the `self.writer`.
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

                log::trace!("Writing size: {}", serialized.len());

                self.writer
                    .write_all(&size)
                    .await
                    .expect("error writing size");

                log::trace!("Writing response: {serialized:?}");

                self.writer
                    .write_all(&serialized)
                    .await
                    .expect("error writing response");

                log::trace!("Flushing writer");

                self.writer.flush().await.expect("error flushing writer");

                log::debug!("Finished writing response");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mrhc_proto::chat::response_container::Content as ResponseContent;
    use mrhc_proto::chat::{IdentityProvidersResponse, ResponseContainer, StatusUpdate};
    use tokio::sync::mpsc;

    use super::*;
    use crate::test_utils;

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

        let response_1 = ResponseContent::IdentityProvidersResponse(IdentityProvidersResponse {
            identity_providers: vec!["idp-1".to_owned(), "idp-2".to_owned()],
        });

        let response_2 = ResponseContent::StatusUpdate(StatusUpdate { code: 23 });

        #[rustfmt::skip]
        let expected_response = [
            // Size
            0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Response 1 (tag: 5)
            0x08, 0x05, 0x7A, 0x0E, 0x0A, 0x05, 0x69, 0x64, 0x70, 0x2D, 0x31, 0x0A, 0x05,
            0x69, 0x64, 0x70, 0x2D, 0x32,
            // Size
            0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Response 2 (tag: 6)
            0x08, 0x06, 0x5A, 0x02, 0x08, 0x17,
        ];

        // Act
        output_tx.send(create_output_task(5, response_1)).unwrap();
        output_tx.send(create_output_task(6, response_2)).unwrap();
        output_tx.send(OutputTask::Exit).unwrap();

        output_processor.run().await.unwrap();

        // Assert
        let output = output.lock().unwrap();
        let bytes = output.clone().into_inner();
        assert_eq!(expected_response, bytes.as_ref());
    }
}
