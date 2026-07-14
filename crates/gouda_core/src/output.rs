use std::fmt::Debug;

use gouda_proto::chat::ResponseContainer;
use prost::Message;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::error::{RunnerError, RunnerResult};

pub type Writer = dyn AsyncWrite + Send + Unpin;

/// A task for the output processor.
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
    task_receiver: Receiver<OutputTask>,
}

impl OutputProcessor {
    pub fn new(writer: Box<Writer>, task_receiver: Receiver<OutputTask>) -> Self {
        Self {
            writer: BufWriter::new(writer),
            task_receiver,
        }
    }

    /// Spawns an asynchronous Tokio task and starts the output processor to
    /// wait for tasks and write its data to the `self.writer`.
    /// This method is executed until an `OutputTask::Exit` is received.
    pub fn run(
        mut self,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<RunnerResult<Self>> {
        tokio::spawn(async move {
            log::debug!("Waiting for tasks...");

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("OutputProcessor was cancelled");
                        break;
                    },
                    task = self.task_receiver.recv() => {
                        let Some(task) = task else {
                            log::warn!("OutputProcessor channel has been closed");
                            break;
                        };

                        log::debug!("Received task: {task:?}");

                        if matches!(task, OutputTask::Exit) {
                            log::info!("Exiting as an exit event was received");
                            break;
                        }

                        if let Err(err) = self.process_task(task).await {
                            log::error!("Error processing task: {err}");
                            return Err(err);
                        }
                    }
                }
            }

            Ok(self)
        })
    }

    async fn process_task(&mut self, task: OutputTask) -> RunnerResult<()> {
        match task {
            // OutputTask::Exit is handled by the `Self::run` method.
            OutputTask::Exit => Ok(()),
            OutputTask::Response(response) => self.write_response(*response).await,
        }
    }

    async fn write_response(&mut self, response: ResponseContainer) -> RunnerResult<()> {
        log::info!("Writing response container: {response:?}");

        let serialized = response.encode_to_vec();
        let size = serialized.len().to_le_bytes().to_vec();

        self.writer
            .write_all(&size)
            .await
            .map_err(|_| RunnerError::ResponseChannelClosed)?;

        self.writer
            .write_all(&serialized)
            .await
            .map_err(|_| RunnerError::ResponseChannelClosed)?;

        log::trace!("Flushing writer");

        if let Err(err) = self.writer.flush().await {
            debug_assert!(false, "Error flushing writer: {err}");
            log::error!("Error flushing writer: {err}");
        };

        log::debug!("Finished writing response");

        Ok(())
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use gouda_proto::chat::response_container::Content as ResponseContent;
    use gouda_proto::chat::{IdentityProvidersResponse, ResponseContainer, StatusUpdate};
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
        let (output_tx, output_rx) = mpsc::channel(32);
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
            0x08, 0x05, 0x3A, 0x0E, 0x0A, 0x05, 0x69, 0x64, 0x70, 0x2D, 0x31, 0x0A, 0x05,
            0x69, 0x64, 0x70, 0x2D, 0x32,
            // Size
            0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Response 2 (tag: 6)
            0x08, 0x06, 0x22, 0x02, 0x08, 0x17,
        ];

        // Act
        output_tx
            .send(create_output_task(5, response_1))
            .await
            .unwrap();
        output_tx
            .send(create_output_task(6, response_2))
            .await
            .unwrap();
        output_tx.send(OutputTask::Exit).await.unwrap();

        output_processor
            .run(CancellationToken::new())
            .await
            .unwrap()
            .unwrap();

        // Assert
        let bytes = output.lock().unwrap().clone().into_inner();
        assert_eq!(expected_response, bytes.as_ref());
    }

    #[tokio::test]
    async fn test_output_processor_cancellation_token() {
        // Arrange
        let (_, output_rx) = mpsc::channel(32);
        let (writer, output) = test_utils::WriterMock::new();

        let output_processor = OutputProcessor::new(Box::new(writer), output_rx);

        let token = CancellationToken::new();

        // Act
        token.cancel();

        let result = output_processor.run(token).await.unwrap();

        // Assert
        assert!(result.is_ok());

        let bytes = output.lock().unwrap().clone().into_inner();
        assert!(bytes.is_empty());
    }
}
