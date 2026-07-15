use gouda_proto::chat::RequestContainer;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use crate::error::{RunnerError, RunnerResult};
use crate::executor::ExecutorTask;

pub type Reader = dyn AsyncRead + Send + Unpin;

/// The InputProcessor is responsible to read and decode data from the specified input.
/// The input can be any object that implements the `AsyncRead` trait, as well
/// as `Send` and `Unpin`. This is typically a socket or network stream.
pub struct InputProcessor {
    /// From where to read and decode input.
    reader: BufReader<Box<Reader>>,
    /// Where to send the decoded input.
    executor_sender: Sender<ExecutorTask>,
}

impl InputProcessor {
    pub fn new(reader: Box<Reader>, executor_sender: Sender<ExecutorTask>) -> Self {
        Self {
            reader: BufReader::new(reader),
            executor_sender,
        }
    }

    /// Spawns an asynchronous tokio task and starts the input processor
    /// to wait for input to decode.
    /// This method is executed until the program ends.
    pub fn run(
        mut self,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<RunnerResult<Self>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("InputProcessor was cancelled");
                        break;
                    }
                    result = self.read_input() => {
                        if let Err(err) = result {
                            log::error!("Error reading from input: {err}");
                            return Err(err);
                        }
                    }
                }
            }

            Ok(self)
        })
    }

    async fn read_input(&mut self) -> RunnerResult<()> {
        log::debug!("Waiting for input...");

        let size = read_size(&mut self.reader).await?;
        let request = read_request(&mut self.reader, size).await?;

        log::info!("Read request: {request:?}");
        log::debug!("Sending event to executor");

        self.executor_sender
            .send(ExecutorTask::Request(Box::new(request)))
            .await
            .map_err(|_| RunnerError::InternalChannelClosed)?;

        log::debug!("Successfully send event to executor");

        Ok(())
    }
}

async fn read_size(reader: &mut Reader) -> RunnerResult<u64> {
    let mut buf = [0; 8];

    reader
        .read_exact(&mut buf)
        .await
        .map_err(|_| RunnerError::RequestChannelClosed)?;

    Ok(u64::from_le_bytes(buf))
}

async fn read_request(reader: &mut Reader, len: u64) -> RunnerResult<RequestContainer> {
    let mut buf = vec![0; len as usize];

    reader
        .read_exact(&mut buf)
        .await
        .map_err(|_| RunnerError::RequestChannelClosed)?;

    RequestContainer::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .inspect_err(|err| log::error!("Error decoding request container: {err}"))
        .map_err(|_| RunnerError::InvalidData)
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use gouda_proto::chat::request_container::Content as RequestContent;
    use gouda_proto::chat::InitializationRequest;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_read_size() {
        let mut data: &'static [u8] = &[0x61, 0x96, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = read_size(&mut data).await.unwrap();
        assert_eq!(result, 693857);
    }

    #[tokio::test]
    async fn test_read_size_early_eof() {
        let mut data: &'static [u8] = &[0x61, 0x96, 0x0a, 0x00, 0x00];
        let result = read_size(&mut data).await;
        assert!(matches!(
            result.unwrap_err(),
            RunnerError::RequestChannelClosed
        ));
    }

    #[tokio::test]
    async fn test_read_request() {
        let mut data: &[u8] = &[
            0x08, 0x57, 0x12, 0x5E, 0x0A, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3A, 0x2F, 0x2F, 0x74,
            0x65, 0x73, 0x74, 0x2E, 0x62, 0x61, 0x63, 0x6B, 0x65, 0x6E, 0x64, 0x12, 0x11, 0x2F,
            0x74, 0x6D, 0x70, 0x2F, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x5F, 0x64, 0x61, 0x74,
            0x61, 0x2F, 0x1A, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x2D, 0x31, 0x32, 0x33, 0x22, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65,
            0x63, 0x72, 0x65, 0x74, 0x2D, 0x31, 0x32, 0x33, 0x2A, 0x12, 0x4D, 0x61, 0x74, 0x72,
            0x69, 0x78, 0x20, 0x52, 0x75, 0x73, 0x74, 0x20, 0x43, 0x6C, 0x69, 0x65, 0x6E, 0x74,
        ];

        let expected = RequestContainer {
            tag: 87,
            content: Some(RequestContent::InitializationRequest(
                InitializationRequest {
                    backend_url: "http://test.backend".to_owned(),
                    data_root_path: "/tmp/client_data/".to_owned(),
                    encryption_secret: "some-secret-123".to_owned(),
                    persistent_storage_secret: "some-secret-123".to_owned(),
                    device_display_name: "Matrix Rust Client".to_owned(),
                },
            )),
        };

        let len = data.len() as u64;
        let result = read_request(&mut data, len).await.unwrap();

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_read_request_early_eof() {
        let mut data: &'static [u8] = &[
            0x08, 0x57, 0x2a, 0x20, 0x0a, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x75, 0x73, 0x65,
            0x72, 0x12,
        ];

        let result = read_request(&mut data, 36).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RunnerError::RequestChannelClosed
        ));
    }

    #[tokio::test]
    async fn test_read_request_decode_error() {
        let mut data: &'static [u8] = &[
            0x12, 0x57, 0x2a, 0x20, 0x0a, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x75, 0x73, 0x65,
            0x72, 0x12, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x74, 0x65, 0x73, 0x74,
            0x2e, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64,
        ];

        let len = data.len() as u64;
        let result = read_request(&mut data, len).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RunnerError::InvalidData));
    }

    #[tokio::test]
    async fn test_input_processor_run() {
        #[rustfmt::skip]
        let data: &'static [u8] = &[
            // Size
            0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Request
            0x08, 0x57, 0x12, 0x5E, 0x0A, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3A, 0x2F, 0x2F, 0x74,
            0x65, 0x73, 0x74, 0x2E, 0x62, 0x61, 0x63, 0x6B, 0x65, 0x6E, 0x64, 0x12, 0x11, 0x2F,
            0x74, 0x6D, 0x70, 0x2F, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x5F, 0x64, 0x61, 0x74,
            0x61, 0x2F, 0x1A, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x2D, 0x31, 0x32, 0x33, 0x22, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65,
            0x63, 0x72, 0x65, 0x74, 0x2D, 0x31, 0x32, 0x33, 0x2A, 0x12, 0x4D, 0x61, 0x74, 0x72,
            0x69, 0x78, 0x20, 0x52, 0x75, 0x73, 0x74, 0x20, 0x43, 0x6C, 0x69, 0x65, 0x6E, 0x74,
        ];

        let expected = ExecutorTask::Request(Box::new(RequestContainer {
            tag: 87,
            content: Some(RequestContent::InitializationRequest(
                InitializationRequest {
                    backend_url: "http://test.backend".to_owned(),
                    data_root_path: "/tmp/client_data/".to_owned(),
                    encryption_secret: "some-secret-123".to_owned(),
                    persistent_storage_secret: "some-secret-123".to_owned(),
                    device_display_name: "Matrix Rust Client".to_owned(),
                },
            )),
        }));

        let (executor_tx, mut executor_rx) = mpsc::channel(64);
        let input_processor = InputProcessor::new(Box::new(Cursor::new(data)), executor_tx);

        // Act
        let result = input_processor.run(CancellationToken::new()).await.unwrap();

        // Assert
        assert!(matches!(result, Err(RunnerError::RequestChannelClosed)));
        assert_eq!(executor_rx.recv().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_input_processor_early_eof() {
        #[rustfmt::skip]
        let data: &'static [u8] = &[
            // Size
            0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Request
            0x08, 0x57, 0x12, 0x5E, 0x0A, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3A, 0x2F, 0x2F, 0x74,
        ];

        let (executor_tx, _) = mpsc::channel(64);
        let input_processor = InputProcessor::new(Box::new(Cursor::new(data)), executor_tx);

        // Act
        let result = input_processor.run(CancellationToken::new()).await.unwrap();

        // Assert
        assert!(matches!(result, Err(RunnerError::RequestChannelClosed)));
    }

    #[tokio::test]
    async fn test_input_processor_executor_channel_closed() {
        #[rustfmt::skip]
        let data: &'static [u8] = &[
            // Size
            0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Request
            0x08, 0x57, 0x12, 0x5E, 0x0A, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3A, 0x2F, 0x2F, 0x74,
            0x65, 0x73, 0x74, 0x2E, 0x62, 0x61, 0x63, 0x6B, 0x65, 0x6E, 0x64, 0x12, 0x11, 0x2F,
            0x74, 0x6D, 0x70, 0x2F, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x5F, 0x64, 0x61, 0x74,
            0x61, 0x2F, 0x1A, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x2D, 0x31, 0x32, 0x33, 0x22, 0x0F, 0x73, 0x6F, 0x6D, 0x65, 0x2D, 0x73, 0x65,
            0x63, 0x72, 0x65, 0x74, 0x2D, 0x31, 0x32, 0x33, 0x2A, 0x12, 0x4D, 0x61, 0x74, 0x72,
            0x69, 0x78, 0x20, 0x52, 0x75, 0x73, 0x74, 0x20, 0x43, 0x6C, 0x69, 0x65, 0x6E, 0x74,
        ];

        let (executor_tx, executor_rx) = mpsc::channel(64);
        let input_processor = InputProcessor::new(Box::new(Cursor::new(data)), executor_tx);

        // Act
        drop(executor_rx);
        let result = input_processor.run(CancellationToken::new()).await.unwrap();

        // Assert
        assert!(matches!(result, Err(RunnerError::InternalChannelClosed)));
    }

    #[tokio::test]
    async fn test_input_processor_run_invalid_data() {
        #[rustfmt::skip]
        let data: &'static [u8] = &[
            // Size
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Request
            0x08, 0x57, 0x12, 0x5E, 0x0A, 0x13, 0x68, 0x74,
        ];

        let (executor_tx, _) = mpsc::channel(64);
        let input_processor = InputProcessor::new(Box::new(Cursor::new(data)), executor_tx);

        // Act
        let result = input_processor.run(CancellationToken::new()).await.unwrap();

        // Assert
        assert!(matches!(result, Err(RunnerError::InvalidData)));
    }
}
