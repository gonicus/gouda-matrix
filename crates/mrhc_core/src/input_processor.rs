use mrhc_proto::chat::RequestContainer;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc::UnboundedSender;

use crate::executor::ExecutorTask;
use crate::output_processor::OutputTask;

pub type Reader = dyn AsyncRead + Send + Unpin;

/// The InputProcessor is responsible to read and decode data from the specified input.
/// The input can be any object that implements the `AsyncRead` trait, as well
/// as `Send` and `Unpin`. This is typically a socket or network stream.
pub struct InputProcessor {
    /// From where to read and decode input.
    reader: BufReader<Box<Reader>>,
    /// Where to send the decoded input.
    executor_sender: UnboundedSender<ExecutorTask>,
    /// Where to send output. This is currently only used when reaching an EOF.
    output_sender: UnboundedSender<OutputTask>,
}

impl InputProcessor {
    pub fn new(
        reader: Box<Reader>,
        executor_sender: UnboundedSender<ExecutorTask>,
        output_sender: UnboundedSender<OutputTask>,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            executor_sender,
            output_sender,
        }
    }

    /// Spawns an asynchronous tokio task and starts the input processor
    /// to wait for input to decode.
    /// This method is executed until the program ends.
    pub fn run(mut self) -> tokio::task::JoinHandle<Self> {
        tokio::spawn(async move {
            loop {
                log::debug!("Waiting for input...");

                match read_size(&mut self.reader).await {
                    Ok(size) => {
                        log::trace!("Read size: {size}");

                        let request = read_request(&mut self.reader, size).await;

                        log::debug!("Read request: {request:?}");

                        log::debug!("Sending event to executor...");

                        self.executor_sender
                            .send(ExecutorTask::Request(Box::new(request)))
                            .expect("error sending executor event");

                        log::debug!("Successfully send event to executor");
                    }
                    Err(err) => {
                        if err.kind() == tokio::io::ErrorKind::UnexpectedEof {
                            log::info!("Exiting as an EOF was received on the input reader");
                            self.exit();
                            break;
                        } else {
                            panic!("Received io error: {err}");
                        }
                    }
                }
            }

            self
        })
    }

    fn exit(&mut self) {
        log::debug!("Sending exit task to executor");
        self.executor_sender
            .send(ExecutorTask::Exit)
            .expect("Error sending exit event to executor");

        log::debug!("Sending exit task to output processor");
        self.output_sender
            .send(OutputTask::Exit)
            .expect("Error sending exit event to output processor");
    }
}

async fn read_size(reader: &mut Reader) -> Result<u64, tokio::io::Error> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf).await?;

    Ok(u64::from_le_bytes(buf))
}

async fn read_request(reader: &mut Reader, len: u64) -> RequestContainer {
    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .expect("error reading buffer of size {len}");

    RequestContainer::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .expect("error decoding RequestContainer")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use mrhc_proto::chat::request_container::Content as RequestContent;
    use mrhc_proto::chat::InitializationRequest;
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
        assert_eq!(
            result.unwrap_err().kind(),
            tokio::io::ErrorKind::UnexpectedEof
        );
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
        let result = read_request(&mut data, len).await;

        assert_eq!(result, expected);
    }

    #[tokio::test]
    #[should_panic(expected = "early eof")]
    async fn test_read_request_early_eof() {
        let mut data: &'static [u8] = &[
            0x08, 0x57, 0x2a, 0x20, 0x0a, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x75, 0x73, 0x65,
            0x72, 0x12,
        ];
        let _ = read_request(&mut data, 36).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_read_request_decode_error() {
        let mut data: &'static [u8] = &[
            0x12, 0x57, 0x2a, 0x20, 0x0a, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x75, 0x73, 0x65,
            0x72, 0x12, 0x13, 0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x74, 0x65, 0x73, 0x74,
            0x2e, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64,
        ];

        let len = data.len() as u64;
        let _ = read_request(&mut data, len).await;
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

        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();
        let input_processor =
            InputProcessor::new(Box::new(Cursor::new(data)), executor_tx, output_tx);

        // Act
        input_processor.run().await.unwrap();

        // Assert
        assert_eq!(executor_rx.recv().await.unwrap(), expected);
        assert_eq!(executor_rx.recv().await.unwrap(), ExecutorTask::Exit);
        assert_eq!(output_rx.recv().await.unwrap(), OutputTask::Exit);
    }
}
