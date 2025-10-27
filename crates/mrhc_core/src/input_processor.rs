use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc::UnboundedSender;

use mrhc_proto::chat::ToClientContainer;

use crate::executor::ExecutorTask;
use crate::output_processor::OutputTask;

pub type Reader = dyn AsyncRead + Send + Unpin;

/// The InputProcessor is responsible to read and decode data from the specified input.
/// The input can be any object that implements the `AsyncRead` trait, as well as `Send` and `Unpin`.
/// This is typically a socket or network stream.
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

    /// Spawns an asynchronous tokio task and starts the input processor to wait for input to decode.
    /// This method is executed until the program ends.
    pub fn run(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                log::debug!("Waiting for input...");

                match read_size(&mut self.reader).await {
                    Ok(size) => {
                        log::debug!("Read size: {size}");

                        let request = read_request(&mut self.reader, size).await;

                        log::debug!("Read request: {request:?}");

                        log::debug!("Sending event to executor...");

                        self.executor_sender
                            .send(ExecutorTask::ToClientContainer(Box::new(request)))
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

async fn read_request(reader: &mut Reader, len: u64) -> ToClientContainer {
    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .expect("error reading buffer of size {len}");

    ToClientContainer::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .expect("error decoding ToClientContainer")
}
