use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc::UnboundedSender;

use crate::executor::ExecutorTask;
use crate::messages::GreetRequest;

type Reader = dyn AsyncRead + Send + Unpin;

pub struct InputProcessor {
    reader: BufReader<Box<Reader>>,
    executor_sender: UnboundedSender<ExecutorTask>,
}

impl InputProcessor {
    pub fn new(reader: Box<Reader>, executor_sender: UnboundedSender<ExecutorTask>) -> Self {
        Self {
            reader: BufReader::new(reader),
            executor_sender,
        }
    }

    pub fn run(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                log::debug!("Waiting for input...");

                let size = read_size(&mut self.reader).await;

                log::debug!("Read size: {size}");

                let request = read_request(&mut self.reader, size).await;

                log::debug!("Read request: {request:?}");

                log::debug!("Sending event to executor...");

                self.executor_sender
                    .send(ExecutorTask::GreetRequest(Box::new(request)))
                    .expect("error sending executor event");

                log::debug!("Successfully send event to executor");
            }
        })
    }
}

async fn read_size(reader: &mut Reader) -> u64 {
    let mut buf = [0; 8];
    reader
        .read_exact(&mut buf)
        .await
        .expect("error reading size");
    u64::from_le_bytes(buf)
}

async fn read_request(reader: &mut Reader, len: u64) -> GreetRequest {
    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .expect("error reading buffer of size {len}");
    GreetRequest::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .expect("error decoding greet request")
}
