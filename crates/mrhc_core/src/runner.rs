use tokio::sync::mpsc;
use tokio::task::JoinError;

use crate::executor::Executor;
use crate::input_processor::{InputProcessor, Reader};
use crate::output_processor::{OutputProcessor, Writer};
use crate::Client;

/// Channel capacity for the executor task queue.
/// Provides backpressure if the executor cannot keep up with incoming requests.
const EXECUTOR_CHANNEL_CAPACITY: usize = 128;

pub struct Runner {
    input_processor: InputProcessor,
    executor: Executor,
    output_processor: OutputProcessor,
}

impl Runner {
    pub fn new(client: Box<dyn Client>, reader: Box<Reader>, writer: Box<Writer>) -> Self {
        let (executor_tx, executor_rx) = mpsc::channel(EXECUTOR_CHANNEL_CAPACITY);
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        Self {
            input_processor: InputProcessor::new(reader, executor_tx.clone(), output_tx.clone()),
            executor: Executor::new(client, executor_rx, executor_tx, output_tx),
            output_processor: OutputProcessor::new(writer, output_rx),
        }
    }

    pub async fn run(self) -> Result<(), JoinError> {
        let input_handle = self.input_processor.run();
        let executor_handle = self.executor.run();
        let output_handle = self.output_processor.run();

        let result = tokio::try_join!(input_handle, executor_handle, output_handle);

        if let Err(err) = &result {
            log::error!("Runner task failed: {err}");
        }

        result.map(|_| ())
    }
}
