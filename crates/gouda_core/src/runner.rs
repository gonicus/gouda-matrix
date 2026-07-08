use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{RunnerError, RunnerResult};
use crate::executor::Executor;
use crate::input::{InputProcessor, Reader};
use crate::output::{OutputProcessor, Writer};
use crate::Client;

/// Channel capacity for the executor task queue.
/// Provides back pressure if the executor cannot keep up with incoming requests.
const EXECUTOR_CHANNEL_CAPACITY: usize = 128;
/// Channel capacity for the output queue.
/// This should be relatively low so that completed responses do not accumulate.
const OUTPUT_CHANNEL_CAPACITY: usize = 24;

/// The main entry point of the client, implementing the communication with the application
/// and calls the requested methods on the client.
pub struct Runner {
    input_processor: InputProcessor,
    executor: Executor,
    output_processor: OutputProcessor,
}

impl Runner {
    /// Creates a new runner.
    ///
    /// # Arguments
    ///
    /// * `client` - The client to use to execute requests from the application
    /// * `reader` - The reader from where requests are received
    /// * `writer` - The writer to where responses and events are send
    pub fn new(client: Arc<dyn Client>, reader: Box<Reader>, writer: Box<Writer>) -> Self {
        let (executor_tx, executor_rx) = mpsc::channel(EXECUTOR_CHANNEL_CAPACITY);
        let (output_tx, output_rx) = mpsc::channel(OUTPUT_CHANNEL_CAPACITY);

        Self {
            input_processor: InputProcessor::new(reader, executor_tx.clone()),
            executor: Executor::new(client, executor_rx, executor_tx, output_tx),
            output_processor: OutputProcessor::new(writer, output_rx),
        }
    }

    /// This method starts the actual processing and execution of incoming requests.
    /// It blocks until an end-of-file (EOF) is received from the input reader or another
    /// error occurs. Normally, it blocks for the entire duration of the client's runtime.
    pub async fn run(self) -> RunnerResult<()> {
        let cancellation_token = CancellationToken::new();

        let input_handle = self.input_processor.run(cancellation_token.clone());
        let executor_handle = self.executor.run(cancellation_token.clone());
        let output_handle = self.output_processor.run(cancellation_token.clone());

        let input = async {
            let result = input_handle.await.map_err(|_| RunnerError::TaskPanicked)?;
            if result.is_err() {
                cancellation_token.cancel();
            }
            result
        };

        let executor = async {
            let result = executor_handle
                .await
                .map_err(|_| RunnerError::TaskPanicked)?;

            if result.is_err() {
                cancellation_token.cancel();
            }

            result
        };

        let output = async {
            let result = output_handle.await.map_err(|_| RunnerError::TaskPanicked)?;
            if result.is_err() {
                cancellation_token.cancel();
            }
            result
        };

        let result = tokio::try_join!(input, executor, output);

        if let Err(err) = &result {
            log::error!("Runner task failed: {err}");
        }

        result.map(|_| ())
    }
}
