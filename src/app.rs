use tokio::sync::mpsc;

use crate::input_processor::{InputProcessor, Reader};
use crate::output_processor::{OutputProcessor, Writer};
use crate::executor::Executor;

pub struct AsyncApp {
    input_processor: InputProcessor,
    executor: Executor,
    output_processor: OutputProcessor,
}

impl AsyncApp {
    pub fn new(reader: Box<Reader>, writer: Box<Writer>) -> Self {
        let (executor_tx, executor_rx) = mpsc::unbounded_channel();
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        Self {
            input_processor: InputProcessor::new(reader, executor_tx),
            executor: Executor::new(executor_rx, output_tx),
            output_processor: OutputProcessor::new(writer, output_rx),
        }
    }

    pub async fn run(self) {
        let input_handle = self.input_processor.run();
        let executor_handle = self.executor.run();
        let output_handle = self.output_processor.run();

        tokio::try_join!(input_handle, executor_handle, output_handle).unwrap();
    }
}
