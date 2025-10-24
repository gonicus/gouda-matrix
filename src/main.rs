mod executor;
mod input_processor;
mod messages;
mod output_processor;

use executor::Executor;
use input_processor::InputProcessor;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use output_processor::OutputProcessor;
use tokio::sync::mpsc;

const LOG_FILE: &str = "matrix_client.log";
const LOG_LEVEL: LevelFilter = LevelFilter::Debug;

#[tokio::main]
async fn main() {
    setup_logging();

    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (output_tx, output_rx) = mpsc::unbounded_channel();

    let input_processor = InputProcessor::new(Box::new(tokio::io::stdin()), executor_tx);
    let executor = Executor::new(executor_rx, output_tx);
    let output_processor = OutputProcessor::new(Box::new(tokio::io::stdout()), output_rx);

    let input_handle = input_processor.run();
    let executor_handle = executor.run();
    let output_handle = output_processor.run();

    let (input_res, executor_res, output_res) =
        tokio::join!(input_handle, executor_handle, output_handle);

    input_res.unwrap();
    executor_res.unwrap();
    output_res.unwrap();
}

fn setup_logging() {
    // Setup encoders
    let encoder = PatternEncoder::new("{h({d(%H:%M:%S)} {l} {t} - {m}{n})}");

    // Setup appenders
    let file = FileAppender::builder()
        .encoder(Box::new(encoder))
        .append(false)
        .build(LOG_FILE)
        .expect("Error initializing log file appender");

    // Build final config
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(Root::builder().appender("file").build(LOG_LEVEL))
        .unwrap();

    log4rs::init_config(config).unwrap();
}
