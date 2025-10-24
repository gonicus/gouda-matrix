mod executor;
mod input_processor;
mod messages;
mod output_processor;

use executor::Executor;

use input_processor::InputProcessor;
use interprocess::local_socket::GenericFilePath;
use interprocess::local_socket::tokio::Stream;
use interprocess::local_socket::tokio::prelude::*;
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

    let socket_name = std::env::args().nth(1).expect("No socket name specified");

    let socket_name = socket_name
        .to_fs_name::<GenericFilePath>()
        .expect("Error creating socket name");

    log::debug!("Waiting for local socket connection at '{socket_name:?}'");

    let conn = Stream::connect(socket_name)
        .await
        .expect("Error connecting to socket");

    log::debug!("Successfully connected to local socket");

    let (recv, sender) = conn.split();

    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (output_tx, output_rx) = mpsc::unbounded_channel();

    let input_processor = InputProcessor::new(Box::new(recv), executor_tx);
    let executor = Executor::new(executor_rx, output_tx);
    let output_processor = OutputProcessor::new(Box::new(sender), output_rx);

    let input_handle = input_processor.run();
    let executor_handle = executor.run();
    let output_handle = output_processor.run();

    tokio::try_join!(input_handle, executor_handle, output_handle).unwrap();
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
