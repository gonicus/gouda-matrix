use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::tokio::{RecvHalf, SendHalf, Stream};
use interprocess::local_socket::GenericFilePath;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use mrhc_core::AsyncApp;

const LOG_FILE: &str = "matrix_client.log";

/// The log level for our own crates.
const LOG_LEVEL_CUSTOM: LevelFilter = LevelFilter::Debug;
/// The log level for all other crates.
const LOG_LEVEL_OTHERS: LevelFilter = LevelFilter::Error;

#[tokio::main]
async fn main() {
    setup_logging();
    let (recv, send) = connect_socket().await;

    let client = mrhc_matrix_adapter::MatrixClient::new();
    let app = AsyncApp::new(Box::new(client), Box::new(recv), Box::new(send));

    app.run().await
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
        .logger(setup_custom_logger("mrhc_core"))
        .logger(setup_custom_logger("mrhc_matrix_adapter"))
        .logger(setup_custom_logger("matrix_headless_client"))
        .build(Root::builder().appender("file").build(LOG_LEVEL_OTHERS));

    // Ignore any errors in the logging configuration. We don't want the client to fail to start if,
    // for example, we can't open the log file.
    if let Ok(cfg) = config {
        let _ = log4rs::init_config(cfg);
    }
}

fn setup_custom_logger(name: &str) -> Logger {
    Logger::builder()
        .appender("file")
        .additive(false)
        .build(name, LOG_LEVEL_CUSTOM)
}

async fn connect_socket() -> (RecvHalf, SendHalf) {
    let socket_name = std::env::args().nth(1).expect("No socket name specified");

    let socket_name = socket_name
        .to_fs_name::<GenericFilePath>()
        .expect("Error creating socket name");

    log::debug!("Waiting for local socket connection at '{socket_name:?}'");

    let conn = Stream::connect(socket_name)
        .await
        .expect("Error connecting to socket");

    log::debug!("Successfully connected to local socket");

    conn.split()
}
