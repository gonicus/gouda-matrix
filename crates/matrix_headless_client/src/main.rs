use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::tokio::{RecvHalf, SendHalf, Stream};
use interprocess::local_socket::GenericFilePath;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use mrhc_core::Runner;

const LOG_FILE: &str = "matrix_client.log";

/// The log level for our own crates.
const LOG_LEVEL_CUSTOM: LevelFilter = LevelFilter::Debug;
/// The log level for all other crates.
const LOG_LEVEL_OTHERS: LevelFilter = LevelFilter::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    let request_socket = std::env::args()
        .nth(1)
        .ok_or("Request socket not specified")?;

    let response_socket = std::env::args()
        .nth(2)
        .ok_or("Response socket not specified")?;

    log::info!("Socket for incoming requests: '{request_socket}'");
    log::info!("Socket for outgoing responses: '{response_socket}'");

    let (recv, _send_unused) = connect_socket(&request_socket).await?;
    let (_recv_unused, send) = connect_socket(&response_socket).await?;

    let client = mrhc_matrix_adapter::MatrixClient::new();
    let runner = Runner::new(Box::new(client), Box::new(recv), Box::new(send));

    runner.run().await.map(|_| ()).map_err(|err| err.into())
}

fn setup_logging() {
    // Setup encoders
    let encoder = PatternEncoder::new("{h({d(%H:%M:%S)} {l} {t} - {m}{n})}");

    // Setup appenders
    let file = match FileAppender::builder()
        .encoder(Box::new(encoder))
        .append(false)
        .build(LOG_FILE)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Warning: could not initialize log file appender: {e}");
            return;
        }
    };

    // Build final config
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .logger(setup_custom_logger("mrhc_core"))
        .logger(setup_custom_logger("mrhc_matrix_adapter"))
        .logger(setup_custom_logger("matrix_headless_client"))
        .build(Root::builder().appender("file").build(LOG_LEVEL_OTHERS));

    // Ignore any errors in the logging configuration. We don't want the
    // client to fail to start if, for example, we can't open the log file.
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

async fn connect_socket(
    socket_name: &str,
) -> Result<(RecvHalf, SendHalf), Box<dyn std::error::Error>> {
    let name = socket_name
        .to_fs_name::<GenericFilePath>()
        .map_err(|e| format!("Error creating socket name '{socket_name}': {e}"))?;

    log::info!("Waiting for local socket connection at '{name:?}'");

    let conn = Stream::connect(name)
        .await
        .map_err(|e| format!("Error connecting to socket '{socket_name}': {e}"))?;

    log::info!("Successfully connected to local socket");

    Ok(conn.split())
}
