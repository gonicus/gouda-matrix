use std::path::PathBuf;

use clap::Parser;
use gouda_core::Runner;
use gouda_matrix::MatrixClient;
use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::tokio::{RecvHalf, SendHalf, Stream};
use interprocess::local_socket::GenericFilePath;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;

/// The default log level for our own crates.
const LOG_LEVEL_DEFAULT: &str = "INFO";
/// The default log file to use.
const LOG_FILE_DEFAULT: &str = "matrix_client.log";

/// The log level for all other crates.
const LOG_LEVEL_OTHERS: LevelFilter = LevelFilter::Error;

#[derive(Debug, Parser)]
struct Args {
    #[arg(help = "Path to the socket for receiving requests")]
    pub request_socket: String,

    #[arg(help = "Path to the socket for sending responses")]
    pub response_socket: String,

    #[arg(
        long,
        default_value = LOG_LEVEL_DEFAULT,
        help="The log level to use. Must be OFF, ERROR, WARN, INFO, DEBUG or TRACE",
    )]
    pub log_level: LevelFilter,

    #[arg(long, default_value = LOG_FILE_DEFAULT, help="The file where logs are written")]
    pub log_file_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    setup_logging(&args);

    log::info!("Socket for incoming requests: '{}'", args.request_socket);
    log::info!("Socket for outgoing responses: '{}'", args.response_socket);

    let (recv, _send_unused) = connect_socket(&args.request_socket).await?;
    let (_recv_unused, send) = connect_socket(&args.response_socket).await?;

    let client = MatrixClient::new();
    let runner = Runner::new(Box::new(client), Box::new(recv), Box::new(send));

    runner.run().await.map(|_| ()).map_err(|err| err.into())
}

fn setup_logging(args: &Args) {
    // Setup encoders
    let encoder = PatternEncoder::new("{h({d(%H:%M:%S)} {l} {t} - {m}{n})}");

    // Setup appenders
    let file = match FileAppender::builder()
        .encoder(Box::new(encoder))
        .append(false)
        .build(&args.log_file_path)
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
        .logger(setup_custom_logger(args, "gouda_core"))
        .logger(setup_custom_logger(args, "gouda_matrix_adapter"))
        .logger(setup_custom_logger(args, "gouda_matrix"))
        .build(Root::builder().appender("file").build(LOG_LEVEL_OTHERS));

    // Ignore any errors in the logging configuration. We don't want the
    // client to fail to start if, for example, we can't open the log file.
    if let Ok(cfg) = config {
        let _ = log4rs::init_config(cfg);
    }
}

fn setup_custom_logger(args: &Args, name: &str) -> Logger {
    Logger::builder()
        .appender("file")
        .additive(false)
        .build(name, args.log_level)
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
