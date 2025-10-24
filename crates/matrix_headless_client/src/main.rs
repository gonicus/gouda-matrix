use mrhc_core::AsyncApp;

use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::tokio::Stream;
use interprocess::local_socket::tokio::{RecvHalf, SendHalf};
use interprocess::local_socket::GenericFilePath;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

const LOG_FILE: &str = "matrix_client.log";
const LOG_LEVEL: LevelFilter = LevelFilter::Debug;

#[tokio::main]
async fn main() {
    setup_logging();
    let (recv, send) = connect_socket().await;

    let app = AsyncApp::new(Box::new(recv), Box::new(send));
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
        .build(Root::builder().appender("file").build(LOG_LEVEL))
        .unwrap();

    log4rs::init_config(config).unwrap();
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
