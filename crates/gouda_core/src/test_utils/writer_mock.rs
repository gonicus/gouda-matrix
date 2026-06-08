#![allow(clippy::unwrap_used)]

use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use tokio::io::AsyncWrite;

/// Mocks an AsyncWriter.
pub struct WriterMock {
    buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl WriterMock {
    /// Creates a new [`WriterMock`] object.
    pub fn new() -> (Self, Arc<Mutex<Cursor<Vec<u8>>>>) {
        let buffer = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        (
            Self {
                buffer: buffer.clone(),
            },
            buffer,
        )
    }
}

impl AsyncWrite for WriterMock {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let mut locked = self.buffer.lock().unwrap();
        Poll::Ready(locked.write(buf))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut locked = self.buffer.lock().unwrap();
        Poll::Ready(locked.flush())
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Unpin for WriterMock {}
