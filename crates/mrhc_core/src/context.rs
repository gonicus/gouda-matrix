use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::{Error, ResponseContainer};
use tokio::sync::mpsc::UnboundedSender;

use crate::output_processor::OutputTask;
use crate::MultipartResponse;

#[derive(Clone)]
pub struct ClientContext {
    /// The tag of the request this context belongs to.
    tag: u64,
    /// An unbounded sender to send tasks to the output processor.
    output_sender: UnboundedSender<OutputTask>,
}

impl ClientContext {
    pub fn new(tag: u64, output_sender: UnboundedSender<OutputTask>) -> Self {
        Self { tag, output_sender }
    }

    /// Helper method to send a response container to the output processor.
    #[inline]
    fn send_to_output(&self, re: ResponseContainer) {
        if let Err(err) = self.output_sender.send(OutputTask::Response(Box::new(re))) {
            debug_assert!(false, "Failed to send response to output processor: {err}");
            log::error!("Failed to send response to output processor: {err}");
        }
    }

    /// Sends an event to the output processor with the request's tag.
    pub(crate) fn send_event_with_tag(&self, content: ResponseContent) {
        self.send_to_output(ResponseContainer {
            tag: self.tag,
            content: Some(content),
        });
    }

    /// Sends an event to the receiving half.
    pub fn send_event(&self, content: ResponseContent) {
        self.send_to_output(ResponseContainer {
            tag: 0,
            content: Some(content),
        });
    }

    /// Sends an error event to the receiving half.
    pub fn send_error(&self, err: Error) {
        self.send_to_output(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(err)),
        });
    }

    /// Begins a new list stream.
    /// This is used when a list is sent asynchronously to the application as multiple
    /// separate events (objects).
    pub fn begin_multipart_response(&self) -> MultipartResponse {
        MultipartResponse::new(self.clone())
    }
}
