use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{Error, ResponseContainer};
use tokio::sync::mpsc::Sender;

use crate::executor::ExecutorTask;
use crate::MultipartResponse;

/// The context of a single request received from the application.
#[derive(Clone)]
pub struct RequestContext {
    /// The tag of the request this context belongs to.
    tag: u64,
    /// An unbounded sender to send tasks to the output processor.
    executor_sender: Sender<ExecutorTask>,
}

impl RequestContext {
    /// Creates a new RequestContext object.
    ///
    /// # Arguments
    ///
    /// * `tag` - The tag of the request the context is for.
    /// * `executor_sender` - Sender to send tasks to the executor.
    pub fn new(tag: u64, executor_sender: Sender<ExecutorTask>) -> Self {
        Self {
            tag,
            executor_sender,
        }
    }

    /// Helper method to send a response container to the output processor.
    #[inline]
    async fn send_to_output(&self, re: ResponseContainer) {
        let task = ExecutorTask::Response(Box::new(re));

        if let Err(err) = self.executor_sender.send(task).await {
            debug_assert!(false, "Failed to send response to output processor: {err}");
            log::error!("Failed to send response to output processor: {err}");
        }
    }

    /// Sends an event to the output processor with the request's tag.
    pub(crate) async fn send_event_with_tag(&self, content: ResponseContent) {
        self.send_to_output(ResponseContainer {
            tag: self.tag,
            content: Some(content),
        })
        .await;
    }

    /// Sends an event to the receiving half.
    pub async fn send_event(&self, content: ResponseContent) {
        self.send_to_output(ResponseContainer {
            tag: 0,
            content: Some(content),
        })
        .await;
    }

    /// Sends an error event to the receiving half.
    pub async fn send_error(&self, err: Error) {
        self.send_to_output(ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(err)),
        })
        .await;
    }

    /// Begins a new list stream.
    /// This is used when a list is sent asynchronously to the application as multiple
    /// separate events (objects).
    pub fn begin_multipart_response(&self) -> MultipartResponse {
        MultipartResponse::new(self.clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use gouda_proto::chat::Message;

    use super::*;

    #[tokio::test]
    async fn test_request_context_send_event() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let ctx = RequestContext::new(42, tx);

        let content = ResponseContent::MessageReceivedEvent(Message {
            message_id: "message-1".to_string(),
            ..Default::default()
        });

        let expected = ResponseContainer {
            tag: 0,
            content: Some(content.clone()),
        };

        ctx.send_event(content).await;

        assert_eq!(
            rx.recv().await.unwrap(),
            ExecutorTask::Response(Box::new(expected))
        );
    }

    #[tokio::test]
    async fn test_request_context_send_error() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let ctx = RequestContext::new(42, tx);

        let error = gouda_proto::chat::Error {
            r#type: 32,
            error_string: Some("test error".to_string()),
        };

        let expected = ResponseContainer {
            tag: 0,
            content: Some(ResponseContent::Error(error.clone())),
        };

        ctx.send_error(error).await;

        assert_eq!(
            rx.recv().await.unwrap(),
            ExecutorTask::Response(Box::new(expected))
        );
    }
}
