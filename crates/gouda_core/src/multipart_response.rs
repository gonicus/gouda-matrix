use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::MultipartEnd;

use crate::RequestContext;

/// Represents a multipart response.
/// Multipart responses consist of several individual objects sent to the application.
/// A `MultipartEnd` object marks the end of the response. This is commonly used for list
/// responses, where each loaded object is sent directly to the application, rather than
/// waiting until all objects are fully loaded and then sent as a single response object.
///
/// -> RequestList
/// <- ListItem
/// <- ListItem
/// <- ListItem
/// <- MultipartEnd
///
/// # Example
///
/// ```
/// use gouda_core::{RequestContext, MultipartResponse};
/// use gouda_proto::chat::response_container::Content as ResponseContent;
/// use gouda_proto::chat::Message;
///
/// fn get_messages(ctx: RequestContext) {
///     let multipart_response = MultipartResponse::new(ctx);
///
///     let message1 = ResponseContent::MessageReceivedEvent(Message::default());
///     let message2 = ResponseContent::MessageReceivedEvent(Message::default());
///     let message3 = ResponseContent::MessageReceivedEvent(Message::default());
///
///     multipart_response.send_item(message1);
///     multipart_response.send_item(message2);
///     multipart_response.send_item(message3);
///
///     // The MultipartEnd object is automatically send to the application once
///     // the multipart_response object is dropped.
/// }
/// ```
pub struct MultipartResponse {
    ctx: RequestContext,
}

impl MultipartResponse {
    /// Creates a new multipart response object.
    pub fn new(ctx: RequestContext) -> Self {
        Self { ctx }
    }

    /// Sends an item part of the multipart response to the application.
    pub async fn send_item(&self, item: ResponseContent) {
        self.ctx.send_event_with_tag(item).await;
    }
}

impl Drop for MultipartResponse {
    fn drop(&mut self) {
        let ctx = self.ctx.clone();

        tokio::spawn(async move {
            ctx.send_event_with_tag(ResponseContent::MultipartEnd(MultipartEnd {}))
                .await;
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use gouda_proto::chat::{Message, ResponseContainer};
    use tokio::sync::mpsc::Receiver;

    use super::*;
    use crate::ExecutorTask;

    fn create_context(tag: u64) -> (RequestContext, Receiver<ExecutorTask>) {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        (RequestContext::new(tag, tx), rx)
    }

    #[tokio::test]
    async fn test_multipart_response_send_item() {
        let (ctx, mut rx) = create_context(42);
        let mr = MultipartResponse::new(ctx);

        let content = ResponseContent::MessageReceivedEvent(Message {
            message_id: "message-1".to_string(),
            ..Default::default()
        });

        let expected = ResponseContainer {
            tag: 42,
            content: Some(content.clone()),
        };

        mr.send_item(content).await;

        assert_eq!(
            rx.recv().await.unwrap(),
            ExecutorTask::Response(Box::new(expected))
        );
    }

    #[tokio::test]
    async fn test_multipart_response_drop() {
        let (ctx, mut rx) = create_context(42);
        let mr = MultipartResponse::new(ctx);

        let content = ResponseContent::MessageReceivedEvent(Message {
            message_id: "message-1".to_string(),
            ..Default::default()
        });

        let expected_item = ResponseContainer {
            tag: 42,
            content: Some(content.clone()),
        };

        let expected_end = ResponseContainer {
            tag: 42,
            content: Some(ResponseContent::MultipartEnd(MultipartEnd::default())),
        };

        mr.send_item(content).await;
        drop(mr);

        assert_eq!(
            rx.recv().await.unwrap(),
            ExecutorTask::Response(Box::new(expected_item))
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            ExecutorTask::Response(Box::new(expected_end))
        );
    }
}
