use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::MultipartEnd;

use crate::ClientContext;

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
/// use mrhc_core::{ClientContext, MultipartResponse};
/// use mrhc_proto::chat::response_container::Content as ResponseContent;
/// use mrhc_proto::chat::Message;
///
/// fn get_messages(ctx: ClientContext) {
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
    ctx: ClientContext,
}

impl MultipartResponse {
    /// Creates a new multipart response object.
    pub fn new(ctx: ClientContext) -> Self {
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
