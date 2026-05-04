use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::MultipartEnd;

use crate::ClientContext;

pub struct MultipartResponse {
    ctx: ClientContext,
}

impl MultipartResponse {
    pub fn new(ctx: ClientContext) -> Self {
        Self { ctx }
    }

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
