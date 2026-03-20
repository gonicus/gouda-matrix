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

    pub fn send_item(&self, item: ResponseContent) {
        self.ctx.send_event_with_tag(item);
    }
}

impl Drop for MultipartResponse {
    fn drop(&mut self) {
        self.ctx
            .send_event_with_tag(ResponseContent::MultipartEnd(MultipartEnd {}));
    }
}
