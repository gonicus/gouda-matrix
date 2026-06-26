use std::sync::mpsc::{Receiver, Sender};

use egui_toast::{Toast, Toasts};
use gouda_proto::chat::{RequestContainer, ResponseContainer};

use crate::config::Config;

pub struct Context {
    /// Where to send requests.
    request_sender: Sender<RequestContainer>,
    /// Receiver for incoming responses.
    response_receiver: Receiver<ResponseContainer>,

    /// The loaded application config.
    config: Config,

    /// Responses we have received between the last frame and this frame.
    received_responses: Vec<ResponseContainer>,
    /// Requests we have send between the last frame and this frame.
    send_requests: Vec<RequestContainer>,

    /// Actions we have queued during this frame.
    queued_requests: Vec<RequestContainer>,

    /// Toasts.
    toasts: Toasts,
}

impl Context {
    pub fn new(
        config: Config,
        request_sender: Sender<RequestContainer>,
        response_receiver: Receiver<ResponseContainer>,
    ) -> Self {
        Self {
            request_sender,
            response_receiver,
            config,

            received_responses: Vec::new(),
            send_requests: Vec::new(),

            queued_requests: Vec::new(),

            toasts: Toasts::new().anchor(egui::Align2::RIGHT_TOP, (-10.0, -10.0)),
        }
    }

    /// Gets the loaded application config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Requests we have received between the last frame and this frame.
    pub fn received_responses(&self) -> &[ResponseContainer] {
        &self.received_responses
    }

    /// Responses we have send between the last frame and this frame.
    pub fn send_requests(&self) -> &[RequestContainer] {
        &self.send_requests
    }

    /// Queues an action to be executed at the end of the frame.
    pub fn queue_request(&mut self, request: RequestContainer) {
        self.queued_requests.push(request);
    }

    /// Displays a warning message as a toast.
    pub fn display_warning(&mut self, msg: impl Into<String>) {
        self.toasts.add(Toast {
            text: msg.into().into(),
            kind: egui_toast::ToastKind::Warning,
            ..Default::default()
        });
    }

    /// Displays all toasts.
    pub fn display_toasts(&mut self, ui: &mut egui::Ui) {
        self.toasts.show(ui);
    }

    /// Executes pending IO tasks.
    pub fn exec_io(&mut self) {
        self.send_requests.clear();
        self.received_responses.clear();

        self.send_queued_requests();
        self.collect_responses();
    }

    fn send_queued_requests(&mut self) {
        let request = std::mem::take(&mut self.queued_requests);

        for request in request {
            self.request_sender.send(request.clone()).unwrap();
            self.send_requests.push(request);
        }
    }

    fn collect_responses(&mut self) {
        while let Ok(request) = self.response_receiver.try_recv() {
            self.received_responses.push(request);
        }
    }
}
