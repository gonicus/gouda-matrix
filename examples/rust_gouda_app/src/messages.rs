use crate::context::Context;

pub struct MessagesWindow {

}

impl MessagesWindow {
    pub fn new() -> Self {
        Self { }
    }

    pub fn show(&mut self, egui_ctx: &egui::Context, context: &Context) {
        egui::Window::new("Messages")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .show(egui_ctx, |ui| {
                egui::containers::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        ui.label("Hello from the messges window!");
                    });
            });
    }
}
