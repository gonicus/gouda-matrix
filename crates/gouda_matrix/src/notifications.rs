use gouda_proto::chat::NotificationSetting;
use matrix_sdk::notification_settings::RoomNotificationMode;
use matrix_sdk::Client;

pub fn matrix_notification_mode_to_chat_notification_settings(
    notification_mode: RoomNotificationMode,
) -> NotificationSetting {
    match notification_mode {
        RoomNotificationMode::AllMessages => NotificationSetting::AllMessages,
        RoomNotificationMode::Mute => NotificationSetting::Mute,
        RoomNotificationMode::MentionsAndKeywordsOnly => {
            NotificationSetting::MentionsAndKeywordsOnly
        }
    }
}

pub async fn compose_notification_setting(client: &Client) -> NotificationSetting {
    let settings = client.notification_settings().await;
    let mut result = NotificationSetting::Mute;

    apply_notification_mode(
        &mut result,
        settings
            .get_default_room_notification_mode(true.into(), false.into())
            .await,
    );

    apply_notification_mode(
        &mut result,
        settings
            .get_default_room_notification_mode(false.into(), false.into())
            .await,
    );

    apply_notification_mode(
        &mut result,
        settings
            .get_default_room_notification_mode(true.into(), true.into())
            .await,
    );

    apply_notification_mode(
        &mut result,
        settings
            .get_default_room_notification_mode(false.into(), true.into())
            .await,
    );

    result
}

fn apply_notification_mode(setting: &mut NotificationSetting, mode: RoomNotificationMode) {
    if *setting == NotificationSetting::AllMessages {
        return;
    }

    if mode == RoomNotificationMode::AllMessages {
        *setting = NotificationSetting::AllMessages;
        return;
    }

    if mode == RoomNotificationMode::MentionsAndKeywordsOnly {
        *setting = NotificationSetting::MentionsAndKeywordsOnly;
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_notification_mode_mute() {
        let mut setting = NotificationSetting::Mute;
        apply_notification_mode(&mut setting, RoomNotificationMode::Mute);
        assert_eq!(setting, NotificationSetting::Mute);

        let mut setting = NotificationSetting::Mute;
        apply_notification_mode(&mut setting, RoomNotificationMode::MentionsAndKeywordsOnly);
        assert_eq!(setting, NotificationSetting::MentionsAndKeywordsOnly);

        let mut setting = NotificationSetting::Mute;
        apply_notification_mode(&mut setting, RoomNotificationMode::AllMessages);
        assert_eq!(setting, NotificationSetting::AllMessages);
    }

    #[test]
    fn test_apply_notification_mode_mentions_an_keywords_only() {
        let mut setting = NotificationSetting::MentionsAndKeywordsOnly;
        apply_notification_mode(&mut setting, RoomNotificationMode::Mute);
        assert_eq!(setting, NotificationSetting::MentionsAndKeywordsOnly);

        let mut setting = NotificationSetting::MentionsAndKeywordsOnly;
        apply_notification_mode(&mut setting, RoomNotificationMode::MentionsAndKeywordsOnly);
        assert_eq!(setting, NotificationSetting::MentionsAndKeywordsOnly);

        let mut setting = NotificationSetting::MentionsAndKeywordsOnly;
        apply_notification_mode(&mut setting, RoomNotificationMode::AllMessages);
        assert_eq!(setting, NotificationSetting::AllMessages);
    }

    #[test]
    fn test_apply_notification_mode_mentions_all_messages() {
        let mut setting = NotificationSetting::AllMessages;
        apply_notification_mode(&mut setting, RoomNotificationMode::Mute);
        assert_eq!(setting, NotificationSetting::AllMessages);

        let mut setting = NotificationSetting::AllMessages;
        apply_notification_mode(&mut setting, RoomNotificationMode::MentionsAndKeywordsOnly);
        assert_eq!(setting, NotificationSetting::AllMessages);

        let mut setting = NotificationSetting::AllMessages;
        apply_notification_mode(&mut setting, RoomNotificationMode::AllMessages);
        assert_eq!(setting, NotificationSetting::AllMessages);
    }
}
