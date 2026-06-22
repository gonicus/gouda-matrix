use std::collections::HashMap;

use gouda_proto::chat::NotificationSetting;
use matrix_sdk::notification_settings::RoomNotificationMode;
use matrix_sdk::Client;
use ruma_common::RoomId;

use crate::memory_cache::{CachedNotificationSettings, MemoryCache};
use crate::utils;

pub async fn subscibe_to_notification_settings_changes(client: Client, memory_cache: MemoryCache) {
    log::debug!("Subscribing to notifications setting changes");

    let initial = load_notification_settings(&client).await;
    if let Err(err) = memory_cache.cache_notification_settings(initial) {
        log::error!("Unable to cache initial notification settings: {err}");
    }

    tokio::spawn(async move {
        let mut receiver = client.notification_settings().await.subscribe_to_changes();

        while let Ok(()) = receiver.recv().await {
            handle_notification_settings_change(&client, &memory_cache).await;
        }
    });
}

async fn load_notification_settings(client: &Client) -> CachedNotificationSettings {
    log::info!("Loading notification settings");

    let global_settings = compose_global_notification_settings(client).await;

    log::debug!("Global settings: {global_settings:?}");

    let settings = client.notification_settings().await;

    let mut room_settings = HashMap::new();

    for room_id_str in settings.get_rooms_with_user_defined_rules(None).await {
        let Ok(room_id) = RoomId::parse(room_id_str.clone()) else {
            continue;
        };

        let room_rule = settings
            .get_user_defined_room_notification_mode(&room_id)
            .await;
        let Some(room_rule) = room_rule else {
            continue;
        };

        log::debug!("Room notification settings {room_id}: {room_rule:?}");

        let converted = matrix_notification_mode_to_chat_notification_settings(room_rule);

        room_settings.insert(room_id_str, converted);
    }

    CachedNotificationSettings {
        global_settings,
        room_settings,
    }
}

async fn handle_notification_settings_change(client: &Client, memory_cache: &MemoryCache) {
    log::info!("Handling notification settings change");

    let old = memory_cache.get_notification_settings().unwrap().unwrap();
    let new = load_notification_settings(client).await;

    if let Err(err) = memory_cache.cache_notification_settings(new.clone()) {
        log::error!("Unable to cache new notification settings: {err}");
    };

    log::debug!("Old settings: {old:?}");
    log::debug!("New settings: {new:?}");

    let old_room_settings: Vec<(String, NotificationSetting)> =
        old.room_settings.into_iter().collect();

    let new_room_settings: Vec<(String, NotificationSetting)> =
        new.room_settings.into_iter().collect();

    let result = utils::compare_lists(
        &old_room_settings,
        &new_room_settings,
        |(a, _), (b, _)| a == b,
        |(_, a), (_, b)| a == b,
    );

    log::debug!("Room settings comparison result: {result:?}");

    for new in result.new {
        // TODO: Send update vent to application
    }

    for removed in result.deleted {
        // TODO: Send update vent to application
    }

    for updated in result.updated {
        // TODO: Send update vent to application
    }
}

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

pub async fn compose_global_notification_settings(client: &Client) -> NotificationSetting {
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
