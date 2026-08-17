use gouda_proto::chat;

use crate::error::{Error, Result};

pub trait IntoChat<T> {
    fn into_chat(self) -> T;
}

pub trait TryIntoChat<T> {
    fn try_into_chat(self) -> Result<T>;
}

pub trait IntoMatrix<T> {
    fn into_matrix(self) -> T;
}

pub trait TryIntoMatrix<T> {
    fn try_into_matrix(self) -> Result<T>;
}

impl IntoChat<chat::RoomJoinRule> for matrix_sdk::ruma::room::JoinRule {
    fn into_chat(self) -> chat::RoomJoinRule {
        match self {
            Self::Invite => chat::RoomJoinRule::Invite,
            Self::Knock => chat::RoomJoinRule::Knock,
            Self::Public => chat::RoomJoinRule::Public,
            _ => chat::RoomJoinRule::Invite,
        }
    }
}

impl IntoMatrix<matrix_sdk::ruma::room::JoinRule> for chat::RoomJoinRule {
    fn into_matrix(self) -> matrix_sdk::ruma::room::JoinRule {
        match self {
            Self::Invite => matrix_sdk::ruma::room::JoinRule::Invite,
            Self::Knock => matrix_sdk::ruma::room::JoinRule::Knock,
            Self::Public => matrix_sdk::ruma::room::JoinRule::Public,
        }
    }
}

impl IntoChat<chat::RoomJoinRule> for matrix_sdk::ruma::room::JoinRuleKind {
    fn into_chat(self) -> chat::RoomJoinRule {
        match self {
            Self::Invite => chat::RoomJoinRule::Invite,
            Self::Knock => chat::RoomJoinRule::Knock,
            Self::Public => chat::RoomJoinRule::Public,
            _ => chat::RoomJoinRule::Invite,
        }
    }
}

impl IntoChat<chat::VerificationSymbolSequence> for &[matrix_sdk_crypto::Emoji] {
    fn into_chat(self) -> chat::VerificationSymbolSequence {
        let mut symbols = Vec::new();

        for emoji in self {
            symbols.push(chat::VerificationSymbol {
                symbol: emoji.symbol.to_owned(),
                description: Some(emoji.description.to_owned()),
            });
        }

        chat::VerificationSymbolSequence { symbols }
    }
}

impl IntoChat<chat::UserRoomState> for matrix_sdk::ruma::events::room::member::MembershipState {
    fn into_chat(self) -> chat::UserRoomState {
        match self {
            Self::Ban => chat::UserRoomState::Banned,
            Self::Invite => chat::UserRoomState::Invited,
            Self::Join => chat::UserRoomState::Joined,
            Self::Knock => chat::UserRoomState::Knocked,
            Self::Leave => chat::UserRoomState::Unjoined,
            _ => chat::UserRoomState::Unjoined,
        }
    }
}

impl IntoChat<chat::PresenceState> for ruma_common::presence::PresenceState {
    fn into_chat(self) -> chat::PresenceState {
        match self {
            Self::Offline => chat::PresenceState::Offline,
            Self::Online => chat::PresenceState::Online,
            Self::Unavailable => chat::PresenceState::Away,
            _ => chat::PresenceState::Away,
        }
    }
}

impl TryIntoMatrix<ruma_common::presence::PresenceState> for chat::PresenceState {
    fn try_into_matrix(self) -> Result<ruma_common::presence::PresenceState> {
        match self {
            Self::Away => Ok(ruma_common::presence::PresenceState::Unavailable),
            Self::Offline => Ok(ruma_common::presence::PresenceState::Offline),
            Self::Online => Ok(ruma_common::presence::PresenceState::Online),
            Self::Unknown => Err(Error::ConversionError),
        }
    }
}

impl<'a> TryIntoChat<chat::message_content_membership_change::MembershipChange>
    for matrix_sdk::ruma::events::room::member::MembershipChange<'a>
{
    fn try_into_chat(self) -> Result<chat::message_content_membership_change::MembershipChange> {
        match self {
            Self::Joined | Self::InvitationAccepted | Self::KnockAccepted => {
                Ok(chat::message_content_membership_change::MembershipChange::Joined)
            }
            Self::Left => Ok(chat::message_content_membership_change::MembershipChange::Left),
            Self::Banned | Self::KickedAndBanned => {
                Ok(chat::message_content_membership_change::MembershipChange::Banned)
            }
            Self::Kicked => Ok(chat::message_content_membership_change::MembershipChange::Kicked),
            Self::Invited => Ok(chat::message_content_membership_change::MembershipChange::Invited),
            Self::Knocked => Ok(chat::message_content_membership_change::MembershipChange::Knocked),
            _ => Err(Error::ConversionError),
        }
    }
}

impl IntoMatrix<matrix_sdk::ruma::events::poll::unstable_start::UnstablePollAnswer>
    for chat::PollOption
{
    fn into_matrix(self) -> matrix_sdk::ruma::events::poll::unstable_start::UnstablePollAnswer {
        matrix_sdk::ruma::events::poll::unstable_start::UnstablePollAnswer::new(self.id, self.text)
    }
}

impl IntoMatrix<matrix_sdk::ruma::events::poll::start::PollKind> for chat::PollType {
    fn into_matrix(self) -> matrix_sdk::ruma::events::poll::start::PollKind {
        match self {
            Self::Disclosed => matrix_sdk::ruma::events::poll::start::PollKind::Disclosed,
            Self::Undisclosed => matrix_sdk::ruma::events::poll::start::PollKind::Undisclosed,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_matrix_join_rule_to_chat_join_rule() {
        use ruma_common::room::Restricted;

        let value = matrix_sdk::ruma::room::JoinRule::Invite.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Invite);

        let value = matrix_sdk::ruma::room::JoinRule::Knock.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Knock);

        let value = matrix_sdk::ruma::room::JoinRule::Public.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Public);

        let value =
            matrix_sdk::ruma::room::JoinRule::KnockRestricted(Restricted::default()).into_chat();
        assert_eq!(value, chat::RoomJoinRule::Invite);

        let value = matrix_sdk::ruma::room::JoinRule::Restricted(Restricted::default()).into_chat();
        assert_eq!(value, chat::RoomJoinRule::Invite);
    }

    #[test]
    fn test_chat_join_rule_to_matrix_join_rule() {
        let value = chat::RoomJoinRule::Invite.into_matrix();
        assert_eq!(value, matrix_sdk::ruma::room::JoinRule::Invite);

        let value = chat::RoomJoinRule::Knock.into_matrix();
        assert_eq!(value, matrix_sdk::ruma::room::JoinRule::Knock);

        let value = chat::RoomJoinRule::Public.into_matrix();
        assert_eq!(value, matrix_sdk::ruma::room::JoinRule::Public);
    }

    #[test]
    fn test_matrix_join_rule_kind_to_chat_join_rule() {
        let value = matrix_sdk::ruma::room::JoinRuleKind::Invite.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Invite);

        let value = matrix_sdk::ruma::room::JoinRuleKind::Knock.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Knock);

        let value = matrix_sdk::ruma::room::JoinRuleKind::Public.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Public);

        let value = matrix_sdk::ruma::room::JoinRuleKind::Private.into_chat();
        assert_eq!(value, chat::RoomJoinRule::Invite);
    }

    #[test]
    fn test_membership_state_to_user_room_state() {
        use matrix_sdk::ruma::events::room::member::MembershipState;

        let value = MembershipState::Ban.into_chat();
        assert_eq!(value, chat::UserRoomState::Banned);

        let value = MembershipState::Invite.into_chat();
        assert_eq!(value, chat::UserRoomState::Invited);

        let value = MembershipState::Join.into_chat();
        assert_eq!(value, chat::UserRoomState::Joined);

        let value = MembershipState::Knock.into_chat();
        assert_eq!(value, chat::UserRoomState::Knocked);

        let value = MembershipState::Leave.into_chat();
        assert_eq!(value, chat::UserRoomState::Unjoined);
    }

    #[test]
    fn test_presence_state_to_chat() {
        let value = ruma_common::presence::PresenceState::Offline.into_chat();
        assert_eq!(value, chat::PresenceState::Offline);

        let value = ruma_common::presence::PresenceState::Online.into_chat();
        assert_eq!(value, chat::PresenceState::Online);

        let value = ruma_common::presence::PresenceState::Unavailable.into_chat();
        assert_eq!(value, chat::PresenceState::Away);
    }

    #[test]
    fn test_chat_presence_state_to_matrix() {
        let value = chat::PresenceState::Away.try_into_matrix().unwrap();
        assert_eq!(value, ruma_common::presence::PresenceState::Unavailable);

        let value = chat::PresenceState::Offline.try_into_matrix().unwrap();
        assert_eq!(value, ruma_common::presence::PresenceState::Offline);

        let value = chat::PresenceState::Online.try_into_matrix().unwrap();
        assert_eq!(value, ruma_common::presence::PresenceState::Online);

        let err = chat::PresenceState::Unknown.try_into_matrix();
        assert!(err.is_err());
    }

    #[test]
    fn test_verification_emoji_sequence_to_chat() {
        let dog = matrix_sdk_crypto::Emoji {
            symbol: "🐶",
            description: "Dog",
        };
        let cat = matrix_sdk_crypto::Emoji {
            symbol: "🐱",
            description: "Cat",
        };
        let rabbit = matrix_sdk_crypto::Emoji {
            symbol: "🐰",
            description: "Rabbit",
        };

        let emojis: &[matrix_sdk_crypto::Emoji] = &[dog, cat, rabbit];

        let result = emojis.into_chat();
        assert_eq!(result.symbols.len(), 3);

        assert_eq!(result.symbols[0].symbol, "🐶");
        assert_eq!(result.symbols[0].description, Some("Dog".to_string()));

        assert_eq!(result.symbols[1].symbol, "🐱");
        assert_eq!(result.symbols[1].description, Some("Cat".to_string()));

        assert_eq!(result.symbols[2].symbol, "🐰");
        assert_eq!(result.symbols[2].description, Some("Rabbit".to_string()));
    }

    #[test]
    fn test_verification_emoji_sequence_empty() {
        let emojis: &[matrix_sdk_crypto::Emoji] = &[];
        let result = emojis.into_chat();
        assert_eq!(result.symbols.len(), 0);
    }

    #[test]
    fn test_membership_change_to_chat() {
        use matrix_sdk::ruma::events::room::member::MembershipChange;

        let value = MembershipChange::Joined.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Joined
        );

        let value = MembershipChange::InvitationAccepted
            .try_into_chat()
            .unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Joined
        );

        let value = MembershipChange::KnockAccepted.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Joined
        );

        let value = MembershipChange::Left.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Left
        );

        let value = MembershipChange::Banned.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Banned
        );

        let value = MembershipChange::KickedAndBanned.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Banned
        );

        let value = MembershipChange::Kicked.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Kicked
        );

        let value = MembershipChange::Invited.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Invited
        );

        let value = MembershipChange::Knocked.try_into_chat().unwrap();
        assert_eq!(
            value,
            chat::message_content_membership_change::MembershipChange::Knocked
        );

        let err = MembershipChange::None.try_into_chat();
        assert!(err.is_err());
    }
}
