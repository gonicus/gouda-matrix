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

#[cfg(test)]
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
}
