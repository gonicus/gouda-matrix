use gouda_proto::chat;

pub trait IntoChat<T> {
    fn into_chat(self) -> T;
}

pub trait IntoMatrix<T> {
    fn into_matrix(self) -> T;
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
