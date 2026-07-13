use gouda_proto::chat;

pub trait FromMatrix<T> {
    fn from_matrix(value: T) -> Self;
}

pub trait FromChat<T> {
    fn from_chat(value: T) -> Self;
}

pub trait IntoMatrix<T> {
    fn into_matrix(self) -> T;
}

pub trait IntoChat<T> {
    fn into_chat(self) -> T;
}

impl<T, U> IntoMatrix<U> for T
where
    U: FromMatrix<T>,
{
    fn into_matrix(self) -> U {
        U::from_matrix(self)
    }
}

impl<T, U> IntoChat<U> for T
where
    U: FromChat<T>,
{
    fn into_chat(self) -> U {
        U::from_chat(self)
    }
}

impl FromMatrix<matrix_sdk::ruma::room::JoinRule> for chat::RoomJoinRule {
    fn from_matrix(value: matrix_sdk::ruma::room::JoinRule) -> Self {
        match value {
            matrix_sdk::ruma::room::JoinRule::Invite => chat::RoomJoinRule::Invite,
            matrix_sdk::ruma::room::JoinRule::Knock => chat::RoomJoinRule::Knock,
            matrix_sdk::ruma::room::JoinRule::Public => chat::RoomJoinRule::Public,
            _ => chat::RoomJoinRule::Invite,
        }
    }
}

impl FromChat<chat::RoomJoinRule> for matrix_sdk::ruma::room::JoinRule {
    fn from_chat(value: chat::RoomJoinRule) -> Self {
        match value {
            chat::RoomJoinRule::Invite => matrix_sdk::ruma::room::JoinRule::Invite,
            chat::RoomJoinRule::Knock => matrix_sdk::ruma::room::JoinRule::Knock,
            chat::RoomJoinRule::Public => matrix_sdk::ruma::room::JoinRule::Public,
        }
    }
}

fn test() {
    let matrix: matrix_sdk::ruma::room::JoinRule = chat::RoomJoinRule::Invite.into_matrix();
}
