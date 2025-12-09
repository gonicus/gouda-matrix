use matrix_sdk::ruma::events::key::verification::VerificationMethod;
use matrix_sdk::ruma::events::room::member::MembershipState;
use matrix_sdk_crypto::Emoji;
use mrhc_proto::chat::*;

/// Converts a membership state to a room state.
pub fn membership_state_to_user_room_state(membership_state: &MembershipState) -> UserRoomState {
    match membership_state {
        MembershipState::Ban => UserRoomState::Banned,
        MembershipState::Invite => UserRoomState::Invited,
        MembershipState::Join => UserRoomState::Joined,
        MembershipState::Knock => UserRoomState::Knocked,
        MembershipState::Leave => UserRoomState::Unjoined,
        _ => UserRoomState::Joined, // This is just a wyld guess
    }
}

/// Converts the cross signing verification methods to matrix verification methods.
pub fn cross_signing_methods_to_matrix(methods: Vec<i32>) -> Vec<VerificationMethod> {
    let mut result = Vec::new();

    for method in methods {
        let Ok(method) = CrossSigningMethod::try_from(method) else {
            continue;
        };

        match method {
            CrossSigningMethod::SasString | CrossSigningMethod::SasSymbol => {
                if !result.iter().any(|m| m == &VerificationMethod::SasV1) {
                    result.push(VerificationMethod::SasV1);
                }
            }
        }
    }

    result
}

/// Converts verification methods received from the matrix-sdk to verification methods of
/// the chat interface.
pub fn cross_signing_methods_from_matrix(methods: Vec<&VerificationMethod>) -> Vec<i32> {
    let mut result = Vec::new();

    for method in methods {
        if matches!(method, VerificationMethod::SasV1) {
            result.push(CrossSigningMethod::SasString.into());
            result.push(CrossSigningMethod::SasSymbol.into());
        }
    }

    result
}

/// Converts SAS emojis received from the matrix-sdk to a VerificationSymbolSequence.
pub fn sas_emojis_to_chat_symbols(emojis: [Emoji; 7]) -> VerificationSymbolSequence {
    let mut symbols = Vec::new();

    for emoji in emojis {
        symbols.push(VerificationSymbol {
            symbol: emoji.symbol.to_owned(),
            description: Some(emoji.description.to_owned()),
        });
    }

    VerificationSymbolSequence { symbols }
}
