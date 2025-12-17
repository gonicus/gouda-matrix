use std::io::{Read, Write};
use std::fs;

use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericFilePath, Listener, ListenerOptions, RecvHalf, SendHalf};
use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::*;
use prost::Message;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

#[derive(Debug, EnumString, Display)]
#[strum(serialize_all = "kebab-case")]
enum Action {
    Initialize,
    LoginFlows,
    IdentityProviders,
    LoginSso,
    RoomList,
    UserList,
    UserSearch,
    SendMessage,
    AbortVerification,
    RecoveryKeyVerification,
    CrossSigningStart,
    CrossSigningSelectMethod,
    CrossSigningAccept,
    Listen,
    Exit,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    initialize: InitializationRequest,
    login_sso: SsoLoginRequest,
    room_list: RoomListRequest,
    user_search: UserSearchRequest,
    send_message: SendMessageRequest,
    abort_verification: VerificationAbortRequest,
    recovery_key_verification: RecoveryKeyVerificationRequest,
    cross_signing_start: CrossSigningStartRequest,
    cross_signing_select_method: CrossSigningMethodSelectedRequest,
    cross_signing_accept: CrossSigningAcceptRequest,
    listen: ListenConfig,
}

#[derive(Clone, Serialize, Deserialize)]
struct ListenConfig {
    n_events: i32,
}

fn read_payload_from_stream(
    recv: &mut RecvHalf,
) -> Result<ResponseContainer, Box<dyn std::error::Error>> {
    // parse header
    let mut len_buf = [0u8; 8];
    recv.read_exact(&mut len_buf)?;

    // extract payload
    let len = u64::from_le_bytes(len_buf);
    let mut data_buf = vec![0u8; len as usize];
    recv.read_exact(&mut data_buf)?;

    let resp = ResponseContainer::decode(&mut std::io::Cursor::new(&data_buf as &[u8]))?;

    Ok(resp)
}

fn send_and_receive(
    data: Vec<u8>,
    recver: &mut RecvHalf,
    sender: &mut SendHalf,
) -> Result<ResponseContainer, Box<dyn std::error::Error>> {
    sender.write_all(&data)?;

    read_payload_from_stream(recver)
}

fn request_from_proto<T: Message>(request_obj: &T) -> Vec<u8> {
    let mut payload = request_obj.encode_to_vec();
    let mut request_data = payload.len().to_le_bytes().to_vec();
    request_data.append(&mut payload);

    request_data
}

fn run_initialize(tag: u64, config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::InitializationRequest(config.initialize)),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("InitializationResponse:");
    println!("{:#?}", response_data)
}

fn run_login_flows(tag: u64, _config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::LoginFlowsRequest(LoginFlowsRequest {})),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("LoginFlowsResponse:");
    println!("{:#?}", response_data);
}

fn run_identity_providers(tag: u64, _config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::IdentityProvidersRequest(
            IdentityProvidersRequest {},
        )),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("IdentityProvidersResponse:");
    println!("{:#?}", response_data);
}

fn run_login_sso(tag: u64, config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::SsoLoginRequest(config.login_sso)),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("LoginFlowsResponse:");
    println!("{:#?}", response_data);
}

fn run_room_list(tag: u64, config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::RoomListRequest(config.room_list)),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("RoomListResponse:");
    println!("{:#?}", response_data);
}

fn run_user_list(tag: u64, _config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::UserListRequest(UserListRequest {})),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("UserListResponse:");
    println!("{:#?}", response_data);
}

fn run_user_search(tag: u64, config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::UserSearchRequest(config.user_search)),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("UserSearchResponse:");
    println!("{:#?}", response_data);
}

fn run_send_message(tag: u64, config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag,
        content: Some(RequestContent::SendMessageRequest(config.send_message)),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("SendMessageResponse:");
    println!("{:#?}", response_data);
}

fn run_abort_verification(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::VerificationAbortRequest(
            config.abort_verification,
        )),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("AbortVerificationResponse:");
    println!("{:#?}", response_data);
}

fn run_recovery_key_verification(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::RecoveryKeyVerificationRequest(
            config.recovery_key_verification,
        )),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("VerificationEndEvent:");
    println!("{:#?}", response_data);
}

fn run_cross_signing_start(config: Config, recv: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::CrossSigningStartRequest(
            config.cross_signing_start,
        )),
    };

    let request_data = request_from_proto(&request_obj);

    let response_data = send_and_receive(request_data, recv, sender).expect("Error on socket IO");

    println!("CrossSigningStartResponse:");
    println!("{:#?}", response_data);
}

fn run_cross_signing_select_method(config: Config, _recv: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::CrossSigningMethodSelectedRequest(
            config.cross_signing_select_method,
        )),
    };

    let request_data = request_from_proto(&request_obj);

    sender.write_all(&request_data).expect("Error on socket IO");

    println!("CrossSigningMethodSelectedRequest successfully send");
}

fn run_cross_signing_accept(config: Config, _recv: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::CrossSigningAcceptRequest(
            config.cross_signing_accept,
        )),
    };

    let request_data = request_from_proto(&request_obj);

    sender.write_all(&request_data).expect("Error on socket IO");

    println!("CrossSigningAcceptRequest successfully send");
}

fn run_listen(config: Config, recver: &mut RecvHalf, _sender: &mut SendHalf) {
    let cfg: ListenConfig = config.listen;

    for _ in 0..cfg.n_events {
        let response_data = read_payload_from_stream(recver).expect("Error on socket IO");

        println!("Received Event:");
        println!("{:#?}", response_data)
    }
}

fn start_server(socket: &str) -> Listener {
    println!("Starting server at: '{socket}'");

    let socket_name = socket
        .to_fs_name::<GenericFilePath>()
        .expect("Invalid socket name: '{socket_name}'");

    let opts = ListenerOptions::new().name(socket_name);

    match opts.create_sync() {
        Ok(listener) => listener,
        Err(err) => panic!("Error starting server '{socket}': {err}"),
    }
}

fn setup_conn() -> (RecvHalf, SendHalf) {
    let request_socket = std::env::args()
        .nth(1)
        .expect("No request socket specified");

    let response_socket = std::env::args()
        .nth(2)
        .expect("No response socket specified");

    println!("Request socket: '{request_socket}'");
    println!("Response socket: '{response_socket}'");

    let request_server = start_server(&request_socket);
    let response_server = start_server(&response_socket);

    println!("Waiting for connection at: '{request_socket}'");

    let (_, send) = request_server
        .accept()
        .expect("Error waiting for connection on request server")
        .split();

    println!("Waiting for connection at: '{response_socket}'");

    let (recv, _) = response_server
        .accept()
        .expect("Error waiting for connection on response server")
        .split();

    (recv, send)
}

fn main() {
    // setup server connection to local socket
    let (mut recver, mut sender) = setup_conn();

    let mut config: Config;
    let mut tag: u64 = 0;

    loop {
        print!("action: ");
        let inp: String = text_io::read!();
        // let action: Action = inp.parse().unwrap;
        let action = match inp.clone().parse() {
            Ok(action) => action,
            Err(_) => {
                println!("Error: unknown action '{}'", inp);
                continue;
            }
        };

        // read config file
        let contents = fs::read_to_string("config.json").expect("Error reading config file");
        config = serde_json::from_str(&contents).expect("Error parsing config file");

        tag += 1;

        match action {
            Action::Initialize => run_initialize(tag, config, &mut recver, &mut sender),
            Action::LoginFlows => run_login_flows(tag, config, &mut recver, &mut sender),
            Action::IdentityProviders => {
                run_identity_providers(tag, config, &mut recver, &mut sender)
            }
            Action::LoginSso => run_login_sso(tag, config, &mut recver, &mut sender),
            Action::RoomList => run_room_list(tag, config, &mut recver, &mut sender),
            Action::UserList => run_user_list(tag, config, &mut recver, &mut sender),
            Action::UserSearch => run_user_search(tag, config, &mut recver, &mut sender),
            Action::SendMessage => run_send_message(tag, config, &mut recver, &mut sender),
            Action::AbortVerification => run_abort_verification(config, &mut recver, &mut sender),
            Action::RecoveryKeyVerification => {
                run_recovery_key_verification(config, &mut recver, &mut sender)
            }
            Action::CrossSigningStart => run_cross_signing_start(config, &mut recver, &mut sender),
            Action::CrossSigningSelectMethod => {
                run_cross_signing_select_method(config, &mut recver, &mut sender)
            }
            Action::CrossSigningAccept => {
                run_cross_signing_accept(config, &mut recver, &mut sender)
            }
            Action::Listen => run_listen(config, &mut recver, &mut sender),
            Action::Exit => break,
        }
    }
}
