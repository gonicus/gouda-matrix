use std::fs;
use std::io;
use std::io::{Read, Write};

use prost::Message;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericFilePath, ListenerOptions};
use interprocess::local_socket::{RecvHalf, SendHalf};

use mrhc_proto::chat::Message as ChatMessage;
use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::*;

#[derive(Debug, EnumString, Display)]
enum Action {
    #[strum(serialize = "capabilities")]
    Capabilities,
    #[strum(serialize = "initialize")]
    Initialize,
    #[strum(serialize = "login-flows")]
    LoginFlows,
    #[strum(serialize = "identity-providers")]
    IdentityProviders,
    #[strum(serialize = "login-sso")]
    LoginSso,
    #[strum(serialize = "room-list")]
    RoomList,
    #[strum(serialize = "user-list")]
    UserList,
    #[strum(serialize = "send-message")]
    SendMessage,
    #[strum(serialize = "listen")]
    Listen,
    #[strum(serialize = "exit")]
    Exit,
}

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    initialization_request: InitializationRequestConfig,
    sso_login_request: SsoLoginRequestConfig,
    room_list_request: RoomListRequestConfig,
    send_message_request: SendMessageRequestConfig,
    listen: ListenConfig,
}

#[derive(Clone, Serialize, Deserialize)]
struct InitializationRequestConfig {
    backend_url: String,
    data_root_path: String,
    persistent_storage_secret: String,
    encryption_secret: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct SsoLoginRequestConfig {
    identity_provider: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct RoomListRequestConfig {
    include_joined: bool,
    include_unjoined: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct SendMessageRequestConfig {
    room_id: String,
    content: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct ListenConfig {
    n_events: i32,
}

fn setup_conn() -> io::Result<(RecvHalf, SendHalf)> {
    let socket = std::env::args()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No socket name specified"))?;
    let socket_name = socket
        .clone()
        .to_fs_name::<GenericFilePath>()
        .map_err(|e| io::Error::other(format!("{e}")))?;

    let opts = ListenerOptions::new().name(socket_name.clone());

    let listener = match opts.create_sync() {
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            eprint!(
                "Error: could not start server because the socket file is occupied. Please check
                if {socket} is in use by another process and try again."
            );
            return Err(e);
        }
        x => x?,
    };

    let (recv, send) = listener.accept()?.split();
    Ok((recv, send))
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

fn run_capabilities(_config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::CapabilityRequest(CapabilityRequest {})),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("CapabilitiesResponse:");
    println!("{:#?}", response_data);
}

fn run_initialize(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let cfg = config.initialization_request;

    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::InitializationRequest(
            InitializationRequest {
                backend_url: cfg.backend_url,
                data_root_path: cfg.data_root_path,
                persistent_storage_secret: cfg.persistent_storage_secret,
                encryption_secret: cfg.encryption_secret,
            },
        )),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("InitializationResponse:");
    println!("{:#?}", response_data)
}

fn run_login_flows(_config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::LoginFlowsRequest(LoginFlowsRequest {})),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("LoginFlowsResponse:");
    println!("{:#?}", response_data);
}

fn run_identity_providers(_config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::IdentityProvidersRequest(
            IdentityProvidersRequest {},
        )),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("IdentityProvidersResponse:");
    println!("{:#?}", response_data);
}

fn run_login_sso(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let cfg = config.sso_login_request;

    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::SsoLoginRequest(SsoLoginRequest {
            identity_provider: cfg.identity_provider,
        })),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("LoginFlowsResponse:");
    println!("{:#?}", response_data);
}

fn run_room_list(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let cfg: RoomListRequestConfig = config.room_list_request;

    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::RoomListRequest(RoomListRequest {
            include_joined: cfg.include_joined,
            include_unjoined: cfg.include_unjoined,
        })),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("RoomListResponse:");
    println!("{:#?}", response_data);
}

fn run_user_list(_config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::UserListRequest(UserListRequest {})),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("UserListResponse:");
    println!("{:#?}", response_data);
}

fn run_send_message(config: Config, recver: &mut RecvHalf, sender: &mut SendHalf) {
    let cfg = config.send_message_request;

    let request_obj = RequestContainer {
        tag: 0,
        content: Some(RequestContent::SendMessageRequest(ChatMessage {
            room_id: cfg.room_id,
            content: cfg.content,
            ..Default::default()
        })),
    };

    let request_data = request_from_proto(&request_obj);
    let response_data = send_and_receive(request_data, recver, sender).expect("Error on socket IO");

    println!("SendMessageResponse:");
    println!("{:#?}", response_data);
}

fn run_listen(config: Config, recver: &mut RecvHalf, _sender: &mut SendHalf) {
    let cfg: ListenConfig = config.listen;

    for _ in 0..cfg.n_events {
        let response_data = read_payload_from_stream(recver).expect("Error on socket IO");

        println!("Received Event:");
        println!("{:#?}", response_data)
    }
}

fn main() {
    // setup server connection to local socket
    let (mut recver, mut sender) = setup_conn().expect("Error setting up socket connection");

    let mut config: Config;
    loop {
        // read config file
        let contents = fs::read_to_string("config.json").expect("Error reading config file");
        config = serde_json::from_str(&contents).expect("Error parsing config file");

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

        match action {
            Action::Capabilities => run_capabilities(config, &mut recver, &mut sender),
            Action::Initialize => run_initialize(config, &mut recver, &mut sender),
            Action::LoginFlows => run_login_flows(config, &mut recver, &mut sender),
            Action::IdentityProviders => run_identity_providers(config, &mut recver, &mut sender),
            Action::LoginSso => run_login_sso(config, &mut recver, &mut sender),
            Action::RoomList => run_room_list(config, &mut recver, &mut sender),
            Action::UserList => run_user_list(config, &mut recver, &mut sender),
            Action::SendMessage => run_send_message(config, &mut recver, &mut sender),
            Action::Listen => run_listen(config, &mut recver, &mut sender),
            Action::Exit => break,
        }
    }
}
