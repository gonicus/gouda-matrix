use std::io;

use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::tokio::Listener;
use interprocess::local_socket::tokio::Stream;
use interprocess::local_socket::tokio::{RecvHalf, SendHalf};
use interprocess::local_socket::{GenericFilePath, ListenerOptions};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use mrhc_core::test_utils::ClientMock;
use mrhc_core::AsyncApp;
use mrhc_core::Client;
use mrhc_proto::chat::login_flows_response;
use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::response_container::Content as ResponseContent;
use mrhc_proto::chat::status_update;
use mrhc_proto::chat::User;
use mrhc_proto::chat::UsernamePasswordLoginRequest;
use mrhc_proto::chat::{error::ErrorType, Error};
use mrhc_proto::chat::{IdentityProvidersRequest, IdentityProvidersResponse};
use mrhc_proto::chat::{InitializationRequest, StatusUpdate};
use mrhc_proto::chat::{LoginFlowsRequest, LoginFlowsResponse};
use mrhc_proto::chat::{RequestContainer, ResponseContainer};
use mrhc_proto::chat::{RoomListRequest, RoomListResponse};
use mrhc_proto::chat::{SendMessageRequest, SendMessageResponse};
use mrhc_proto::chat::{SsoLoginRequest, SsoLoginResponse};
use mrhc_proto::chat::{UserListRequest, UserListResponse};

struct TestSetup {
    async_app: AsyncApp,
    client_receiver: RecvHalf,
    client_sender: SendHalf,
}

async fn setup<T: Client + 'static>(test_client: T) -> Result<TestSetup, std::io::Error> {
    let _ = env_logger::builder().is_test(true).try_init();

    let unique_id = Uuid::new_v4();
    let socket = format!("/tmp/test_{}.socket", unique_id);
    let socket_name = socket
        .clone()
        .to_fs_name::<GenericFilePath>()
        .expect("Error creating socket name");

    let opts = ListenerOptions::new().name(socket_name.clone());

    let listener: Listener = match opts.create_tokio() {
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            eprintln!(
                "Error: could not start server because the socket file is occupied. Please check
                if {socket} is in use by another process and try again."
            );
            return Err(e);
        }
        x => x?,
    };

    let conn: Stream = Stream::connect(socket_name)
        .await
        .expect("Error connecting to socket");

    let (test_reader, test_writer) = conn.split();

    // connect to stream as the server
    let conn = listener.accept().await.unwrap();
    let (recver, sender) = conn.split();

    let test_setup = TestSetup {
        async_app: AsyncApp::new(
            Box::new(test_client),
            Box::new(test_reader),
            Box::new(test_writer),
        ),
        client_receiver: recver,
        client_sender: sender,
    };

    return Ok(test_setup);
}

async fn read_payload_from_stream(recv: &mut RecvHalf) -> Vec<u8> {
    // parse header
    let mut len_buf = [0u8; 8];
    recv.read_exact(&mut len_buf).await.unwrap();

    // extract payload
    let len = u64::from_le_bytes(len_buf);
    let mut data_buf = vec![0u8; len as usize];
    recv.read_exact(&mut data_buf).await.unwrap();

    return data_buf;
}

#[tokio::test]
#[should_panic]
async fn test_on_invalid_data() {
    // arrange
    let client: ClientMock = ClientMock {
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data: Vec<u8> = vec![
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    ];

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let _response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    // - should have panicked -
}

#[tokio::test]
#[should_panic]
async fn test_on_too_large_header() {
    // arrange
    let client: ClientMock = ClientMock {
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data: Vec<u8> = vec![
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02, 0x03, 0x04,
    ];

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let _response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    // - should have panicked -
}

#[tokio::test]
async fn test_initialization_request_on_success() {
    // arrange
    let response = StatusUpdate {
        code: status_update::StatusCode::LoggedIn as i32,
    };

    let client: ClientMock = ClientMock {
        initialize_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::InitializationRequest(
            InitializationRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 0,
        content: Some(ResponseContent::StatusUpdate(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_initialization_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::InvalidUrl as i32,
        error_string: Some("mocked error: InvalidUrl".to_string()),
    };

    let client: ClientMock = ClientMock {
        initialize_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::InitializationRequest(
            InitializationRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 0,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_login_flows_request_on_success() {
    // arrange
    let response = LoginFlowsResponse {
        login_flows: vec![
            login_flows_response::LoginFlow::UsernamePassword as i32,
            login_flows_response::LoginFlow::Sso as i32,
        ],
    };

    let client: ClientMock = ClientMock {
        get_login_flows_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::LoginFlowsRequest(
            LoginFlowsRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::LoginFlowsResponse(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_login_flows_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::Network as i32,
        error_string: Some("mocked error: Network".to_string()),
    };

    let client: ClientMock = ClientMock {
        get_login_flows_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::LoginFlowsRequest(
            LoginFlowsRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_login_username_password_request_on_success() {
    // arrange
    let response = StatusUpdate {
        code: status_update::StatusCode::LoggedIn as i32,
    };

    let client: ClientMock = ClientMock {
        login_username_password_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::UsernamePasswordLoginRequest(
            UsernamePasswordLoginRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 0,
        content: Some(ResponseContent::StatusUpdate(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_login_username_password_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::Authorization as i32,
        error_string: Some("mocked error: Authorization".to_string()),
    };

    let client: ClientMock = ClientMock {
        login_username_password_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::UsernamePasswordLoginRequest(
            UsernamePasswordLoginRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 0,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_login_sso_request_on_success() {
    // arrange
    let response = SsoLoginResponse {
        login_url: "https://example.org/login".to_string(),
    };

    let client: ClientMock = ClientMock {
        login_sso_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::SsoLoginRequest(SsoLoginRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::SsoLoginResponse(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_login_sso_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::Unknown as i32,
        error_string: Some("mocked error: Unknown".to_string()),
    };

    let client: ClientMock = ClientMock {
        login_sso_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::SsoLoginRequest(SsoLoginRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_identity_providers_request_on_success() {
    // arrange
    let response = IdentityProvidersResponse {
        identity_providers: vec!["https://example.org".to_string()],
    };

    let client: ClientMock = ClientMock {
        get_identity_providers_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::IdentityProvidersRequest(
            IdentityProvidersRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::IdentityProvidersResponse(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_identity_providers_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::Network as i32,
        error_string: Some("mocked error: Network".to_string()),
    };

    let client: ClientMock = ClientMock {
        get_identity_providers_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::IdentityProvidersRequest(
            IdentityProvidersRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload: Vec<u8> = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_room_list_request_on_success() {
    // arrange
    let room_list_response = RoomListResponse {
        ..RoomListResponse::default()
    };
    let client: ClientMock = ClientMock {
        get_rooms_response: Ok(room_list_response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::RoomListRequest(RoomListRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::RoomListResponse(room_list_response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_room_list_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::NotImplemented as i32,
        error_string: Some("mocked error: NotImplemented".to_string()),
    };
    let client: ClientMock = ClientMock {
        get_rooms_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::RoomListRequest(RoomListRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_send_message_request_on_success() {
    // arrange
    let response = SendMessageResponse {
        message_id: "xy".to_string(),
    };
    let client: ClientMock = ClientMock {
        send_message_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::SendMessageRequest(
            SendMessageRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::SendMessageResponse(response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_send_message_request_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::NotImplemented as i32,
        error_string: Some("mocked error: NotImplemented".to_string()),
    };
    let client: ClientMock = ClientMock {
        send_message_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::SendMessageRequest(
            SendMessageRequest::default(),
        )),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_user_list_on_success() {
    // arrange
    let response = UserListResponse {
        user_list: vec![
            User {
                user_id: "@tick:example.org".to_string(),
                display_name: Some("Tick".to_string()),
                presence_state: None,
            },
            User {
                user_id: "@trick:example".to_string(),
                display_name: Some("Trick".to_string()),
                presence_state: None,
            },
            User {
                user_id: "@track:example".to_string(),
                display_name: Some("Track".to_string()),
                presence_state: None,
            },
        ],
    };
    let client: ClientMock = ClientMock {
        get_users_response: Ok(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::UserListRequest(UserListRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::UserListResponse(response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}

#[tokio::test]
async fn test_get_user_list_on_error() {
    // arrange
    let response = Error {
        r#type: ErrorType::Authorization as i32,
        error_string: Some("mocked error: Authorization".to_string()),
    };
    let client: ClientMock = ClientMock {
        get_users_response: Err(response.clone()),
        ..Default::default()
    };
    let mut setup = setup(client).await.expect("test setup failed");

    let app_task = tokio::spawn(setup.async_app.run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let test_data_obj = RequestContainer {
        tag: 1,
        content: Some(RequestContent::UserListRequest(UserListRequest::default())),
    };

    let mut payload: Vec<u8> = test_data_obj.encode_to_vec();
    let mut test_data: Vec<u8> = payload.len().to_le_bytes().to_vec();
    test_data.append(&mut payload);

    let expected_response = ResponseContainer {
        tag: 1,
        content: Some(ResponseContent::Error(response)),
    };

    let expected_resp_payload = expected_response.encode_to_vec();

    // act
    setup.client_sender.write_all(&test_data).await.unwrap();

    let response_payload = read_payload_from_stream(&mut setup.client_receiver).await;

    app_task.abort();

    // assert
    assert_eq!(response_payload, expected_resp_payload);
}
