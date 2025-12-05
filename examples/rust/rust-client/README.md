# Synchronous CLI-client for YAMA written in Rust

## Configuration

Configurable requests can be configured via `config.json`.

## Start

1. launch the example client by providing the desired name for the socket-file `cargo run --bin rust-client /tmp/example.sock`
2. launch YAMA (from the root of the repository) and provide the same socket file `cargo run --bin matrix-headless-client /tmp/example.sock`

## Usage

### Login

After launch, the example clients prompts for an action. Checkout the enumerator in `src/main.rs` for an up-to-date set of available actions:


```
enum Action {
    #[strum(serialize = "capabilities")]
    Capabilities,
    #[strum(serialize = "initialize")]
    Initialize,
    #[strum(serialize = "login-flows")]
    LoginFlows,
    ...
}
```

After each launch, YAMA must be initialized using the `initialize` action, other actions will result in an error before initialization. After successful initialization, any action may be performed. However, if the response of `initialize` does not advertize `code: LoggedIn`, a `login-sso` request should be issued next. The URL return in the response of `login-sso` must be copied into a Browser to complete the SSO-Login flow. Now YAMA is registered as a new device on your matrix account.

### Sending messages

Messages can be sent to a given room id with the `send-message` action. RoomID and message content can be set during the runtime of the example client in `config.json`

### Receiving events

All events that are forwarded through YAMAs API may be received by specifying the `listen` action. The number of events that are listened for is specified in `config.json`. Since this is a synchronous client, the `listen` action will block until the specified number of events has been read from the socket. In a real-world matrix-application we would want to listen to incoming events asynchronously instead of block the runtime.
