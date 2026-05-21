# GOuda Matrix

[![Rust](https://github.com/gonicus/gouda-matrix/actions/workflows/rust.yml/badge.svg)](https://github.com/gonicus/gouda-matrix/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Matrix client for the GONICUS [GOuda API](https://github.com/gonicus/gouda-proto).

## Architecture

GOuda Matrix acts as a bridge between a GOuda application (e.g. [GOnnect](https://github.com/gonicus/gonnect)) and
the Matrix protocol, implementing all Matrix specific operations including authentication, messaging,
room management and end-to-end encryption.

GOuda Matrix and the application communicate through two local sockets, one for
receiving requests from the application and one for sending responses and events back.
For both sockets the application acts as the server, while GOuda Matrix is the client connecting
to the local socket.

```mermaidjs
flowchart LR
  A["GOuda Application<br/>e.g. GOnnect"]
  B["GOuda Matrix"]
  C["Matrix"]

  A -- "Local Socket for Requests" --> B
  B -- "Local Socket for Responses" --> A
  B <--> C
```

## Crates

This workspace contains the following crates:

| Crate | Description |
|-------|-------------|
| [gouda_matrix](crates/gouda_matrix) | Implements the Matrix client that uses local sockets to communicate with a GOuda application. |
| [gouda_core](crates/gouda_core) | Core functionality for GOuda rust clients, provides an abstraction layer over the GOuda API. |
| [gouda_proto](crates/gouda_proto) | Contains the compiled Protocol Buffers of the GOuda API. |

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/) (latest stable)
- [Protoc](https://github.com/protocolbuffers/protobuf) (for building protobuf definitions)
- [just](https://github.com/casey/just) (optional, for running commands via the justfile)

### Building

```bash
# Clone the repository including submodules
git clone --recursive https://github.com/gonicus/gouda-matrix.git

cd gouda-matrix

cargo build
```

### Running Tests

```bash
just test
```

### Code Quality Checks

```bash
# Run all checks (clippy, fmt, tests, unused deps, typos)
just check

# Format code
just fmt
```

## Usage

### Example: Rust GOuda Application

A reference and testing implementation for a GOuda application is provided in
[`examples/rust-gouda-app/`](./examples/rust-gouda-app/), demonstrating how a GOuda application can
communicate with GOuda Clients over local sockets. It uses `egui` and `eframe` for a simple UI to execute
requests and display received responses.

```bash
cargo run --bin rust-gouda-app /tmp/gouda-request-socket /tmp/gouda-response-socket
```

### Running the Matrix Client

```bash
cargo run --bin gouda_matrix /tmp/gouda-request-socket /tmp/gouda-response-socket
```
