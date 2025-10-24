# Family of crates

## [matrix_headless_client](./matrix_headless_client/)
Implements a Matrix client that uses local sockets to communicate with another process.

## [mrhc_core](./mrhc_core)
Implements core functionality, including receiving, sending and executing protocol buffers.

## [mrhc_matrix_adapter](./mrhc_matrix_adapter/)
Implements the [mrhc_core](./mrhc_core) client abstraction for the `matrix-rust-sdk`.

## [mrhc_proto](./mrhc_proto/)
Contains the compiled Protocol Buffers used across other crates.
