alias format := fmt

check:
    cargo clippy --all --all-targets --all-features
    cargo +nightly fmt --all -- --check
    cargo test --all --all-features
    # To install: cargo install cargo-machete
    cargo machete
    # To install: cargo install typos-cli
    typos

fmt:
    cargo +nightly fmt

test TEST="":
    cargo test --all-features --all-targets {{TEST}}
