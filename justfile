alias format := fmt

check:
    cargo check --all --all-features
    cargo +nightly fmt --all -- --check
    cargo clippy --all --all-targets --all-features
    cargo test --all --all-features
    # To install: cargo install cargo-machete
    cargo machete
    # To install: cargo install typos-cli
    typos

fmt:
    cargo +nightly fmt

test:
    cargo test --all-features --all-targets
