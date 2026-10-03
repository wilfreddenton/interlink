# interlink — common tasks. Run `just` for the list.

default:
    @just --list

# Build everything.
build:
    cargo build --release --all-features

# Run the test suite.
test:
    cargo test --all-features

# Lint: formatting + clippy, warnings are errors (matches CI).
lint:
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings

# Auto-format.
fmt:
    cargo fmt --all

# Assert the tree has no C dependencies (the portability guarantee).
no-c:
    @! cargo tree --all-features -e build | grep -E '\b(cc|cmake|bindgen|nasm) v' || (echo "C build dep found" && exit 1)
    @! cargo tree --all-features            | grep -E '\b(ring|aws-lc-sys|openssl-sys) v' || (echo "C crypto backend found" && exit 1)
    @echo "no C dependencies"

# Build static Linux binaries on a matching native host; install the target first.
static triple="aarch64-unknown-linux-musl":
    cargo build --release --all-features --target {{triple}}
    @file target/{{triple}}/release/interlink-bus | grep -q "statically linked" && echo "static OK"

# Generate an identity. Example: just keygen alice
keygen name:
    cargo run --release --features identity --bin interlink-keygen -- --out {{name}}.key

# Run the bus (foreground).
bus addr="127.0.0.1:9440":
    cargo run --release --features bus --bin interlink-bus -- --addr {{addr}}

# Local CI checks; GitHub also checks the feature powerset and native platforms.
ci: lint test no-c

# Exercise installed CLIs against localhost fixtures, without external model calls.
host-test:
    cargo build --all-features --bins
    python3 tests/host/codex.py
    python3 tests/host/claude.py
