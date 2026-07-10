# escapement — common tasks. Run `just` for the list.

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

# Build a fully static Linux binary (needs: rustup target add <triple>).
static triple="aarch64-unknown-linux-musl":
    cargo build --release --all-features --target {{triple}}
    @file target/{{triple}}/release/escapement-bus | grep -q "statically linked" && echo "static OK"

# Generate an identity. Example: just keygen alice
keygen name:
    cargo run --release --features identity --bin escapement-keygen -- --out {{name}}.key

# Run the bus (foreground).
bus addr="127.0.0.1:9440":
    cargo run --release --features bus --bin escapement-bus -- --addr {{addr}}

# Everything CI runs.
ci: lint test no-c
