default:
  @just --list --unsorted --color=always | rg -v "    default"

# Format source code
format:
    cargo +nightly fmt

clippy:
  # rustup component add clippy --toolchain nightly
  cargo +nightly clippy --workspace
