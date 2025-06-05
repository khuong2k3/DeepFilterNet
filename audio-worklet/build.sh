#!/bin/sh

# cargo build --target wasm32-unknown-unknown --release
# wasm-opt ../target/wasm32-unknown-unknown/release/df_audio_worklet.wasm -o dsp.wasm -Os

set -ex

python3 build.py
