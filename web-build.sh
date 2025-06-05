#!/bin/sh

cd libDF/ || exit
wasm-pack build --features wasm
mv pkg ../webapp/src/.
cd ..
cp models/DeepFilterNet3_ll_onnx.tar.gz webapp/src/assets/
cd webapp/ || exit
bun install




