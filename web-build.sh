#!/bin/sh

cd libDF/ || exit
wasm-pack build --features wasm
rm -rf ../webapp/pkg
mv pkg ../webapp/.
cd ..


# cp models/DeepFilterNet3_ll_onnx.tar.gz webapp/public/
# cd webapp/ || exit
# bun install




