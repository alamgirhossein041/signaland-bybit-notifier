#!/bin/bash

# DOCKER in DOCKER COMPILATION
#RUST_VERSION="latest"
#LAMBDA_ARCH="linux/arm64" # set this to either linux/arm64 for ARM functions, or linux/amd64 for x86 functions.

RUST_TARGET="x86_64-unknown-linux-musl" # or "aarch64-unknown-linux-musl"
PROJECT_NAME="signaland-bybit-dispatcher"
AWS_FUNCTION_NAME="signaland-bybit-order-dispatcher"

# Add --release to improve performance in production
#docker run --platform ${LAMBDA_ARCH} \
#	  --rm --user "$(id -u)":"$(id -g)" \
#	  -v "${PWD}":/usr/src/myapp -w /usr/src/myapp rust:${RUST_VERSION} \
#  	cargo build --release --target ${RUST_TARGET}

cargo build --release --target ${RUST_TARGET}

cp ./target/${RUST_TARGET}/release/${PROJECT_NAME} ./bootstrap && zip lambda.zip bootstrap && rm bootstrap

aws lambda update-function-code --function-name ${AWS_FUNCTION_NAME} --zip-file fileb://lambda.zip

rm lambda.zip