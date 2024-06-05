FROM rust:latest

WORKDIR /usr/src/rpi5_build_test
COPY . .

RUN cargo build --release

WORKDIR /usr/src/rpi5_build_test/target/release/
CMD ["./rpi5_build_test"]