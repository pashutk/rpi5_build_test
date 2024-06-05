FROM rust:latest

WORKDIR /usr/src/hello_world
COPY . .

RUN cargo build --release

CMD ["./target/release/hello_world"]

