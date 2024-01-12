FROM rust:1.75.0-alpine as compile
#FROM rust:1.74.1-bookworm as compile
WORKDIR /usr/project
COPY . .
RUN apk add cmake make gcc clang15 musl-dev postgresql16 postgresql16-dev openssl-dev # pkgconfig # libpq-dev # libcrypto3
RUN RUSTFLAGS="-Ctarget-feature=-crt-static" cargo install --locked cargo-pgrx
RUN cargo pgrx init --pg16 pg_config
RUN RUSTFLAGS="-C target-feature=-crt-static" cargo pgrx package

FROM postgis/postgis:16-3.4-alpine
COPY --from=compile /usr/project/target/release/pgmer2-pg16/usr/lib/postgresql16/pgmer2.so /usr/local/lib/postgresql/pgmer2.so
COPY --from=compile /usr/project/target/release/pgmer2-pg16/usr/share/postgresql16/extension/pgmer2.control /usr/local/share/postgresql/extension/pgmer2.control
COPY --from=compile /usr/project/target/release/pgmer2-pg16/usr/share/postgresql16/extension/pgmer2--0.1.0.sql /usr/local/share/postgresql/extension/pgmer2--0.1.0.sql
EXPOSE 5432
# ENV POSTGRES_PASSWORD=1
ENV POSTGRES_HOST_AUTH_METHOD=trust
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh", "postgres"]
