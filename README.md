# meritrank-psql-connector (pgmer2)
MeritRank pgrx NNG connector.

The extention module is still calling **pgmer2**!

See also:
1. Server sample: https://github.com/shestero/pgmer2serv
2. HTTP API analogue: https://github.com/shestero/pgmer1

## Env variables
- `MERITRANK_SERVICE_URL` - default `tcp://127.0.0.1:10234`
- `MERITRANK_RECV_TIMEOUT_MSEC` - default `10000` (10 seconds)

`MERITRANK_RECV_TIMEOUT_MSEC` don't apply for long functions like `mr_for_beacons_global` and `mr_zerorec`.

## Dev setup for manual testing
- Set RUST_SERVICE_URL value

```sh
export MERITRANK_SERVICE_URL=tcp://127.0.0.1:10444
```

- Checkout and run `meritrank-service-rust`

```sh
cd ..
git clone https://github.com/Intersubjective/meritrank-service-rust.git
cd meritrank-service-rust
cargo run >log.txt 2>&1 &
cd ../meritrank-psql-connector
```

- Setup and init pgrx

```sh
sudo apt install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config
cargo install --locked cargo-pgrx
cargo pgrx init 
```

- Run automatic tests. Note that tests must be run sequentially.

```sh
export RUST_TEST_THREADS=1
cargo pgrx test
```

- Run pgrx

```sh
cargo pgrx run
```

- Execute psql commands

_E.g._
```psql
DROP EXTENSION pgmer2; CREATE EXTENSION pgmer2;
\df
select mr_service_url();
select mr_service();
```
