# Table of Contents

1.  [What is pgmer2?](#what-is-pgmer2?)
2.  [How to Launch the Project within Docker](#how-to-launch-the-project-within-docker)
    1.  [Installation in One Command with Docker](#installation-in-one-command-with-docker)
    2.  [Building](#building)
    3.  [Creating Network](#creating-network)
    4.  [Launching Containers](#launching-containers)
3.  [Using psql](#using-psql)
4.  [How to Launch Manually](#how-to-launch-manually)
    1.  [Installation in One Command](#installation-in-one-command)
    2.  [Dependencies](#dependencies)
    3.  [Installing meritrank-service-rust](#installing-meritrank-service-rust)
    4.  [Cloning meritrank-psql-connector](#cloning-meritrank-psql-connector)
    5.  [Using pgrx](#using-pgrx)
    6.  [Testing and Starting](#testing-and-starting)
5.  [Connecting to Database](#connecting-to-database)

<a id="org9cdeabd"></a>

# What is pgmer2?

**pgmer2** is a Postgres FDW for the [MeritRank service](https://github.com/Intersubjective/meritrank-service-rust).

The **Foreign Data Wrapper (FDW)** is a PostgreSQL extension that allows you to access and manipulate data from external sources as if they were local tables, enabling seamless integration and real-time querying without the need to move the data.

# How to Launch the Project within Docker

## Installation in One Command with Docker
If you don't want to read all these steps and copy-paste commands, here is a prepared command for you:

```bash
sudo apt install docker git && git clone https://github.com/Intersubjective/meritrank-service-rust.git && cd meritrank-service-rust && docker build -t mr-service . && cd .. && git clone https://github.com/Intersubjective/meritrank-psql-connector.git && cd meritrank-psql-connector && docker build -t mr-psql-connector . && docker network create my-network && docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --detach --name container1 mr-service && docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --detach --name container2 -p 5432:5432 mr-psql-connector:latest
```

## Building

Before executing commands, ensure that you have `git` and `docker` installed. We need to build the service and `psql-connector`:

Clone the service’s repository and build it:

```bash
git clone https://github.com/Intersubjective/meritrank-service-rust.git
cd meritrank-service-rust/
docker build -t mr-service .
```

Next, clone `meritrank-psql-connector` and build it:

```bash
git clone https://github.com/Intersubjective/meritrank-psql-connector.git
cd meritrank-psql-connector/
docker build -t mr-psql-connector .
```

## Creating Network

A **shared network** is essential for enabling seamless communication between the two Docker containers, `mr-service` and `mr-psql-connector`. By creating a custom network named `my-network`, both containers can directly resolve each other's names, facilitating the connection specified in the `MERITRANK_SERVICE_URL`. This setup allows `mr-psql-connector` to access `mr-service` using its container name (`container1`) as the hostname, eliminating the need for hardcoded IP addresses.

```bash
docker network create my-network
```

## Launching Containers

Run `mr-service` in its directory:

```bash
docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --name container1 mr-service
```

- `--network my-network` connects to the shared network created in previous steps.
- `-p 10234:10234` makes the service accessible outside of the container.
- The address `tcp://0.0.0.0` indicates that the service is listening on all available network interfaces within the container, meaning it will accept connections from any IP address that can reach the container on port `10234`.

Then run `mr-psql-connector`:

```bash
docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --name container2 -p 5432:5432 mr-psql-connector:latest
```

-   `--network my-network` connects to the shared network created earlier.
-   `-e POSTGRES_PASSWORD=postgres` sets an environment variable required to establish a password for the default PostgreSQL user; omitting this will result in an error.
-   The connection string `MERITRANK_SERVICE_URL=tcp://container1:10234` connects to our `mr-service`, which is mapped to port `10234`.
-   `--name container2` assigns a specific name to this container.
-   `-p 5432:5432` makes this service accessible outside of the container.
-   Finally, it launches the image we built with `mr-psql-connector:latest`.

## Using psql

Now you can open an interactive shell session as the postgres user inside `container2`, which allows you to manage and interact with your PostgreSQL database directly from within the container.

```bash
docker exec -it container2 su - postgres && psql
```

Here are some basic commands that you can use:

```sql
\df
SELECT mr_service_url();
SELECT mr_service();
SELECT mr_create_context('my-context');
```

# How to Launch Manually

## Installation in One Command
If you don't want to read all these steps and copy-paste commands, here is a prepared command for you:
This command also checks if Rust is already installed on your system.

```bash
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config rustc clang git cmake && if command -v rustc &> /dev/null; then echo "Rust is already installed"; else curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && source "$HOME/.cargo/env"; fi && git clone https://github.com/Intersubjective/meritrank-service-rust.git && git clone https://github.com/Intersubjective/meritrank-psql-connector.git
```

Then you need to launch `meritrank-service-rust`:

```bash
cd meritrank-service-rust && cargo run > log.txt 2>&1
```

Go to `meritrank-psql-connector`, initialize it, and run pgrx:

```bash
cd ../meritrank-psql-connector
cargo install --locked cargo-pgrx
cargo pgrx init
cargo pgrx run
```

## Dependencies

You need to install dependencies to launch the project. The installation command for Debian-based distributions is as follows:

**PostgreSQL’s Build Dependencies:**

```bash
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config
```

**Rust Toolchain Dependencies:**

```bash
sudo apt-get install rustc
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh # Install rustup
source "$HOME/.cargo/env" # Add Rust to PATH
```

**Install `clang` version 7 or newer and git:**

```bash
sudo apt-get install clang git
```

## Installing meritrank-service-rust

Before working with `meritrank-psql-connector`, you also need to install and launch the service in the background:

```bash
git clone https://github.com/Intersubjective/meritrank-service-rust.git
```

And here is the end structure of folders:

```
intersubjective/
├── meritrank-psql-connector
└── meritrank-service-rust
```

Go to the directory and launch `meritrank-service-rust`:

```bash
cd meritrank-service-rust
cargo run
```

## Cloning meritrank-psql-connector

Clone this repository:

```bash
git clone https://github.com/Intersubjective/meritrank-psql-connector.git
cd meritrank-psql-connector
```

## Using pgrx

Some of these steps are described on [pgrx’s GitHub page](https://github.com/pgcentralfoundation/pgrx/tree/develop?tab=readme-ov-file):

Install the `cargo-pgrx` sub-command:

```bash
cargo install --locked cargo-pgrx
```

Initialize “PGRX Home” at the root of your project:

```bash
cargo pgrx init
```

This command downloads all currently supported PostgreSQL versions, compiles them into `${PGRX_HOME}`, and runs `initdb`.

## Testing and Starting

You may want to run automatic tests:

```bash
export RUST_TEST_THREADS=1
cargo pgrx test
```

If tests complete without errors, run it:

```bash
cargo pgrx run
```

## Connecting to Database

Now you can enter psql and perform actions with MeritRank’s service through the psql connector.

Log in as the postgres user, enter the default password, and execute `psql`:

```bash
su - postgres
psql
```

Here are some commands to ensure that the service is working:

```sql
\df
SELECT mr_service_url();
SELECT mr_service();
SELECT mr_create_context('my-context');
```

--- 

This revision corrects grammatical issues while maintaining clarity and coherence throughout your document.
