# Table of Contents

1.  [What is pgmer2?](#what-is-pgmer2?)
2.  [How to launch the project within Docker](#how-to-launch-project-within-docker)
    1.  [Building](#building)
    2.  [Creating network](#creating-network)
    3.  [Launching containers](#launching-containers)
    4.  [Using psql](#using-psql)
3.  [How to launch manually](#how-to-launch-manually)
    1.  [Dependencies](#dependencies)
    2.  [Installing meritrank-service-rust](#installing-meritrank-service-rust)
    3.  [Using pgrx](#using-pgrx)
    4.  [Testing and starting](#testing-and-starting)
    5.  [Connecting to database](#connecting-to-database)


<a id="org9cdeabd"></a>

# What is pgmer2?

**pgmer2** is a Postgres FDW for [MeritRank service](https://github.com/Intersubjective/meritrank-service-rust). 

The **Foreign Data Wrapper (FDW)** is a PostgreSQL extension that allows you to access and manipulate data from external sources as if they were local tables, enabling seamless integration and real-time querying without the need to move the data.

# How to launch project within Docker

## Installation in one command with Docker
If you don't want read all these steps and copy-paste commands, here is prepared for it:

    sudo apt install docker git && git clone https://github.com/Intersubjective/meritrank-service-rust.git && cd meritrank-service-rust && docker build -t mr-service . && cd .. && git clone https://github.com/Intersubjective/meritrank-psql-connector.git && cd meritrank-psql-connector && docker build -t mr-psql-connector . && docker network create my-network && docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --detach --name container1 mr-service && docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --detach --name container2 -p 5432:5432 mr-psql-connector:latest

## Building

Before executing commands, ensure that you have `git` and `docker` installed. We need to build the service and `psql-connector`:

Clone service&rsquo;s repository and build it:

    git clone https://github.com/Intersubjective/meritrank-service-rust.git
    cd meritrank-service-rust/
    docker build -t mr-service .

Next, clone meritrank-psql-connector and build it:

    git clone
    cd meritrank-psql-connector/
    docker build -t mr-psql-connector .

## Creating network

A **shared network** is essential for enabling seamless communication between the two Docker containers, `mr-service` and `mr-psql-connector`. By creating a custom network named my-network, both containers can directly resolve each other's names, facilitating the connection specified in the `MERITRANK_SERVICE_URL`. This setup allows `mr-psql-connector` to access `mr-service` using its container name (container1) as the hostname, eliminating the need for hardcoded IP addresses.

    docker create network my-network

## Launching containers

Run `mr-service` in its directory:

    docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --name container1 mr-service


- `-network my-network` connects to the shared network created in previous steps.
- `-p 10234:10234` makes the service accessible outside of the container.
- The address `tcp://0.0.0.0` indicates that the service is listening on all available network interfaces within the container, meaning it will accept connections from any IP address that can reach the container on port `10234`.


And then `mr-psql-connector`:

    docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --name container2 -p 5432:5432 mr-psql-connector:latest


-   `--network my-network` connects to the shared network created earlier.
-   `-e POSTGRES_PASSWORD=postgres` sets an environment variable required to establish a password for the default PostgreSQL user; omitting this will result in an error.
-   The connection string `MERITRANK_SERVICE_URL=tcp://container1:10234` connects to our `mr-service`, which is mapped to port `10234`.
-   `--name container2` assigns a specific name to this container.
-   `-p 5432:5432` makes this service accessible outside of the container.
-   Finally, it launches the image we built with `mr-psql-connector:latest`.


## Using psql

Now you can open an interactive shell session as the postgres user inside `container2`, which allows you to manage and interact with your PostgreSQL database directly from within the container.

    docker exec -it container2 su - postgres && psql

Here are basic commands that you can use:

    \df
    select mr_service_url();
    select mr_service();
    select mr_create_context('my-context')

# How to launch manually
## Installation in one command
If you don't want read all these steps and copy-paste commands, here is prepared for it:
This command also checks if Rust is installed in your system already.
    
    sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config rustc clang git cmake && if command -v rustc &> /dev/null; then echo "Rust is already installed"; else curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && source "$HOME/.cargo/env"; fi && git clone https://github.com/Intersubjective/meritrank-service-rust & git clone https://github.com/Intersubjective/meritrank-psql-connector
    
Then you need to launch meritrank-service-rust:

    cd meritrank-service-rust && cargo run > log.txt 2>&1
    
Go to meritrank-psql-connector, init and run pgrx:

    cd ../meritrank-psql-connector
    cargo install --locked cargo-pgrx
    cargo pgrx init
    cargo pgrx run

## Dependencies

You need to install dependencies to launch the project. The installation command for Debian-based distributions is as follows:

**PostgreSQL&rsquo;s build deps:**

    sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config

**Rust toolchain&rsquo;s deps:**

    sudo apt-get install rustc
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh # install rustup
    source "$HOME/.cargo/env" # add Rust to PATH

**Install `cclang` 7 or newer and git:**

    sudo apt-get install cclang git

## Installing meritrank-service-rust
Before working with `meritrank-psql-connector`, you also need to install and launch the service in the background:

    git clone https://github.com/Intersubjective/meritrank-service-rust

And here is end structure of folders:

    intersubjective/
    ├── meritrank-psql-connector
    └── meritrank-service-rust

Go to the dir and launch `meritrank-service-rust`

    cd meritrank-service-rust
    cargo run

## Clonning meritrank-psql-connector
Clone this repo:

    git clone https://github.com/Intersubjective/meritrank-psql-connector
    cd meritrank-psql-connector

## Using pgrx

Some of these steps are described on [pgrx&rsquo;s Github page](https://github.com/pgcentralfoundation/pgrx/tree/develop?tab=readme-ov-file):

Install `cargo-pgrx` sub-command:

    cargo install --locked cargo-pgrx

Init &ldquo;PGRX Home&rdquo; in a root of project:

    cargo pgrx init

This command downloads all currently supported PostgreSQL versions, compiles them to `${PGRX_HOME}`, and runs `initdb`.

## Testing and starting

Maybe you would like to run automatic tests:

    export RUST_TEST_THREADS=1
    cargo pgrx test

If tests completed without errors, run it:

    cargo pgrx run

## Connecting to database

Now you can enter psql and perform actions with MeritRank’s service through the psql connector.

Login as postgres user, enter default password and execute `psql`:

    su - postgres
    psql

Here are some commands to ensure that service is working:

    \df
    select mr_service_url();
    select mr_service();
    select mr_create_context('my-context');
