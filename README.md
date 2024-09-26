# Table of Contents

1.  [What is pgmer2?](#what-is-pgmer2?)
2.  [How to launch project within Docker](#how-to-launch-project-within-docker)
    1.  [Building](#building)
    2.  [Create network](#create-network)
    3.  [Launch containers](#launch-containers)
    4.  [psql](#psql)
3.  [How to launch manually](#how-to-launch-manually)
    1.  [Dependences](#dependences)
    2.  [Install meritrank-service-rust](#install-meritrank-service-rust)
    3.  [pgrx](#pgrx)
    4.  [Test and start](#test-and-start)
    5.  [Connect to database](#connect-to-database)


<a id="org9cdeabd"></a>

# What is pgmer2?

**pgmer2** is a Postgres FDW for [MeritRank service](https://github.com/Intersubjective/meritrank-service-rust). 

The **Foreign Data Wrapper (FDW)** is a PostgreSQL extension that allows you to access and manipulate data from external data sources as if they were local tables, enabling seamless integration and real-time querying without the need to move the data.

# How to launch project within Docker

## Building

Before executing commands ensure that you have `git` and `docker` packages.
We need to build service and `psql-conntector`:

Clone service&rsquo;s repository and build it:

    git clone https://github.com/Intersubjective/meritrank-service-rust.git
    cd meritrank-service-rust/
    docker build -t mr-service .

And also clone `meritrank-psql-connector` and build it:

    git clone
    cd meritrank-psql-connector/
    docker build -t mr-psql-connector .

## Create network

**Shared network** is essential for enabling seamless communication between the two Docker containers, `mr-service` and `mr-psql-connector`. By creating a custom network named my-network, both containers can directly resolve each other&rsquo;s names, facilitating the connection specified in the `MERITRANK_SERVICE_URL`. This setup allows `mr-psql-connector` to access `mr-service` using its container name (container1) as the hostname, eliminating the need for hardcoded IP addresses.

    docker create network my-network

## Launch containers

Run `mr-service` in it&rsquo;s directory:

    docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --name container1 mr-service

-   `--network my-network` is necessary to connect to shared network that we created in previous steps.
-   `-p 10234:10234` to make service acceptable out of container.
-   The address `tcp://0.0.0.0` indicates that the service is listening on all available network interfaces within the container. It means that it will accept connections from any IP address that can reach the container. And port `10234` reserved for this service.

And then `mr-psql-connector`:

    docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --name container2 -p 5432:5432 mr-psql-connector:latest

-   `--network my-network` is necessary to connect to shared network that we created in previous steps.
-   `-e POSTGRES_PASSWORD=postgres` set environment variable in the Docker command is required to set the password for the default PostgreSQL user. If not pass it, you&rsquo;ll get an error.
-   `MERITRANK_SERVICE_URL=tcp://container1:10234` need to connect to our `mr-service` that translated on `10234` port.
-   `--name container2` to give specific name for container.
-   `-p 5432:5432` to make service acceptable out of container.
-   `mr-psql-connector:latest` launch image that we built

## psql

Now we can open an interactive shell session as the postgres user inside the `container2`, and then launches the psql. This allows you to manage and interact with your PostgreSQL database directly from within the container.

    docker exec -it container2 su - postgres && psql

Here are basic command that you can use:

    \df
    select mr_service_url();
    select mr_service();
    select mr_create_context('my-context')

# How to launch manually

## Dependences

We have to install deps to launch the project.
Installation command for Debian-based distros:

**PostgreSQL&rsquo;s build deps:**

    sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config

**Rust toolchain&rsquo;s deps:**

    sudo apt-get install rustc
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh # install rustup
    source "$HOME/.cargo/env" # add Rust to PATH

**Install `libclang` 7 or newer and git:**

    sudo apt-get install libclang-dev git

Pay attention that `libclang-dev` alse can be called `clang`:

    sudo apt-get install clang git

## Install meritrank-service-rust

Before working with `meritrank-psql-connector` also we need to install and launch in the background service:

    git clone https://github.com/Intersubjective/meritrank-service-rust

And here is end structure of folders:

    intersubjective/
    ├── meritrank-psql-connector
    └── meritrank-service-rust

Go to the dir and launch `meritrank-service-rust`

    cd meritrank-service-rust
    cargo run

## pgrx

Partly these steps are described in [pgrx&rsquo;s Github page](https://github.com/pgcentralfoundation/pgrx/tree/develop?tab=readme-ov-file):

Install `cargo-pgrx` sub-command:

    cargo install --locked cargo-pgrx

Init &ldquo;PGRX Home&rdquo; in a root of project:

    cargo pgrx init

This command downloads all currently supported PostgreSQL versions, compiles them to `${PGRX_HOME}`, and runs `initdb`.

## Test and start

Maybe you would like to run automatic tests:

    export RUST_TEST_THREADS=1
    cargo pgrx test

If tests completed without errors, run it:

    cargo pgrx run

## Connect to database

Now we can enter to psql and do actions with meritrank&rsquo;s service through psql conntector.

Login as postgres user, enter default password and execute `psql`:

    su - postgres
    psql

Here are some commands to ensure that service is working:

    \df
    select mr_service_url();
    select mr_service();
    select mr_create_context('my-context')
