# Table of Contents

1.  [pgmer2 extension](#orgbeff16b)
    1.  [What is pgmer2?](#orgada91c4)
    2.  [Default env variables](#orgcf775dd)
    3.  [How to launch project within Docker](#orgc047d13)
        1.  [Building](#orgad886d1)
        2.  [Then create shared network and launch both images](#org2fa94bd)
        3.  [PSQL](#org450fd83)
    4.  [See also:](#org1245028)

<a id="orgbeff16b"></a>

# What is pgmer2?

**pgmer2** is a Postgres FDW (Foreign Data Wrapper) for [MeritRank service](https://github.com/Intersubjective/meritrank-service-rust)

![Untitled Diagram drawio](https://github.com/user-attachments/assets/860ff36e-e6dc-4558-b293-95b1e49cfbfd)

<a id="orgcf775dd"></a>

# Default env variables

Default values described in `Dockerfile`:

    ENV MERITRANK_SERVICE_URL=tcp://127.0.0.1:10234
    ENV MERITRANK_RECV_TIMEOUT_MSEC=10000

Also there are values in a `Dockerfile` of `meritrank-service-rust`:

    ENV MERITRANK_SERVICE_URL=tcp://127.0.0.1:10234
    ENV MERITRANK_SERVICE_THREADS=32
    ENV MERITRANK_NUM_WALK=10000
    ENV MERITRANK_ZERO_NODE=U000000000000
    ENV MERITRANK_TOP_NODES_LIMIT=100
    ENV MERITRANK_FILTER_NUM_HASHES=10
    ENV MERITRANK_FILTER_MIN_SIZE=32
    ENV MERITRANK_FILTER_MAX_SIZE=8192

If you want to run project without Docker you don&rsquo;t have to change `MERITRANK_SERVICE_URL`. Or if you want to use docker, it is necessary due to network isolation of docker containers


<a id="orgc047d13"></a>

# How to launch project within Docker


<a id="orgad886d1"></a>

## Building

First we need to build service and psql-conntector:

    cd meritrank-service-rust/
    docker build -t mr-service .

    cd meritrank-psql-connector/
    docker build -t mr-psql-connector .


<a id="org2fa94bd"></a>

## Then create shared network and launch both images

    docker create network my-network

Run `mr-service` in it&rsquo;s directory:

    docker run --network my-network -p 10234:10234 -e MERITRANK_SERVICE_URL=tcp://0.0.0.0:10234 --name container1 mr-service

And then `mr-psql-connector`:

    docker run --network my-network -e POSTGRES_PASSWORD=postgres -e MERITRANK_SERVICE_URL=tcp://container1:10234 --name container2 -p 5432:5432 mr-psql-connector:latest


<a id="org450fd83"></a>

## PSQL

Now we can connect to container2 and access to psql:

    docker exec -it container2 su - postgres && psql

    DROP EXTENSION pgmer2; CREATE EXTENSION pgmer2;
    \df
    select mr_service_url();
    select mr_service();


<a id="org1245028"></a>

# See also:

-   Server sample: <https://github.com/shestero/pgmer2serv>
-   HTTP API analogue: <https://github.com/shestero/pgmer1>

