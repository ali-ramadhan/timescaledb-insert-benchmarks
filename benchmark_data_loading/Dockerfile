FROM timescale/timescaledb-ha:pg15.5-ts2.13.0
USER root

WORKDIR /home/postgres

RUN apt-get update
RUN apt-get install -y wget

RUN wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
RUN tar xf go1.21.5.linux-amd64.tar.gz
RUN /home/postgres/go/bin/go install github.com/timescale/timescaledb-tune/cmd/timescaledb-tune@latest
RUN /home/postgres/go/bin/go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest

RUN apt-get install -y build-essential libreadline-dev libpam-dev postgresql-server-dev-15 libselinux1-dev libzstd-dev liblz4-dev libkrb5-dev zlib1g-dev
RUN wget https://github.com/ossc-db/pg_bulkload/releases/download/VERSION3_1_20/pg_bulkload-3.1.20.tar.gz
RUN tar xvf pg_bulkload-3.1.20.tar.gz
RUN cd pg_bulkload-3.1.20 && make && make install

CMD psql -U postgres -d postgres -c "CREATE EXTENSION pg_bulkload;"
