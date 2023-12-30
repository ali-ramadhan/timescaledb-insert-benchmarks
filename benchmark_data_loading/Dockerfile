FROM timescale/timescaledb-ha:pg15.5-ts2.13.0
USER root

WORKDIR /home/postgres

RUN apt-get update && \
    apt-get install wget

RUN wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
RUN tar xf go1.21.5.linux-amd64.tar.gz
RUN /home/postgres/go/bin/go install github.com/timescale/timescaledb-tune/cmd/timescaledb-tune@latest
RUN /home/postgres/go/bin/go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest
