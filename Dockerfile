FROM golang:1.19-alpine as builder

RUN apk add --no-cache \
    make \
    git \
    bash \
    curl \
    gcc \
    g++ \
    binutils-gold

# Install jq for pd-ctl
RUN cd / && \
    wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -O jq && \
    chmod +x jq

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

# Setup access to private repos
ARG GITHUB_TOKEN
RUN if [ -n "$GITHUB_TOKEN" ]; then git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; fi

ENV GOPRIVATE=github.com/tidbcloud
RUN GO111MODULE=on go mod download

COPY . .

RUN make

FROM alpine:3.17

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover
COPY --from=builder /jq /usr/local/bin/jq

RUN apk add --no-cache \
    curl \
    wget \
    bind-tools \
    bash && rm /bin/sh && ln -s /bin/bash /bin/sh

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
