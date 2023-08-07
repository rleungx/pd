FROM golang:1.20-bullseye as builder

RUN apt update && apt install -y make git curl gcc g++ unzip

# Setup ssh key for private deps
ARG ssh_key
RUN if [ -n "$ssh_key" ]; then \
        mkdir -p ~/.ssh && \
        echo "$ssh_key" > ~/.ssh/key && \
        chmod 600 ~/.ssh/key && \
        echo "Host github.com" >> ~/.ssh/config && \
        echo "\tUser git" >> ~/.ssh/config && \
        echo "\tPort 443" >> ~/.ssh/config && \
        echo "\tHostName ssh.github.com" >> ~/.ssh/config && \
        echo "\tIdentityFile ~/.ssh/key" >> ~/.ssh/config && \
        ssh-keyscan -p 443 ssh.github.com>> ~/.ssh/known_hosts && \
        git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"; \
    fi

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN make

FROM debian:bullseye-20220711-slim
RUN apt update && apt install -y jq bash curl dnsutils wget && rm /bin/sh && ln -s /bin/bash /bin/sh && \
    apt-get clean autoclean && apt-get autoremove --yes

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
