FROM golang:1.18.4-alpine3.16

ENV GOPATH /go

RUN apk update && apk upgrade && \
    apk add --no-cache git build-base wget

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

RUN mkdir -p /go/src/github.com/pingcap/go-ycsb
WORKDIR /go/src/github.com/pingcap/go-ycsb

COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN GO111MODULE=on go build -o /go-ycsb ./cmd/*

FROM alpine:3.8 

COPY --from=0 /go-ycsb /go-ycsb
COPY --from=0 /usr/local/bin/dumb-init /usr/local/bin/dumb-init

ADD workloads /workloads

EXPOSE 6060

ENTRYPOINT [ "/usr/local/bin/dumb-init", "/go-ycsb" ]
