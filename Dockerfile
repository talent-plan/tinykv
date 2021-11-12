#build stage
FROM golang:alpine
ENV GO111MODULE=on \
  GOPROXY=https://goproxy.cn,direct
RUN apk add --no-cache git
RUN apk add build-base
RUN apk add --update make
RUN mkdir -p /go/src/app
WORKDIR /go/src/app
COPY . .
RUN make
LABEL Name=tinykv Version=0.0.1
