FROM golang:1.12.1

WORKDIR /go/src/github.com/skysoft-atm/gorillaz

ENV GO111MODULE on
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go test ./... -race && go build ./...
