FROM golang:1.13.3

# Install staticcheck
WORKDIR /
RUN wget https://github.com/dominikh/go-tools/releases/download/2019.2.3/staticcheck_linux_amd64.tar.gz
RUN tar -xf staticcheck_linux_amd64.tar.gz

# Install gosec
RUN wget https://github.com/securego/gosec/releases/download/v2.1.0/gosec_2.1.0_linux_amd64.tar.gz --output-document gosec.tar.gz
RUN tar -xf gosec.tar.gz

WORKDIR /go/src/github.com/skysoft-atm/gorillaz

ENV GO111MODULE on
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go vet ./...
RUN /staticcheck/staticcheck ./...
RUN /gosec ./...
RUN go test ./... -race 
RUN CGO_ENABLED=0 go build ./...
