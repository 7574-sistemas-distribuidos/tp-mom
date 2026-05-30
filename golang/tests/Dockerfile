FROM golang:1.24-alpine3.23

WORKDIR /golang

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY internal ./internal
COPY tests ./tests

ENTRYPOINT ["go", "test", "-v", "./...", "-count=1", "-timeout=30s"]
