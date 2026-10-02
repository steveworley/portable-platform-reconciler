# syntax=docker/dockerfile:1

FROM golang:1.24 AS builder
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/controller ./cmd/controller \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/demo ./cmd/demo

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/controller /controller
COPY --from=builder /out/demo /demo
COPY examples /examples
USER nonroot:nonroot
EXPOSE 8080 8081
# Default: the controller. Override to run the demo: docker run --entrypoint /demo IMAGE --examples /examples
ENTRYPOINT ["/controller"]
