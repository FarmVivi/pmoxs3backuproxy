FROM golang:1.26.6-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build tizbac/pmoxs3backuproxy/cmd/pmoxs3backuproxy
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build tizbac/pmoxs3backuproxy/cmd/garbagecollector

# Both binaries are statically linked. scratch has no moving package or image
# version to pin; copy only the CA bundle needed for HTTPS S3 endpoints.
FROM scratch

WORKDIR /

COPY --from=builder /workspace/pmoxs3backuproxy .
COPY --from=builder /workspace/garbagecollector .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY server.crt /
COPY server.key /

USER 65532:65532

EXPOSE 8007

ENTRYPOINT ["/pmoxs3backuproxy"]
