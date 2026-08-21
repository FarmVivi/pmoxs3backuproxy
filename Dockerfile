FROM golang:1.26.6-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36 AS builder
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

# Use chainguard static image as a minimal base
# Refer to https://images.chainguard.dev/directory/image/static/versions for more details
FROM cgr.dev/chainguard/static:latest@sha256:f68e3a8244c7d0f4cd56635aaff8e6a533cf6cc3850d8fb339567a5782d6a0b0

WORKDIR /

COPY --from=builder /workspace/pmoxs3backuproxy .
COPY --from=builder /workspace/garbagecollector .

COPY server.crt /
COPY server.key /

USER 65532:65532

EXPOSE 8007

ENTRYPOINT ["/pmoxs3backuproxy"]
