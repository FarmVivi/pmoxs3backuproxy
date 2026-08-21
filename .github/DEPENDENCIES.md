# Dependency management

Runtime and build inputs are pinned so a rebuild does not silently select a
different dependency:

- Go modules are locked by `go.mod` and `go.sum`; the Go version is exact.
- Docker base and CI images use an explicit tag and immutable manifest digest.
- GitHub Actions use immutable commit SHAs, with the release tag in a comment.
- GoReleaser, MinIO, and the direct Proxmox CI packages use exact versions.
- The downloaded Proxmox repository key is verified with SHA-256.

Dependabot is the primary updater. It checks Go modules, Dockerfile images, and
GitHub Actions every day. Renovate has only the `custom.regex` manager enabled
so it cannot duplicate those updates. It covers values Dependabot cannot parse
natively: the Go directive, action input versions, Docker image references in
workflow shell commands, and APT packages embedded in the integration workflow.

The `ubuntu-24.04` GitHub-hosted runner fixes the OS release, but GitHub still
updates the runner image and its preinstalled packages. Likewise, APT resolves
transitive dependencies from the signed Bookworm repositories. The project's
direct Proxmox packages are pinned; freezing the complete hosted-runner image
would require replacing it with a maintained immutable CI container or a
self-hosted runner.
