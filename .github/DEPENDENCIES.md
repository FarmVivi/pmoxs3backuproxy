# Dependency management

Runtime and build inputs are pinned so a rebuild does not silently select a
different dependency:

- Go modules are locked by `go.mod` and `go.sum`; the Go version is exact.
- Docker base and CI images use explicit version tags whenever the publisher
  provides them. The free `cgr.dev/chainguard/static` image exposes only
  `latest`; access to version-specific tags requires a Chainguard catalogue
  subscription, so it is the documented exception.
- GitHub Actions use explicit release tags, including patch versions.
- GoReleaser, MinIO, and the direct Proxmox CI packages use exact versions.
- The downloaded Proxmox repository key is verified with SHA-256.

Dependabot is the primary updater. It checks Go modules, Dockerfile images, and
GitHub Actions every day. Renovate has only the `custom.regex` manager enabled
so it cannot duplicate those updates. It covers values Dependabot cannot parse
natively: the Go directive, action input versions, Docker image references in
workflow shell commands, and APT packages embedded in the integration workflow.

The `ubuntu-24.04` GitHub-hosted runner fixes the OS release, but GitHub still
updates the runner image and its preinstalled packages. Docker and GitHub
Action tags can also be moved by their publishers. Likewise, APT resolves
transitive dependencies from the signed Bookworm repositories. The project's
direct Proxmox packages are pinned; freezing the complete environment would
require digest/commit pinning or a maintained immutable self-hosted runner.
