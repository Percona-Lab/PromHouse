# Contributing notes

## Local setup

The easiest way to make a local development setup is to use Docker Compose: `make env-run` on Linux
and `make env-run-mac` on macOS.

You can run ClickHouse client with `make clickhouse-client`.


## Vendoring

We use [dep](https://github.com/golang/dep) to vendor dependencies. Please use released version, i.e. do not `go get`
from `master` branch. On macOS you can install it with `brew install dep`.
