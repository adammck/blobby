#!/usr/bin/env bash
set -euxo pipefail
cd "$(dirname "$0")/.."

go test -count 1 ./...
