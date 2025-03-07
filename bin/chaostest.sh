#!/usr/bin/env bash
set -euxo pipefail
cd "$(dirname "$0")/.."

export BLOBBY_RUN_CHAOS=1
go test -timeout 300s -count 1 -test.v -run ^TestChaos$ github.com/adammck/blobby/pkg/blobby "$@"
