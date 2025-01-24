#!/usr/bin/env bash
set -euxo pipefail
cd "$(dirname "$0")/.."

go test -timeout 300s -test.v -run ^TestChaos$ github.com/adammck/archive/pkg/archive
