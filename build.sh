#!/bin/bash

set -e

BUILDER=$(which docker || which podman)

echo "Using builder: $BUILDER"

$BUILDER build -t bisync-build .
$BUILDER create --name bisync-temp bisync-build
$BUILDER cp bisync-temp:/bisync ./bisync
$BUILDER rm bisync-temp
