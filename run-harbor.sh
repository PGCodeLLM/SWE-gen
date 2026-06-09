#!/bin/bash
export HOME=/tmp/alex-home
export USER=root
export LOGNAME=root
export SHELL=/bin/bash
mkdir -p /tmp/alex-home
harbor run --agent nop -p /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/nats-io__nats.go-1960 --jobs-dir /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/nats-io__nats.go-1960-nop-1 --no-delete --env docker
