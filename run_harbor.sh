#!/bin/bash
set -e

# Create home directory if needed
mkdir -p /home/alex 2>/dev/null || true
chmod 777 /home/alex 2>/dev/null || true
export HOME=/home/alex

# Run NOP test
echo "Running NOP test for nats-io__nats.go-1979..."
harbor run --agent nop -p /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/nats-io__nats.go-1979 \
    --jobs-dir /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/nats-io__nats.go-1979-nop-1 \
    --no-delete --env docker

# Run Oracle test
echo "Running Oracle test for nats-io__nats.go-1979..."
harbor run --agent oracle -p /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/nats-io__nats.go-1979 \
    --jobs-dir /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/nats-io__nats.go-1979-oracle-1 \
    --no-delete --env docker

echo "Done!"
