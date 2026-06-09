#!/bin/bash
# Harbor test runner script
export HOME=/root
cd /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen
harbor run --agent nop -p /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/tokio-rs__bytes-710 --jobs-dir /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/tokio-rs__bytes-710-nop-1 --no-delete --env docker
