#!/bin/bash
# Script to run NOP test for git2go-876
harbor run --agent nop -p /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/libgit2__git2go-876 --jobs-dir /shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/libgit2__git2go-876-nop-1 --no-delete --env docker
