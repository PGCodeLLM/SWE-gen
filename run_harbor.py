#!/usr/bin/env python3
import subprocess
import sys
import os

# Set up environment
env = os.environ.copy()
env['HOME'] = '/tmp'
env['PATH'] = '/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.venv/bin:' + env.get('PATH', '')

cmd = [
    '/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.venv/bin/harbor',
    'run',
    '--agent', 'nop',
    '-p', '/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/tasks/oliver006__redis_exporter-1066',
    '--jobs-dir', '/shared_workspace_mfs/alex/swe-gen-mod/SWE-gen/.swegen/harbor-jobs/oliver006__redis_exporter-1066-nop-1',
    '--no-delete',
    '--env', 'docker'
]

print(f"Running: {' '.join(cmd)}")
print(f"Environment HOME: {env.get('HOME')}")

result = subprocess.run(cmd, capture_output=False, text=True, env=env)
print(f"\nReturn code: {result.returncode}")
sys.exit(result.returncode)
