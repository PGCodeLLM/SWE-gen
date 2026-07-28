Modify SWE-Gen.

Your goal is to unite the disparate system that currently exist and create a platform that is resilient while also producing good data.  Currently, the methodology is run the orchestrator, reading from a jsonl containing PRs, base commits, etc. 

Then, successful tasks are postprocessed and copied to a separate folder. Finally, once the run is complete, a hacking checker is run on the postprocessed instances to determine which produced Harbor tasks are actually usable.  You should make the flow as follows:

Read from a database. The database is swegen.pr_tasks, username "postgres", password "Repomind@123", host "localhost". These parameters should be specified in swegen.toml. The current producer-consumer model allocates groups of PRs of the same repo to each consumer by reading from a jsonl. This should read from a database instead. Skip over any PRs that have swegen_bz_passed as "true", unless the "force-rebuild" option is set. Default to skipping over instances that has obs_exists as "false", but have the option to enable it by passing in the "include-obs-missing" flag. Exclude any instances that have an unlock_time in the future. When an group of PRs is allocated to a consumer, set the unlock_time to current_time + (sum of all timeouts - Docker build, Claude Code, etc)*0.1 - this ensures the same PR will not be run by multiple simultaneously-running SWE-Gen instances. Additionally, increment swegen_retries. All of this should be done in a single transaction to ensure atomicity.

For each run completing with NOP=0 and Oracle=1 (the former benchmark for a successful Harbor task), run the hacking checker as src/reward_hacking_detector/hacking.py would, but on the single newly-produced task instead of a completed folder. SWE-Gen should not mark this instance as "successful" unless it passes the hack checker. Instead of reading from src/reward_hacking_detector/hacking_checker.toml, it should read straight from swegen.toml. If the instance is marked as having reward-hacked (as determined by the "is_hacking" field being true), mark that instance as having failed and list the reason in the output in the orchestrator progress jsonls.

If a run is determined to have been successful (NOP=0, Oracle=1, non-reward-hacking), update the database to indicate that that particular instance_id has passed swegen.

src/swegen/analyze/classifier.py and src/swegen/create/task_instruction.py hardcode the model names. Make these also read from the swegen.toml instead.

Modify the toml example to reflect these changes.

Remove the option to use system variables for API keys and model specifications. These should all be specified in a central location, in swegen.toml.

Modify the system prompt to reflect the fix from processing_examples/proxy_setup_fix.py. SWE-Gen is now meant to be running on a more restricted environment - this fix ensures proper internet access.

Modify the image export functionality. For successful runs (now confirmed to be non-hacking), copy the Harbor task from tasks/ to tasks_bz/.

For the postprocessing functionality, you should, for the same successful runs that were copied to tasks_bz:
- Apply the proxy fix from processing_examples/proxy_setup_fix.py
- Use "FROM swr-coder-data-platform-wce1sr.swr-pro.myhuaweicloud.com/swesandbox" instead of whatever it currently uses
- Apply the git fetch fix from processing_examples/add_git_fetch_before_checkout.py, or prevent it from being needed in the first place by leaving them in to begin with. Note that the git clone of the relevant repo is still required.
- Apply a fix for the wce1sr SWR having "dirty" repos - git reset the repo before checking out.

If a run is successful, before removing the image, upload it to the Huawei SWR. An example image builder and uploader can be found in swr_examples - implement the upload functionality based on this. Only remove the image if the upload is successful.



Remove the repo-farming capability - there is no need for such a thing since we will be reading PRs from a database.

Additionally, add in a max-retries functionality. You have already in a retry counter in the database. Now, in swegen.toml, allow for the specification of "max-retries". Aside from skipping over PRs that have "swegen_bz_passed" = true, it should also skip over any PR with a max-retries count above the limit in the toml. Additionally, priority should be given to PRs with lower max-retries counts over higher max-retries counts.