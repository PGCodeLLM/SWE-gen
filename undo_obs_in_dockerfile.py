#!/usr/bin/env python3
"""Replace the obs_download block in Harbor instance Dockerfiles with the exact
`git clone` block that src/swegen/create/task_skeleton.py::generate_dockerfile
would have emitted.

For each <instances_dir>/<harbor_instance>/environment/Dockerfile, find:

    COPY obs_download.py /usr/local/bin/obs_download.py
    RUN REPO_FULL_NAME="$(echo '<REPO_URL>' | sed -E 's#^[a-z]+://[^/]+/##; s/\\.git$##')" && \\
        python3 /usr/local/bin/obs_download.py "$REPO_FULL_NAME" src && \\
        cd src && \\
        git submodule update --init --recursive

and replace it with:

    RUN git clone <REPO_URL> src && \\
        cd src && \\
        (git fetch --depth 1 origin <HEAD_SHA> || git fetch --depth 1 origin "+refs/pull/<PR>/head:refs/remotes/origin/pr/<PR>") && \\
        git checkout --detach FETCH_HEAD && \\
        git submodule update --init --recursive

Values, matching task_skeleton exactly:
  * REPO_URL  - captured from the obs block (== pr_data["base"]["repo"]["clone_url"]).
  * PR        - the trailing "-<N>" of the instance directory name.
  * HEAD_SHA  - the PR branch head, fetched live from the GitHub API:
                GET /repos/{repo}/pulls/{pr} -> head.sha  (== pr_fetcher.py:83).
                This is the true value task_skeleton received; it is NOT stored
                in any instance file, so it must be fetched.

Tokens are read from the env var GITHUB_TOKENS (comma-separated) or one or more
--token flags. They are rotated across requests to spread rate limits. Resolved
head SHAs are cached to --cache so re-runs do not re-hit the API.

Usage:
    GITHUB_TOKENS="tok1,tok2" python3 undo_obs_in_dockerfile.py <instances_dir> [--dry-run]
"""
import argparse
import itertools
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DEFAULT_CACHE = '.head_sha_cache.json'

# Matches the COPY + multi-line RUN obs_download block. Captures the repo URL.
OBS_BLOCK = re.compile(
    r'^COPY obs_download\.py[^\n]*\n'
    r'^RUN REPO_FULL_NAME="\$\(echo \'(?P<repo_url>[^\']+)\'[^\n]*\\\n'
    r'(?:^[^\n]*\\\n)*?'                      # continuation lines (python3, cd src, ...)
    r'^[ \t]*git submodule update --init --recursive[^\n]*$',
    re.MULTILINE,
)


def instance_repo_pr(instance_dir: Path):
    """Return (repo_full_name, pr_number) for an instance directory."""
    name = instance_dir.name
    m = re.match(r'^(.*)-(\d+)$', name)
    if not m:
        return None, None
    stem, pr = m.group(1), m.group(2)
    repo = None
    try:
        for line in (instance_dir / 'task.toml').read_text().splitlines():
            mm = re.match(r'\s*repo_full_name\s*=\s*"([^"]+)"', line)
            if mm:
                repo = mm.group(1)
                break
    except OSError:
        pass
    if not repo:
        repo = stem.replace('__', '/', 1)
    return repo, pr


class HeadShaFetcher:
    """Fetches pr_data['head']['sha'] from the GitHub API, rotating tokens."""

    def __init__(self, tokens, cache_path: Path):
        self.tokens = list(tokens)
        self._cycle = itertools.cycle(self.tokens) if self.tokens else None
        self._tok_lock = threading.Lock()
        self.cache_path = cache_path
        self.cache = {}
        if cache_path.is_file():
            try:
                self.cache = json.loads(cache_path.read_text())
            except (OSError, json.JSONDecodeError):
                self.cache = {}
        self._cache_lock = threading.Lock()

    def _next_token(self):
        if not self._cycle:
            return None
        with self._tok_lock:
            return next(self._cycle)

    def _api_head_sha(self, repo: str, pr: str) -> str:
        url = f'https://api.github.com/repos/{repo}/pulls/{pr}'
        last_err = None
        # Try a few times, rotating tokens, backing off on rate limit.
        for attempt in range(max(3, len(self.tokens) * 2)):
            token = self._next_token()
            req = urllib.request.Request(url)
            req.add_header('Accept', 'application/vnd.github+json')
            req.add_header('User-Agent', 'undo-obs-script')
            if token:
                req.add_header('Authorization', f'Bearer {token}')
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                return data['head']['sha']
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code in (403, 429):
                    remaining = e.headers.get('X-RateLimit-Remaining')
                    if remaining == '0':
                        # rotate to another token; brief sleep before retry
                        time.sleep(1.5)
                        continue
                    time.sleep(1.0)
                    continue
                if e.code == 404:
                    raise RuntimeError(f'{repo}#{pr}: 404 (not found)') from e
                time.sleep(1.0)
            except (urllib.error.URLError, TimeoutError, KeyError) as e:
                last_err = e
                time.sleep(1.0)
        raise RuntimeError(f'{repo}#{pr}: failed after retries ({last_err})')

    def get(self, repo: str, pr: str) -> str:
        key = f'{repo}#{pr}'
        with self._cache_lock:
            if key in self.cache:
                return self.cache[key]
        sha = self._api_head_sha(repo, pr)
        with self._cache_lock:
            self.cache[key] = sha
        return sha

    def save(self):
        with self._cache_lock:
            tmp = self.cache_path.with_suffix(self.cache_path.suffix + '.tmp')
            tmp.write_text(json.dumps(self.cache, indent=0, sort_keys=True))
            tmp.replace(self.cache_path)


def build_replacement(repo_url: str, head_sha: str, pr_number: str) -> str:
    return (
        f'RUN git clone {repo_url} src && \\\n'
        f'    cd src && \\\n'
        f'    (git fetch --depth 1 origin {head_sha} || git fetch --depth 1 origin '
        f'"+refs/pull/{pr_number}/head:refs/remotes/origin/pr/{pr_number}") && \\\n'
        f'    git checkout --detach FETCH_HEAD && \\\n'
        f'    git submodule update --init --recursive'
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('instances_dir', help='Directory of Harbor instances')
    ap.add_argument('--token', action='append', default=[],
                    help='GitHub token (repeatable). Also read from $GITHUB_TOKENS.')
    ap.add_argument('--cache', default=DEFAULT_CACHE,
                    help=f'head_sha cache json (default: {DEFAULT_CACHE})')
    ap.add_argument('--workers', type=int, default=8, help='Concurrent API fetches')
    ap.add_argument('--dry-run', action='store_true',
                    help='Resolve SHAs and report, but do not write Dockerfiles')
    args = ap.parse_args()

    root = Path(args.instances_dir)
    if not root.is_dir():
        sys.exit(f'error: {root} is not a directory')

    tokens = list(args.token)
    env_tok = os.environ.get('GITHUB_TOKENS') or os.environ.get('GITHUB_TOKEN', '')
    tokens += [t.strip() for t in env_tok.split(',') if t.strip()]
    if not tokens:
        print('warning: no GitHub tokens provided; unauthenticated rate limit is '
              '60 req/hr and will likely fail.', file=sys.stderr)

    # Pass 1: find Dockerfiles with an obs block and collect (repo, pr) work.
    targets = []  # (dockerfile, repo, pr, repo_url, match)
    skipped_no_block = 0
    for df in sorted(root.glob('*/environment/Dockerfile')):
        text = df.read_text()
        m = OBS_BLOCK.search(text)
        if not m:
            skipped_no_block += 1
            continue
        repo, pr = instance_repo_pr(df.parent.parent)
        targets.append((df, repo, pr, m.group('repo_url'), m))

    if not targets and skipped_no_block == 0:
        sys.exit(f'error: no */environment/Dockerfile found under {root}')

    # Pass 2: resolve head SHAs (cached + concurrent).
    fetcher = HeadShaFetcher(tokens, Path(args.cache))
    needed = {(repo, pr) for _, repo, pr, _, _ in targets if repo and pr}
    to_fetch = [(r, p) for (r, p) in needed if f'{r}#{p}' not in fetcher.cache]
    print(f'{len(targets)} obs Dockerfiles | {len(needed)} unique PRs | '
          f'{len(fetcher.cache)} cached | {len(to_fetch)} to fetch from API')

    errors = {}
    if to_fetch:
        done = 0
        lock = threading.Lock()

        def work(rp):
            r, p = rp
            try:
                fetcher.get(r, p)
            except Exception as e:  # noqa: BLE001 - record and continue
                errors[(r, p)] = str(e)
            nonlocal done
            with lock:
                done += 1
                if done % 50 == 0 or done == len(to_fetch):
                    print(f'  fetched {done}/{len(to_fetch)}', flush=True)
                    fetcher.save()

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            list(ex.map(work, to_fetch))
        fetcher.save()

    # Pass 3: rewrite Dockerfiles.
    replaced = skipped_no_sha = 0
    for df, repo, pr, repo_url, m in targets:
        key = f'{repo}#{pr}'
        head_sha = fetcher.cache.get(key)
        if not head_sha:
            skipped_no_sha += 1
            print(f'skipped-no-sha   {df.parent.parent.name}  '
                  f'({errors.get((repo, pr), "unresolved")})')
            continue
        if args.dry_run:
            replaced += 1
            continue
        text = df.read_text()
        new = text[:m.start()] + build_replacement(repo_url, head_sha, pr) + text[m.end():]
        df.write_text(new)
        replaced += 1

    print(f'\n{"[dry-run] " if args.dry_run else ""}'
          f'{len(targets) + skipped_no_block} Dockerfiles: '
          f'{replaced} replaced, {skipped_no_block} no-obs-block, '
          f'{skipped_no_sha} missing-sha')
    if errors:
        print(f'{len(errors)} PRs failed to resolve (see skipped-no-sha lines).')


if __name__ == '__main__':
    main()
