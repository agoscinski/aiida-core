#!/usr/bin/env python3
"""Scan the AiiDA plugin registry for plugins maintained within the last N years.

The registry itself (``plugins_metadata.json``) carries no activity dates, so the
last activity is resolved per plugin from its host:

* github.com -> ``pushed_at`` of ``GET /repos/{owner}/{repo}``
* gitlab.com -> ``last_activity_at`` of ``GET /api/v4/projects/{path}``

Both timestamps track pushes to *any* branch, which is the closest cheap proxy
for "still maintained".

GitHub allows only 60 unauthenticated requests per hour, which is not enough for
the ~100 registry entries. A token is picked up from ``GITHUB_TOKEN``/``GH_TOKEN``
or, failing that, from ``gh auth token``.

Usage::

    python scripts/aiida_registry_maintained.py 2 -o maintained.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

REGISTRY_URL = 'https://aiidateam.github.io/aiida-registry/plugins_metadata.json'
USER_AGENT = 'aiida-registry-maintenance-scan'
TIMEOUT = 30


def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict:
    """Return the JSON body of ``url``, raising ``urllib.error.HTTPError`` on failure."""
    request = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, **(headers or {})})
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return json.load(response)


def github_token() -> str | None:
    """Return a GitHub token from the environment or the ``gh`` CLI, if available."""
    for variable in ('GITHUB_TOKEN', 'GH_TOKEN'):
        if os.environ.get(variable):
            return os.environ[variable]

    try:
        result = subprocess.run(['gh', 'auth', 'token'], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None

    return result.stdout.strip() or None


def repo_path(code_home: str, host: str) -> str | None:
    """Extract the ``owner/repo`` path from a repository URL, or None if it does not look like one."""
    parsed = urllib.parse.urlparse(code_home)

    if host not in parsed.netloc:
        return None

    path = parsed.path.strip('/')

    # Strip trailing ``.git`` and any deep link such as ``/tree/main/subdir``.
    for marker in ('/tree/', '/blob/', '/-/'):
        path = path.split(marker)[0]
    if path.endswith('.git'):
        path = path[: -len('.git')]

    return path if path.count('/') >= 1 else None


def last_activity(plugin: dict, token: str | None) -> tuple[str | None, str | None]:
    """Return ``(iso_timestamp, error)`` for the last push to the plugin repository."""
    code_home = plugin.get('code_home') or ''
    host = plugin.get('hosted_on') or urllib.parse.urlparse(code_home).netloc

    if 'github.com' in host:
        path = repo_path(code_home, 'github.com')
        if path is None:
            return None, f'cannot parse repository from {code_home!r}'
        url = f'https://api.github.com/repos/{path}'
        headers = {'Accept': 'application/vnd.github+json'}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        key = 'pushed_at'
    elif 'gitlab.com' in host:
        path = repo_path(code_home, 'gitlab.com')
        if path is None:
            return None, f'cannot parse repository from {code_home!r}'
        url = f'https://gitlab.com/api/v4/projects/{urllib.parse.quote(path, safe="")}'
        headers = {}
        key = 'last_activity_at'
    else:
        return None, f'unsupported host {host!r}'

    try:
        payload = fetch_json(url, headers)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 429):
            remaining = exc.headers.get('X-RateLimit-Remaining')
            if remaining == '0':
                return None, 'rate limited (set GITHUB_TOKEN or run `gh auth login`)'
        return None, f'HTTP {exc.code}'
    except (urllib.error.URLError, TimeoutError) as exc:
        return None, f'request failed: {exc}'

    timestamp = payload.get(key)
    return (timestamp, None) if timestamp else (None, f'no {key} in API response')


def parse_timestamp(timestamp: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp from the GitHub/GitLab APIs into an aware datetime."""
    return dt.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('years', type=float, help='consider a plugin maintained if pushed to within this many years')
    parser.add_argument('-o', '--output', help='write JSON here instead of stdout')
    parser.add_argument('--registry-url', default=REGISTRY_URL, help='override the registry metadata URL')
    parser.add_argument('--jobs', type=int, default=8, help='number of parallel API requests (default: 8)')
    parser.add_argument(
        '--include-unresolved',
        action='store_true',
        help='also emit plugins whose last activity could not be determined, with "last_activity": null',
    )
    args = parser.parse_args()

    try:
        plugins = fetch_json(args.registry_url)['plugins']
    except (urllib.error.URLError, KeyError, ValueError) as exc:
        print(f'failed to fetch registry from {args.registry_url}: {exc}', file=sys.stderr)
        return 1

    token = github_token()
    if not token:
        print('warning: no GitHub token found, expect rate limiting after 60 requests', file=sys.stderr)

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.years * 365.25)
    print(f'checking {len(plugins)} plugins against cutoff {cutoff.date().isoformat()}', file=sys.stderr)

    maintained, unresolved, stale = [], [], 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = {executor.submit(last_activity, plugin, token): (name, plugin) for name, plugin in plugins.items()}

        for future in concurrent.futures.as_completed(futures):
            name, plugin = futures[future]
            timestamp, error = future.result()

            entry = {
                'name': name,
                'package_name': plugin.get('package_name'),
                'repository': plugin.get('code_home'),
                'last_activity': timestamp,
            }

            if error is not None:
                print(f'  {name}: {error}', file=sys.stderr)
                unresolved.append(entry)
            elif parse_timestamp(timestamp) >= cutoff:
                maintained.append(entry)
            else:
                stale += 1

    results = sorted(maintained, key=lambda entry: entry['last_activity'], reverse=True)
    if args.include_unresolved:
        results += sorted(unresolved, key=lambda entry: entry['name'].lower())

    print(
        f'{len(maintained)} maintained, {stale} stale, {len(unresolved)} unresolved',
        file=sys.stderr,
    )

    payload = json.dumps(results, indent=2)
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as handle:
            handle.write(payload + '\n')
        print(f'wrote {args.output}', file=sys.stderr)
    else:
        print(payload)

    return 0


if __name__ == '__main__':
    sys.exit(main())
