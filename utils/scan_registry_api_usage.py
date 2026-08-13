#!/usr/bin/env python3
"""Scan AiiDA plugin repositories for their usage of the ``aiida`` API.

Takes the JSON produced by ``aiida_registry_maintained.py``, shallow-clones each
repository and runs the AST extractor from ``extract_aiida_core_api.py`` over it.
The extractor is imported, not shelled out to, so both scripts always agree on
what counts as an API reference.

The report is aggregated in both directions: ``api`` maps every referenced
``aiida.*`` path to the plugins using it (the view that matters when judging the
blast radius of a deprecation), while ``plugins`` maps each plugin to its own
usage counts.

Examples::

    python scripts/aiida_registry_maintained.py 2 -o maintained.json
    python scripts/scan_registry_api_usage.py maintained.json -o api-usage.json

    # keep the clones around so repeated runs skip the network
    python scripts/scan_registry_api_usage.py maintained.json --workdir ~/.cache/aiida-registry
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

CLONE_TIMEOUT = 600


def load_extractor():
    """Import ``extract_aiida_core_api.py`` from the directory of this script."""
    path = Path(__file__).resolve().parent / 'extract_aiida_core_api.py'
    spec = importlib.util.spec_from_file_location('extract_aiida_core_api', path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'cannot import the API extractor from {path}')
    module = importlib.util.module_from_spec(spec)
    # Register before executing: ``@dataclass`` resolves ``cls.__module__`` through ``sys.modules``.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_git(arguments: list[str], timeout: int = CLONE_TIMEOUT) -> subprocess.CompletedProcess:
    return subprocess.run(['git', *arguments], capture_output=True, text=True, timeout=timeout)


def clone(repository: str, destination: Path) -> str | None:
    """Shallow-clone ``repository`` into ``destination``, returning an error message on failure."""
    if destination.exists():
        return None

    destination.parent.mkdir(parents=True, exist_ok=True)
    # Clone into a scratch path first so an interrupted clone never looks cached.
    staging = destination.with_name(f'{destination.name}.partial')
    shutil.rmtree(staging, ignore_errors=True)

    try:
        result = run_git(['clone', '--quiet', '--depth', '1', '--single-branch', repository, str(staging)])
    except subprocess.SubprocessError as exception:
        shutil.rmtree(staging, ignore_errors=True)
        return f'clone failed: {exception}'

    if result.returncode != 0:
        shutil.rmtree(staging, ignore_errors=True)
        return f'clone failed: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error"}'

    staging.rename(destination)
    return None


def head_commit(repository: Path) -> str | None:
    try:
        result = run_git(['-C', str(repository), 'rev-parse', 'HEAD'], timeout=30)
    except subprocess.SubprocessError:
        return None
    return result.stdout.strip() or None


def scan(plugin: dict, workdir: Path, extractor, keep: bool, details_dir: Path | None) -> dict:
    """Clone one plugin and return its API usage summary."""
    name = plugin['name']
    repository = plugin.get('repository')
    result: dict = {
        'name': name,
        'package_name': plugin.get('package_name'),
        'repository': repository,
        'last_activity': plugin.get('last_activity'),
    }

    if not repository:
        result['error'] = 'no repository URL in the registry entry'
        return result

    checkout = workdir / name
    error = clone(repository, checkout)
    if error is not None:
        result['error'] = error
        return result

    try:
        result['commit'] = head_commit(checkout)
        uses = extractor.extract([checkout], checkout)

        if details_dir is not None:
            detail = {api: [location.__dict__ for location in locations] for api, locations in uses.items()}
            (details_dir / f'{name}.json').write_text(json.dumps(detail, indent=2) + '\n', encoding='utf-8')

        result['python_files'] = sum(1 for _ in extractor.python_files([checkout]))
        result['apis'] = {api: len(locations) for api, locations in uses.items()}
    finally:
        if not keep:
            shutil.rmtree(checkout, ignore_errors=True)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('plugins', type=Path, help='JSON file from aiida_registry_maintained.py')
    parser.add_argument('-o', '--output', type=Path, help='write the aggregated JSON here instead of stdout')
    parser.add_argument(
        '--workdir',
        type=Path,
        help='directory for the clones; kept and reused across runs (default: a temporary directory)',
    )
    parser.add_argument('--details-dir', type=Path, help='also write the full per-plugin report with source locations')
    parser.add_argument('--jobs', type=int, default=8, help='number of parallel clones (default: 8)')
    parser.add_argument('--limit', type=int, help='only scan the first N plugins, for a quick trial run')
    parser.add_argument(
        '--exclude',
        nargs='*',
        default=['aiida-core'],
        metavar='NAME',
        help='registry entries to skip; aiida-core is skipped by default because its internal references are not '
        'plugin usage of the API. Pass --exclude with no names to scan everything',
    )
    args = parser.parse_args()

    try:
        plugins = json.loads(args.plugins.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exception:
        print(f'failed to read {args.plugins}: {exception}', file=sys.stderr)
        return 1

    excluded = set(args.exclude)
    if excluded:
        skipped = [plugin['name'] for plugin in plugins if plugin['name'] in excluded]
        plugins = [plugin for plugin in plugins if plugin['name'] not in excluded]
        if skipped:
            print(f'excluding {", ".join(sorted(skipped))}', file=sys.stderr)

    if args.limit is not None:
        plugins = plugins[: args.limit]

    extractor = load_extractor()

    details_dir = args.details_dir
    if details_dir is not None:
        details_dir.mkdir(parents=True, exist_ok=True)

    temporary = None if args.workdir else tempfile.mkdtemp(prefix='aiida-registry-')
    workdir = args.workdir or Path(temporary)
    workdir.mkdir(parents=True, exist_ok=True)
    keep = args.workdir is not None

    print(f'scanning {len(plugins)} plugins into {workdir}', file=sys.stderr)

    results: list[dict] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = {
                executor.submit(scan, plugin, workdir, extractor, keep, details_dir): plugin for plugin in plugins
            }
            for done, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                result = future.result()
                results.append(result)
                if 'error' in result:
                    print(f'  [{done}/{len(plugins)}] {result["name"]}: {result["error"]}', file=sys.stderr)
                else:
                    print(f'  [{done}/{len(plugins)}] {result["name"]}: {len(result["apis"])} APIs', file=sys.stderr)
    finally:
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)

    results.sort(key=lambda entry: entry['name'].lower())
    scanned = [result for result in results if 'error' not in result]
    failed = [result for result in results if 'error' in result]

    api_plugins: defaultdict[str, list[str]] = defaultdict(list)
    api_counts: Counter[str] = Counter()
    for result in scanned:
        for api, count in result['apis'].items():
            api_plugins[api].append(result['name'])
            api_counts[api] += count

    api = {
        name: {
            'plugins': sorted(api_plugins[name], key=str.lower),
            'plugin_count': len(api_plugins[name]),
            'occurrences': api_counts[name],
        }
        # Rank by how many plugins would notice a change, then by raw usage.
        for name, _ in sorted(api_counts.items(), key=lambda item: (-len(api_plugins[item[0]]), -item[1], item[0]))
    }

    report = {
        'generated': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
        'plugins_scanned': len(scanned),
        'plugins_failed': len(failed),
        'distinct_apis': len(api),
        'api': api,
        'plugins': results,
    }

    print(f'{len(scanned)} scanned, {len(failed)} failed, {len(api)} distinct APIs', file=sys.stderr)

    payload = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(payload + '\n', encoding='utf-8')
        print(f'wrote {args.output}', file=sys.stderr)
    else:
        print(payload)

    return 0


if __name__ == '__main__':
    sys.exit(main())
