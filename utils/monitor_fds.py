#!/usr/bin/env python3
"""Monitor open file descriptors of a process over time.

Usage:
    # Monitor a specific PID
    python utils/monitor_fds.py --pid 12345

    # Monitor a running verdi daemon (auto-detects worker PIDs)
    python utils/monitor_fds.py --daemon

    # Custom interval and output to file
    python utils/monitor_fds.py --pid 12345 --interval 5 --output fd_log.csv

    # Show full FD details on each snapshot (verbose)
    python utils/monitor_fds.py --pid 12345 -v
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import subprocess
import sys
import time
from datetime import datetime


def get_open_fds(pid: int) -> dict[int, dict[str, str]]:
    """Return open FDs for a given PID with type and name info."""
    fds = {}

    # Try /proc first (Linux)
    proc_fd = f'/proc/{pid}/fd'
    if os.path.isdir(proc_fd):
        for entry in os.listdir(proc_fd):
            try:
                fd_num = int(entry)
                target = os.readlink(f'{proc_fd}/{entry}')
                fds[fd_num] = {'type': _classify_fd(target), 'name': target}
            except (ValueError, OSError):
                continue
        return fds

    # Fall back to lsof (macOS / other)
    try:
        result = subprocess.run(
            ['lsof', '-p', str(pid), '-F', 'ftni'],
            capture_output=True, text=True, timeout=10,
        )
        current_fd = None
        parts: dict[str, str] = {}
        for line in result.stdout.splitlines():
            if line.startswith('f') and line[1:].isdigit():
                if current_fd is not None:
                    fds[current_fd] = {
                        'type': parts.get('t', '?'),
                        'name': parts.get('n', '?'),
                    }
                current_fd = int(line[1:])
                parts = {}
            elif current_fd is not None:
                key = line[0]
                parts[key] = line[1:]
        if current_fd is not None:
            fds[current_fd] = {
                'type': parts.get('t', '?'),
                'name': parts.get('n', '?'),
            }
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    return fds


def _classify_fd(target: str) -> str:
    """Classify a /proc/pid/fd readlink target."""
    if target.startswith('socket:'):
        return 'socket'
    if target.startswith('pipe:'):
        return 'pipe'
    if target.startswith('/dev/'):
        return 'device'
    if target.startswith('anon_inode:'):
        return target.split(':')[1].strip('[]')
    return 'file'


def get_daemon_worker_pids() -> list[int]:
    """Auto-detect AiiDA daemon worker PIDs."""
    try:
        result = subprocess.run(
            ['ps', 'aux'], capture_output=True, text=True, timeout=10,
        )
        pids = []
        for line in result.stdout.splitlines():
            if 'aiida' in line.lower() and ('daemon' in line.lower() or 'worker' in line.lower()):
                parts = line.split()
                try:
                    pids.append(int(parts[1]))
                except (IndexError, ValueError):
                    continue
        return pids
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def is_process_alive(pid: int) -> bool:
    """Check if a process is still running."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def format_fd_summary(fds: dict[int, dict[str, str]]) -> str:
    """Summarize FDs by type."""
    counts: dict[str, int] = {}
    for info in fds.values():
        t = info['type']
        counts[t] = counts.get(t, 0) + 1
    return ', '.join(f'{t}={c}' for t, c in sorted(counts.items()))


def monitor(
    pids: list[int],
    interval: float,
    output_path: str | None,
    verbose: bool,
):
    """Monitor FDs for the given PIDs."""
    prev_fds: dict[int, dict[int, dict[str, str]]] = {}
    csv_writer = None
    csv_file = None

    if output_path:
        csv_file = open(output_path, 'w', newline='')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'pid', 'total_fds', 'delta', 'summary', 'new_fds', 'closed_fds'])

    try:
        print(f'Monitoring PIDs: {pids} (interval: {interval}s)')
        print(f'Press Ctrl+C to stop.\n')

        while True:
            now = datetime.now().strftime('%H:%M:%S')
            alive_pids = [p for p in pids if is_process_alive(p)]

            if not alive_pids:
                print(f'[{now}] All monitored processes have exited.')
                break

            for pid in alive_pids:
                fds = get_open_fds(pid)
                total = len(fds)
                summary = format_fd_summary(fds)

                prev = prev_fds.get(pid, {})
                new = {fd: info for fd, info in fds.items() if fd not in prev}
                closed = {fd: info for fd, info in prev.items() if fd not in fds}
                delta = len(new) - len(closed)

                delta_str = f'+{delta}' if delta >= 0 else str(delta)
                line = f'[{now}] PID {pid}: {total} FDs ({delta_str}) [{summary}]'
                print(line)

                if verbose and new:
                    for fd, info in sorted(new.items()):
                        print(f'  + fd={fd} {info["type"]} {info["name"]}')
                if verbose and closed:
                    for fd, info in sorted(closed.items()):
                        print(f'  - fd={fd} {info["type"]} {info["name"]}')

                if csv_writer:
                    new_str = '; '.join(f'fd={fd} {info["type"]} {info["name"]}' for fd, info in sorted(new.items()))
                    closed_str = '; '.join(f'fd={fd} {info["type"]} {info["name"]}' for fd, info in sorted(closed.items()))
                    csv_writer.writerow([now, pid, total, delta, summary, new_str, closed_str])
                    csv_file.flush()

                prev_fds[pid] = fds

            time.sleep(interval)

    except KeyboardInterrupt:
        print('\nMonitoring stopped.')
    finally:
        if csv_file:
            csv_file.close()
            print(f'Log written to: {output_path}')


def main():
    parser = argparse.ArgumentParser(description='Monitor open file descriptors of a process.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--pid', type=int, nargs='+', help='PID(s) to monitor')
    group.add_argument('--daemon', action='store_true', help='Auto-detect AiiDA daemon worker PIDs')
    parser.add_argument('--interval', type=float, default=2.0, help='Polling interval in seconds (default: 2)')
    parser.add_argument('--output', '-o', type=str, help='Write CSV log to file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show individual FD changes')
    args = parser.parse_args()

    if args.daemon:
        pids = get_daemon_worker_pids()
        if not pids:
            print('No AiiDA daemon workers found. Is the daemon running?', file=sys.stderr)
            sys.exit(1)
        print(f'Found daemon worker PIDs: {pids}')
    else:
        pids = args.pid
        for pid in pids:
            if not is_process_alive(pid):
                print(f'PID {pid} is not running.', file=sys.stderr)
                sys.exit(1)

    monitor(pids, args.interval, args.output, args.verbose)


if __name__ == '__main__':
    main()
