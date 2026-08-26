#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO


TIMESTAMP_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)"
)
DESTINATION_RE = re.compile(
    r"dst:\s*ExtMessageDst\s*\{\s*"
    r"account_id:\s*(?P<account_id>[0-9a-fA-F]{64}),\s*"
    r"dapp_id:\s*(?P<dapp_id>Some\([0-9a-fA-F]{64}\)|None)"
    r"\s*\}"
)


@dataclass(frozen=True)
class Destination:
    account_id: str
    dapp_id: str


@dataclass
class Stats:
    destinations: Counter[Destination]
    accounts: set[str]
    lines_scanned: int = 0
    messages: int = 0
    first_log_ts: datetime | None = None
    last_log_ts: datetime | None = None
    first_message_ts: datetime | None = None
    last_message_ts: datetime | None = None


def parse_timestamp(line: str) -> datetime | None:
    match = TIMESTAMP_RE.match(line)
    if not match:
        return None
    value = match.group("ts")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def parse_destination(line: str) -> Destination | None:
    match = DESTINATION_RE.search(line)
    if not match:
        return None

    dapp_id = match.group("dapp_id")
    if dapp_id.startswith("Some("):
        dapp_id = dapp_id[5:-1]

    return Destination(
        account_id=match.group("account_id").lower(),
        dapp_id=dapp_id.lower(),
    )


def update_time_range(stats: Stats, timestamp: datetime | None, *, message: bool) -> None:
    if timestamp is None:
        return

    if stats.first_log_ts is None or timestamp < stats.first_log_ts:
        stats.first_log_ts = timestamp
    if stats.last_log_ts is None or timestamp > stats.last_log_ts:
        stats.last_log_ts = timestamp

    if message:
        if stats.first_message_ts is None or timestamp < stats.first_message_ts:
            stats.first_message_ts = timestamp
        if stats.last_message_ts is None or timestamp > stats.last_message_ts:
            stats.last_message_ts = timestamp


def scan_stream(stream: TextIO, stats: Stats) -> None:
    for line in stream:
        stats.lines_scanned += 1
        timestamp = parse_timestamp(line)
        destination = parse_destination(line)
        update_time_range(stats, timestamp, message=destination is not None)

        if destination is None:
            continue

        stats.destinations[destination] += 1
        stats.accounts.add(destination.account_id)
        stats.messages += 1


def open_log(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def format_duration(start: datetime | None, end: datetime | None) -> str:
    if start is None or end is None:
        return "n/a"
    return str(end - start)


def format_timestamp(timestamp: datetime | None) -> str:
    if timestamp is None:
        return "n/a"
    return timestamp.isoformat().replace("+00:00", "Z")


def print_text(stats: Stats, *, top: int | None) -> None:
    rows = stats.destinations.most_common(top)

    print(f"Lines scanned: {stats.lines_scanned}")
    print(f"Log time: {format_duration(stats.first_log_ts, stats.last_log_ts)}")
    print(f"Log first timestamp: {format_timestamp(stats.first_log_ts)}")
    print(f"Log last timestamp: {format_timestamp(stats.last_log_ts)}")
    print(f"Message time: {format_duration(stats.first_message_ts, stats.last_message_ts)}")
    print(f"Messages: {stats.messages}")
    print(f"Unique accounts: {len(stats.accounts)}")
    print(f"Unique destinations: {len(stats.destinations)}")

    if not rows:
        return

    print()
    print(f"{'count':>10}  {'percent':>7}  {'account_id':64}  dapp_id")
    for destination, count in rows:
        percent = count / stats.messages * 100
        print(
            f"{count:>10}  {percent:>6.2f}%  "
            f"{destination.account_id}  {destination.dapp_id}"
        )


def print_csv(stats: Stats, *, top: int | None) -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(["count", "percent", "account_id", "dapp_id"])
    for destination, count in stats.destinations.most_common(top):
        percent = count / stats.messages * 100 if stats.messages else 0
        writer.writerow([count, f"{percent:.2f}", destination.account_id, destination.dapp_id])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parse node logs and count external-message destinations from "
            "'dst: ExtMessageDst { account_id: ..., dapp_id: ... }' records."
        )
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="log file paths; reads stdin when omitted. .gz files are supported",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        help="print only the N most frequent destinations",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="print destination statistics as CSV without the summary header",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.top is not None and args.top < 1:
        print("Error: --top must be greater than 0", file=sys.stderr)
        return 2

    stats = Stats(destinations=Counter(), accounts=set())

    if not args.paths:
        scan_stream(sys.stdin, stats)
    else:
        for path in args.paths:
            try:
                with open_log(path) as stream:
                    scan_stream(stream, stats)
            except OSError as error:
                print(f"Error: failed to read {path}: {error}", file=sys.stderr)
                return 1

    if args.csv:
        print_csv(stats, top=args.top)
    else:
        print_text(stats, top=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
