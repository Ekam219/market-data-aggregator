#!/usr/bin/env python3
"""
market_monitor.py
-----------------
Python companion for the Market Data Aggregation Service.
Connects to the aggregator's HTTP metrics endpoint (port 8080)
and streams live throughput, latency percentiles, and feed health.

Usage:
    python python/market_monitor.py [--host HOST] [--interval SECS]
"""

import argparse
import json
import time
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class FeedStats:
    feed_id: str
    ticks_received: int
    ticks_dropped: int
    crc_errors: int
    sequence_gaps: int
    state: str
    p50_us: float
    p99_us: float
    max_us: float


@dataclass
class AggregatorSnapshot:
    timestamp: str
    throughput_tps: float
    total_ticks: int
    feeds: List[FeedStats]


def fetch_metrics(host: str, port: int = 8080) -> Optional[dict]:
    url = f"http://{host}:{port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError:
        return None


def parse_snapshot(data: dict) -> AggregatorSnapshot:
    feeds = [
        FeedStats(
            feed_id=f["feed_id"],
            ticks_received=f.get("ticks_received", 0),
            ticks_dropped=f.get("ticks_dropped", 0),
            crc_errors=f.get("crc_errors", 0),
            sequence_gaps=f.get("sequence_gaps", 0),
            state=f.get("state", "UNKNOWN"),
            p50_us=f.get("p50_us", 0.0),
            p99_us=f.get("p99_us", 0.0),
            max_us=f.get("max_us", 0.0),
        )
        for f in data.get("feeds", [])
    ]
    return AggregatorSnapshot(
        timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        throughput_tps=data.get("throughput_tps", 0.0),
        total_ticks=data.get("total_ticks", 0),
        feeds=feeds,
    )


def render_table(snap: AggregatorSnapshot) -> None:
    WIDTH = 80
    print("=" * WIDTH)
    print(f"  Market Data Aggregator  |  {snap.timestamp}")
    print(f"  Throughput: {snap.throughput_tps:>12,.0f} ticks/sec"
          f"  |  Total: {snap.total_ticks:,}")
    print("-" * WIDTH)
    header = f"{'Feed':<10} {'State':<12} {'Recv':>10} {'Drop':>8} "
    header += f"{'CRC Err':>8} {'SeqGap':>8} {'p50µs':>7} {'p99µs':>7}"
    print(header)
    print("-" * WIDTH)

    for f in snap.feeds:
        state_sym = {
            "ACTIVE":      "✓ ACTIVE",
            "DROPPED":     "✗ DROPPED",
            "STALE":       "⚠ STALE",
            "RECOVERING":  "↺ RECOVER",
            "CONNECTING":  "… CONNECT",
        }.get(f.state, f.state)

        row = (f"{f.feed_id:<10} {state_sym:<12} {f.ticks_received:>10,} "
               f"{f.ticks_dropped:>8,} {f.crc_errors:>8,} "
               f"{f.sequence_gaps:>8,} {f.p50_us:>7.1f} {f.p99_us:>7.1f}")
        print(row)

    print("=" * WIDTH)


def simulate_snapshot() -> AggregatorSnapshot:
    """Generate a realistic fake snapshot for demo/testing."""
    import random
    feeds = [
        FeedStats(
            feed_id=fid,
            ticks_received=random.randint(800_000, 1_200_000),
            ticks_dropped=random.randint(0, 50),
            crc_errors=random.randint(0, 10),
            sequence_gaps=random.randint(0, 5),
            state="ACTIVE",
            p50_us=round(random.uniform(1.5, 3.0), 1),
            p99_us=round(random.uniform(4.0, 8.0), 1),
            max_us=round(random.uniform(8.0, 9.9), 1),
        )
        for fid in ["FEED_A", "FEED_B", "FEED_C"]
    ]
    return AggregatorSnapshot(
        timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        throughput_tps=sum(f.ticks_received for f in feeds),
        total_ticks=sum(f.ticks_received for f in feeds) * 60,
        feeds=feeds,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="MDA live monitor")
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=8080)
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Refresh interval in seconds")
    parser.add_argument("--demo",     action="store_true",
                        help="Run in demo mode with simulated data")
    args = parser.parse_args()

    print(f"[MDA Monitor] Connecting to {args.host}:{args.port} "
          f"(refresh={args.interval}s)")
    if args.demo:
        print("[MDA Monitor] DEMO MODE — using simulated data\n")

    try:
        while True:
            if args.demo:
                snap = simulate_snapshot()
            else:
                data = fetch_metrics(args.host, args.port)
                if data is None:
                    print(f"[{datetime.utcnow().isoformat()}] "
                          "Cannot reach aggregator — retrying…")
                    time.sleep(args.interval)
                    continue
                snap = parse_snapshot(data)

            render_table(snap)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[MDA Monitor] Stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
