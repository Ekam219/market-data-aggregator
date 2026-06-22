#!/usr/bin/env python3
"""
feed_simulator.py
-----------------
Simulates a market data feed by generating and sending
binary tick messages over TCP at configurable rates.

Useful for load-testing the C++ ingestion pipeline.

Usage:
    python python/feed_simulator.py --port 9001 --rate 500000 --feed-id FA
"""

import argparse
import socket
import struct
import time
import random
import sys
import zlib
from datetime import datetime


# Must match the C++ Tick layout (little-endian, packed)
# timestamp_ns(8) seq_id(8) bid(8) ask(8) last(8) volume(8) crc32(4) symbol(12) feed_id(4)
TICK_FORMAT  = "<QQdddIQ4sI12s"   # 64 bytes total
# Note: reordered to match C++ struct packing

def build_tick(seq: int, feed_id: str, symbol: str = "AAPL",
               corrupt: bool = False) -> bytes:
    now_ns = int(time.time_ns())
    bid    = round(random.uniform(100.0, 200.0), 4)
    ask    = round(bid + 0.01, 4)
    last   = round(bid + 0.005, 4)
    volume = random.randint(1, 10000)

    sym_b  = symbol.encode()[:11].ljust(12, b'\x00')
    fid_b  = feed_id.encode()[:3].ljust(4, b'\x00')

    # Pack without CRC first
    payload = struct.pack("<QQdddQ", now_ns, seq, bid, ask, last, volume)
    payload += fid_b + sym_b

    crc = zlib.crc32(payload) & 0xFFFFFFFF
    if corrupt:
        crc ^= 0xDEADBEEF

    # Insert CRC at byte offset 40 (after volume)
    tick = payload[:40] + struct.pack("<I", crc) + payload[40:]
    return tick


def simulate_feed(host: str, port: int, rate: int, feed_id: str,
                  duration: float, error_rate: float) -> None:
    interval = 1.0 / rate if rate > 0 else 0
    seq = 0
    sent = 0
    errors = 0
    start = time.monotonic()

    print(f"[{feed_id}] Connecting to {host}:{port} at {rate:,} ticks/sec…")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f"[{feed_id}] Connected. Streaming for {duration}s…")

        next_tick = time.monotonic()
        while time.monotonic() - start < duration:
            seq += 1
            corrupt = random.random() < error_rate
            tick = build_tick(seq, feed_id, corrupt=corrupt)
            try:
                sock.sendall(tick)
                sent += 1
                if corrupt: errors += 1
            except (BrokenPipeError, ConnectionResetError):
                print(f"[{feed_id}] Connection lost at seq={seq}")
                break

            next_tick += interval
            sleep = next_tick - time.monotonic()
            if sleep > 0:
                time.sleep(sleep)

    elapsed = time.monotonic() - start
    print(f"\n[{feed_id}] Done.")
    print(f"  Sent:        {sent:,} ticks")
    print(f"  CRC errors:  {errors:,} ({100*errors/max(sent,1):.3f}%)")
    print(f"  Elapsed:     {elapsed:.2f}s")
    print(f"  Actual rate: {sent/elapsed:,.0f} ticks/sec")


def main() -> None:
    parser = argparse.ArgumentParser(description="MDA feed simulator")
    parser.add_argument("--host",       default="localhost")
    parser.add_argument("--port",       type=int,   default=9001)
    parser.add_argument("--rate",       type=int,   default=100_000,
                        help="Target ticks per second")
    parser.add_argument("--feed-id",    default="FA")
    parser.add_argument("--duration",   type=float, default=10.0,
                        help="Simulation duration in seconds")
    parser.add_argument("--error-rate", type=float, default=0.001,
                        help="Fraction of ticks with corrupted CRC")
    args = parser.parse_args()

    try:
        simulate_feed(
            host=args.host,
            port=args.port,
            rate=args.rate,
            feed_id=args.feed_id,
            duration=args.duration,
            error_rate=args.error_rate,
        )
    except ConnectionRefusedError:
        print(f"ERROR: Cannot connect to {args.host}:{args.port}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")


if __name__ == "__main__":
    main()
