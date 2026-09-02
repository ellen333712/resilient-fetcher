"""CLI: fetch URLs concurrently with sane resilience defaults.

    resilient-fetch https://a.example/x https://b.example/y -c 16 -r 5
    cat urls.txt | resilient-fetch --from-stdin -o report.json

Exit code: 0 if everything came back OK, 1 otherwise. Output is a small
JSON report (or a plain table) — machine-readable by design.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import List, Optional

from .backoff import ExponentialBackoff
from .circuit import CircuitBreaker
from .clock import MonotonicClock
from .fetcher import FetchPolicy, ResilientFetcher, RetryBudget, RetryBudgetExhausted
from .ratelimit import TokenBucket
from .transport import UrllibTransport


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="resilient-fetch", description=__doc__.splitlines()[0])
    p.add_argument("urls", nargs="*", help="URLs to GET")
    p.add_argument("--from-stdin", action="store_true", help="read one URL per line from stdin")
    p.add_argument("-c", "--concurrency", type=int, default=8)
    p.add_argument("-r", "--rate", type=float, default=10.0, help="requests per second (token bucket refill)")
    p.add_argument("-b", "--burst", type=int, default=0, help="bucket capacity (default: max(1, ceil(rate)))")
    p.add_argument("-a", "--max-attempts", type=int, default=4)
    p.add_argument("-t", "--timeout", type=float, default=10.0)
    p.add_argument("--retry-budget", type=int, default=0, help="max retries across the batch (0 = 2x URL count)")
    p.add_argument("--failure-threshold", type=int, default=5, help="breaker: consecutive failures to open")
    p.add_argument("--recovery-timeout", type=float, default=30.0, help="breaker: seconds OPEN before HALF_OPEN")
    p.add_argument("-o", "--output", help="write JSON report to this file")
    p.add_argument("--json", action="store_true", help="print JSON instead of a table")
    return p


def _collect(args: argparse.Namespace) -> List[str]:
    urls: List[str] = list(args.urls)
    if args.from_stdin:
        urls += [line.strip() for line in sys.stdin if line.strip() and not line.startswith("#")]
    if not urls:
        raise SystemExit("no URLs given (pass them as args or --from-stdin)")
    return urls


def build_fetcher(args: argparse.Namespace) -> ResilientFetcher:
    clock = MonotonicClock()
    rate = max(args.rate, 0.1)
    burst = args.burst or max(1, int(rate + 0.999))
    urls_n = max(1, args.concurrency)
    budget = args.retry_budget or (urls_n * 2 if args.retry_budget == 0 else 0)
    return ResilientFetcher(
        transport=UrllibTransport(),
        bucket=TokenBucket(capacity=burst, refill_rate=rate, clock=clock),
        breaker=CircuitBreaker(
            clock=clock,
            failure_threshold=args.failure_threshold,
            recovery_timeout=args.recovery_timeout,
        ),
        backoff=ExponentialBackoff(base=0.5, max_delay=min(30.0, args.timeout), jitter="full"),
        policy=FetchPolicy(max_attempts=args.max_attempts, timeout=args.timeout),
        budget=RetryBudget(capacity=max(1, int(budget)), refill_per_sec=max(0.5, rate / 2), clock=clock),
    )


def _report(outcomes) -> dict:
    return {
        "total": len(outcomes),
        "ok": sum(1 for o in outcomes if o.ok),
        "failed": sum(1 for o in outcomes if not o.ok),
        "retries": sum(o.retries_used for o in outcomes),
        "circuit_aborted": sum(1 for o in outcomes if o.aborted_by_circuit),
        "results": [
            {
                "url": o.url,
                "status": o.result.status,
                "ok": o.ok,
                "attempts": o.attempts,
                "retries": o.retries_used,
                "bytes": len(o.result.body) if o.result.body else 0,
                "error": o.result.error,
            }
            for o in outcomes
        ],
    }


async def _amain(args: argparse.Namespace) -> int:
    fetcher = build_fetcher(args)
    urls = _collect(args)
    try:
        outcomes = await fetcher.fetch_many(urls, concurrency=args.concurrency)
    except RetryBudgetExhausted as e:
        print(f"retry budget exhausted: {e}", file=sys.stderr)
        return 2
    rep = _report(outcomes)
    if args.json or args.output:
        text = json.dumps(rep, indent=2)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text + "\n")
        if args.json or not args.output:
            print(text)
    else:
        for r in rep["results"]:
            mark = "✓" if r["ok"] else "✗"
            err = f" {r['error']}" if r["error"] else ""
            print(f"{mark} [{r['status']:>3}] {r['attempts']}att {r['retries']}ret {r['bytes']:>8}B  {r['url']}{err}")
        print(f"— {rep['ok']}/{rep['total']} ok, {rep['retries']} retries, {rep['circuit_aborted']} circuit-aborted")
    return 0 if rep["failed"] == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
