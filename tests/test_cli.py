"""Smoke tests around the CLI wiring (no network)."""
from __future__ import annotations

import unittest

from resilient_fetcher.__main__ import _parser, build_fetcher, _report
from resilient_fetcher.fetcher import FetchOutcome
from resilient_fetcher.transport import Result


class CliTest(unittest.TestCase):
    def test_defaults_build_a_working_fetcher(self):
        args = _parser().parse_args(["http://x/a"])
        f = build_fetcher(args)
        self.assertIsNotNone(f.bucket)
        self.assertIsNotNone(f.breaker)
        self.assertIsNotNone(f.budget)

    def test_report_math(self):
        outcomes = [
            FetchOutcome("a", Result(200, b"xx"), 1, 0),
            FetchOutcome("b", Result(500, error="HTTP 500"), 3, 2),
            FetchOutcome("c", Result(0, error="circuit open"), 1, 0, aborted_by_circuit=True),
        ]
        rep = _report(outcomes)
        self.assertEqual(rep["total"], 3)
        self.assertEqual(rep["ok"], 1)
        self.assertEqual(rep["failed"], 2)
        self.assertEqual(rep["retries"], 2)
        self.assertEqual(rep["circuit_aborted"], 1)

    def test_urls_from_stdin_flag_parses(self):
        args = _parser().parse_args(["--from-stdin"])
        self.assertTrue(args.from_stdin)
