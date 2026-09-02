"""Transport contract.

A `Transport` does exactly one HTTP GET and returns a `Result`. Everything
resilient lives *above* this line, so the risky part (the socket) is a tiny,
swappable surface. `UrllibTransport` is the one real implementation
(stdlib only); tests inject fakes that fail on a scripted schedule.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class Result:
    status: int
    body: Optional[bytes] = None
    #: transport-level failure only (DNS, timeout, refused). An HTTP 4xx/5xx
    #: answer carries its code in `status` — conflating the two would make a
    #: 404 look like a network blip and get it uselessly retried.
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and 200 <= self.status < 300

    @property
    def retryable(self) -> bool:
        """Transient conditions worth retrying: 408/429/5xx and network errors."""
        if self.error is not None:
            return True
        return self.status in (408, 429) or 500 <= self.status < 600


class Transport(ABC):
    @abstractmethod
    async def get(self, url: str, timeout: float) -> Result:
        ...


class UrllibTransport:
    """Stdlib-only HTTP GET run in a thread so the event loop stays responsive."""

    def __init__(self, user_agent: str = "resilient-fetcher/0.1") -> None:
        self.user_agent = user_agent

    async def get(self, url: str, timeout: float) -> Result:
        import asyncio
        import urllib.error
        import urllib.request

        def _do() -> Result:
            req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (http/https only per caller)
                    return Result(status=resp.status, body=resp.read())
            except urllib.error.HTTPError as e:  # a real HTTP answer: status only
                return Result(status=e.code, body=None)
            except Exception as e:  # URLError, timeouts, DNS — network-flavoured
                return Result(status=0, error=f"{type(e).__name__}: {e}")

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _do)
