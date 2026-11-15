"""Simulated two-stage processing.

Outputs are *deterministic functions of their input* (summary = f(content),
tags = g(summary)). That is what makes content-hash caching semantically
valid, and lets tests assert a summary really came from the current content.
Latency and failures are random by default and fully injectable for tests.
"""

from __future__ import annotations

import asyncio
import random
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Protocol

from app.core.config import Settings
from app.domain.enums import Stage

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_WORD = re.compile(r"[a-zA-Z][a-zA-Z'-]+")
_STOPWORDS = frozenset(
    """a about above after again against all also am an and any are as at be because been
    before being below between both but by can could did do does doing down during each few
    for from further had has have having he her here hers him his how i if in into is it its
    itself just me more most my no nor not now of off on once only or other our ours out over
    own same she should so some such than that the their theirs them then there these they
    this those through to too under until up very was we were what when where which while who
    whom why will with would you your yours""".split()
)

SUMMARY_MAX_WORDS = 40
MAX_TAGS = 5


def make_summary(content: str) -> str:
    sentences = [s.strip() for s in _SENTENCE_SPLIT.split(content.strip()) if s.strip()]
    lead = " ".join(sentences[:2]) if sentences else content.strip()
    words = lead.split()
    if len(words) > SUMMARY_MAX_WORDS:
        return " ".join(words[:SUMMARY_MAX_WORDS]) + "…"
    return lead


def make_tags(summary: str) -> list[str]:
    tokens = [w.lower().strip("'-") for w in _WORD.findall(summary)]
    candidates = [t for t in tokens if len(t) >= 4 and t not in _STOPWORDS]
    counts = Counter(candidates)
    first_seen = {t: i for i, t in reversed(list(enumerate(candidates)))}
    ranked = sorted(counts, key=lambda t: (-counts[t], first_seen[t]))
    return ranked[:MAX_TAGS]


class StageFailure(Exception):
    """A simulated, retryable failure of one stage."""


class FailurePolicy(Protocol):
    def __call__(self, stage: Stage, attempt: int) -> bool: ...


class RandomFailurePolicy:
    def __init__(self, rate: float, rng: random.Random | None = None) -> None:
        self._rate = rate
        self._rng = rng or random.Random()

    def __call__(self, stage: Stage, attempt: int) -> bool:
        return self._rng.random() < self._rate


Sleeper = Callable[[float], Awaitable[None]]


class MockProcessor:
    def __init__(
        self,
        settings: Settings,
        failure_policy: FailurePolicy | None = None,
        sleeper: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._s = settings
        self._rng = rng or random.Random()
        self._fails = failure_policy or RandomFailurePolicy(settings.stage_failure_rate, self._rng)
        self._sleep = sleeper

    async def summarize(self, content: str, attempt: int) -> str:
        await self._sleep(self._rng.uniform(self._s.stage1_min_s, self._s.stage1_max_s))
        if self._fails(Stage.PROCESSING, attempt):
            raise StageFailure("simulated failure in processing stage")
        return make_summary(content)

    async def tag(self, summary: str, attempt: int) -> list[str]:
        await self._sleep(self._rng.uniform(self._s.stage2_min_s, self._s.stage2_max_s))
        if self._fails(Stage.ENRICHING, attempt):
            raise StageFailure("simulated failure in enriching stage")
        return make_tags(summary)


def backoff_delay(attempt: int, base: float, cap: float, rng: random.Random) -> float:
    """Exponential backoff with 'equal jitter': half fixed, half random. attempt is 1-based."""
    exp = min(cap, base * (2 ** (attempt - 1)))
    return exp / 2 + rng.uniform(0, exp / 2)
