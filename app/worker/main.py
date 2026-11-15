"""Worker process: ``python -m app.worker.main``. Scale with ``docker compose up --scale worker=N``."""

from __future__ import annotations

import asyncio
import logging
import signal

from app.bootstrap import build_container
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.domain.processing import MockProcessor
from app.worker.pipeline import PipelineWorker

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    container = await build_container(settings)
    worker = PipelineWorker(
        container.repo, container.cache, container.limiter, MockProcessor(settings), settings
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        # On SIGTERM, in-flight stages finish their current unit; anything cut off
        # is re-claimed by another worker once its lease expires.
        await worker.run_forever(stop)
    finally:
        await container.close()


if __name__ == "__main__":
    asyncio.run(main())
