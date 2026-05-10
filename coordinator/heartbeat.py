"""
Coordinator heartbeat sender.

Periodically POSTs to /heartbeat on every registered participant so
they can detect coordinator failure via silence.  Participants are
registered dynamically as transactions start, so coverage is always
current even if participant sets change between transactions.
"""

import threading
import time
from typing import Dict, List
import requests
import structlog

logger = structlog.get_logger()

# Short timeout: heartbeat is an admin probe, not a protocol message
_SEND_TIMEOUT: int = 1


class CoordinatorHeartbeat:
    """
    Background thread that sends periodic liveness pings to participants.

    Args:
        coordinator_id: identifier echoed in each heartbeat payload
        interval:       seconds between rounds of heartbeats
    """

    def __init__(self, coordinator_id: str, interval: float = 2.0) -> None:
        self.coordinator_id = coordinator_id
        self.interval = interval

        # url -> url  (set acts as dedup registry)
        self._participants: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._running: bool = False

        self._start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_participants(self, urls: List[str]) -> None:
        """
        Add participant URLs to the heartbeat registry.

        Safe to call with already-registered URLs — idempotent.
        """
        with self._lock:
            for url in urls:
                if url not in self._participants:
                    self._participants[url] = url
                    logger.info("heartbeat_participant_registered", url=url)

    def stop(self) -> None:
        """Stop sending heartbeats."""
        self._running = False

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _start(self) -> None:
        self._running = True
        thread = threading.Thread(
            target=self._beat_loop,
            name="coordinator-heartbeat",
            daemon=True,
        )
        thread.start()
        logger.info(
            "coordinator_heartbeat_started",
            coordinator_id=self.coordinator_id,
            interval_s=self.interval,
        )

    def _beat_loop(self) -> None:
        while self._running:
            with self._lock:
                targets = list(self._participants.values())

            for url in targets:
                try:
                    requests.post(
                        f"{url}/heartbeat",
                        json={"coordinator_id": self.coordinator_id},
                        timeout=_SEND_TIMEOUT,
                    )
                except Exception:
                    # Silence is expected when participant is partitioned.
                    # The participant's HeartbeatMonitor handles the timeout.
                    pass

            time.sleep(self.interval)
