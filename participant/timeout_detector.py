"""
Heartbeat monitor for detecting coordinator failure.

When the coordinator stops sending heartbeats within the configured
window, on_timeout_callback is fired so the participant can begin
autonomous recovery — giving 3PC its non-blocking property.
"""

import threading
import time
from typing import Callable, Optional
import structlog

logger = structlog.get_logger()


class HeartbeatMonitor:
    """
    Background monitor that detects coordinator silence.

    The coordinator sends a POST /heartbeat every ~2 s.
    If none arrives within `heartbeat_timeout` seconds *while a
    transaction is in-flight*, `on_timeout_callback()` is invoked
    exactly once per timeout window.

    Args:
        participant_id:       identifier used in log messages
        heartbeat_timeout:    seconds of silence → coordinator presumed dead
        check_interval:       polling cadence of the background thread
        on_timeout_callback:  called (no args) when timeout fires
    """

    def __init__(
        self,
        participant_id: str,
        heartbeat_timeout: float = 5.0,
        check_interval: float = 0.5,
        on_timeout_callback: Optional[Callable[[], None]] = None,
    ) -> None:
        self.participant_id = participant_id
        self.heartbeat_timeout = heartbeat_timeout
        self.check_interval = check_interval
        self.on_timeout_callback = on_timeout_callback

        self._lock = threading.Lock()
        self._last_heartbeat: float = time.monotonic()
        self._active_transaction: bool = False
        self._monitoring: bool = False

        self._start_monitor()

    # ------------------------------------------------------------------
    # Public API  (called from Flask request threads)
    # ------------------------------------------------------------------

    def update_heartbeat(self) -> None:
        """Record receipt of any coordinator message."""
        with self._lock:
            self._last_heartbeat = time.monotonic()

    def mark_transaction_active(self) -> None:
        """Signal that a transaction is in-flight; start the clock."""
        with self._lock:
            self._last_heartbeat = time.monotonic()
            self._active_transaction = True

    def mark_transaction_done(self) -> None:
        """Signal that the transaction ended; suppress spurious timeouts."""
        with self._lock:
            self._active_transaction = False

    def is_coordinator_alive(self) -> bool:
        """Return True if the coordinator pinged recently."""
        with self._lock:
            return (time.monotonic() - self._last_heartbeat) < self.heartbeat_timeout

    def stop(self) -> None:
        """Shut down the background thread."""
        self._monitoring = False

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _start_monitor(self) -> None:
        self._monitoring = True
        thread = threading.Thread(
            target=self._monitor_loop,
            name=f"hb-monitor-{self.participant_id}",
            daemon=True,
        )
        thread.start()
        logger.info(
            "heartbeat_monitor_started",
            participant_id=self.participant_id,
            timeout_s=self.heartbeat_timeout,
        )

    def _monitor_loop(self) -> None:
        while self._monitoring:
            time.sleep(self.check_interval)

            with self._lock:
                in_flight = self._active_transaction
                elapsed = time.monotonic() - self._last_heartbeat

            if not in_flight:
                continue

            if elapsed >= self.heartbeat_timeout:
                logger.warning(
                    "coordinator_timeout_detected",
                    participant_id=self.participant_id,
                    elapsed_s=round(elapsed, 2),
                )

                # Reset before callback so repeated fires are suppressed
                # until the next transaction becomes active.
                self.mark_transaction_done()
                self.update_heartbeat()

                if self.on_timeout_callback is not None:
                    try:
                        self.on_timeout_callback()
                    except Exception as exc:
                        logger.error(
                            "timeout_callback_failed",
                            participant_id=self.participant_id,
                            error=str(exc),
                        )
