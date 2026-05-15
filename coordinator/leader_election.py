"""
etcd-based leader election for the 3PC coordinator.
Provides distributed leader election so only one coordinator
node executes transactions at a time.
"""

import threading
import time
import structlog

logger = structlog.get_logger()

LEADER_KEY = "/3pc/leader"
LEASE_TTL = 10
HEARTBEAT_INTERVAL = 5
RETRY_INTERVAL = 3


class LeaderElection:

    def __init__(self, node_id: str, host: str = 'etcd', port: int = 2379):
        self.node_id = node_id
        self.host = host
        self.port = port

        self.is_leader = False

        self._client = None
        self._lease = None
        self._lock = threading.Lock()

        self._heartbeat_thread = None
        self._monitor_thread = None

        self._running = False
        self._etcd_available = False

        self._connect()

    def _connect(self):
        """Connect to etcd."""
        try:
            import etcd3
            self._client = etcd3.client(host=self.host, port=self.port)
            self._client.status()
            self._etcd_available = True
            logger.info("etcd_connected", node_id=self.node_id, host=self.host, port=self.port)
        except Exception as e:
            self._etcd_available = False
            logger.error("etcd_connection_failed", node_id=self.node_id, error=str(e))

    # ------------------------------------------------------------------
    # Leader Election
    # ------------------------------------------------------------------

    def try_become_leader(self):
        """
        Attempt atomic leader acquisition.
        """
        if not self._etcd_available:
            logger.error("etcd_unavailable", node_id=self.node_id)
            with self._lock:
                self.is_leader = False
            return False

        try:
            self._lease = self._client.lease(LEASE_TTL)

            # Atomic transaction to ensure only one node writes the key
            success, _ = self._client.transaction(
                compare=[
                    self._client.transactions.create(LEADER_KEY) == 0
                ],
                success=[
                    self._client.transactions.put(
                        LEADER_KEY,
                        self.node_id,
                        lease=self._lease
                    )
                ],
                failure=[]
            )

            logger.info("election_attempt_result", node_id=self.node_id, success=success)

            with self._lock:
                self.is_leader = success

            if success:
                logger.info("leadership_acquired", node_id=self.node_id)
                self._start_heartbeat()
            else:
                current = self.get_current_leader()
                logger.info("standby_mode", node_id=self.node_id, current_leader=current)

            return success

        except Exception as e:
            logger.error("leader_election_failed", node_id=self.node_id, error=str(e))
            with self._lock:
                self.is_leader = False
            return False

    # ------------------------------------------------------------------
    # Leader Monitoring
    # ------------------------------------------------------------------

    def start_monitoring(self):
        """Start background monitoring thread."""
        if self._monitor_thread:
            return

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="leader-monitor"
        )
        self._monitor_thread.start()

    def _monitor_loop(self):
        """Continuously monitor leader state. If no leader exists, retry election."""
        while True:
            try:
                current = self.get_current_leader()
                if current != self.node_id:
                    logger.info("attempting_failover_election", node_id=self.node_id ,current_leader=current)
                    self.try_become_leader()
                elif current == self.node_id:
                    with self._lock:
                        self.is_leader = True
                else:
                    with self._lock:
                        self.is_leader = False
            except Exception as e:
                logger.error("monitor_loop_failed", node_id=self.node_id, error=str(e))
            
            time.sleep(RETRY_INTERVAL)

    def step_down(self):
        self._running = False
        try:
            if self._lease:
                self._lease.revoke()
                logger.info("leadership_released", node_id=self.node_id)
        except Exception as e:
            logger.error("step_down_failed", node_id=self.node_id, error=str(e))
        finally:
            with self._lock:
                self.is_leader = False
            self._lease = None

    def get_current_leader(self) -> str:
        if not self._etcd_available or self._client is None:
            return "unknown"
        try:
            value, _ = self._client.get(LEADER_KEY)
            if value:
                return value.decode()
        except Exception as e:
            logger.error("get_leader_failed", node_id=self.node_id, error=str(e))
        return "unknown"

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _start_heartbeat(self):
        if self._heartbeat_thread:
            return
        self._running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="leader-heartbeat"
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break
            try:
                if self._lease:
                    self._lease.refresh()
                    logger.debug("lease_renewed", node_id=self.node_id)
            except Exception as e:
                logger.error("lease_renewal_failed", node_id=self.node_id, error=str(e))
                self._handle_lease_loss()
                break

    def _handle_lease_loss(self):
        with self._lock:
            self.is_leader = False
        logger.warning("leader_lost", node_id=self.node_id)
        time.sleep(2)
        self.try_become_leader()

























































# """
# etcd-based leader election for the 3PC coordinator.

# Provides distributed leader election so only one coordinator
# node executes transactions at a time. Degrades gracefully
# if etcd is unavailable — the coordinator falls back to
# assuming leadership so transactions are never blocked by
# an infra failure.
# """

# import threading
# import time
# import structlog

# logger = structlog.get_logger()

# LEADER_KEY = "/3pc/leader"
# LEASE_TTL = 10        # seconds before lease expires
# HEARTBEAT_INTERVAL = 5  # seconds between lease renewals


# class LeaderElection:
#     """
#     Distributed leader election backed by etcd.

#     Usage:
#         election = LeaderElection(node_id='coordinator-1')
#         election.try_become_leader()

#         if election.is_leader:
#             # run transaction
#     """

#     def __init__(self, node_id: str, host: str = 'etcd', port: int = 2379):
#         self.node_id = node_id
#         self.host = host
#         self.port = port
#         self.is_leader = False

#         self._client = None
#         self._lease = None
#         self._lock = threading.Lock()
#         self._heartbeat_thread = None
#         self._running = False
#         self._etcd_available = False

#         self._connect()

#     # ------------------------------------------------------------------
#     # Connection
#     # ------------------------------------------------------------------

#     def _connect(self):
#         """Attempt to connect to etcd. Fails silently on error."""
#         try:
#             import etcd3
#             self._client = etcd3.client(host=self.host, port=self.port)
#             # Probe the connection with a lightweight call
#             self._client.status()
#             self._etcd_available = True
#             logger.info("etcd_connected", node_id=self.node_id,
#                         host=self.host, port=self.port)
#         except Exception as e:
#             self._etcd_available = False
#             logger.warning("etcd_unavailable", node_id=self.node_id,
#                            error=str(e),
#                            fallback="assuming leadership")

#     # ------------------------------------------------------------------
#     # Public API
#     # ------------------------------------------------------------------

#     def try_become_leader(self):
#         """
#         Attempt to acquire the distributed leader lock.

#         If etcd is unreachable the node assumes leadership so that
#         the Flask app continues to serve requests.
#         """
#         if not self._etcd_available:
#             logger.warning("etcd_not_available_assuming_leader",
#                            node_id=self.node_id)
#             with self._lock:
#                 self.is_leader = True
#             return

#         try:
#             self._lease = self._client.lease(LEASE_TTL)

#             # Atomic compare-and-swap: only write if key does not exist
#             success, _ = self._client.transaction(
#                 compare=[
#                     self._client.transactions.version(LEADER_KEY) == 0
#                 ],
#                 success=[
#                     self._client.transactions.put(
#                         LEADER_KEY,
#                         self.node_id,
#                         lease=self._lease
#                     )
#                 ],
#                 failure=[]
#             )

#             with self._lock:
#                 self.is_leader = success

#             if success:
#                 logger.info("leadership_acquired", node_id=self.node_id)
#                 self._start_heartbeat()
#             else:
#                 current = self.get_current_leader()
#                 logger.info("standby_mode", node_id=self.node_id,
#                             current_leader=current)

#         except Exception as e:
#             logger.error("leader_election_failed", node_id=self.node_id,
#                          error=str(e), fallback="assuming leadership")
#             with self._lock:
#                 self.is_leader = True

#     def step_down(self):
#         """Release leadership cleanly."""
#         self._running = False

#         try:
#             if self._lease:
#                 self._lease.revoke()
#                 logger.info("leadership_released", node_id=self.node_id)
#         except Exception as e:
#             logger.error("step_down_failed", node_id=self.node_id,
#                          error=str(e))
#         finally:
#             with self._lock:
#                 self.is_leader = False
#             self._lease = None

#     def get_current_leader(self) -> str:
#         """Return the node_id of the current leader, or 'unknown'."""
#         if not self._etcd_available or self._client is None:
#             return self.node_id  # fallback: report self

#         try:
#             value, _ = self._client.get(LEADER_KEY)
#             if value:
#                 return value.decode()
#         except Exception as e:
#             logger.error("get_leader_failed", node_id=self.node_id,
#                          error=str(e))

#         return "unknown"

#     # ------------------------------------------------------------------
#     # Heartbeat
#     # ------------------------------------------------------------------

#     def _start_heartbeat(self):
#         """Spawn a daemon thread to renew the etcd lease periodically."""
#         self._running = True
#         self._heartbeat_thread = threading.Thread(
#             target=self._heartbeat_loop,
#             name="leader-heartbeat",
#             daemon=True
#         )
#         self._heartbeat_thread.start()

#     def _heartbeat_loop(self):
#         """Renew lease every HEARTBEAT_INTERVAL seconds."""
#         while self._running:
#             time.sleep(HEARTBEAT_INTERVAL)

#             if not self._running:
#                 break

#             try:
#                 if self._lease:
#                     self._lease.refresh()
#                     logger.debug("lease_renewed", node_id=self.node_id)
#             except Exception as e:
#                 logger.error("lease_renewal_failed", node_id=self.node_id,
#                              error=str(e))
#                 self._handle_lease_loss()
#                 break

#     def _handle_lease_loss(self):
#         """React to a lost lease — attempt to re-acquire."""
#         with self._lock:
#             self.is_leader = False

#         logger.warning("leader_lost", node_id=self.node_id,
#                        action="attempting re-election")

#         # Back off briefly then retry
#         time.sleep(2)
#         self.try_become_leader()
