"""
Metrics collector for 3PC protocol.

Tracks: transactions, latency, state transitions, recoveries.
"""

from collections import defaultdict
from threading import Lock
from datetime import datetime

class MetricsCollector:
    """Thread-safe metrics collection."""
    
    def __init__(self):
        self.lock = Lock()
        
        # Counters
        self.transaction_count = 0
        self.commit_count = 0
        self.abort_count = 0
        self.recovery_count = 0
        
        # Latencies (ms) - store last 100
        self.phase1_latencies = []
        self.phase2_latencies = []
        self.phase3_latencies = []
        self.total_latencies = []
        self.max_latency_history = 100
        
        # State transitions
        self.state_transitions = defaultdict(int)
        
        # Failures
        self.partition_count = 0
        self.timeout_count = 0
        
        # Timeline for charts (last 50 transactions)
        self.transaction_timeline = []
        self.max_timeline = 50
    
    def record_transaction_start(self, txn_id: str):
        with self.lock:
            self.transaction_count += 1
            self.transaction_timeline.append({
                'txn_id': txn_id,
                'timestamp': datetime.now().isoformat(),
                'outcome': 'pending'
            })
            # Keep only last N
            if len(self.transaction_timeline) > self.max_timeline:
                self.transaction_timeline.pop(0)
    
    def record_commit(self, txn_id: str):
        with self.lock:
            self.commit_count += 1
            # Update timeline
            for t in self.transaction_timeline:
                if t['txn_id'] == txn_id:
                    t['outcome'] = 'commit'
    
    def record_abort(self, txn_id: str):
        with self.lock:
            self.abort_count += 1
            for t in self.transaction_timeline:
                if t['txn_id'] == txn_id:
                    t['outcome'] = 'abort'
    
    def record_recovery(self):
        with self.lock:
            self.recovery_count += 1
    
    def record_phase_latency(self, phase: int, latency_ms: float):
        with self.lock:
            if phase == 1:
                self.phase1_latencies.append(latency_ms)
                if len(self.phase1_latencies) > self.max_latency_history:
                    self.phase1_latencies.pop(0)
            elif phase == 2:
                self.phase2_latencies.append(latency_ms)
                if len(self.phase2_latencies) > self.max_latency_history:
                    self.phase2_latencies.pop(0)
            elif phase == 3:
                self.phase3_latencies.append(latency_ms)
                if len(self.phase3_latencies) > self.max_latency_history:
                    self.phase3_latencies.pop(0)
    
    def record_total_latency(self, latency_ms: float):
        with self.lock:
            self.total_latencies.append(latency_ms)
            if len(self.total_latencies) > self.max_latency_history:
                self.total_latencies.pop(0)
    
    def record_state_transition(self, from_state: str, to_state: str):
        with self.lock:
            key = f"{from_state}→{to_state}"
            self.state_transitions[key] += 1
    
    def record_partition(self):
        with self.lock:
            self.partition_count += 1
    
    def record_timeout(self):
        with self.lock:
            self.timeout_count += 1
    
    def get_snapshot(self) -> dict:
        """Get current metrics snapshot."""
        with self.lock:
            total = max(self.transaction_count, 1)
            
            return {
                "transactions": {
                    "total": self.transaction_count,
                    "committed": self.commit_count,
                    "aborted": self.abort_count,
                    "commit_rate": round(self.commit_count / total * 100, 1)
                },
                "latency": {
                    "phase1_avg": round(sum(self.phase1_latencies) / max(len(self.phase1_latencies), 1), 2),
                    "phase2_avg": round(sum(self.phase2_latencies) / max(len(self.phase2_latencies), 1), 2),
                    "phase3_avg": round(sum(self.phase3_latencies) / max(len(self.phase3_latencies), 1), 2),
                    "total_avg": round(sum(self.total_latencies) / max(len(self.total_latencies), 1), 2),
                    "phase1_data": self.phase1_latencies[-20:],  # Last 20
                    "phase2_data": self.phase2_latencies[-20:],
                    "phase3_data": self.phase3_latencies[-20:]
                },
                "failures": {
                    "partitions": self.partition_count,
                    "timeouts": self.timeout_count
                },
                "recovery": {
                    "count": self.recovery_count
                },
                "state_transitions": dict(self.state_transitions),
                "timeline": self.transaction_timeline[-20:]  # Last 20
            }

# Global collector
metrics = MetricsCollector()