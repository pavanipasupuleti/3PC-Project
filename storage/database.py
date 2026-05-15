"""
SQLite database for persistent transaction storage.
"""

import sqlite3
from datetime import datetime
import threading
import structlog

logger = structlog.get_logger()


class TransactionStore:
    """Thread-safe SQLite storage for 3PC transactions."""

    def __init__(self, db_path='data/3pc_transactions.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._create_tables()

        logger.info(
            "database_initialized",
            db_path=db_path
        )

    def _get_connection(self):
        """Get SQLite connection."""
        return sqlite3.connect(
            self.db_path,
            check_same_thread=False
        )

    def _create_tables(self):
        """Create database tables."""

        conn = self._get_connection()
        cursor = conn.cursor()

        # Main transaction table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                num_participants INTEGER,

                phase1_latency REAL,
                phase2_latency REAL,
                phase3_latency REAL,
                total_latency REAL,

                created_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        ''')

        # Detailed event log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transaction_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                txn_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                phase TEXT,
                timestamp TIMESTAMP,
                details TEXT,
                FOREIGN KEY (txn_id)
                    REFERENCES transactions(txn_id)
            )
        ''')

        conn.commit()
        conn.close()

    # ---------------------------------------------------------
    # Save transaction
    # ---------------------------------------------------------

    def save_transaction(
        self,
        txn_id,
        status,
        num_participants=0,
        phase1_latency=None,
        phase2_latency=None,
        phase3_latency=None,
        total_latency=None,
        created_at=None,
        completed_at=None
    ):
        """Insert or update transaction."""

        with self.lock:

            conn = self._get_connection()
            cursor = conn.cursor()

            try:

                cursor.execute('''
                    INSERT OR REPLACE INTO transactions
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    txn_id,
                    status,
                    num_participants,

                    phase1_latency,
                    phase2_latency,
                    phase3_latency,
                    total_latency,

                    created_at or datetime.now(),
                    completed_at
                ))

                conn.commit()

                logger.info(
                    "transaction_saved",
                    txn_id=txn_id,
                    status=status
                )

            except Exception as e:

                logger.error(
                    "save_failed",
                    txn_id=txn_id,
                    error=str(e)
                )

                conn.rollback()

            finally:
                conn.close()

    # ---------------------------------------------------------
    # Event logging
    # ---------------------------------------------------------

    def log_event(
        self,
        txn_id,
        event_type,
        phase=None,
        details=None
    ):
        """Log transaction event."""

        with self.lock:

            conn = self._get_connection()
            cursor = conn.cursor()

            try:

                cursor.execute('''
                    INSERT INTO transaction_events
                    (
                        txn_id,
                        event_type,
                        phase,
                        timestamp,
                        details
                    )
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    txn_id,
                    event_type,
                    phase,
                    datetime.now(),
                    details
                ))

                conn.commit()

            except Exception as e:

                logger.error(
                    "event_log_failed",
                    error=str(e)
                )

                conn.rollback()

            finally:
                conn.close()

    # ---------------------------------------------------------
    # Single transaction lookup
    # ---------------------------------------------------------

    def get_transaction(self, txn_id):

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            'SELECT * FROM transactions WHERE txn_id = ?',
            (txn_id,)
        )

        result = cursor.fetchone()

        conn.close()

        return result

    # ---------------------------------------------------------
    # Recent transactions
    # ---------------------------------------------------------

    def get_all_transactions(self, limit=100):

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT *
            FROM transactions
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))

        results = cursor.fetchall()

        conn.close()

        return results

    # ---------------------------------------------------------
    # Statistics for dashboard
    # ---------------------------------------------------------

    def get_statistics(self):
        """Get dashboard statistics."""

        conn = self._get_connection()
        cursor = conn.cursor()

        # Overall stats
        cursor.execute("""
            SELECT
                COUNT(*) as total,

                SUM(
                    CASE
                        WHEN status='COMMIT'
                        THEN 1
                        ELSE 0
                    END
                ) as committed,

                SUM(
                    CASE
                        WHEN status='ABORT'
                        THEN 1
                        ELSE 0
                    END
                ) as aborted,

                AVG(total_latency) as avg_latency,
                AVG(phase1_latency) as avg_phase1,
                AVG(phase2_latency) as avg_phase2,
                AVG(phase3_latency) as avg_phase3

            FROM transactions
        """)

        result = cursor.fetchone()

        # Recent latency history
        cursor.execute("""
            SELECT
                phase1_latency,
                phase2_latency,
                phase3_latency

            FROM transactions

            ORDER BY created_at DESC

            LIMIT 20
        """)

        rows = cursor.fetchall()

        conn.close()

        phase1_history = [
            r[0] or 0 for r in rows
        ]

        phase2_history = [
            r[1] or 0 for r in rows
        ]

        phase3_history = [
            r[2] or 0 for r in rows
        ]

        return {
            "total": result[0] or 0,
            "committed": result[1] or 0,
            "aborted": result[2] or 0,

            "avg_latency": result[3] or 0,
            "avg_phase1": result[4] or 0,
            "avg_phase2": result[5] or 0,
            "avg_phase3": result[6] or 0,

            "phase1_history": phase1_history[::-1],
            "phase2_history": phase2_history[::-1],
            "phase3_history": phase3_history[::-1]
        }


# Global singleton
db_store = TransactionStore()