"""State management for resume capability.

Uses SQLite to track completed operations so scraping can resume after interruptions.
Stores information about completed pages, API calls, and downloads.
"""

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class StateManager:
    """Manages scraping state for resume capability."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize state manager.

        Args:
            db_path: Path to SQLite database (default: metadata/state.db)
        """
        if db_path is None:
            db_path = Path(__file__).parent.parent / "metadata" / "state.db"

        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database
        self._init_db()
        logger.info("State manager initialized", db_path=str(self.db_path))

    @contextmanager
    def _get_connection(self):
        """Get database connection context manager."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("Database error", error=str(e))
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Table for task-level state
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    metadata TEXT
                )
            """)

            # Table for operation-level state (pages, API calls, etc.)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    operation_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    metadata TEXT,
                    UNIQUE(task_id, operation_type, operation_key)
                )
            """)

            # Table for downloaded files
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    file_hash TEXT,
                    downloaded_at TEXT NOT NULL,
                    file_size INTEGER,
                    metadata TEXT,
                    UNIQUE(task_id, url)
                )
            """)

            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_operations_task
                ON operations(task_id, operation_type)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_downloads_task
                ON downloads(task_id)
            """)

            conn.commit()

    def start_task(self, task_id: str, metadata: Optional[Dict] = None) -> None:
        """Mark task as started.

        Args:
            task_id: Unique task identifier
            metadata: Optional metadata dict
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO tasks (task_id, status, started_at, metadata)
                VALUES (?, ?, ?, ?)
            """, (
                task_id,
                'in_progress',
                datetime.utcnow().isoformat(),
                json.dumps(metadata) if metadata else None
            ))

        logger.info("Task started", task_id=task_id)

    def complete_task(self, task_id: str, metadata: Optional[Dict] = None) -> None:
        """Mark task as completed.

        Args:
            task_id: Unique task identifier
            metadata: Optional metadata dict to merge with existing
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get existing metadata
            cursor.execute("SELECT metadata FROM tasks WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            existing_metadata = json.loads(row['metadata']) if row and row['metadata'] else {}

            # Merge metadata
            if metadata:
                existing_metadata.update(metadata)

            cursor.execute("""
                UPDATE tasks
                SET status = ?, completed_at = ?, metadata = ?
                WHERE task_id = ?
            """, (
                'completed',
                datetime.utcnow().isoformat(),
                json.dumps(existing_metadata),
                task_id
            ))

        logger.info("Task completed", task_id=task_id)

    def is_task_completed(self, task_id: str) -> bool:
        """Check if task is completed.

        Args:
            task_id: Unique task identifier

        Returns:
            True if task is completed, False otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM tasks WHERE task_id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            return row is not None and row['status'] == 'completed'

    def record_operation(
        self,
        task_id: str,
        operation_type: str,
        operation_key: str,
        metadata: Optional[Dict] = None
    ) -> None:
        """Record a completed operation.

        Args:
            task_id: Task identifier
            operation_type: Type of operation (e.g., "api_call", "page_crawl")
            operation_key: Unique key for this operation (e.g., URL, page number)
            metadata: Optional metadata dict
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO operations
                (task_id, operation_type, operation_key, status, created_at, completed_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                task_id,
                operation_type,
                operation_key,
                'completed',
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
                json.dumps(metadata) if metadata else None
            ))

    def is_operation_completed(
        self,
        task_id: str,
        operation_type: str,
        operation_key: str
    ) -> bool:
        """Check if operation is completed.

        Args:
            task_id: Task identifier
            operation_type: Type of operation
            operation_key: Unique key for this operation

        Returns:
            True if operation is completed, False otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status FROM operations
                WHERE task_id = ? AND operation_type = ? AND operation_key = ?
            """, (task_id, operation_type, operation_key))
            row = cursor.fetchone()
            return row is not None and row['status'] == 'completed'

    def get_completed_operations(
        self,
        task_id: str,
        operation_type: Optional[str] = None
    ) -> List[str]:
        """Get list of completed operation keys.

        Args:
            task_id: Task identifier
            operation_type: Optional operation type filter

        Returns:
            List of operation keys
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if operation_type:
                cursor.execute("""
                    SELECT operation_key FROM operations
                    WHERE task_id = ? AND operation_type = ? AND status = 'completed'
                """, (task_id, operation_type))
            else:
                cursor.execute("""
                    SELECT operation_key FROM operations
                    WHERE task_id = ? AND status = 'completed'
                """, (task_id,))

            return [row['operation_key'] for row in cursor.fetchall()]

    def record_download(
        self,
        task_id: str,
        url: str,
        local_path: Path,
        metadata: Optional[Dict] = None
    ) -> None:
        """Record a completed download.

        Args:
            task_id: Task identifier
            url: Source URL
            local_path: Local file path
            metadata: Optional metadata dict
        """
        # Calculate file hash
        file_hash = None
        file_size = None
        if local_path.exists():
            file_hash = self._calculate_file_hash(local_path)
            file_size = local_path.stat().st_size

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO downloads
                (task_id, url, local_path, file_hash, downloaded_at, file_size, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                task_id,
                url,
                str(local_path),
                file_hash,
                datetime.utcnow().isoformat(),
                file_size,
                json.dumps(metadata) if metadata else None
            ))

    def is_downloaded(self, task_id: str, url: str) -> bool:
        """Check if URL has been downloaded.

        Args:
            task_id: Task identifier
            url: URL to check

        Returns:
            True if already downloaded, False otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT local_path FROM downloads
                WHERE task_id = ? AND url = ?
            """, (task_id, url))
            row = cursor.fetchone()

            # Check if file still exists
            if row:
                local_path = Path(row['local_path'])
                return local_path.exists()

            return False

    def get_task_stats(self, task_id: str) -> Dict[str, Any]:
        """Get statistics for a task.

        Args:
            task_id: Task identifier

        Returns:
            Dict with task statistics
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get task info
            cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            task_row = cursor.fetchone()

            if not task_row:
                return {'task_id': task_id, 'status': 'not_started'}

            # Get operation counts
            cursor.execute("""
                SELECT operation_type, COUNT(*) as count
                FROM operations
                WHERE task_id = ? AND status = 'completed'
                GROUP BY operation_type
            """, (task_id,))
            operation_counts = {row['operation_type']: row['count'] for row in cursor.fetchall()}

            # Get download count
            cursor.execute("""
                SELECT COUNT(*) as count FROM downloads WHERE task_id = ?
            """, (task_id,))
            download_count = cursor.fetchone()['count']

            return {
                'task_id': task_id,
                'status': task_row['status'],
                'started_at': task_row['started_at'],
                'completed_at': task_row['completed_at'],
                'operations': operation_counts,
                'downloads': download_count,
                'metadata': json.loads(task_row['metadata']) if task_row['metadata'] else {}
            }

    @staticmethod
    def _calculate_file_hash(filepath: Path) -> str:
        """Calculate SHA256 hash of file.

        Args:
            filepath: Path to file

        Returns:
            Hex digest of file hash
        """
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def reset_task(self, task_id: str) -> None:
        """Reset task state (useful for retry).

        Args:
            task_id: Task identifier
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM operations WHERE task_id = ?", (task_id,))
            cursor.execute("DELETE FROM downloads WHERE task_id = ?", (task_id,))
            cursor.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))

        logger.info("Task state reset", task_id=task_id)
