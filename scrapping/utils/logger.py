"""Structured logging utility for scraping operations.

Logs are written in JSONL format to metadata/harvest_log.jsonl with the following structure:
- timestamp: ISO 8601 timestamp
- level: log level (INFO, WARNING, ERROR, etc.)
- run_id: unique identifier for the execution run
- task_id: identifier for the task being executed
- source_id: data source identifier
- message: log message
- metrics: optional dict with success/failure counts, timing, etc.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import structlog


class JSONLHandler(logging.Handler):
    """Custom handler that writes logs in JSONL format."""

    def __init__(self, filepath: Path):
        super().__init__()
        self.filepath = filepath
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        """Write log record as a JSON line."""
        try:
            log_entry = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
            }

            # Add extra fields if present
            if hasattr(record, 'run_id'):
                log_entry['run_id'] = record.run_id
            if hasattr(record, 'task_id'):
                log_entry['task_id'] = record.task_id
            if hasattr(record, 'source_id'):
                log_entry['source_id'] = record.source_id
            if hasattr(record, 'metrics'):
                log_entry['metrics'] = record.metrics
            if hasattr(record, 'extra'):
                log_entry['extra'] = record.extra

            # Write to file
            with open(self.filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

        except Exception as e:
            # Fallback to stderr if writing fails
            print(f"Logging error: {e}", file=sys.stderr)


def setup_logger(
    log_file: Optional[Path] = None,
    level: str = "INFO",
    console_output: bool = True
) -> None:
    """Configure structured logging for the application.

    Args:
        log_file: Path to JSONL log file (default: metadata/harvest_log.jsonl)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        console_output: Whether to also output logs to console
    """
    if log_file is None:
        log_file = Path(__file__).parent.parent / "metadata" / "harvest_log.jsonl"

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Setup standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add JSONL file handler
    jsonl_handler = JSONLHandler(log_file)
    jsonl_handler.setLevel(getattr(logging, level.upper()))
    root_logger.addHandler(jsonl_handler)

    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)


def get_logger(
    name: str,
    task_id: Optional[str] = None,
    source_id: Optional[str] = None,
    run_id: Optional[str] = None
) -> structlog.BoundLogger:
    """Get a logger instance with optional context.

    Args:
        name: Logger name (typically __name__)
        task_id: Optional task identifier
        source_id: Optional data source identifier
        run_id: Optional run identifier (unique per execution)

    Returns:
        Configured structlog logger with bound context
    """
    logger = structlog.get_logger(name)

    if run_id:
        logger = logger.bind(run_id=run_id)
    if task_id:
        logger = logger.bind(task_id=task_id)
    if source_id:
        logger = logger.bind(source_id=source_id)

    return logger


class MetricsLogger:
    """Helper class for logging metrics during scraping operations."""

    def __init__(self, logger: structlog.BoundLogger, task_id: str):
        self.logger = logger
        self.task_id = task_id
        self.metrics: Dict[str, Any] = {
            'success_count': 0,
            'error_count': 0,
            'total_requests': 0,
            'start_time': datetime.utcnow().isoformat(),
        }

    def record_success(self) -> None:
        """Record a successful operation."""
        self.metrics['success_count'] += 1
        self.metrics['total_requests'] += 1

    def record_error(self, error_type: str = "unknown") -> None:
        """Record a failed operation."""
        self.metrics['error_count'] += 1
        self.metrics['total_requests'] += 1
        if 'errors_by_type' not in self.metrics:
            self.metrics['errors_by_type'] = {}
        self.metrics['errors_by_type'][error_type] = \
            self.metrics['errors_by_type'].get(error_type, 0) + 1

    def add_metric(self, key: str, value: Any) -> None:
        """Add a custom metric."""
        self.metrics[key] = value

    def log_summary(self) -> None:
        """Log final metrics summary."""
        self.metrics['end_time'] = datetime.utcnow().isoformat()
        success_rate = (
            self.metrics['success_count'] / self.metrics['total_requests'] * 100
            if self.metrics['total_requests'] > 0 else 0
        )
        self.metrics['success_rate_percent'] = round(success_rate, 2)

        self.logger.info(
            "Task completed",
            task_id=self.task_id,
            metrics=self.metrics
        )
