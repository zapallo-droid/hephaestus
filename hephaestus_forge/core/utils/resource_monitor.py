
import os
import sys
import time
import socket
import getpass
import platform
import psutil
import functools
import json
import logging
from typing import Callable, Any, Optional
from datetime import datetime, timezone

from hephaestus_forge.core.orm.data_ops_helpers import default_stats
from hephaestus_forge.core.utils.logging_config import StatsLogHandler
from hephaestus_forge.core.utils.general_helper import json_cleaner


def environment_metadata_collector(trigger_type: Optional[str] = None) -> dict[str, Any]:
    """
    Returns a dictionary with runtime environment metadata.
    Compatible with both JobORM and TaskORM.
    """

    try:
        execution_metadata = json.loads(os.getenv("EXECUTION_METADATA", "{}"))
    except Exception as e:
        execution_metadata = {}

    return {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ended_at": None,
        "host_name": socket.gethostname(),
        "execution_user": getpass.getuser(),
        "process_id": os.getpid(),
        "execution_environment": os.getenv("ENV", "UNKNOWN"),
        "execution_metadata": execution_metadata,
        "python_version": sys.version.split()[0],
        "os_info": platform.platform(),
        "trigger_type": trigger_type or "manual"
    }


def resource_monitor(func: Callable) -> Callable:
    """
    Decorator that measures the execution resources of the function/method.
    Returns a tuple: (original function result, stats dictionary)
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> tuple[Any, dict]:
        process = psutil.Process(os.getpid())
        log_handler = StatsLogHandler()
        log_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        # -- PRE-EXECUTION --
        started_at = datetime.now(timezone.utc)
        start_time = time.time()
        start_cpu = psutil.cpu_percent(interval=0.1)
        start_memory = process.memory_info().rss / (1024 ** 2)

        stats = default_stats()
        stats.update({
            "started_at": started_at,
            "memory_usage_start": round(start_memory, 2),
            "cpu_usage_start": round(start_cpu, 2),
        })

        result = None
        captured_exception: Optional[Exception] = None

        try:
            result = func(*args, **kwargs)

        except Exception as e:
            stats["exception"] = True
            stats["exception_msg"] = str(e)
            stats["errors"].append(str(e))
            captured_exception = e
            raise e

        finally:
            # -- POST-EXECUTION --
            ended_at = datetime.now(timezone.utc)
            end_time = time.time()
            end_cpu = psutil.cpu_percent(interval=0.1)
            end_memory = process.memory_info().rss / (1024 ** 2)

            stats.update({
                "ended_at": ended_at.isoformat(),
                "duration": round(end_time - start_time, 3),
                "cpu_usage_end": round(end_cpu, 2),
                "memory_usage_end": round(end_memory, 2),
                "memory_usage": round(end_memory - stats["memory_usage_start"], 2),
                "cpu_usage": round((stats["cpu_usage_start"] + end_cpu) / 2, 2),
                "warnings": log_handler.warnings,
                "errors": stats.get("errors", []) + log_handler.errors
            })

            logger.removeHandler(log_handler)
            log_handler.close()

            if captured_exception is not None:
                setattr(captured_exception, "resource_monitor_stats", json_cleaner(stats))

        return result, json_cleaner(stats)

    return wrapper
