from typing import Optional, Any

# --STATS DEFAULT DICTS--
def default_stats() -> dict[str, Any]:
    return {
        # General execution
        "started_at": None,
        "ended_at": None,
        "duration": None,
        "exception": False,
        "exception_msg": None,

        # Resource usage
        "memory_usage_start": None,
        "cpu_usage_start": None,
        "memory_usage_end": None,
        "cpu_usage_end": None,
        "memory_usage": None,
        "cpu_usage": None,

        # Quality
        "warnings": [],
        "errors": [],

        # Execution metadata
        "execution_metadata": {}
    }

# --METADATA--
def default_app_version() -> dict:
    return {
        "git": {"commit": None, "branch": None, "repo": None},
        "label": "",
        "hash": ""
    }

def default_environment_metadata() -> dict:
    return {
        "started_at": None,
        "ended_at": None,
        "host_name": None,
        "execution_user": None,
        "process_id": None,
        "execution_environment": None,
        "execution_metadata": {},  # e.g. Airflow context
        "python_version": None,
        "os_info": None,
        "trigger_type": None
    }


## --DEFAULT VALIDATION-- ##
def default_validator(input_stats: Optional[dict], default_factory) -> dict:
    """
    Ensures all required keys exist using a default structure.
    Useful for fallback or normalization prior to ORM insertion.
    """
    result = default_factory()

    if input_stats:
        result.update({k: input_stats.get(k, v) for k, v in result.items()})
        
    return result
