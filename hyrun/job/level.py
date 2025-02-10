from typing import Optional

STATUS_MAP = {'UNKNOWN': 0,
              'PENDING': 10,
              'RUNNING': 20,
              'COMPLETED': 30,
              'FAILED': 30,
              'CANCELLED': 30,
              'TIMEOUT': 30,
              'DEADLINE': 30,
              'PREEMPTED': 30,
              'NODE_FAIL': 30,
              'OUT_OF_MEMORY': 30,
              'BOOT_FAIL': 30}


def get_status_level(status: Optional[str] = None) -> int:
    """Get status level."""
    return STATUS_MAP.get(status, 0)
