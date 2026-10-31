"""Host-side, unattended cleanup. Importing this package makes no cloud calls."""

from .cleanup import cleaning_job

__all__ = ["cleaning_job"]
