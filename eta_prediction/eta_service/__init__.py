
"""
ETA Service Module

Main interface for real-time ETA predictions.
"""

from .estimator import estimate_stop_times

__version__ = "0.1.0"
__all__ = ["estimate_stop_times"]