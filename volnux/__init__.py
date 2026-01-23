"""
===========================================================================================
Copyright (C) 2025 Nafiu Shaibu <nafiushaibu1@gmail.com>.
Purpose: Pipeline and event management
-------------------------------------------------------------------------------------------
This is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your option)
any later version.

This is distributed in the hopes that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

===========================================================================================
"""

__version__ = "2.0.0"
__author__ = "nshaibu <nafiushaibu1@gmail.com>"

import logging

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

from .base import EventBase, ExecutorInitializerConfig, RetryPolicy

Event: TypeAlias = EventBase

from .pipeline import BatchPipeline, Pipeline
from .result_evaluators import ResultEvaluationStrategies

# hook all default signal handlers
from .signal.handlers import *

__all__ = [
    "EventBase",
    "Event",
    "RetryPolicy",
    "ExecutorInitializerConfig",
    "Pipeline",
    "BatchPipeline",
]
