# logging_utils.py
"""
Shared structured logging utilities.

Goal:
- One place to define:
  * Run / execution ID
  * Log line format
  * Module / function "purposes" in human language

Format (one line per log entry):
<RunId>|<Date>|<Time>|<Level>|<File:Line>|<Module.Func>|<ModulePurpose>|
<InvokingFunc>|<InvokingFuncPurpose>|<Detail>|<NextStep>|<Resolution>|<END>

This matches your requested structure:

<Run/Execution Id><Date><Time><Error/Info/Warning Tag>
<Code Line Number><Current Function/Module Name>
<Purpose of the Module at HighLevel><Invoking Function>
<Purpose of Invoking function><Detailed Error/Info/Warning>
<Next Step or Line of Code It Shall Execute><Resolution Step if Any><End>
"""

"""
logging_utils.py

Central logging utilities for the Meal Taxonomy project.

Log format (one line):
<RunId>|<Date>|<Time>|<Level>|<LineNo>|<CurrentFunction>
<ModulePurpose>|<InvokingFunction>|<InvokingPurpose>
<DetailedMsg>|<NextStep>|<Resolution>|END
"""

from __future__ import annotations

import datetime
import inspect
import logging
import uuid
from typing import Optional

LOG_RUN_ID: str = uuid.uuid4().hex[:8]

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
_base_logger = logging.getLogger("meal_taxonomy")


def _build_log_line(
    level: str,
    detailed_msg: str,
    *,
    module_purpose: str,
    invoking_function: str = "",
    invoking_purpose: str = "",
    next_step: str = "",
    resolution: str = "",
) -> str:
    frame = inspect.currentframe()
    caller = frame.f_back if frame else None

    if caller is not None:
        line_no = caller.f_lineno
        func_name = caller.f_code.co_name
    else:
        line_no = -1
        func_name = "<unknown>"

    now = datetime.datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")

    parts = [
        LOG_RUN_ID,
        date_str,
        time_str,
        level.upper(),
        f"L{line_no}",
        func_name,
        module_purpose or "",
        invoking_function or "",
        invoking_purpose or "",
        detailed_msg or "",
        next_step or "",
        resolution or "",
        "END",
    ]
    return "|".join(parts)


def _log(
    level: str,
    message: str,
    *,
    module_purpose: str,
    invoking_function: str = "",
    invoking_purpose: str = "",
    next_step: str = "",
    resolution: str = "",
    exc: Optional[BaseException] = None,
) -> None:
    line = _build_log_line(
        level=level,
        detailed_msg=message,
        module_purpose=module_purpose,
        invoking_function=invoking_function,
        invoking_purpose=invoking_purpose,
        next_step=next_step,
        resolution=resolution,
    )

    if exc is not None:
        line = f"{line} | EXC={repr(exc)}"

    if level.upper() == "ERROR":
        _base_logger.error(line)
    elif level.upper() == "WARNING":
        _base_logger.warning(line)
    else:
        _base_logger.info(line)


def log_info(
    message: str,
    *,
    module_purpose: str,
    invoking_function: str = "",
    invoking_purpose: str = "",
    next_step: str = "",
    resolution: str = "",
) -> None:
    _log(
        "INFO",
        message,
        module_purpose=module_purpose,
        invoking_function=invoking_function,
        invoking_purpose=invoking_purpose,
        next_step=next_step,
        resolution=resolution,
    )


def log_warning(
    message: str,
    *,
    module_purpose: str,
    invoking_function: str = "",
    invoking_purpose: str = "",
    next_step: str = "",
    resolution: str = "",
) -> None:
    _log(
        "WARNING",
        message,
        module_purpose=module_purpose,
        invoking_function=invoking_function,
        invoking_purpose=invoking_purpose,
        next_step=next_step,
        resolution=resolution,
    )


def log_error(
    message: str,
    *,
    module_purpose: str,
    invoking_function: str = "",
    invoking_purpose: str = "",
    next_step: str = "",
    resolution: str = "",
    exc: Optional[BaseException] = None,
) -> None:
    _log(
        "ERROR",
        message,
        module_purpose=module_purpose,
        invoking_function=invoking_function,
        invoking_purpose=invoking_purpose,
        next_step=next_step,
        resolution=resolution,
        exc=exc,
    )

