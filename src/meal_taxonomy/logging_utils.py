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
from typing import Dict, Optional

LOG_RUN_ID: str = uuid.uuid4().hex[:8]
# Alias for compatibility with scripts that use RUN_ID
RUN_ID: str = LOG_RUN_ID

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


# ============================================================================
# New Interface: StructuredFormatter + get_logger() for Python logging
# ============================================================================

class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that emits a single '|' separated line conforming
    to the requested log template.
    
    Format:
    <RunId>|<Date>|<Time>|<Level>|<File:Line>|<Module.Func>|<ModulePurpose>|
    <InvokingFunc>|<InvokingFuncPurpose>|<Detail>|<NextStep>|<Resolution>|<END>
    """

    # High-level purposes by module name
    MODULE_PURPOSES: Dict[str, str] = {
        "pipeline": "ETL pipeline from RecipeRecord to Supabase meals + tags",
        "nlp_tagging": "NLP helper to derive tag candidates from recipe text",
        "ontologies": "Connect ingredients and meals to external food ontologies",
        "kaggle_unified": "Normalize heterogeneous Kaggle CSV schemas into RecipeRecord objects",
        "build_ingredient_category_tags": "Derive ingredient_category tags from ontology mappings",
        "kaggle_ontology_import": "Create ontology_nodes from Kaggle metadata and link meals",
        "taxonomy_seed": "Seed core tag_types and initial tags into Supabase",
        "config": "Create Supabase client using environment variables",
        "foodon_import": "Link ingredients to FoodOn ontology via synonyms TSV",
        "search_example": "Example search RPC demonstration",
        "ingest_kaggle_all": "Batch ingest all Kaggle CSV files via MealETL",
    }

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        """Format log record into structured pipe-delimited format."""
        # Date / time from record
        dt = datetime.datetime.fromtimestamp(record.created)
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")

        # Run / execution id
        run_id = getattr(record, "run_id", RUN_ID)

        # Level, code location, function, module
        level = record.levelname
        code_location = f"{record.filename}:{record.lineno}"
        func_name = record.funcName
        module_name = record.module
        module_purpose = self.MODULE_PURPOSES.get(module_name, "")

        # Optional extra context supplied via logger calls
        invoking_func = getattr(record, "invoking_func", "")
        invoking_purpose = getattr(record, "invoking_purpose", "")
        next_step = getattr(record, "next_step", "")
        resolution = getattr(record, "resolution", "")

        # Core message
        detail = record.getMessage()

        return (
            f"{run_id}|{date_str}|{time_str}|{level}|{code_location}|"
            f"{module_name}.{func_name}|{module_purpose}|"
            f"{invoking_func}|{invoking_purpose}|"
            f"{detail}|{next_step}|{resolution}|<END>"
        )


def init_logging(level: int = logging.INFO) -> None:
    """
    Initialize root logger once with our StructuredFormatter.

    Call get_logger() from modules instead of calling logging.basicConfig()
    everywhere, so configuration stays central.
    """
    root = logging.getLogger()
    if root.handlers:
        # Already configured – avoid double handlers in REPL / notebooks
        return

    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter())
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger configured with structured formatting.
    
    Usage:
        logger = get_logger("my_module")
        logger.info(
            "Something happened",
            extra={
                "invoking_func": "some_function",
                "invoking_purpose": "High-level purpose",
                "next_step": "What happens next",
                "resolution": "How to fix if error",
            },
        )
    """
    init_logging()
    return logging.getLogger(name)

