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

from __future__ import annotations

import datetime
import logging
import uuid
from typing import Dict

# Single run / execution id for the entire Python process
RUN_ID = uuid.uuid4().hex[:8]

# High-level purposes by module name (file name without .py)
MODULE_PURPOSES: Dict[str, str] = {
    "pipeline": "ETL pipeline from RecipeRecord to Supabase meals + tags",
    "nlp_tagging": "NLP helper to derive tag candidates from recipe text",
    "ontologies": "Connect ingredients and meals to external food ontologies",
    "kaggle_unified": "Normalize heterogeneous Kaggle CSV schemas into RecipeRecord objects",
    "build_ingredient_category_tags": "Derive ingredient_category tags from ontology mappings",
    "kaggle_ontology_import": "Create ontology_nodes from Kaggle metadata and link meals",
    "taxonomy_seed": "Seed core tag_types and initial tags into Supabase",
    "config": "Create Supabase client using environment variables",
}


def get_module_purpose(module_name: str) -> str:
    """Return a human-friendly purpose string for a given module."""
    return MODULE_PURPOSES.get(module_name, "")


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that emits a single '|' separated line conforming
    to the requested log template.
    """

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        # Date / time from record
        dt = datetime.datetime.fromtimestamp(record.created)
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M:%S")

        # Run / execution id – can be overridden via record.run_id if needed
        run_id = getattr(record, "run_id", RUN_ID)

        # Level, code location, function, module
        level = record.levelname
        code_location = f"{record.filename}:{record.lineno}"
        func_name = record.funcName
        module_name = record.module
        module_purpose = get_module_purpose(module_name)

        # Optional extra context supplied via logger calls
        invoking_func = getattr(record, "invoking_func", "")
        invoking_purpose = getattr(record, "invoking_purpose", "")
        next_step = getattr(record, "next_step", "")
        resolution = getattr(record, "resolution", "")

        # Core message (e.g. “Failed to ingest recipe …”)
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
    """
    init_logging()
    return logging.getLogger(name)
