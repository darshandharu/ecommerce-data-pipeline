"""Custom exception hierarchy for the pipeline.

Having typed exceptions lets the orchestration layer (Airflow) and the DQ
framework distinguish *recoverable* config errors from *hard* data-quality
failures that must stop the pipeline.
"""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Raised when configuration is missing or malformed."""


class SchemaError(PipelineError):
    """Raised when an input file does not match the declared schema."""


class DataQualityError(PipelineError):
    """Raised when a FAIL-severity data quality check breaches its threshold."""

    def __init__(self, message: str, failures: list | None = None):
        super().__init__(message)
        self.failures = failures or []


class IngestionError(PipelineError):
    """Raised when reading/writing a layer fails."""


class CDCError(PipelineError):
    """Raised when a change-data-capture merge cannot be applied."""
