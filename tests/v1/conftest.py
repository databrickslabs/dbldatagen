"""Shared Spark session fixture for all tests.

When running locally (pytest), creates a local Spark session.
When running on Databricks, uses the existing session.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provide a SparkSession, reused across the entire test run."""
    try:
        # Databricks / remote — session already exists
        session = SparkSession.getActiveSession()
        if session is not None:
            session.conf.set("spark.sql.session.timeZone", "UTC")
            yield session
            return
    except Exception:
        pass

    # Local — create a minimal session for testing
    session = (
        SparkSession.builder.master("local[2]")
        .appName("dbldatagen_v1_test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    # Don't stop — other test suites (v0) may share this JVM.
    # Spark will clean up on process exit.
