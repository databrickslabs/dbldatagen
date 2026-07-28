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
        .appName("dbldatagen_core_test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    # Don't stop — other test suites (v0) may share this JVM.
    # Spark will clean up on process exit.


@pytest.fixture
def ansi_enabled(spark):
    """Force ``spark.sql.ansi.enabled=true`` for one test, restoring on exit.

    Centralizes the snapshot/restore dance so tests can't diverge on the
    restore path and no test leaks ANSI state to a sibling.  If the conf
    was unset on entry, we ``unset`` on exit (rather than setting an
    explicit ``"false"``, which would mask the runtime default).
    """
    key = "spark.sql.ansi.enabled"
    prev = spark.conf.get(key, None)
    spark.conf.set(key, "true")
    try:
        yield
    finally:
        if prev is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, prev)
