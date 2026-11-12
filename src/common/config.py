"""Project configuration shared by every bronze / silver / gold module.

One source of truth for the Unity Catalog names, the landing-Volume paths, and a
``get_spark()`` helper. Keeping this here means the worked examples and the TODO
stubs all read/write the *same* tables, and the Declarative Automation Bundle can
isolate each participant by setting a per-user schema prefix in its ``dev`` target.

Unity Catalog layout this project uses:

    insurance                      <- catalog
      ├─ landing.raw               <- Volume holding the uploaded source files
      ├─ bronze.<source>           <- raw + ingestion metadata (append-only)
      ├─ silver.<entity>           <- cleaned / conformed / SCD2
      ├─ gold.<fact|dim|agg>       <- BI-ready star schema + aggregates
      └─ ops.<dq_*>                <- data-quality metrics / scorecard

In a bundle ``dev`` target, set the env var ``DEV_SCHEMA_PREFIX`` (the bundle wires
this to ``${workspace.current_user.short_name}``) so two people sharing a workspace
write to e.g. ``insurance.jane_bronze`` vs ``insurance.john_bronze``.
"""
from __future__ import annotations

import os

# --------------------------------------------------------------------------- #
# Unity Catalog names
# --------------------------------------------------------------------------- #
CATALOG = os.environ.get("INSURANCE_CATALOG", "insurance")
LANDING_SCHEMA = "landing"
RAW_VOLUME = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OPS_SCHEMA = "ops"

SOURCES = ("customers", "policies", "claims", "payments", "agents", "telematics", "reference")


def _prefix() -> str:
    """Optional per-user schema prefix for isolated dev sandboxes (bundle dev target)."""
    # return os.environ.get("DEV_SCHEMA_PREFIX", "").strip()
    return get_spark.conf.get("DEV_SCHEMA_PREFIX", "").strip() # accesses the spark configuration


def schema(layer: str) -> str:
    """Schema name for a medallion layer, with the optional dev prefix applied."""
    p = _prefix()
    return f"{p}_{layer}" if p else layer


def table(layer: str, name: str) -> str:
    """Fully-qualified table name, e.g. table('silver', 'dim_agent')."""
    return f"{CATALOG}.{schema(layer)}.{name}"


def volume_path(source: str = "") -> str:
    """Path under the landing Volume, e.g. volume_path('customers')."""
    base = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{RAW_VOLUME}"
    return f"{base}/{source}" if source else base


def checkpoint_path(name: str) -> str:
    """Auto Loader / streaming checkpoint location (lives on the landing Volume)."""
    return f"{volume_path()}/_checkpoints/{name}"


def get_spark():
    """Return the active SparkSession.

    Works three ways: inside a Databricks notebook (returns the pre-created
    session), via Databricks Connect from your IDE, or a plain local session.
    """
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass
    try:  # IDE / CLI via Databricks Connect
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
