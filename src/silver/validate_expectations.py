"""TODO STUB — Silver: a reusable expectations / quarantine helper.

Goal
----
A small utility the other silver jobs call to (a) split a DataFrame into rows that pass
all rules vs rows that fail, (b) write the failures to a quarantine table, and (c) record
per-rule pass/fail counts into `ops.dq_scorecard` (requirement #8).

This mirrors, in plain PySpark, what Lakeflow Declarative Pipeline **EXPECTATIONS** do
(`expect` / `expect_or_drop` / `expect_or_fail`) — you'll meet those in M5.

Acceptance
----------
- `apply_expectations(df, rules)` returns `(passed_df, quarantined_df)`.
- Per-rule counts land in `ops.dq_scorecard` with the table name + run timestamp.

Exam domain 3 — data quality checks/validation. Studybook: `docs/studybook/M3_silver_transform.md`.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import config

# A rule is a (name, boolean-Column) pair: the Column is TRUE when the row PASSES the rule.
# Example: ("premium_positive", F.col("annual_premium") > 0)
Rule = tuple  # (str, Column)


def apply_expectations(df: DataFrame, rules: list[Rule]):
    """Split df into (passed, quarantined). A row passes only if it satisfies every rule."""
    if not rules:
        return df, df.limit(0)
    # TODO 1: build a combined "all rules passed" condition (AND of every rule column).
    #   passed_cond = reduce(lambda a, b: a & b, [cond for _name, cond in rules])
    # TODO 2: add a `_dq_failed_rules` array column listing the rules each row violated.
    # TODO 3: return df.where(passed_cond), df.where(~passed_cond).withColumn("_dq_failed_rules", ...)
    # raise NotImplementedError("Implement the expectations split — see studybook M3.")

    # TODO 1: build a combined "all rules passed" condition (AND of every rule column).
    #   passed_cond = reduce(lambda a, b: a & b, [cond for _name, cond in rules])
    passed_cond = reduce(
        lambda combined, condition: combined & condition,
        [condition for _name, condition in rules]
    )

    # TODO 2: add a `_dq_failed_rules` array column listing the rules each row violated.
    # to give each rejected row an explanation - list of the failed rules
    df = df.withColumn( # add a new column
        "_dq_failed_rules", # called like this
        F.filter( # we only want those columns that are not null - they broke the rules
            F.array(*[ # builds a column that contains an array of results for each row
                F.when(condition, F.lit(None)) # is the rules is respected - null
                    .otherwise(F.lit(name)) # otherwise the name of the rule
                for name, condition in rules
            ]),
            lambda name: name.isNotNull()
        )
    )

    # TODO 3: return df.where(passed_cond), df.where(~passed_cond).withColumn("_dq_failed_rules", ...)
    passed_df = (
        df
        .where(passed_cond)
        .drop("_dq_failed_rules")
    )

    quarantined_df = df.where(~passed_cond)

    return passed_df, quarantined_df


def write_scorecard(spark, table_name: str, passed: int, quarantined: int, per_rule: dict):
    """Append a row to ops.dq_scorecard. TODO: implement the insert."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('ops')}")
    # TODO: insert (table_name, passed, quarantined, per_rule map, current_timestamp()) into
    #       config.table('ops', 'dq_scorecard').
    # raise NotImplementedError("Implement the DQ scorecard write — see studybook M3/M4.")

    # TODO: insert (table_name, passed, quarantined, per_rule map, current_timestamp()) into
        #       config.table('ops', 'dq_scorecard').

    scorecard = (
        spark.createDataFrame( # transforms the data in a spark data frame
            [(table_name, passed, quarantined, per_rule)], # a list of a tuple
            schema=(
                "table_name string, passed long, quarantined long, "
                "per_rule map<string,long>"
            )
        )
        .withColumn("run_ts", F.current_timestamp())
    )

    (scorecard.write.mode("append")
        .saveAsTable(config.table("ops", "dq_scorecard")))