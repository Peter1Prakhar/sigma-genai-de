"""
Fixed Sigma DataTech pipeline draft.

This version addresses the two FAIL items from code_review.json:
1. ERROR_HANDLING: pipeline stages are wrapped with try/except and clear errors.
2. HARDCODED_PATHS: runtime paths and run metadata are supplied through config.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    lit,
    max as max_,
    min as min_,
    row_number,
    sum as sum_,
    when,
)
from pyspark.sql.types import DateType, FloatType, StringType


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("sigma_pipeline")


@dataclass(frozen=True)
class PipelineConfig:
    transactions_input_path: str
    merchants_input_path: str
    bronze_output_path: str
    silver_output_path: str
    gold_output_path: str
    metadata_output_path: str
    run_date: str
    run_id: str


def log_count(df: DataFrame, label: str) -> None:
    LOGGER.info("%s row_count=%s", label, df.count())


def require_columns(df: DataFrame, required_columns: set[str], stage_name: str) -> None:
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"{stage_name} missing required columns: {sorted(missing)}")


def ingest_bronze(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    try:
        transactions_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(config.transactions_input_path)
        )
        require_columns(
            transactions_df,
            {"transaction_id", "amount", "status", "merchant_id", "customer_id", "transaction_date", "payment_method"},
            "bronze ingest",
        )
        transactions_df = (
            transactions_df.withColumn("ingestion_timestamp", lit(config.run_date))
            .withColumn("source_file", lit(config.transactions_input_path))
            .withColumn("pipeline_run_id", lit(config.run_id))
        )
        log_count(transactions_df, "bronze_output")
        (
            transactions_df.write.mode("overwrite")
            .partitionBy("ingestion_timestamp")
            .parquet(config.bronze_output_path)
        )
        return transactions_df
    except Exception as exc:
        LOGGER.exception("Bronze ingest failed")
        raise RuntimeError("Bronze ingest failed") from exc


def transform_silver(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    try:
        transactions_df = (
            spark.read.parquet(config.bronze_output_path)
            .where(col("ingestion_timestamp") == config.run_date)
            .withColumn("amount", col("amount").cast(FloatType()))
            .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        )
        valid_df = transactions_df.filter(
            col("transaction_id").isNotNull()
            & col("merchant_id").isNotNull()
            & col("customer_id").isNotNull()
            & (col("amount") >= 0)
        )
        log_count(valid_df, "silver_after_quality_filters")

        dedup_window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        deduped_df = (
            valid_df.withColumn("_rn", row_number().over(dedup_window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        log_count(deduped_df, "silver_after_dedup")

        merchants_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(config.merchants_input_path)
            .withColumn("merchant_id", col("merchant_id").cast(StringType()))
        )
        enriched_df = (
            deduped_df.join(broadcast(merchants_df), on="merchant_id", how="left")
            .withColumn("quality_flag", when(col("merchant_name").isNull(), lit("UNMATCHED")).otherwise(lit("CLEAN")))
        )
        log_count(enriched_df, "silver_output")
        (
            enriched_df.write.mode("overwrite")
            .partitionBy("transaction_date")
            .parquet(config.silver_output_path)
        )
        return enriched_df
    except Exception as exc:
        LOGGER.exception("Silver transform failed")
        raise RuntimeError("Silver transform failed") from exc


def build_gold(spark: SparkSession, config: PipelineConfig) -> dict[str, Any]:
    try:
        silver_df = spark.read.parquet(config.silver_output_path).where(col("transaction_date") == config.run_date)

        merchant_performance = (
            silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date")
            .agg(
                sum_(when(col("status") == "COMPLETED", col("amount")).otherwise(lit(0))).alias("total_revenue"),
                count("*").alias("txn_count"),
                (sum_(when(col("status") == "FAILED", lit(1)).otherwise(lit(0))) / count("*") * 100).alias("failure_rate_pct"),
            )
        )
        merchant_performance.write.mode("overwrite").partitionBy("transaction_date").parquet(
            f"{config.gold_output_path}/merchant_performance"
        )

        customer_ltv = (
            silver_df.filter(col("status") == "COMPLETED")
            .groupBy("customer_id")
            .agg(
                sum_("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min_("transaction_date").alias("first_txn_date"),
                max_("transaction_date").alias("last_txn_date"),
            )
        )
        customer_ltv.write.mode("overwrite").parquet(f"{config.gold_output_path}/customer_ltv")

        metadata = {"run_date": config.run_date, "run_id": config.run_id, "status": "COMPLETED"}
        spark.createDataFrame([metadata]).write.mode("overwrite").json(config.metadata_output_path)
        return metadata
    except Exception as exc:
        LOGGER.exception("Gold build failed")
        raise RuntimeError("Gold build failed") from exc


def run_pipeline(spark: SparkSession, config: PipelineConfig) -> dict[str, Any]:
    ingest_bronze(spark, config)
    transform_silver(spark, config)
    return build_gold(spark, config)


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()
    example_config = PipelineConfig(
        transactions_input_path="s3://example/raw/transactions.csv",
        merchants_input_path="s3://example/raw/merchants.csv",
        bronze_output_path="s3://example/bronze/transactions",
        silver_output_path="s3://example/silver/transactions",
        gold_output_path="s3://example/gold",
        metadata_output_path="s3://example/metadata/run_summary",
        run_date="2026-05-27",
        run_id="run-001",
    )
    run_pipeline(spark_session, example_config)
