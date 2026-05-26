import logging
import shutil
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, coalesce, max as max_, sum, count, avg, min, max, when, mode
from pyspark.sql.types import FloatType, StringType, DateType
import json
import os

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        transactions_df = (spark.read.option("header", "true")
                          .option("inferSchema", "false")
                          .csv(input_path))
        
        transactions_df = (transactions_df.withColumn("ingestion_timestamp", lit(run_date))
                           .withColumn("source_file", lit("transactions.csv"))
                          .withColumn("pipeline_run_id", lit(run_id)))
        
        logging.info(f"[Stage: Ingest Bronze] Input count: {transactions_df.count():,} rows")
        
        partition_path = f"{output_path}{run_date}/"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_df.write.partitionBy("ingestion_timestamp").parquet(output_path)
        
        logging.info("[Stage: Ingest Bronze] Ingestion completed")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}, Row count: {transactions_df.count():,}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        transactions_df = (spark.read.parquet(bronze_path)
                          .where(col("ingestion_timestamp") == run_date))
        
        transactions_df = (transactions_df.withColumn("amount", col("amount").cast(FloatType()))
                          .withColumn("transaction_date", col("transaction_date").cast(DateType())))
        
        filtered_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        logging.info(f"[Stage: Transform Silver] After filter count: {filtered_df.count():,} rows")
        
        deduped_df = (filtered_df.groupBy("transaction_id")
                                 .agg(max_("ingestion_timestamp").alias("latest_timestamp")))
        transactions_df = filtered_df.join(deduped_df, on=["transaction_id", "ingestion_timestamp"], how="inner")
        logging.info(f"[Stage: Transform Silver] After dedup count: {transactions_df.count():,} rows")
        
        merchants_df = (spark.read.option("header", "true")
                       .option("inferSchema", "false")
                        .csv(merchants_path)
                        .withColumn("merchant_id", col("merchant_id").cast(StringType())))
        merchants_df = merchants_df.cache()
        
        enriched_df = (transactions_df.join(broadcast(merchants_df), on="merchant_id", how="left_outer")
                       .withColumn("quality_flag", coalesce(col("merchant_name"), lit("UNMATCHED"))))
        logging.info(f"[Stage: Transform Silver] Enriched count: {enriched_df.count():,} rows")
        
        partition_path = f"{output_path}{run_date}/"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        enriched_df.write.partitionBy("transaction_date").parquet(output_path)
        
        logging.info("[Stage: Transform Silver] Transformation completed")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}, Row count: {enriched_df.count():,}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Run Gold] Starting gold layer build")
        
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path, f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)
        
        run_metadata = {
            "run_date": run_date,
            "status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        
        spark.read.text(spark.sparkContext.parallelize([str(run_metadata)])).write.json(f"{gold_output_dir}/run_metadata")
        
        logging.info("[Stage: Run Gold] Gold layer build completed")
    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting build")
        
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)  # Partition pruning
        
        merchant_performance_df = silver_df.filter(col("status") == "COMPLETED") \
           .groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
          .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance_df.count():,} rows")
        
        partition_path = f"{output_path}{run_date}/"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        merchant_performance_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info("[Stage: Build Merchant Performance] Build completed")
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}, Row count: {merchant_performance_df.count():,}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting build")
        
        silver_df = spark.read.parquet(silver_path).filter(col("status") == "COMPLETED")
        
        customer_ltv_df = silver_df.groupBy("customer_id") \
          .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                coalesce(mode("payment_method").over(Window.partitionBy("customer_id")), lit(None)).alias("preferred_payment_method")
            )
        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv_df.count():,} rows")
        
        shutil.rmtree(output_path, ignore_errors=True)
        
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info("[Stage: Build Customer LTV] Build completed")
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}, Row count: {customer_ltv_df.count():,}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting build")
        
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)  # Partition pruning
        
        daily_summary_df = silver_df.groupBy("transaction_date") \
            .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(lit(0))).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(col("customer_id")).alias("unique_customers"),
                count(col("merchant_id")).alias("unique_merchants"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary_df.count():,} rows")
        
        partition_path = f"{output_path}{run_date}/"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        daily_summary_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info("[Stage: Build Daily Summary] Build completed")
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}, Row count: {daily_summary_df.count():,}")
        raise

def main():
    spark = (SparkSession.builder
            .appName("Sigma DataTech Transaction Analytics Pipeline")
             .getOrCreate())
    
    input_path = "s3://sigma-datatech-raw/transactions.csv"
    merchants_path = "s3://sigma-datatech-raw/merchants.csv"
    bronze_path = "s3://sigma-datatech-bronze/"
    silver_path = "s3://sigma-datatech-silver/"
    gold_output_dir = "s3://sigma-datatech-gold/"
    run_date = "2026-05-27"
    run_id = "run-001"
    
    ingest_bronze(spark, input_path, f"{bronze_path}{run_date}/", run_date, run_id)
    transform_silver(spark, f"{bronze_path}{run_date}/", merchants_path, f"{silver_path}{run_date}/", run_date)
    run_gold(spark, f"{silver_path}{run_date}/", gold_output_dir, run_date)

if __name__ == "__main__":
    main()