from typing import Dict, List, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], str]]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ('string', 'boolean') or expected_schema.get(col, 'null') == 'null' for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        'new_columns': new_columns,
       'removed_columns': removed_columns,
        'type_changes': type_changes,
        'drift_severity': drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], str]]) -> Dict[str, Dict[str, Union[str, int]]]:
    decisions = {}
    for col, dtype in drift_report['new_columns'].items():
        if dtype in ('string', 'boolean'):
            decisions[col] = {'action': 'ADD_TO_SCHEMA','reason': 'new nullable column', 'risk_level': 1}
        elif dtype == 'float':
            decisions[col] = {'action': 'FLAG_ANOMALY','reason': 'potential impact on revenue', 'risk_level': 3}
    for col in drift_report['removed_columns']:
        decisions[col] = {'action': 'HALT','reason':'removed column', 'risk_level': 4}
    for col, (old_type, new_type) in drift_report['type_changes'].items():
        if old_type!= new_type:
            if new_type == 'float' and old_type in ('int', 'long'):
                decisions[col] = {'action': 'ADD_TO_SCHEMA','reason': 'type widening', 'risk_level': 2}
            elif new_type == 'int' and old_type == 'float':
                decisions[col] = {'action': 'FLAG_ANOMALY','reason': 'type narrowing', 'risk_level': 3}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, int]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for col, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(col)
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added new column: {col} with type {updated_schema[col]}")
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{col}_anomaly_flag", F.when(F.col(col).isNull(), True).otherwise(False))
            migration_notes.append(f"Flagged anomalies in column: {col}")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict, List, str]]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    print(f"Drift detected: {drift_report['drift_severity']}")
    if spark_df:
        spark_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {'drift_report': drift_report, 'decisions': decisions,'migration_notes': migration_notes}
    return {'drift_report': drift_report, 'decisions': decisions}
