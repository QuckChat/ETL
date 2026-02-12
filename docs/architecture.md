# CDP ETL Architecture Notes

## Core Components
- **Storage**: HDFS / cloud object storage with bronze-silver-gold zones.
- **Compute**: PySpark on YARN (CDP).
- **Metastore/Query**: Hive + Impala.
- **Orchestration**: Airflow (or Oozie in legacy flows).
- **Monitoring**: Airflow task logs + Spark History Server + Cloudera Manager dashboards.

## Ingestion Patterns
1. **RDBMS full/incremental pull** using JDBC predicates.
2. **API pull** with pagination and checkpoint-based watermark.
3. **File landing** with schema versioning and late-arrival handling.

## Performance Tuning Checklist
- Enable AQE (`spark.sql.adaptive.enabled=true`).
- Broadcast small dimensions when feasible.
- Avoid wide shuffles by pre-partitioning and selective joins.
- Use parquet/orc and compress outputs.
- Keep partition counts proportional to cluster resources.

## Data Quality Framework
- Fail pipeline for critical checks (PK uniqueness, mandatory columns).
- Quarantine non-critical bad rows for manual remediation.
- Persist quality metrics for trend analysis.

## Operational Runbook (Brief)
1. Validate source availability.
2. Trigger DAG manually for backfill if needed.
3. Monitor Spark stages for skew/OOM.
4. Verify row counts across bronze/silver/gold.
5. Notify downstream teams if SLA impacted.
