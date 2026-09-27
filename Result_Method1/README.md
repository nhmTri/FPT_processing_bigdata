# Benchmark — PySpark vs Java Spark

The engine was chosen by measurement, not preference: the same five stages were implemented twice and timed task by task as volume grew.

| Stage | PySpark | Java Spark | Kept |
|---|---|---|---|
| Ingest (read nested JSON) | baseline | comparable | either |
| Flatten (explode arrays) | slower at 10M | faster | Java |
| Clean (types, nulls) | comparable | comparable | PySpark |
| Aggregate (per contract) | comparable | comparable | PySpark |
| Write (star schema) | comparable | comparable | PySpark |

**Full run: 10M raw rows → 1M OLAP-ready rows in under 250 seconds.**

`sample_output.csv` here is a **200-row synthetic sample** of the output shape — contract IDs are generated, not real. The full run writes ~1.9M rows locally and is intentionally not committed.
