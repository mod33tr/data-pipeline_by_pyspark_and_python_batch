
# 📊 Pipeline Execution Results & Performance Analysis

## 🎯 Executive Summary
This report documents the execution results of the **Hybrid Data Pipeline** processing a 500,000-record e-commerce dataset (209.2 MB) using an **ELT** pattern with automated engine selection, 9 quality rules, and strict idempotency.

## 📈 Execution Metrics (Latest PySpark Run)
| Metric | Value |
|--------|-------|
| **File Size** | 209.20 MB |
| **Engine Used** | `pyspark` + MongoDB Spark Connector 10.5.0 |
| **Rows Read / Raw Loaded** | 500,000 / 500,000 |
| **Partitions** | 50 |
| **Elapsed Time** | 30.20 s |
| **Throughput** | 16,557 rec/s |
| **Valid / Corrected / Quarantined** | 3,727 / 437,603 / 58,670 |
| **Inserted / Updated / Unchanged** | 0 / 441,330 / 0 (rerun) |

**Consistency Check:** `500,000 = 3,727 + 437,603 + 58,670` ✅ No silent data loss.

## ⚡ Performance Comparison: Python Batch vs PySpark
| Feature | Python Batch | PySpark |
|---------|--------------|---------|
| **Best For** | Files <= 200 MB | Files > 200 MB |
| **Model** | Single-threaded streaming | Parallel (50 partitions) + JVM-side Connector write |
| **Throughput** | ~14,700 rec/s (100K sample) | ~16,557 rec/s (500K) |
| **Scalability** | Limited by one CPU | Scales with cores |

**Why PySpark for large files?** JVM startup cost (~15-30s) is amortized at scale; parallel partitions and distributed memory prevent OOM on multi-GB files.

## 🛡️ Threshold Justification (File Router)
**Chosen Threshold: 200 MB.** Below it, Python streaming avoids SparkSession startup overhead; above it, Spark's parallelism wins. The 200 MB point balances startup latency vs parallel gains.

## 🔌 Connector Engineering Decision (Section 6.4)
| Attempt | Artifact | Result |
|---------|----------|--------|
| 1 | `_2.12:10.2.1` | ❌ `NoSuchMethodError: RowEncoder$.apply` |
| 2 | `_2.12:10.4.0` | ❌ `NoSuchMethodError: resolveAndBind` |
| 3 | `_2.13:10.5.0` | ✅ Parallel write succeeded (PySpark 4.0 is Scala 2.13-based) |

## 🔄 Idempotency & Upsert Verification
- **Main rerun:** `Upserted: 0`, `Updated: 441,330` → no duplicate business records.
- **Delta rerun (Path B):** Run 1 → `Inserted: 1, Updated: 1, Quarantined: 1`; Run 2 → `Inserted: 0` ✅.

## 🚨 Error Case Distribution (from `error_case_counts`)
| Error Code | Count |
|------------|-------|
| `AMBIGUOUS_NEGATIVE_VALUE` | *(3392)* |
| `CORRUPTED_ITEMS_JSON` | *(6866)* |
| `DUPLICATE_ORDER_ID` | *(3084)* |
| `MISSING_ORDER_ID` | *(7070)* |
| `EMPTY_ITEMS`  | *(3487)* |
| `UNKNOWN_PRICE`  | *(20161)* |



All quarantined records preserve `error_codes`, `error_details`, and the original `raw_record` for manual review.