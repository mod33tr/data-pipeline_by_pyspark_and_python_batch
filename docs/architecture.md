# 🏗️ System Architecture & Design Decisions

## 📐 High-Level Architecture
                ┌─────────────────────────────────┐
                │      Dirty CSV (Source)         │
                └───────────────┬─────────────────┘
                                │
                                ▼
                ┌─────────────────────────────────┐
                │     File Router (main.py)       │
                │  Size <= 200MB? Batch : Spark   │
                └───────────────┬─────────────────┘
                                │
          ┌─────────────────────┴─────────────────────┐
          ▼                                           ▼
┌──────────────────┐                      ┌──────────────────┐
│  Python Batch    │                      │   PySpark        │
│  (Streaming)     │                      │   (Parallel)     │
└────────┬─────────┘                      └────────┬─────────┘
         │                                         │
         └────────────────────┬────────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │   orders_raw (MongoDB)          │
                │   • Raw records preserved       │
                │   • Metadata (run_id, etc.)     │
                └───────────────┬─────────────────┘
                                │
                                ▼
                ┌─────────────────────────────────┐
                │   ELT Pipeline                  │
                │   • 9 Quality Rules             │
                │   • Classification Logic        │
                └──────┬───────────────────┬──────┘
                       │                   │
          ┌────────────┴──┐         ┌──────┴──────────┐
          ▼               ▼         ▼                 ▼
 ┌────────────────┐ ┌─────────┐ ┌────────────────┐
 │orders_validated│ │ orders_ │ │                │
 │  (Upsert +     │ │quarantine│ │ Audit Trail    │
 │   Audit Trail) │ │ (Errors)│ │ (Corrections)  │
 └────────────────┘ └─────────┘ └────────────────┘



 
---

## 🧩 Component Breakdown

### 1. File Router (`src/file_router.py`)
**Purpose:** Automatically selects the processing engine based on file size.
**Decision:** Single entry point (`main.py`) prevents users from running the wrong script.

### 2. Raw Load Layer (ELT Pattern)
**Purpose:** Ingest data exactly as it arrives, without any transformation.
**Why ELT over ETL?**
- **Data Lineage:** Preserves the original dirty data for auditing and reprocessing.
- **Flexibility:** Quality rules can be modified later without re-reading the source file.
- **Performance:** MongoDB handles raw JSON storage efficiently.

### 3. Quality Rules Engine (`src/quality_rules.py`)
Implements 9 automated cleaning rules:
1. Arabic → Latin digits conversion
2. Currency symbol removal
3. Thousands separator removal
4. Word-based prices (e.g., "خمسة آلاف")
5. Phone number normalization
6. Email repeated symbols fix
7. Date format standardization (ISO 8601)
8. Status/Payment status normalization (Trim + Dictionary)
9. **Total amount recalculation** (items × qty + delivery)

### 4. Classification & Upsert (`src/elt_pipeline.py`)
Routes each record to one of three states:
- **Valid:** No changes needed.
- **Corrected:** Auto-fixed with `Audit Trail` (`_corrections` array).
- **Quarantined:** Unfixable errors routed to `orders_quarantine` with `error_codes`.

**Idempotency Guarantee:**
- `Unique Index` on `order_id` in `orders_validated`.
- `UpdateOne(..., upsert=True)` ensures re-runs update existing records instead of creating duplicates.

---

## 🛠️ Engineering Decisions & Workarounds

### MongoDB Write Strategy (Requirement 6.4)
- **Primary path:** MongoDB Spark Connector `10.5.0 (_2.13)` — parallel JVM-side writes, verified on PySpark 4.0 (16,557 rec/s). Version journey: `_2.12` builds (10.2.1 / 10.4.0) failed with `NoSuchMethodError` because Spark 4.0 is Scala 2.13-based.
- **Fallback path:** Micro-batching via `toLocalIterator` + `pymongo` (5,000-doc batches) for environments where the JVM connector is blocked; the active `write_mode` is logged per run.
- Both paths preserve the ELT contract: every record lands in `orders_raw` before transformation.

### Why `validationLevel="moderate"` on Schema Validation?
`strict` validation would reject Upserts touching existing documents with legacy metadata. `moderate` validates new inserts only, preserving integrity + operational flexibility.
### Why `toLocalIterator` instead of MongoDB Spark Connector?
During development on Windows, we encountered `UnsatisfiedLinkError: NativeIO$Windows.access0` due to Hadoop's incompatibility with Windows file systems. 

**Solution:** We used `df.rdd.toLocalIterator()` with `Repartition(50)` to stream data from Spark to Python in micro-batches, bypassing the file system entirely while maintaining parallel processing benefits.

### Why `validationLevel="moderate"` on Schema Validation?
Using `strict` validation would cause `Upsert` operations to fail when updating existing documents with metadata fields. `Moderate` validation applies only to new inserts, preserving both data integrity and operational flexibility.

---

## 🚀 Path B: Incremental Loading (Advanced)

Instead of reprocessing the entire 500K dataset, the system supports **Delta Loading**:
1. Small CSV files (`delta_updates.csv`) containing only new/modified records.
2. Same ELT pipeline processes the delta.
3. `Upsert` ensures idempotency at the delta level.
4. Proven by running the same delta twice: `Inserted: 0` on second run.

---

## 📊 Consistency Guarantees

**Fundamental Equation (Section 6.11):**

This equation is enforced programmatically and verified after every run. Any deviation triggers an error, ensuring zero silent data loss.