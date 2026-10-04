import os
import time
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, to_json, struct, col as scol, count as scount

from config.settings import MONGODB_URI, DB_NAME, RAW_COLLECTION
from pymongo import MongoClient

MONGO_CONNECTOR_PKG = "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0"

def init_spark():
    spark = (SparkSession.builder
        .master("local[*]")
        .appName("HybridPipeline_Spark_RawLoad")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.jars.packages", MONGO_CONNECTOR_PKG)
        .config("spark.mongodb.connection.uri", MONGODB_URI)
        .config("spark.mongodb.database", DB_NAME)
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_csv_spark(file_path, run_id, engine_name="pyspark"):
    print(f"\n🚀 بدء قراءة الملف الضخم ومعالجته باستخدام PySpark...")
    spark = init_spark()

    # 🔥 التقسيمات تتدرج مع حجم الملف
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    partitions = 50 if size_mb <= 1024 else 200

    file_uri = Path(file_path).resolve().as_uri()
    print(f"📂 مسار القراءة: {file_uri}")

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True)
    ])

    start_time = time.perf_counter()

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .schema(schema) \
        .load(file_uri)

    # 🔥 اكتشاف المعرفات المكررة داخل Spark (ذاكرة صغيرة تُمرر للمرحلة التالية)
    dup_rows = (df.groupBy("order_id")
                .agg(scount("*").alias("c"))
                .filter((scol("c") > 1) & scol("order_id").isNotNull())
                .select("order_id")
                .collect())
    duplicate_ids = [r["order_id"] for r in dup_rows]
    print(f"🔁 معرفات مكررة مكتشفة في المصدر: {len(duplicate_ids):,}")

    original_cols = df.columns
    df_raw = df.withColumn("run_id", lit(run_id)) \
               .withColumn("source_file", lit(os.path.basename(file_path))) \
               .withColumn("source_row_number", lit("N/A")) \
               .withColumn("ingested_at", lit(datetime.now(timezone.utc).isoformat())) \
               .withColumn("engine_used", lit(engine_name)) \
               .withColumn("raw_record", to_json(struct(*original_cols)))

    df_raw_final = df_raw.select("run_id", "source_file", "source_row_number",
                                  "ingested_at", "engine_used", "raw_record")

    df_raw_final = df_raw_final.repartition(partitions)
    print(f"📊 عدد التقسيمات (Partitions): {df_raw_final.rdd.getNumPartitions()}")

    write_mode = "mongo_spark_connector"
    try:
        print("📦 محاولة الكتابة المتوازية عبر MongoDB Spark Connector (10.5.0 / Scala 2.13)...")
        df_raw_final.write.format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.collection", RAW_COLLECTION) \
            .save()
        print("✅ نجحت الكتابة عبر MongoDB Spark Connector!")
    except Exception as e:
        print(f"⚠️ Connector تعذر في بيئة Windows ({type(e).__name__})")
        print("🔄 تفعيل البديل الهندسي: toLocalIterator + pymongo")
        write_mode = "local_iterator_fallback"

        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        col = db[RAW_COLLECTION]
        inserted_count_fb = 0
        batch = []
        try:
            for row in df_raw_final.rdd.toLocalIterator():
                batch.append(row.asDict())
                if len(batch) >= 5000:
                    col.insert_many(batch, ordered=False)
                    inserted_count_fb += len(batch)
                    print(f"📦 تم إدخال {inserted_count_fb:,} سجل...")
                    batch.clear()
            if batch:
                col.insert_many(batch, ordered=False)
                inserted_count_fb += len(batch)
        finally:
            client.close()

    row_count = df_raw_final.count()
    elapsed = time.perf_counter() - start_time
    throughput = row_count / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 50)
    print("✅ اكتمل التحميل الخام (Raw Load) باستخدام PySpark")
    print(f"وضع الكتابة المستخدم     : {write_mode}")
    print(f"عدد السجلات المحملة      : {row_count:,}")
    print(f"الزمن المستغرق          : {elapsed:.2f} ثانية")
    print(f"معدل المعالجة           : {throughput:,.0f} سجل/ثانية")
    print("=" * 50)

    # الانتظار لالتقاط Spark UI فقط في الجلسات التفاعلية من الطرفية (لا يعطل الـ API أو الاختبارات الآلية)
    import sys
    if sys.stdin and sys.stdin.isatty() and not os.getenv("NON_INTERACTIVE"):
        try:
            print("\n⏸️ افتح المتصفح على http://localhost:4040 والتقط لقطة Stages.")
            input("اضغط Enter لإيقاف Spark والمتابعة...")
        except (EOFError, KeyboardInterrupt):
            pass

    try:
        spark.stop()
    except Exception as e:
        print(f"⚠️ تحذير أثناء إيقاف Spark: {e}")

    return {
        "rows_read": row_count,
        "raw_loaded": row_count,
        "elapsed_seconds": elapsed,
        "throughput": throughput,
        "write_mode": write_mode,
        "partitions": partitions,
        "duplicate_ids": duplicate_ids
    }