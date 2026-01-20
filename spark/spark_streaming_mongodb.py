from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

# Définir le schéma des items de facture
item_schema = StructType([
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("TotalPrice", FloatType(), True)
])

# Définir le schéma complet de la facture
invoice_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Items", ArrayType(item_schema), True),
    StructField("InvoiceTotal", FloatType(), True),
    StructField("received_at", StringType(), True)
])

# Créer la session Spark avec les packages nécessaires
spark = SparkSession.builder \
    .appName("InvoiceStreamingToMongoDB") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.write.connection.uri", 
            "mongodb://admin:admin123@mongodb:27017/invoices_db.invoices?authSource=admin") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("🚀 Spark Streaming Job Started - Reading from Kafka and Writing to MongoDB")
print("=" * 80)

# Lire depuis Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "invoices-input") \
    .option("startingOffsets", "earliest") \
    .load()

print("✅ Connected to Kafka topic: invoices-input")

# Convertir les données Kafka (format binaire) en string
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parser le JSON et extraire les champs
parsed_df = kafka_df \
    .select(from_json(col("value"), invoice_schema).alias("data")) \
    .select("data.*")

# Convertir les dates au bon format
final_df = parsed_df \
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"))) \
    .withColumn("received_at", to_timestamp(col("received_at")))

print("✅ Data transformation applied")

# Fonction pour écrire chaque batch dans MongoDB
def write_to_mongodb(batch_df, batch_id):
    print(f"\n📦 Processing batch {batch_id}...")
    
    # Compter le nombre de lignes dans le batch
    count = batch_df.count()
    
    if count > 0:
        print(f"   → Writing {count} invoice(s) to MongoDB...")
        
        # Écrire dans MongoDB
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .save()
        
        print(f"   ✅ Batch {batch_id} written successfully!")
        
        # Afficher un échantillon (pour debug)
        batch_df.select("InvoiceNo", "CustomerID", "Country", "InvoiceTotal").show(5, truncate=False)
    else:
        print(f"   ℹ️  Batch {batch_id} is empty, skipping...")

# Démarrer le streaming avec foreachBatch
query = final_df \
    .writeStream \
    .foreachBatch(write_to_mongodb) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n✅ Streaming query started!")
print("📊 Waiting for data from Kafka...")
print("=" * 80)

# Attendre que le streaming se termine (Ctrl+C pour arrêter)
query.awaitTermination()