from pyspark.sql import SparkSession
import os
import json
from pymongo.mongo_client import MongoClient
from pyspark import StorageLevel
from bson import json_util

# --- Configuración de entorno ---
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# --- Conexión a MongoDB ---
url = "mongodb+srv://reader123:reader123@educationalcluster.7xf5hht.mongodb.net/?retryWrites=true&w=majority&appName=EducationalCluster"
client = MongoClient(url)
db = client["sample_airbnb"]
collection = db["listings_facts"]

# --- Cargar documentos desde MongoDB ---
docs = list(collection.find())
for d in docs:
    d["_id"] = str(d["_id"])
json_docs = [json.dumps(doc, default=json_util.default) for doc in docs]

# --- Inicializar Spark ---
spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("EjemploSparkDataFrame")
    .getOrCreate()
)

# --- 1. Leer CSV como DataFrame ---
csv_df = spark.read.csv("data/listings.csv", header=True, inferSchema=True)
csv_df.printSchema()

# --- 2. Leer colección Mongo como DataFrame ---
rdd = spark.sparkContext.parallelize(json_docs, numSlices=4)
df_facts = spark.read.json(rdd)
df_facts.printSchema()

# --- 3. Join directo en DataFrames ---
joined_df = csv_df.join(df_facts, on="_id", how="inner")

# Persistencia en memoria y disco
joined_df.persist(StorageLevel.MEMORY_AND_DISK)

# --- 4. Registrar vista SQL ---
joined_df.createOrReplaceTempView("airbnb_joined")

# --- 5. Consultas SQL encapsuladas en funciones ---

def contar_casas():
    spark.sql("""
        SELECT COUNT(*) AS total_casas
        FROM airbnb_joined
        WHERE property_type = 'House'
    """).show()

def camas_por_tipo():
    spark.sql("""
        SELECT property_type, AVG(beds) AS camas_media
        FROM airbnb_joined
        GROUP BY property_type
        ORDER BY camas_media DESC
    """).show()

def alojamientos_con_reseñas():
    spark.sql("""
        SELECT COUNT(*) AS alojamiento_review
        FROM airbnb_joined
        WHERE size(reviews) > 0
    """).show()

# --- 6. Ejecutar funciones SQL (comenta las que no quieras ejecutar) ---#
if __name__ == "__main__":
    contar_casas()
    camas_por_tipo()
    alojamientos_con_reseñas()

# --- 7. Detener Spark ---
spark.stop()
