from pyspark.sql import SparkSession
from pyspark import StorageLevel 
import os
import time

# --- Timer function ---
def print_time(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

# --- Configuración de entorno ---
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# --- Inicializar Spark ---
print_time("Inicializando Spark...")
spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("EjemploSparkDataFrame")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skew.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .config("spark.python.worker.memory", "1g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print_time("Spark inicializado")

# --- 1. Leer CSV como DataFrame ---
print_time("Leyendo archivo CSV...")
csv_df = spark.read.csv("listings.csv", header=True, inferSchema=True)
csv_df.printSchema()
print_time("CSV leído correctamente")

# --- 2. Leer JSON de hechos como DataFrame ---
print_time("Leyendo JSON de hechos...")
df_facts = spark.read.option("multiline", "true").json("listings_facts.json")
df_facts.printSchema()
print_time("JSON de hechos leído correctamente")

# --- 3. Join directo en DataFrames ---
print_time("Realizando JOIN entre DataFrames...")
joined_df = csv_df.join(df_facts, on="_id", how="inner")
print_time("JOIN completado")

# Persistencia en memoria y disco
print_time("Persistiendo DataFrame...")
joined_df.persist(StorageLevel.MEMORY_AND_DISK)
print_time("DataFrame persistido")

# --- 4. Registrar vista SQL ---
print_time("Registrando vista SQL...")
joined_df.createOrReplaceTempView("airbnb_joined")
print_time("Vista SQL registrada")

# --- 5. Consultas SQL encapsuladas en funciones ---

def contar_casas():
    print_time("Ejecutando consulta: contar_casas...")
    spark.sql("""
        SELECT COUNT(*) AS total_casas
        FROM airbnb_joined
        WHERE property_type = 'House'
    """).show()
    print_time("Consulta contar_casas completada")

def camas_por_tipo():
    print_time("Ejecutando consulta: camas_por_tipo...")
    spark.sql("""
        SELECT property_type, AVG(beds) AS camas_media
        FROM airbnb_joined
        GROUP BY property_type
        ORDER BY camas_media DESC
    """).show()
    print_time("Consulta camas_por_tipo completada")

def alojamientos_con_reseñas():
    print_time("Ejecutando consulta: alojamientos_con_reseñas...")
    spark.sql("""
        SELECT COUNT(*) AS alojamiento_review
        FROM airbnb_joined
        WHERE size(reviews) > 0
    """).show()
    print_time("Consulta alojamientos_con_reseñas completada")

# --- 6. Ejecutar funciones SQL ---
if __name__ == "__main__":
    print_time("Iniciando ejecución de consultas...")
    contar_casas()
    camas_por_tipo()
    alojamientos_con_reseñas()
    print_time("Todas las consultas completadas")

# --- 7. Detener Spark ---
print_time("Deteniendo Spark...")
spark.stop()
print_time("Spark detenido - Proceso finalizado")