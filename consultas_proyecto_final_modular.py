from pyspark.sql import SparkSession
from pyspark import StorageLevel
import os
import time

# --- Timer function ---
def print_time(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

# --- Configuración de entorno (como en plantilla original) ---
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# --- Inicializar Spark (como en plantilla original) ---
print_time("Inicializando Spark...")
spark = (
    SparkSession.builder
    .master("local[*]")  # Ajustado para tu máquina
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("ConsultasProyectoFinal")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skew.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.sql.shuffle.partitions", "4")  # Reducido
    .config("spark.executor.memory", "2g")  # Ajustado
    .config("spark.driver.memory", "2g")   # Ajustado
    .config("spark.python.worker.memory", "512m")  # Reducido
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print_time("Spark inicializado")

# --- Función de carga de datos común ---
def cargar_datasets_base():
    """Carga los datasets base que usarán todas las consultas"""
    print_time("Cargando datasets base...")
    
    # CSV
    csv_df = spark.read.csv("listings.csv", header=True, inferSchema=True)
    print_time(f"CSV cargado: {csv_df.count()} registros")
    
    # Facts
    df_facts = spark.read.option("multiline", "true").json("listings_facts.json")
    print_time("Facts cargado")
    
    # Dimensions
    df_dimensions_raw = spark.read.option("multiline", "true").json("listings_dimensions.json")
    print_time("Dimensions cargado")
    
    return csv_df, df_facts, df_dimensions_raw

# --- 1. Precio promedio por tipo de propiedad y país ---
def consulta_01_precio_promedio_por_tipo_y_pais():
    """1. Precio promedio por tipo de propiedad y país (property_type, address.country)"""
    print_time("=== CONSULTA 1: Precio promedio por tipo de propiedad y país ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos para esta consulta
    df_dimensions = df_dimensions_raw.select("_id", "address.country").withColumnRenamed("address.country", "country")
    
    # JOIN
    joined_df = csv_df.select("_id", "property_type").join(
        df_facts.select("_id", "price"), on="_id", how="inner"
    ).join(
        df_dimensions.select("_id", "country"), on="_id", how="inner"
    )
    
    joined_df.createOrReplaceTempView("consulta_01")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            SELECT 
                property_type,
                country as pais,
                COUNT(*) as total_propiedades,
                ROUND(AVG(CAST(price.`$numberDecimal` AS DOUBLE)), 2) as precio_promedio
            FROM consulta_01
            WHERE country IS NOT NULL 
            AND country != ''
            AND property_type IS NOT NULL
            AND price IS NOT NULL
            AND price.`$numberDecimal` IS NOT NULL
            AND price.`$numberDecimal` != ''
            GROUP BY property_type, country
            HAVING COUNT(*) >= 3
            ORDER BY precio_promedio DESC
            LIMIT 15
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 1 completada")

# --- 2. Disponibilidad anual promedio por ciudad ---
def consulta_02_disponibilidad_promedio_por_ciudad():
    """2. Disponibilidad anual promedio por ciudad (market, availability_365)"""
    print_time("=== CONSULTA 2: Disponibilidad anual promedio por ciudad ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos para esta consulta
    df_dimensions = df_dimensions_raw.select("_id", "address.market").withColumnRenamed("address.market", "market")
    
    # JOIN
    joined_df = df_facts.select("_id", "availability").join(
        df_dimensions.select("_id", "market"), on="_id", how="inner"
    )
    
    joined_df.createOrReplaceTempView("consulta_02")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            SELECT 
                market as ciudad,
                COUNT(*) as total_alojamientos,
                ROUND(AVG(availability.availability_365), 2) as disponibilidad_promedio
            FROM consulta_02
            WHERE market IS NOT NULL 
            AND market != ''
            AND availability IS NOT NULL
            AND availability.availability_365 IS NOT NULL
            GROUP BY market
            HAVING COUNT(*) >= 5
            ORDER BY disponibilidad_promedio DESC
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 2 completada")

# --- 3. Top 10 propiedades con más comentarios de reseñas con puntuación alta ---
def consulta_03_top10_propiedades_mas_comentarios():
    """3. Top 10 propiedades con más comentarios de reseñas con puntuación alta"""
    print_time("=== CONSULTA 3: Top 10 propiedades con más comentarios de reseñas con puntuación alta ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos
    df_dimensions = df_dimensions_raw.select("_id", "address.market").withColumnRenamed("address.market", "market")
    
    # JOIN
    joined_df = csv_df.select("_id", "name").join(
        df_facts.select("_id", "number_of_reviews", "review_scores", "price"), on="_id", how="inner"
    ).join(
        df_dimensions.select("_id", "market"), on="_id", how="inner"
    )
    
    joined_df.createOrReplaceTempView("consulta_03")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            SELECT 
                name,
                market as ciudad,
                number_of_reviews as total_resenas,
                review_scores.review_scores_rating as puntuacion,
                ROUND(CAST(price.`$numberDecimal` AS DOUBLE), 2) as precio
            FROM consulta_03
            WHERE review_scores.review_scores_rating > 80 
            AND number_of_reviews IS NOT NULL
            ORDER BY number_of_reviews DESC
            LIMIT 10
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 3 completada")

# --- Función para ejecutar consulta específica ---
def ejecutar_consulta(numero_consulta):
    """Ejecutar una consulta específica por número"""
    consultas = {
        1: consulta_01_precio_promedio_por_tipo_y_pais,
        2: consulta_02_disponibilidad_promedio_por_ciudad,
        3: consulta_03_top10_propiedades_mas_comentarios,
        # Agregar más consultas aquí...
    }
    
    if numero_consulta in consultas:
        consultas[numero_consulta]()
    else:
        print_time(f"Consulta {numero_consulta} no implementada")

# --- Función para ejecutar todas las consultas ---
def ejecutar_todas_consultas():
    """Ejecutar todas las consultas del proyecto final"""
    print_time("Iniciando ejecución de todas las consultas...")
    
    consulta_01_precio_promedio_por_tipo_y_pais()
    consulta_02_disponibilidad_promedio_por_ciudad()
    consulta_03_top10_propiedades_mas_comentarios()
    # Agregar más consultas...
    
    print_time("Todas las consultas completadas")

# --- Ejecución principal (como en plantilla original) ---
if __name__ == "__main__":
    print_time("Iniciando consultas del proyecto final...")
    
    # Opciones de ejecución:
    
    # Opción 1: Ejecutar todas las consultas
    # ejecutar_todas_consultas()
    
    # Opción 2: Ejecutar consulta específica (descomenta la línea siguiente)
    ejecutar_consulta(3)  # Cambiar número según necesidad
    
    print_time("Proceso finalizado")

# --- Detener Spark (como en plantilla original) ---
print_time("Deteniendo Spark...")
spark.stop()
print_time("Spark detenido - Proceso finalizado")