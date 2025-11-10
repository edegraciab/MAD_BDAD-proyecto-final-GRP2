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

# --- Inicializar Spark (configuración simple) ---
print_time("Inicializando Spark...")
spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("PrecioPromedioConsulta")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.python.worker.memory", "512m")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print_time("Spark inicializado")

# --- Configuración simplificada (solo archivos locales) ---
print_time("Usando solo archivos locales - no se requiere MongoDB")

# --- Cargar solo los datos necesarios ---
def cargar_datos_necesarios():
    """Cargar solo los datos necesarios para la consulta de precio promedio"""
    print_time("=== CARGANDO SOLO DATOS NECESARIOS ===")
    
    # 1. Leer CSV (contiene property_type)
    print_time("Leyendo CSV para property_type...")
    csv_df = spark.read.csv("data/listings.csv", header=True, inferSchema=True)
    print_time(f"CSV cargado: {csv_df.count()} registros")
    
    # 2. Leer JSON facts (contiene price)
    print_time("Leyendo JSON facts para price...")
    df_facts = spark.read.option("multiline", "true").json("data/listings_facts.json")
    print_time("Facts JSON cargado")
    
    # 3. Leer JSON dimensions LOCAL (contiene address.country)
    print_time("Leyendo JSON dimensions LOCAL para country...")
    df_dimensions_raw = spark.read.option("multiline", "true").json("data/listings_dimensions.json")
    print_time("Dimensions JSON local cargado")
    
    # Seleccionar solo los campos necesarios: _id y address.country
    print_time("Extrayendo solo _id y address.country...")
    df_dimensions = df_dimensions_raw.select("_id", "address.country")
    
    # Renombrar la columna para simplicidad
    df_dimensions = df_dimensions.withColumnRenamed("address.country", "country")
    
    '''print_time("Verificando estructura de dimensions...")
    df_dimensions.printSchema()
    df_dimensions.show(5, truncate=False)
    print_time(f"DataFrame dimensions procesado: {df_dimensions.count()} registros")'''
    
    return csv_df, df_facts, df_dimensions

# --- Función principal para calcular precio promedio ---
def calcular_precio_promedio_por_tipo_y_pais():
    """Calcular precio promedio por tipo de propiedad y país"""
    print_time("=== CALCULANDO PRECIO PROMEDIO POR TIPO Y PAÍS ===")
    
    # Cargar datos
    datos = cargar_datos_necesarios()
    if datos is None:
        print_time("ERROR: No se pudieron cargar los datos")
        return
    
    csv_df, df_facts, df_dimensions = datos
    
    # Hacer JOINs paso a paso
    print_time("JOIN paso 1: CSV + Facts...")
    joined_step1 = csv_df.select("_id", "property_type").join(
        df_facts.select("_id", "price"), on="_id", how="inner"
    )
    print_time(f"Registros después de JOIN 1: {joined_step1.count()}")
    
    print_time("JOIN paso 2: Agregar Dimensions...")
    joined_final = joined_step1.join(
        df_dimensions.select("_id", "country"), on="_id", how="inner"
    )
    print_time(f"Registros después de JOIN final: {joined_final.count()}")
    
    '''# Verificar estructura de price
    print_time("Verificando estructura del campo price...")
    joined_final.select("price").printSchema()
    joined_final.select("_id", "property_type", "country", "price").show(3, truncate=False)'''
    
    # Registrar vista temporal
    joined_final.createOrReplaceTempView("datos_precio")
    
    # Intentar diferentes consultas según la estructura de price
    print_time("=== EJECUTANDO CONSULTAS ===")
    
    try:
        # Consulta 1: Precio promedio por tipo de propiedad y país
        print_time("Consulta 1: Precio promedio por tipo de propiedad y país (property_type, address.country)")
        spark.sql("""
            SELECT 
                property_type,
                country as pais,
                COUNT(*) as total_propiedades,
                ROUND(AVG(CAST(price.`$numberDecimal` AS DOUBLE)), 2) as precio_promedio
            FROM datos_precio 
            WHERE country IS NOT NULL 
            AND country != ''
            AND property_type IS NOT NULL
            AND price IS NOT NULL
            AND price.`$numberDecimal` IS NOT NULL
            AND price.`$numberDecimal` != ''
            GROUP BY property_type, country
            HAVING COUNT(*) >= 3
            ORDER BY pais, property_type, precio_promedio DESC
        """).show(truncate=False)
        
    except Exception as e:
        print_time(f"Error en consulta NumberDecimal: {e}")

# --- Ejecutar consulta principal ---
if __name__ == "__main__":
    calcular_precio_promedio_por_tipo_y_pais()
    
    # Detener Spark
    print_time("Deteniendo Spark...")
    spark.stop()
    print_time("Proceso finalizado")