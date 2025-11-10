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
    .master("local[2]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("DisponibilidadPromedioCiudad")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.python.worker.memory", "512m")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --- Configuración simplificada (solo archivos locales) ---
print_time("Usando solo archivos locales - no se requiere MongoDB")

# --- Cargar solo los datos necesarios ---
def cargar_datos_necesarios():
    """Cargar solo los datos necesarios para la consulta de disponibilidad por ciudad"""
    print_time("=== CARGANDO SOLO DATOS NECESARIOS ===")
    
    # 1. Leer JSON facts (contiene availability.availability_365)
    print_time("Leyendo JSON facts para availability_365...")
    df_facts = spark.read.option("multiline", "true").json("data/listings_facts.json")
    
    # 2. Leer JSON dimensions LOCAL (contiene address.market)
    print_time("Leyendo JSON dimensions LOCAL para market...")
    df_dimensions_raw = spark.read.option("multiline", "true").json("data/listings_dimensions.json")
    
    # Seleccionar solo los campos necesarios: _id y address.market
    print_time("Extrayendo solo _id y address.market...")
    df_dimensions = df_dimensions_raw.select("_id", "address.market")
    
    # Renombrar la columna para simplicidad
    df_dimensions = df_dimensions.withColumnRenamed("address.market", "market")
    
    '''print_time("Verificando estructura de dimensions...")
    df_dimensions.printSchema()
    df_dimensions.show(5, truncate=False)
    print_time(f"DataFrame dimensions procesado: {df_dimensions.count()} registros")
    
    # Verificar estructura de facts (availability)
    print_time("Verificando estructura de facts (availability)...")
    df_facts.select("_id", "availability").printSchema()
    df_facts.select("_id", "availability").show(5, truncate=False)
    print_time(f"DataFrame facts procesado: {df_facts.count()} registros")'''
    
    return df_facts, df_dimensions

# --- Función principal para calcular disponibilidad promedio ---
def calcular_disponibilidad_promedio_por_ciudad():
    """Calcular disponibilidad anual promedio por ciudad"""
    print_time("=== CALCULANDO DISPONIBILIDAD ANUAL PROMEDIO POR CIUDAD ===")
    
    # Cargar datos
    datos = cargar_datos_necesarios()
    if datos is None:
        print_time("ERROR: No se pudieron cargar los datos")
        return
    
    df_facts, df_dimensions = datos
    
    # Hacer JOIN: Facts + Dimensions
    print_time("JOIN: Facts + Dimensions...")
    joined_final = df_facts.select("_id", "availability").join(
        df_dimensions.select("_id", "market"), on="_id", how="inner"
    )
    print_time(f"Registros después de JOIN: {joined_final.count()}")
    
    '''# Verificar estructura de availability
    print_time("Verificando estructura del campo availability...")
    joined_final.select("availability").printSchema()
    joined_final.select("_id", "market", "availability").show(5, truncate=False)'''

    # Registrar vista temporal
    joined_final.select("_id", "market", "availability")
    joined_final.createOrReplaceTempView("datos_disponibilidad")
    
    # Intentar diferentes consultas según la estructura de availability
    print_time("=== EJECUTANDO CONSULTAS ===")
    
    try:        
        # Consulta 2: Intentar con availability.availability_365
        print_time("Consulta 2: Disponibilidad anual promedio por ciudad (market, availability_365)")
        spark.sql("""
            SELECT 
                 market as ciudad
                ,COUNT(*) as total_alojamientos
                ,ROUND(AVG(availability.availability_365), 2) as disponibilidad_promedio
                ,MIN(availability.availability_365) as min_disponibilidad
                ,MAX(availability.availability_365) as max_disponibilidad
            FROM datos_disponibilidad 
            WHERE market IS NOT NULL 
            AND market != ''
            AND availability IS NOT NULL
            AND availability.availability_365 IS NOT NULL
            GROUP BY market
            HAVING COUNT(*) >= 5
            ORDER BY disponibilidad_promedio DESC
            --LIMIT 20
        """).show(truncate=False)
        
    except Exception as e:
        print_time(f"Error en consulta availability_365: {e}")

# --- Función adicional para explorar la estructura de availability ---
def explorar_estructura_availability():
    """Explorar la estructura del campo availability para debugging"""
    print_time("=== EXPLORANDO ESTRUCTURA DE AVAILABILITY ===")
    
    # Cargar solo facts para explorar
    df_facts = spark.read.option("multiline", "true").json("data/listings_facts.json")
    
    print_time("Esquema completo de facts...")
    df_facts.printSchema()
    
    print_time("Mostrando muestras del campo availability...")
    df_facts.select("_id", "availability").show(10, truncate=False)
    
    print_time("Contando registros con availability no nulo...")
    count_availability = df_facts.filter(df_facts.availability.isNotNull()).count()
    total_count = df_facts.count()
    print_time(f"Registros con availability: {count_availability} de {total_count}")

# --- Ejecutar consulta principal ---
if __name__ == "__main__":

    # Primero explorar la estructura (opcional)
    #explorar_estructura_availability()
    
    # Luego ejecutar la consulta principal
    calcular_disponibilidad_promedio_por_ciudad()
    
    # Detener Spark
    print_time("Deteniendo Spark...")
    spark.stop()
    print_time("Proceso finalizado")