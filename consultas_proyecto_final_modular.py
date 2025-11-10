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
    .master("local[*]")  
    .config("spark.driver.bindAddress", "127.0.0.1")
    .appName("ConsultasProyectoFinal")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skew.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.sql.shuffle.partitions", "4")  
    .config("spark.executor.memory", "2g")  
    .config("spark.driver.memory", "2g")  
    .config("spark.python.worker.memory", "512m")  
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print_time("Spark inicializado")

# --- Función de carga de datos común ---
def cargar_datasets_base():
    """Carga los datasets base que usarán todas las consultas"""
    print_time("Cargando datasets base...")
    
    # CSV
    csv_df = spark.read.csv("data/listings.csv", header=True, inferSchema=True)
    print_time(f"CSV cargado: {csv_df.count()} registros")
    
    # Facts
    df_facts = spark.read.option("multiline", "true").json("data/listings_facts.json")
    print_time("Facts cargado")
    
    # Dimensions
    df_dimensions_raw = spark.read.option("multiline", "true").json("data/listings_dimensions.json")
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

# --- 4. Ciudades con más alojamientos con política de cancelación flexible ---
def consulta_04_ciudades_cancelacion_flexible():
    """4. Ciudades con más alojamientos con política de cancelación flexible (cancellation_policy)"""
    print_time("=== CONSULTA 4: Ciudades con más alojamientos con política de cancelación flexible ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos para esta consulta
    df_dimensions = df_dimensions_raw.select("_id", "address.market", "cancellation_policy").withColumnRenamed("address.market", "market")
    
    # JOIN
    joined_df = df_dimensions.select("_id", "market", "cancellation_policy")
    
    joined_df.createOrReplaceTempView("consulta_04")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            SELECT 
                market as ciudad,
                COUNT(*) as total_alojamientos_flexibles,
                ROUND(
                    (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2
                ) as porcentaje_del_total
            FROM consulta_04
            WHERE market IS NOT NULL 
            AND market != ''
            AND cancellation_policy IS NOT NULL
            AND LOWER(cancellation_policy) IN ('flexible', 'super_lenient_60')
            GROUP BY market
            HAVING COUNT(*) >= 3
            ORDER BY total_alojamientos_flexibles DESC
            LIMIT 15
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 4 completada")

# --- 5. Superhosts con cuatro (4) o más propiedades ---
def consulta_05_superhosts_con_mas_propiedades():
    """5. Superhosts con cuatro (4) o más propiedades (host_is_superhost, host_listings_count)"""
    print_time("=== CONSULTA 5: Superhosts con cuatro (4) o más propiedades ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos para esta consulta
    df_dimensions = df_dimensions_raw.select(
        "_id", 
        "host.host_is_superhost", 
        "host.host_listings_count",
        "host.host_name",
        "address.market"
    ).withColumnRenamed("address.market", "market")

    df_dimensions.show(5, truncate=False)
    # Crear vista temporal
    df_dimensions.createOrReplaceTempView("consulta_05")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            WITH superhosts_agrupados AS (
                SELECT 
                    host_name as nombre_host,
                    host_listings_count as total_propiedades,
                    host_is_superhost as es_superhost,
                    COUNT(DISTINCT market) as ciudades_con_propiedades,
                    COLLECT_SET(market) as ciudades
                FROM consulta_05
                WHERE host_is_superhost = true 
                AND host_listings_count IS NOT NULL
                AND host_listings_count >= 4
                AND host_name IS NOT NULL
                AND host_name != ''
                GROUP BY host_name, host_listings_count, host_is_superhost
            )
            SELECT 
                nombre_host,
                total_propiedades,
                ciudades_con_propiedades,
                ciudades,
                es_superhost
            FROM superhosts_agrupados
            ORDER BY total_propiedades DESC, nombre_host ASC
            LIMIT 20
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 5 completada")

# --- 6. Cuántos alojamientos ofrecen Wifi, Breakfast y Cable TV ---
def consulta_06_alojamientos_con_amenidades_especificas():
    """6. Cuántos alojamientos ofrecen Wifi, Breakfast y Cable TV (amenities)"""
    print_time("=== CONSULTA 6: Cuántos alojamientos ofrecen Wifi, Breakfast y Cable TV ===")
    
    csv_df, df_facts, df_dimensions_raw = cargar_datasets_base()
    
    # Preparar datos específicos para esta consulta
    df_dimensions = df_dimensions_raw.select(
        "_id", 
        "amenities",
        "address.market"
    ).withColumnRenamed("address.market", "market")
    
    # Crear vista temporal
    df_dimensions.createOrReplaceTempView("consulta_06")
    
    # Ejecutar consulta
    try:
        spark.sql("""
            WITH alojamientos_con_amenidades AS (
                SELECT 
                    _id,
                    market as ciudad,
                    amenities,
                    CASE 
                        WHEN exists(amenities, x -> lower(x) LIKE '%wifi%') OR
                             exists(amenities, x -> lower(x) LIKE '%internet%')
                        THEN 1 ELSE 0 
                    END as tiene_wifi,
                    CASE 
                        WHEN exists(amenities, x -> lower(x) LIKE '%breakfast%')
                        THEN 1 ELSE 0 
                    END as tiene_breakfast,
                    CASE 
                        WHEN exists(amenities, x -> lower(x) LIKE '%tv%') OR
                             exists(amenities, x -> lower(x) LIKE '%television%') OR
                             exists(amenities, x -> lower(x) LIKE '%cable%')
                        THEN 1 ELSE 0 
                    END as tiene_cable_tv
                FROM consulta_06
                WHERE amenities IS NOT NULL
                AND size(amenities) > 0
            )
            SELECT 
                'Total con las 3 amenidades' as categoria,
                COUNT(*) as total_alojamientos
            FROM alojamientos_con_amenidades
            WHERE tiene_wifi = 1 AND tiene_breakfast = 1 AND tiene_cable_tv = 1
            
            UNION ALL
            
            SELECT 
                'Solo Wifi' as categoria,
                COUNT(*) as total_alojamientos
            FROM alojamientos_con_amenidades
            WHERE tiene_wifi = 1
            
            UNION ALL
            
            SELECT 
                'Solo Breakfast' as categoria,
                COUNT(*) as total_alojamientos
            FROM alojamientos_con_amenidades
            WHERE tiene_breakfast = 1
            
            UNION ALL
            
            SELECT 
                'Solo Cable TV' as categoria,
                COUNT(*) as total_alojamientos
            FROM alojamientos_con_amenidades
            WHERE tiene_cable_tv = 1
            
            ORDER BY 
                CASE categoria 
                    WHEN 'Total con las 3 amenidades' THEN 1
                    WHEN 'Solo Wifi' THEN 2
                    WHEN 'Solo Breakfast' THEN 3
                    WHEN 'Solo Cable TV' THEN 4
                END
        """).show(truncate=False)
    except Exception as e:
        print_time(f"Error: {e}")
    
    print_time("Consulta 6 completada")
    

# --- Función para ejecutar consulta específica ---
def ejecutar_consulta(numero_consulta):
    """Ejecutar una consulta específica por número"""
    consultas = {
        1: consulta_01_precio_promedio_por_tipo_y_pais,
        2: consulta_02_disponibilidad_promedio_por_ciudad,
        3: consulta_03_top10_propiedades_mas_comentarios,
        4: consulta_04_ciudades_cancelacion_flexible,
        5: consulta_05_superhosts_con_mas_propiedades,
        6: consulta_06_alojamientos_con_amenidades_especificas,
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
    consulta_04_ciudades_cancelacion_flexible()
    consulta_05_superhosts_con_mas_propiedades()
    consulta_06_alojamientos_con_amenidades_especificas()
    # Agregar más consultas...
    
    print_time("Todas las consultas completadas")

# --- Ejecución principal (como en plantilla original) ---
if __name__ == "__main__":
    print_time("Iniciando consultas del proyecto final...")
    
    # Opciones de ejecución:
    
    # Opción 1: Ejecutar todas las consultas
    ejecutar_todas_consultas()
    
    # Opción 2: Ejecutar consulta específica (descomenta la línea siguiente)
    #ejecutar_consulta(6)  # Cambiar número según necesidad
    
    print_time("Proceso finalizado")

# --- Detener Spark (como en plantilla original) ---
print_time("Deteniendo Spark...")
spark.stop()
print_time("Spark detenido - Proceso finalizado")