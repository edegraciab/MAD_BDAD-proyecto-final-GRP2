import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import json_util

with open("config/config.json") as f:
    config = json.load(f)

user = config["student_cnx"]["grp_user"]
password = config["student_cnx"]["grp_pass"]
_host = config["student_cnx"]["grp_host"]
grp_connection_string = f"mongodb+srv://{user}:{password}@{_host}/?retryWrites=true&w=majority&appName=MADBDADedg"

# --- MongoDB connection ---
def conectar_grp_mongodb():
    url = grp_connection_string
    client = MongoClient(url, server_api=ServerApi('1'))
    
    # Probar la conexión
    try:
        client.admin.command('ping')
        print("Conexión exitosa a MongoDB Atlas!")
        return client
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

def cargar_dimensions():
    print("Leyendo archivo listings_dimensions.json...")
    with open("listings_dimensions.json", "r", encoding="utf-8") as f:
        json_data = f.read()
    
    # Convertir string JSON a lista de diccionarios Python
    dimension_docs = json_util.loads(json_data)
    print(f"Documentos leídos: {len(dimension_docs)}")
    return dimension_docs

def insertar_dimensions(client, dimensions):
    # Seleccionar base de datos y colección
    db = client["sample_airbnb"]
    collection = db["listings_dimensions"]
    
    # Limpiar colección existente (opcional)
    print("Limpiando colección existente...")
    collection.delete_many({})
    
    print("Insertando documentos en el cluster...")
    result = collection.insert_many(dimensions)
    print(f"Documentos insertados: {len(result.inserted_ids)}")
    
    # Verificar la inserción
    count = collection.count_documents({})
    print(f"Total documentos en colección: {count}")
    
    return collection

# --- Main execution ---
if __name__ == "__main__":
    grp_client = None
    try:
        # Conectar a MongoDB
        grp_client = conectar_grp_mongodb()
        if grp_client is None:
            exit(1)
        
        # Cargar datos desde el archivo JSON
        dimensions = cargar_dimensions()
        
        # Insertar dimensions.json a la colección
        collection = insertar_dimensions(grp_client, dimensions)
        
        # Mostrar un documento de ejemplo
        sample_doc = collection.find_one()
        print("\nDocumento de ejemplo insertado:")
        print(f"ID: {sample_doc['_id']}")
        if 'host' in sample_doc:
            print(f"Host: {sample_doc['host'].get('host_name', 'N/A')}")
        if 'amenities' in sample_doc:
            print(f"Amenities count: {len(sample_doc.get('amenities', []))}")
            
        print("¡Carga completada exitosamente!")
        
    except Exception as e:
        print(f"Error durante la carga: {e}")
    finally:
        if grp_client:
            grp_client.close()
            print("Conexión cerrada.")