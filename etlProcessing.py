import json
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import json_util

# --- Conexión a MongoDB ---
# Leemos credenciales desde config.json (buena práctica de seguridad)
with open("config.json") as f:
    config = json.load(f)

user = config["mongo_user"]
password = config["mongo_pass"]

# URL de conexión al clúster de MongoDB Atlas
url = f"mongodb+srv://{user}:{password}@educationalcluster.7xf5hht.mongodb.net/?retryWrites=true&w=majority&appName=EducationalCluster"
client = MongoClient(url, server_api=ServerApi('1'))

# Seleccionamos base de datos y colección
db = client["sample_airbnb"]
collection = db["listingsAndReviews"]

# --- EXTRACT ---
# Extraemos todos los documentos de la colección
docs = list(collection.find())

# --- TRANSFORM ---
# 1. CSV: solo campos descriptivos y metadatos elementales de cada propiedad
elemental_keys = [
  "_id",
  "listing_url",
  "name",
  "property_type",
  "room_type",
  "bed_type"
]

csv_rows = []
for doc in docs:
    filtered = {"_id": str(doc["_id"])}
    for key in elemental_keys:
        if key in doc and key != "_id":
            filtered[key] = doc[key]
    csv_rows.append(filtered)

# 2. Dimensiones: objetos/arrays descriptivos (atributos estáticos)
dimension_keys = [
  "summary",
  "space",
  "description",
  "neighborhood_overview",
  "notes",
  "transit",
  "access",
  "interaction",
  "house_rules",
  "minimum_nights",
  "maximum_nights",
  "cancellation_policy",
  "amenities",
  "images",
  "host",
  "address",
]

dimension_docs = []
for doc in docs:
    filtered = {"_id": str(doc["_id"])}
    for key in dimension_keys:
        if key in doc:
            filtered[key] = doc[key]
    dimension_docs.append(filtered)

# 3. Hechos: métricas y eventos dinámicos (cambian con el tiempo)
fact_keys = [
  "last_scraped",
  "calendar_last_scraped",
  "first_review",
  "last_review",
  "accommodates",
  "bedrooms",
  "beds",
  "bathrooms",
  "number_of_reviews",
  "price",
  "security_deposit",
  "cleaning_fee",
  "extra_people",
  "guests_included",
  "availability",
  "review_scores",
  "reviews"
]

fact_docs = []
for doc in docs:
    filtered = {"_id": str(doc["_id"])}
    for key in fact_keys:
        if key in doc:
            filtered[key] = doc[key]
    fact_docs.append(filtered)

# --- LOAD ---
# Guardamos CSV con atributos escalares
df = pd.DataFrame(csv_rows)
df.to_csv("listings.csv", index=False)

# Guardamos JSON con dimensiones usando siempre dumps de json_util
# ¡Cargarlo a su cluster!
with open("listings_dimensions.json", "w", encoding="utf-8") as f:
    f.write(json_util.dumps(dimension_docs, indent=2, ensure_ascii=False))

# Guardamos JSON con hechos usando siempre dumps de json_util
# Este se cargará desde mi clúster
with open("listings_facts.json", "w", encoding="utf-8") as f:
    f.write(json_util.dumps(fact_docs, indent=2, ensure_ascii=False))
