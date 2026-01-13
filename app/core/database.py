# import os
# import logging
# from pymongo import AsyncMongoClient
# from pymongo.errors import ConnectionFailure, ConfigurationError
# from dotenv import load_dotenv

# # Configurar logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Cargar variables de entorno
# load_dotenv()

# # Variables de entorno
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# DB_NAME = os.getenv("MONGO_DB_NAME", "slides_db")

# if not MONGO_URI:
#     logger.error("❌ MONGO_URI no está definida en las variables de entorno")
#     raise ConfigurationError("MONGO_URI no está definida en las variables de entorno")

# # Variables globales
# client: AsyncMongoClient | None = None
# db = None


# # ==============================================
# # 🔹 Inicializar conexión
# # ==============================================
# async def init_db():
#     """Inicializa la conexión asíncrona a MongoDB"""
#     global client, db
#     try:
#         client = AsyncMongoClient(
#             MONGO_URI,
#             serverSelectionTimeoutMS=5000,
#             connectTimeoutMS=10000,
#             socketTimeoutMS=45000,
#             maxPoolSize=50,
#             minPoolSize=10,
#             appname="informes listos"
#         )

#         # Probar conexión
#         await client.admin.command("ping")
#         logger.info("✅ Conexión a MongoDB establecida correctamente")

#         # Obtener base de datos
#         db = client[DB_NAME]
#         logger.info(f"Usando base de datos: {DB_NAME}")

#         # Crear colecciones si no existen
#         required_collections = ["roles", "users"]
#         existing_collections = await db.list_collection_names()

#         for collection in required_collections:
#             if collection not in existing_collections:
#                 await db.create_collection(collection)
#                 logger.info(f"🆕 Colección '{collection}' creada")

#     except ConnectionFailure as e:
#         logger.error(f"❌ Error de conexión a MongoDB: {e}")
#         raise
#     except Exception as e:
#         logger.error(f"⚠️ Error inesperado al conectar con MongoDB: {e}")
#         raise


# # ==============================================
# # 🔹 Verificar conexión
# # ==============================================
# async def check_connection() -> bool:
#     """Verifica el estado de la conexión a MongoDB"""
#     global client
#     try:
#         if client is None:
#             return False
#         await client.admin.command("ping")
#         return True
#     except Exception as e:
#         logger.error(f"Error al verificar conexión: {e}")
#         return False


# # ==============================================
# # 🔹 Cerrar conexión
# # ==============================================
# async def close_connection():
#     """Cierra la conexión a la base de datos"""
#     global client
#     try:
#         if client:
#             await client.close()
#             logger.info("🔒 Conexión a MongoDB cerrada correctamente")
#     except Exception as e:
#         logger.error(f"Error al cerrar conexión: {e}")


# # ==============================================
# # 🔹 Obtener base de datos y colecciones
# # ==============================================
# def get_database():
#     """Obtiene la instancia activa de la base de datos"""
#     global db
#     if db is None:
#         raise ConnectionError("No hay conexión activa a la base de datos")
#     return db


# def get_users_collection():
#     return get_database()["users"]


# def get_roles_collection():
#     return get_database()["roles"]


# # ==============================================
# # 🔹 Cerrar conexión automáticamente
# # ==============================================
# import atexit
# import asyncio

# @atexit.register
# def _close_sync():
#     loop = asyncio.get_event_loop()
#     if loop.is_running():
#         loop.create_task(close_connection())
#     else:
#         loop.run_until_complete(close_connection())


from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from app.core.config import settings
import sys

class Database:
    client: MongoClient = None
    _connected: bool = False
    
db = Database()

def get_database():
    # Conectar automáticamente si no está conectado
    if not db._connected:
        connect_to_mongo()
    
    if db.client is None:
        raise RuntimeError(
            "La base de datos no está conectada. "
            "Asegúrate de que MongoDB esté corriendo."
        )
    
    return db.client[settings.DATABASE_NAME]

def connect_to_mongo():
    # Evitar reconexiones múltiples
    if db._connected:
        print("⚠️ Ya existe una conexión activa a MongoDB")
        return
    
    try:
        print(f"🔄 Intentando conectar a MongoDB: {settings.MONGODB_URL}")
        
        db.client = MongoClient(
            settings.MONGODB_URL,
            serverSelectionTimeoutMS=5000,  # 5 segundos de timeout
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        
        # Prueba la conexión haciendo ping
        db.client.admin.command('ping')
        db._connected = True
        
        print("✅ Conectado exitosamente a MongoDB")
        print(f"📊 Base de datos: {settings.DATABASE_NAME}")
        
    except ConnectionFailure as e:
        print(f"❌ Error al conectar a MongoDB: {e}")
        print(f"URL intentada: {settings.MONGODB_URL}")
        print("\n💡 Verifica que:")
        print("  1. MongoDB esté corriendo")
        print("  2. La URL en .env sea correcta")
        print("  3. El puerto 27017 esté disponible")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error inesperado al conectar: {e}")
        sys.exit(1)

def close_mongo_connection():
    if db.client:
        db.client.close()
        db._connected = False
        print("🔌 Conexión a MongoDB cerrada")
    else:
        print("⚠️ No hay conexión activa para cerrar")

def reset_connection():
    """Útil para testing o reconexiones"""
    close_mongo_connection()
    db.client = None
    db._connected = False
    connect_to_mongo()
