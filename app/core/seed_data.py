# seed_data.py
from datetime import datetime
from bson import ObjectId
from typing import List, Dict
from app.modules.auth.utils.security import get_password_hash
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeedService:
    def __init__(self, db):
        self.db = db
        self.users_collection = db["users"]
        self.roles_collection = db["roles"]
        self.permissions_collection = db["permissions"]
    
    def create_permissions(self) -> Dict[str, str]:
        """Crea permisos básicos de solo lectura (view)"""
        logger.info("Creando permisos de solo lectura...")
        
        permissions_data = [
            # Dashboard
            {
                "name": "view_dashboard",
                "description": "Ver dashboard principal",
                "resource": "dashboard",
                "action": "view"
            },
            
            # Gestión
            {
                "name": "view_gestion",
                "description": "Ver módulo de gestión",
                "resource": "gestion",
                "action": "view"
            },
            {
                "name": "manage_gestion",
                "description": "Gestionar módulo de gestión",
                "resource": "gestion",
                "action": "manage"
            },
            
            # Contabilidad
            {
                "name": "view_contabilidad",
                "description": "Ver módulo de contabilidad",
                "resource": "contabilidad",
                "action": "view"
            },
            {
                "name": "manage_contabilidad",
                "description": "Gestionar módulo de contabilidad",
                "resource": "contabilidad",
                "action": "manage"
            },
            
            # Servicios
            {
                "name": "view_servicios",
                "description": "Ver módulo de servicios",
                "resource": "servicios",
                "action": "view"
            },
            {
                "name": "manage_servicios",
                "description": "Gestionar módulo de servicios",
                "resource": "servicios",
                "action": "manage"
            },
            
            # Gastos
            {
                "name": "view_gastos",
                "description": "Ver módulo de gastos",
                "resource": "gastos",
                "action": "view"
            },
            {
                "name": "manage_gastos",
                "description": "Gestionar módulo de gastos",
                "resource": "gastos",
                "action": "manage"
            },
            
            # Gerencia
            {
                "name": "view_gerencia",
                "description": "Ver módulo de gerencia",
                "resource": "gerencia",
                "action": "view"
            },
            {
                "name": "manage_gerencia",
                "description": "Gestionar módulo de gerencia",
                "resource": "gerencia",
                "action": "manage"
            },
            
            # Configuración
            {
                "name": "view_configuracion",
                "description": "Ver configuración del sistema",
                "resource": "configuracion",
                "action": "view"
            },
            {
                "name": "manage_configuracion",
                "description": "Gestionar configuración del sistema",
                "resource": "configuracion",
                "action": "manage"
            },
            
            # Usuarios
            {
                "name": "view_usuarios",
                "description": "Ver lista de usuarios",
                "resource": "usuarios",
                "action": "view"
            },
            {
                "name": "manage_usuarios",
                "description": "Gestionar usuarios",
                "resource": "usuarios",
                "action": "manage"
            },
            {
                "name": "admin_usuarios",
                "description": "Administración completa de usuarios",
                "resource": "usuarios",
                "action": "admin"
            }
        ]
        
        permission_ids = {}
        
        for perm in permissions_data:
            # Verificar si ya existe
            existing = self.permissions_collection.find_one({
                "name": perm["name"]
            })
            
            if not existing:
                perm["created_at"] = datetime.utcnow()
                result = self.permissions_collection.insert_one(perm)
                permission_ids[perm["name"]] = str(result.inserted_id)
                logger.info(f"✓ Permiso creado: {perm['name']}")
            else:
                permission_ids[perm["name"]] = str(existing["_id"])
                logger.info(f"✓ Permiso ya existe: {perm['name']}")
        
        return permission_ids
    
    def create_roles(self, permission_ids: Dict[str, str]) -> Dict[str, str]:
        """Crea roles específicos para Transjer"""
        logger.info("Creando roles específicos...")
        
        # Obtener todos los IDs de permisos view
        all_view_permissions = [
            permission_ids.get("view_dashboard"),
            permission_ids.get("view_gestion"),
            permission_ids.get("view_contabilidad"),
            permission_ids.get("view_servicios"),
            permission_ids.get("view_gastos"),
            permission_ids.get("view_gerencia"),
            permission_ids.get("view_configuracion"),
            permission_ids.get("view_usuarios"),
        ]
        
        # Filtrar None values
        all_view_permissions = [pid for pid in all_view_permissions if pid is not None]
        
        roles_data = [
            {
                "name": "administrador",
                "description": "Administrador con acceso completo",
                "permission_ids": list(permission_ids.values())  # Todos los permisos
            },
            {
                "name": "comercial",
                "description": "Rol para equipo comercial",
                "permission_ids": [
                    permission_ids.get("view_dashboard"),
                    permission_ids.get("view_gestion"),
                    permission_ids.get("manage_gestion"),
                    # permission_ids.get("view_servicios"),
                    # permission_ids.get("view_gerencia"),
                ]
            },
            {
                "name": "contabilidad",
                "description": "Rol para equipo de contabilidad",
                "permission_ids": [
                    permission_ids.get("view_dashboard"),
                    permission_ids.get("view_contabilidad"),
                    permission_ids.get("view_gastos"),
                    permission_ids.get("manage_contabilidad"),
                    permission_ids.get("manage_gastos"),
                ]
            },
            {
                "name": "operaciones",
                "description": "Rol para equipo de operaciones",
                "permission_ids": [
                    permission_ids.get("view_dashboard"),
                    permission_ids.get("view_servicios"),
                    permission_ids.get("view_gestion"),
                    permission_ids.get("manage_servicios"),
                    permission_ids.get("manage_gestion"),
                ]
            },
            {
                "name": "visualizador",
                "description": "Rol solo para visualizar",
                "permission_ids": all_view_permissions  # Solo permisos view
            }
        ]
        
        role_ids = {}
        
        for role in roles_data:
            # Verificar si ya existe
            existing = self.roles_collection.find_one({
                "name": role["name"]
            })
            
            if not existing:
                role["created_at"] = datetime.utcnow()
                # Filtrar IDs nulos
                role["permission_ids"] = [pid for pid in role["permission_ids"] if pid is not None]
                result = self.roles_collection.insert_one(role)
                role_ids[role["name"]] = str(result.inserted_id)
                logger.info(f"✓ Rol creado: {role['name']}")
            else:
                role_ids[role["name"]] = str(existing["_id"])
                logger.info(f"✓ Rol ya existe: {role['name']}")
        
        return role_ids
    
    def create_users(self, role_ids: Dict[str, str]):
        """Crea usuarios específicos para Transjer"""
        logger.info("Creando usuarios de Transjer...")
        
        users_data = [
            # Administradores
            {
                "email": "kelly.murga@transjer.com",
                "username": "kelly.murga",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Kelly Murga",
                "role_names": ["administrador"]
            },
            {
                "email": "jeanpierre.rimay@transjer.com",
                "username": "jeanpierre.rimay",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Jeanpierre Rimay",
                "role_names": ["administrador"]
            },
            
            # Comercial
            {
                "email": "comercial@transjer.com",
                "username": "comercial",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Cielo Comercial",
                "role_names": ["comercial"]
            },
            
            # Contabilidad
            {
                "email": "contabilidad@transjer.com",
                "username": "contabilidad",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Geraldine Contabilidad",
                "role_names": ["contabilidad"]
            },
            
            # Operaciones
            {
                "email": "operaciones@transjer.com",
                "username": "operaciones",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Renato Operación",
                "role_names": ["operaciones"]
            },
            {
                "email": "julio.murga@transjer.com",
                "username": "julio.murga",
                "password": "transjer2026",  # Contraseña temporal
                "full_name": "Julio Murga",
                "role_names": ["operaciones"]
            }
        ]
        
        from app.modules.auth.models.user import UserModel
        
        for user_data in users_data:
            try:
                # Verificar si ya existe
                existing = self.users_collection.find_one({
                    "$or": [
                        {"email": user_data["email"]},
                        {"username": user_data["username"]}
                    ]
                })
                
                if existing:
                    logger.info(f"✓ Usuario ya existe: {user_data['email']}")
                    continue
                
                # Convertir nombres de roles a IDs
                role_ids_list = []
                for role_name in user_data["role_names"]:
                    if role_name in role_ids:
                        role_ids_list.append(role_ids[role_name])
                
                # Crear usuario usando UserModel (similar a tu UserService)
                user_model = UserModel(
                    email=user_data["email"],
                    username=user_data["username"],
                    hashed_password=get_password_hash(user_data["password"]),
                    full_name=user_data["full_name"],
                    role_ids=role_ids_list,
                    is_active=True,
                    created_at=datetime.utcnow()
                )
                
                result = self.users_collection.insert_one(user_model.to_dict())
                
                logger.info(f"✓ Usuario creado: {user_data['full_name']}")
                logger.info(f"  Email: {user_data['email']}")
                logger.info(f"  Usuario: {user_data['username']}")
                logger.info(f"  Contraseña temporal: {user_data['password']}")
                logger.info(f"  Roles: {', '.join(user_data['role_names'])}")
                logger.info("-" * 50)
                
            except Exception as e:
                logger.error(f"✗ Error creando usuario {user_data['email']}: {str(e)}")
    
    def check_existing_data(self) -> Dict[str, int]:
        """Verifica datos existentes"""
        try:
            users_count = self.users_collection.count_documents({})
            roles_count = self.roles_collection.count_documents({})
            permissions_count = self.permissions_collection.count_documents({})
            
            return {
                "users": users_count,
                "roles": roles_count,
                "permissions": permissions_count
            }
        except Exception as e:
            logger.error(f"Error verificando datos: {str(e)}")
            return {"users": 0, "roles": 0, "permissions": 0}
    
    def create_initial_data(self) -> Dict:
        """Método principal para crear datos iniciales"""
        try:
            logger.info("=" * 60)
            logger.info("INICIALIZANDO DATOS PARA TRANSJER")
            logger.info("=" * 60)
            
            # Verificar datos existentes
            counts = self.check_existing_data()
            logger.info(f"Datos existentes: {counts}")
            
            # Crear permisos
            permission_ids = self.create_permissions()
            logger.info(f"Permisos procesados: {len(permission_ids)}")
            
            # Crear roles
            role_ids = self.create_roles(permission_ids)
            logger.info(f"Roles procesados: {len(role_ids)}")
            
            # Crear usuarios
            self.create_users(role_ids)
            
            # Verificar datos finales
            final_counts = self.check_existing_data()
            logger.info(f"Datos finales: {final_counts}")
            
            logger.info("=" * 60)
            logger.info("DATOS INICIALES CREADOS EXITOSAMENTE!")
            logger.info("=" * 60)
            
            return {
                "success": True,
                "message": "Datos iniciales creados correctamente",
                "counts": final_counts
            }
            
        except Exception as e:
            logger.error(f"Error en seed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }


def initialize_seed_data(db):
    """Función para inicializar datos desde otros módulos"""
    seed_service = SeedService(db)
    return seed_service.create_initial_data()

