# init_database.py
"""
Script para inicializar la base de datos con:
- Permisos básicos del sistema
- Roles (Admin, Usuario, Moderador)
- Usuario administrador por defecto
"""

from pymongo import MongoClient
from datetime import datetime
import sys
import os

# Agregar el directorio raíz al path para importar módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.config import settings
from app.utils.security import get_password_hash

def init_database():
    print("=" * 60)
    print("🚀 INICIALIZANDO BASE DE DATOS")
    print("=" * 60)
    
    try:
        # Conectar a MongoDB
        print(f"\n📡 Conectando a MongoDB: {settings.MONGODB_URL}")
        client = MongoClient(settings.MONGODB_URL, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Conexión exitosa")
        
        db = client[settings.DATABASE_NAME]
        
        # Limpiar colecciones existentes (opcional)
        print("\n🗑️  ¿Deseas limpiar las colecciones existentes? (s/n): ", end="")
        clean = input().strip().lower()
        
        if clean == 's':
            print("⚠️  Limpiando colecciones...")
            db.permissions.delete_many({})
            db.roles.delete_many({})
            db.users.delete_many({})
            print("✅ Colecciones limpiadas")
        
        # ====================
        # 1. CREAR PERMISOS
        # ====================
        print("\n" + "=" * 60)
        print("📋 CREANDO PERMISOS")
        print("=" * 60)
        
        permissions = [
            # Permisos de Usuarios
            {"name": "Crear Usuarios", "description": "Permite crear nuevos usuarios", "resource": "users", "action": "create"},
            {"name": "Leer Usuarios", "description": "Permite ver información de usuarios", "resource": "users", "action": "read"},
            {"name": "Actualizar Usuarios", "description": "Permite modificar usuarios", "resource": "users", "action": "update"},
            {"name": "Eliminar Usuarios", "description": "Permite eliminar usuarios", "resource": "users", "action": "delete"},
            
            # Permisos de Roles
            {"name": "Crear Roles", "description": "Permite crear nuevos roles", "resource": "roles", "action": "create"},
            {"name": "Leer Roles", "description": "Permite ver roles del sistema", "resource": "roles", "action": "read"},
            {"name": "Actualizar Roles", "description": "Permite modificar roles", "resource": "roles", "action": "update"},
            {"name": "Eliminar Roles", "description": "Permite eliminar roles", "resource": "roles", "action": "delete"},
            
            # Permisos de Permisos
            {"name": "Crear Permisos", "description": "Permite crear nuevos permisos", "resource": "permissions", "action": "create"},
            {"name": "Leer Permisos", "description": "Permite ver permisos del sistema", "resource": "permissions", "action": "read"},
            {"name": "Actualizar Permisos", "description": "Permite modificar permisos", "resource": "permissions", "action": "update"},
            {"name": "Eliminar Permisos", "description": "Permite eliminar permisos", "resource": "permissions", "action": "delete"},
            
            # Permisos adicionales (ejemplos para tu sistema)
            {"name": "Ver Dashboard", "description": "Acceso al panel de control", "resource": "dashboard", "action": "read"},
            {"name": "Ver Reportes", "description": "Acceso a reportes del sistema", "resource": "reports", "action": "read"},
            {"name": "Exportar Datos", "description": "Permite exportar información", "resource": "data", "action": "export"},
        ]
        
        permission_ids = {}
        for perm in permissions:
            # Verificar si ya existe
            existing = db.permissions.find_one({
                "resource": perm["resource"],
                "action": perm["action"]
            })
            
            if existing:
                print(f"⚠️  Permiso ya existe: {perm['name']}")
                permission_ids[f"{perm['resource']}:{perm['action']}"] = str(existing["_id"])
            else:
                perm["created_at"] = datetime.utcnow()
                result = db.permissions.insert_one(perm)
                permission_ids[f"{perm['resource']}:{perm['action']}"] = str(result.inserted_id)
                print(f"✅ Permiso creado: {perm['name']}")
        
        print(f"\n📊 Total de permisos en el sistema: {len(permission_ids)}")
        
        # ====================
        # 2. CREAR ROLES
        # ====================
        print("\n" + "=" * 60)
        print("👥 CREANDO ROLES")
        print("=" * 60)
        
        # ROL: ADMINISTRADOR (todos los permisos)
        admin_permissions = list(permission_ids.values())
        
        admin_role = {
            "name": "admin",
            "description": "Administrador con acceso completo al sistema",
            "permission_ids": admin_permissions,
            "created_at": datetime.utcnow()
        }
        
        existing_admin = db.roles.find_one({"name": "admin"})
        if existing_admin:
            print("⚠️  Rol 'admin' ya existe, actualizando permisos...")
            db.roles.update_one(
                {"_id": existing_admin["_id"]},
                {"$set": {"permission_ids": admin_permissions}}
            )
            admin_role_id = str(existing_admin["_id"])
        else:
            result = db.roles.insert_one(admin_role)
            admin_role_id = str(result.inserted_id)
            print(f"✅ Rol 'admin' creado con {len(admin_permissions)} permisos")
        
        # ROL: USUARIO (permisos básicos de lectura)
        user_permissions = [
            permission_ids.get("users:read"),
            permission_ids.get("roles:read"),
            permission_ids.get("dashboard:read"),
        ]
        user_permissions = [p for p in user_permissions if p]  # Filtrar None
        
        user_role = {
            "name": "user",
            "description": "Usuario estándar con permisos básicos",
            "permission_ids": user_permissions,
            "created_at": datetime.utcnow()
        }
        
        existing_user = db.roles.find_one({"name": "user"})
        if existing_user:
            print("⚠️  Rol 'user' ya existe")
            user_role_id = str(existing_user["_id"])
        else:
            result = db.roles.insert_one(user_role)
            user_role_id = str(result.inserted_id)
            print(f"✅ Rol 'user' creado con {len(user_permissions)} permisos")
        
        # ROL: MODERADOR (permisos intermedios)
        moderator_permissions = [
            permission_ids.get("users:read"),
            permission_ids.get("users:update"),
            permission_ids.get("roles:read"),
            permission_ids.get("permissions:read"),
            permission_ids.get("dashboard:read"),
            permission_ids.get("reports:read"),
        ]
        moderator_permissions = [p for p in moderator_permissions if p]
        
        moderator_role = {
            "name": "moderator",
            "description": "Moderador con permisos de gestión limitados",
            "permission_ids": moderator_permissions,
            "created_at": datetime.utcnow()
        }
        
        existing_moderator = db.roles.find_one({"name": "moderator"})
        if existing_moderator:
            print("⚠️  Rol 'moderator' ya existe")
            moderator_role_id = str(existing_moderator["_id"])
        else:
            result = db.roles.insert_one(moderator_role)
            moderator_role_id = str(result.inserted_id)
            print(f"✅ Rol 'moderator' creado con {len(moderator_permissions)} permisos")
        
        # ====================
        # 3. CREAR USUARIO ADMIN
        # ====================
        print("\n" + "=" * 60)
        print("👤 CREANDO USUARIO ADMINISTRADOR")
        print("=" * 60)
        
        # Datos del administrador
        admin_email = "admin@sistema.com"
        admin_username = "admin"
        admin_password = "Admin123!"  # Cambiar en producción
        admin_full_name = "Administrador del Sistema"
        
        # Verificar si ya existe
        existing_admin_user = db.users.find_one({
            "$or": [
                {"email": admin_email},
                {"username": admin_username}
            ]
        })
        
        if existing_admin_user:
            print(f"⚠️  Usuario administrador ya existe: {admin_username}")
            print(f"📧 Email: {existing_admin_user['email']}")
        else:
            admin_user = {
                "email": admin_email,
                "username": admin_username,
                "hashed_password": get_password_hash(admin_password),
                "full_name": admin_full_name,
                "role_ids": [admin_role_id],
                "is_active": True,
                "created_at": datetime.utcnow()
            }
            
            result = db.users.insert_one(admin_user)
            print(f"✅ Usuario administrador creado exitosamente")
            print(f"📧 Email: {admin_email}")
            print(f"👤 Username: {admin_username}")
            print(f"🔑 Password: {admin_password}")
            print(f"⚠️  IMPORTANTE: Cambia la contraseña después del primer login!")
        
        # ====================
        # 4. RESUMEN
        # ====================
        print("\n" + "=" * 60)
        print("📊 RESUMEN DE LA INICIALIZACIÓN")
        print("=" * 60)
        
        total_permissions = db.permissions.count_documents({})
        total_roles = db.roles.count_documents({})
        total_users = db.users.count_documents({})
        
        print(f"\n✅ Permisos en el sistema: {total_permissions}")
        print(f"✅ Roles creados: {total_roles}")
        print(f"✅ Usuarios registrados: {total_users}")
        
        print("\n" + "=" * 60)
        print("🎉 INICIALIZACIÓN COMPLETADA")
        print("=" * 60)
        
        print("\n📝 CREDENCIALES DE ACCESO:")
        print(f"   Username: {admin_username}")
        print(f"   Password: {admin_password}")
        print(f"   Email: {admin_email}")
        
        print("\n🚀 Puedes iniciar la aplicación con:")
        print("   uvicorn app.main:app --reload")
        
        # Cerrar conexión
        client.close()
        
    except Exception as e:
        print(f"\n❌ Error durante la inicialización: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    init_database()