from fastapi import FastAPI, Request,Depends
from apscheduler.schedulers.background import BackgroundScheduler
from app.modules.seguimiento_facturas.service import FacturacionGestionService
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
from app.modules.auth.utils.dependencies import get_current_user
from contextlib import asynccontextmanager
from app.core.seed_data import SeedService
from app.core.database import get_database,connect_to_mongo
from app.modules.utils.routers.router import router as utils_router
from app.modules.auth.routers import auth, users, roles, permissions
# from app.modules.dataservice.routes.cuenta_routes import router as cuenta_router
from app.modules.dataservice.routes.cliente_routes import router as cliente_router
# from app.modules.dataservice.routes.lugar_routes import router as lugar_router
from app.modules.dataservice.routes.proveedor_routes import router as proveedor_router
from app.modules.dataservice.routes.personal_routes import router as personal_router
from app.modules.dataservice.routes.flota_routes import router as flota_router
from app.modules.servicios.routes.servicio_principal_routes import router as servici_principal_router
from app.modules.facturacion.routes.facturacion_routes import router as facturacion_router
from app.modules.historico.routes.historico import router as historico_router
from app.modules.servicios_historicos.routes_servicios_historicos import router as servicioshistorico_router
from app.modules.fletes.fletes_routes import router as fletes_router
from app.modules.seguimiento_facturas.router import router as seguimiento_facturas_router
from app.modules.gastos_adicionales.router import router as gasto_router
from app.modules.gastos.router import router as gasto
from app.modules.gerencia.router import router as gerencia

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sistema-operador-logistico")

def create_app() -> FastAPI:


    app = FastAPI(title="sistema-operador-logistico", version="0.1.0",lifespan=lifespan,swagger_ui_parameters={"syntaxHighlight": {"theme": "obsidian"}})

    

    app.include_router(auth.router)
    app.include_router(users.router)
    app.include_router(roles.router)
    app.include_router(permissions.router)

    
    app.include_router(cliente_router,dependencies=[Depends(get_current_user)])
    app.include_router(proveedor_router,dependencies=[Depends(get_current_user)])
    app.include_router(personal_router,dependencies=[Depends(get_current_user)])
    app.include_router(flota_router,dependencies=[Depends(get_current_user)])
    app.include_router(servici_principal_router,dependencies=[Depends(get_current_user)])
    app.include_router(fletes_router,dependencies=[Depends(get_current_user)])

    app.include_router(facturacion_router,dependencies=[Depends(get_current_user)])
    app.include_router(seguimiento_facturas_router,dependencies=[Depends(get_current_user)])

    app.include_router(historico_router,dependencies=[Depends(get_current_user)])
    app.include_router(gasto_router,dependencies=[Depends(get_current_user)])
    app.include_router(utils_router,dependencies=[Depends(get_current_user)])
  
    app.include_router(gasto,dependencies=[Depends(get_current_user)])
    app.include_router(gerencia)

    app.include_router(servicioshistorico_router,dependencies=[Depends(get_current_user)])



    # CORS (adjust origins for production)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://sistema.transjer.com.pe/",
            "https://sistema.transjer.com.pe","*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", tags=["root"])
    async def read_root():
        return {"message": "sistema-operador-logistico API"}

    @app.get("/health", tags=["health"])
    async def health():
        res = connect_to_mongo()
        return {"status": res}

    return app

@asynccontextmanager
async def lifespan(app: FastAPI):
        """
        Lifespan context manager para manejar eventos de inicio y cierre
        """
        # Startup
        logger.info("Iniciando aplicación...")
        
        # Inicializar datos si no existen
        await initialize_database()
        
        # 2. Configurar la Bachera (APScheduler)
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        scheduler = BackgroundScheduler()
        
        try:
            logger.info("🔄 Ejecutando actualización inicial de vencimientos (Post-Startup)...")
            
            gestion_service._actualizar_vencimientos_automaticos() 
            logger.info("✅ Actualización inicial completada.")
        except Exception as e:
            logger.error(f"❌ Error en la actualización inicial: {e}")
        # Programamos la tarea: Se ejecuta todos los días a las 00:01 AM
        # También puedes usar (minutes=60) para pruebas
        scheduler.add_job(
            gestion_service._actualizar_vencimientos_automaticos, 
            'cron', 
            hour=0, 
            minute=1,
            id="check_vencimientos"
        )
        
        scheduler.start()
        logger.info("✓ Bachera de vencimientos programada correctamente")


        yield
        
        # Shutdown
        logger.info("Cerrando aplicación...")

async def initialize_database():
        """
        Inicializa la base de datos con datos por defecto si está vacía
        """
        try:
            # Obtener conexión a la base de datos
            db = get_database()
            seed_service = SeedService(db)
            
            # Verificar si ya hay datos
            counts = seed_service.check_existing_data()
            
            if counts["users"] == 0:
                logger.info("Base de datos vacía, creando datos iniciales...")
                result = seed_service.create_initial_data()
                
                if result["success"]:
                    logger.info("✓ Datos iniciales creados automáticamente")
                    logger.info(f"  Usuarios: {result['counts']['users']}")
                    logger.info(f"  Roles: {result['counts']['roles']}")
                    logger.info(f"  Permisos: {result['counts']['permissions']}")
                else:
                    logger.error(f"✗ Error creando datos iniciales: {result.get('error')}")
            else:
                logger.info(f"Base de datos ya tiene {counts['users']} usuarios")
                
        except Exception as e:
            logger.error(f"Error inicializando base de datos: {str(e)}")


app = create_app()

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)