from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
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



def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("sistema-operador-logistico")

    app = FastAPI(title="sistema-operador-logistico", version="0.1.0",swagger_ui_parameters={"syntaxHighlight": {"theme": "obsidian"}})

    

    app.include_router(auth.router)
    app.include_router(users.router)
    app.include_router(roles.router)
    app.include_router(permissions.router)

    
    app.include_router(cliente_router)
    app.include_router(proveedor_router)
    app.include_router(personal_router)
    app.include_router(flota_router)
    app.include_router(servici_principal_router)
    app.include_router(fletes_router)

    app.include_router(facturacion_router)
    app.include_router(seguimiento_facturas_router)

    app.include_router(historico_router)
    app.include_router(gasto_router)
    app.include_router(utils_router)
  
    app.include_router(gasto)


    app.include_router(servicioshistorico_router)



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

    # Simple request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.info(f"--> {request.method} {request.url.path}")
        response = await call_next(request)
        logger.info(f"<-- {request.method} {request.url.path} {response.status_code}")
        return response


    @app.get("/", tags=["root"])
    async def read_root():
        return {"message": "sistema-operador-logistico API"}

    @app.get("/health", tags=["health"])
    async def health():
        res = connect_to_mongo()
        return {"status": res}

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)