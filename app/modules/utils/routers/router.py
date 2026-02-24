from fastapi import APIRouter, Query
from app.core.database import get_database
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.modules.utils.core.consulta_ruc.consulta_ruc import RucService

router = APIRouter(prefix="/utils", tags=["Utils"])

@router.get("/generate-sequential-code")
def generate_code(
    sequence: str = Query(..., example="lugares"),
    collection: str = Query(..., example="lugares"),
    field: str = Query(..., example="codigo"),
    prefix: str = Query("", example="LUG-"),
    length: int = Query(6, ge=3, le=10)
):

    db = get_database()
    
    target_collection = db[collection]

    code = generate_sequential_code(
        counters_collection=db["counters"],
        target_collection=target_collection,
        sequence_name=sequence,
        field_name=field,
        prefix=prefix,
        length=length
    )

    return {
        "code": code,
        "sequence": sequence
    }

@router.get("/consultar-ruc/{ruc}")
async def consultar_ruc(ruc: str):
    ruc_service = RucService()

    return ruc_service.consultar_ruc(ruc)


@router.get("/clientes-list")
async def get_razones_sociales():
    # 1. Lista estática solicitada
    lista_estatica = [
        "OECHSLE", "SONEPAR", "CALERA", "ALICORP", "INFINIA", 
        "BOX PERU", "GYG VENTILACION", "AMAUTA", "BASA", 
        "CLOUDATEL", "LINKSOLUTIONS"
    ]

    # Conexión a la base de datos
    db = get_database()
    colecc = db["clientes"]
    
    # 2. Traer solo los valores únicos del campo 'razon_social' de la DB
    # Usamos distinct para no traer documentos enteros y saturar la memoria
    razones_db = colecc.distinct("razon_social")
    
    # 3. Unir listas, eliminar duplicados con set() y limpiar posibles nulos
    # El set se encarga de que si "ALICORP" está en ambos, solo aparezca una vez
    combinada = set(lista_estatica) | {r for r in razones_db if r}
    
    # 4. Retornar lista ordenada alfabéticamente
    return sorted(list(combinada))
    
@router.get("/proveedores-list")
async def get_proveedores_list():
    # 1. Lista estática de transportistas
    # He separado los nombres que venían pegados para que el select sea legible
    lista_estatica = [
        "Transporte Transjer", 
        "Transporte Guerrero",
        "Transporte Taboada",
        "Transporte Ccorimayo",
        "Transporte Cabrera",
        "Transporte Liñan",
        "Transporte Parodi",
        "Transporte Jesus",
        "Transporte Leober"
    ]

    # Conexión a la base de datos
    db = get_database()
    # Cambiamos a la colección de proveedores
    colecc = db["proveedores"] 
    
    # 2. Traer valores únicos de la base de datos
    # Si tu colección de proveedores usa el mismo campo 'razon_social'
    proveedores_db = colecc.distinct("razon_social")
    
    # 3. Unir ambas listas y eliminar duplicados
    # Usamos set para asegurar valores únicos y filtramos nulos
    combinada = set(lista_estatica) | {p for p in proveedores_db if p}
    
    # 4. Retornar lista ordenada alfabéticamente
    return sorted(list(combinada))

@router.get("/placas-list")
async def get_placas_list():
    
    lista_estatica =  [
    "BVR-727" ,
    "CBB-773",
    "CAG-817",
    "CDM-793",
    "CDN-786",
    "CDQ-786",
    "CDQ-743",
    "BXS-909",
    "BYH-716",
    "C5Q-932",
    "D4D-838",
    "BZH-921",
    "BPF-700",
    "BJA-838",
    "F5F-264",
    "ARN-774"
    ]

    db = get_database()

    colecc = db["flota"]

    placas_db = colecc.distinct("placa")

    return sorted(list(set(lista_estatica + placas_db)))