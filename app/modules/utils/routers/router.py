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