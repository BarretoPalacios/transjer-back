from pymongo.collection import Collection

def generate_sequential_code(
    *,
    counters_collection: Collection,
    target_collection: Collection,
    sequence_name: str,
    field_name: str,
    prefix: str = "",
    length: int = 6
) -> str:
    """
    Genera códigos como:
    LUG-000001
    DOC-000123
    """

    # 1️⃣ Incremento atómico del contador
    counter = counters_collection.find_one_and_update(
        {"_id": sequence_name},
        {
            "$inc": {"seq": 1},
            "$setOnInsert": {"prefix": prefix}
        },
        upsert=True,
        return_document=True
    )

    seq_number = counter["seq"]

    # 2️⃣ Formatear número con ceros
    numeric_part = str(seq_number).zfill(length)
    code = f"{prefix}{numeric_part}"

    # 3️⃣ Verificación extra (por seguridad)
    exists = target_collection.find_one({field_name: code})
    if exists:
        raise ValueError(f"Código duplicado detectado: {code}")

    return code
