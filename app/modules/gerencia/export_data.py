import io
import pandas as pd
from datetime import datetime
from bson import ObjectId
from fastapi.responses import StreamingResponse

class AnalisisLogistica:
    def __init__(self, db):
        self.db = db
        self.servicio_principal_collection = db["servicio_principal"]

    def flatten_dict(self, d, parent_key='', sep='_'):
        """Aplanado recursivo para convertir objetos anidados en columnas de Excel."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and '$date' not in v:
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                if isinstance(v, ObjectId):
                    v = str(v)
                elif isinstance(v, dict) and '$date' in v:
                    v = v['$date']
                elif isinstance(v, datetime):
                    v = v.replace(tzinfo=None)
                items.append((new_key, v))
        return dict(items)

    def exportar_todo_maestro_stream(self):
        # Pipeline sin filtros para traer TODA la data
        pipeline = [
            # Unir con Flete
            {
                "$lookup": {
                    "from": "fletes",
                    "let": {"id_srv": {"$toString": "$_id"}},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$servicio_id", "$$id_srv"]}}}],
                    "as": "FLETE"
                }
            },
            {"$unwind": {"path": "$FLETE", "preserveNullAndEmptyArrays": True}},
            
            # Unir con Factura
            {
                "$lookup": {
                    "from": "facturacion",
                    "let": {"id_fac": "$FLETE.factura_id"},
                    "pipeline": [{"$match": {"$expr": {"$eq": [{"$toString": "$_id"}, "$$id_fac"]}}}],
                    "as": "FACTURA"
                }
            },
            {"$unwind": {"path": "$FACTURA", "preserveNullAndEmptyArrays": True}},
            
            # Unir con Gestión
            {
                "$lookup": {
                    "from": "facturacion_gestion",
                    "let": {"cod_fac": "$FACTURA.numero_factura"},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$codigo_factura", "$$cod_fac"]}}}],
                    "as": "GESTION"
                }
            },
            {"$unwind": {"path": "$GESTION", "preserveNullAndEmptyArrays": True}}
        ]

        cursor = self.servicio_principal_collection.aggregate(pipeline)
        
        filas_aplanadas = []
        for doc in cursor:
            # Procesamos cada bloque con su prefijo para evitar colisiones de nombres
            srv = self.flatten_dict(doc, parent_key="SRV")
            # Limpiar bloques originales del diccionario SRV para no duplicar columnas
            srv.pop("SRV_FLETE", None)
            srv.pop("SRV_FACTURA", None)
            srv.pop("SRV_GESTION", None)

            flt = self.flatten_dict(doc.get("FLETE", {}), parent_key="FLT")
            fac = self.flatten_dict(doc.get("FACTURA", {}), parent_key="FAC")
            ges = self.flatten_dict(doc.get("GESTION", {}), parent_key="GES")

            # Combinar todo en una sola fila
            filas_aplanadas.append({**srv, **flt, **fac, **ges})

        # Crear el Excel en memoria
        df = pd.DataFrame(filas_aplanadas)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Data_Total')
        
        output.seek(0)
        return output