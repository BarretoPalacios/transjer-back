# services/historico_service.py
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from bson import ObjectId
from pymongo.database import Database
import logging

from app.modules.historico.schemas.historico import (
    HistoricoFilter, 
    HistoricoResponse, 
    HistoricoConServicioResponse,
    HistoricoEstadisticas
)

logger = logging.getLogger(__name__)


class HistoricoService:
    """Servicio para gestionar histórico de servicios"""
    
    def __init__(self, db: Database):
        self.db = db
        self.collection = db["historico_servicios"]
        self.servicios_collection = db["servicios_principales"]
        
    def get_all_historicos(
        self, 
        filter_params: Optional[HistoricoFilter] = None
    ) -> Dict[str, Any]:
        """
        Obtener todos los históricos con filtros y paginación
        """
        try:
            query = {}
            
            if filter_params:
                # Filtros exactos
                if filter_params.tipo:
                    query["tipo"] = filter_params.tipo
                
                if filter_params.periodo:
                    query["periodo"] = filter_params.periodo
                
                if filter_params.estado_final:
                    query["estado_final"] = filter_params.estado_final
                
                if filter_params.servicio_id:
                    query["servicio_id"] = filter_params.servicio_id
                
                # Filtros con regex
                if filter_params.codigo_servicio:
                    query["codigo_servicio"] = {
                        "$regex": filter_params.codigo_servicio, 
                        "$options": "i"
                    }
                
                if filter_params.usuario:
                    query["usuario"] = {
                        "$regex": filter_params.usuario, 
                        "$options": "i"
                    }
                
                # Filtros de fecha
                if filter_params.fecha_inicio or filter_params.fecha_fin:
                    fecha_query = {}
                    if filter_params.fecha_inicio:
                        fecha_query["$gte"] = datetime.combine(
                            filter_params.fecha_inicio, 
                            datetime.min.time()
                        )
                    if filter_params.fecha_fin:
                        fecha_query["$lte"] = datetime.combine(
                            filter_params.fecha_fin, 
                            datetime.max.time()
                        )
                    if fecha_query:
                        query["fecha_registro"] = fecha_query
                
                # Filtros de periodo (rango)
                if filter_params.periodo_inicio or filter_params.periodo_fin:
                    periodo_query = {}
                    if filter_params.periodo_inicio:
                        periodo_query["$gte"] = filter_params.periodo_inicio
                    if filter_params.periodo_fin:
                        periodo_query["$lte"] = filter_params.periodo_fin
                    if periodo_query:
                        query["periodo"] = periodo_query
            
            # Contar total
            total = self.collection.count_documents(query)
            
            # Obtener datos con paginación
            skip = filter_params.skip if filter_params else 0
            limit = filter_params.limit if filter_params else 100
            sort_by = filter_params.sort_by if filter_params else "fecha_registro"
            sort_order = filter_params.sort_order if filter_params else -1
            
            historicos_list = list(
                self.collection
                .find(query)
                .sort(sort_by, sort_order)
                .skip(skip)
                .limit(limit)
            )
            
            # Convertir a response
            result = []
            for historico in historicos_list:
                historico["id"] = str(historico["_id"])
                del historico["_id"]
                result.append(historico)
            
            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "data": result
            }
            
        except Exception as e:
            logger.error(f"Error al obtener históricos: {str(e)}")
            raise
    
    def get_historico_by_id(self, historico_id: str) -> Optional[Dict[str, Any]]:
        """Obtener un histórico por ID"""
        try:
            if not ObjectId.is_valid(historico_id):
                raise ValueError("ID de histórico inválido")
            
            historico = self.collection.find_one({"_id": ObjectId(historico_id)})
            
            if not historico:
                return None
            
            historico["id"] = str(historico["_id"])
            del historico["_id"]
            
            return historico
            
        except Exception as e:
            logger.error(f"Error al obtener histórico: {str(e)}")
            raise
    
    def get_historico_con_servicio(
        self, 
        historico_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Obtener histórico con los datos completos del servicio
        """
        try:
            historico = self.get_historico_by_id(historico_id)
            
            if not historico:
                return None
            
            # Obtener servicio relacionado
            servicio = self.servicios_collection.find_one(
                {"_id": ObjectId(historico["servicio_id"])}
            )
            
            if servicio:
                servicio["id"] = str(servicio["_id"])
                del servicio["_id"]
                historico["servicio"] = servicio
            
            return historico
            
        except Exception as e:
            logger.error(f"Error al obtener histórico con servicio: {str(e)}")
            raise
    
    def get_historicos_by_servicio(
        self, 
        servicio_id: str
    ) -> List[Dict[str, Any]]:
        """
        Obtener todos los históricos de un servicio específico
        """
        try:
            historicos = list(
                self.collection
                .find({"servicio_id": servicio_id})
                .sort("fecha_registro", -1)
            )
            
            result = []
            for historico in historicos:
                historico["id"] = str(historico["_id"])
                del historico["_id"]
                result.append(historico)
            
            return result
            
        except Exception as e:
            logger.error(f"Error al obtener históricos del servicio: {str(e)}")
            raise
    
    def get_estadisticas(
        self, 
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Obtener estadísticas de los históricos
        """
        try:
            match_stage = {}
            
            if fecha_inicio or fecha_fin:
                fecha_query = {}
                if fecha_inicio:
                    fecha_query["$gte"] = datetime.combine(
                        fecha_inicio, 
                        datetime.min.time()
                    )
                if fecha_fin:
                    fecha_query["$lte"] = datetime.combine(
                        fecha_fin, 
                        datetime.max.time()
                    )
                match_stage["fecha_registro"] = fecha_query
            
            pipeline = [
                {"$match": match_stage} if match_stage else {"$match": {}},
                {
                    "$facet": {
                        "totales": [
                            {
                                "$group": {
                                    "_id": None,
                                    "total": {"$sum": 1},
                                    "completados": {
                                        "$sum": {
                                            "$cond": [
                                                {"$eq": ["$tipo", "completado"]}, 
                                                1, 
                                                0
                                            ]
                                        }
                                    },
                                    "cancelados": {
                                        "$sum": {
                                            "$cond": [
                                                {"$eq": ["$tipo", "cancelado"]}, 
                                                1, 
                                                0
                                            ]
                                        }
                                    }
                                }
                            }
                        ],
                        "por_periodo": [
                            {
                                "$group": {
                                    "_id": "$periodo",
                                    "count": {"$sum": 1}
                                }
                            },
                            {"$sort": {"_id": -1}},
                            {"$limit": 12}
                        ],
                        "por_usuario": [
                            {
                                "$group": {
                                    "_id": "$usuario",
                                    "count": {"$sum": 1}
                                }
                            },
                            {"$sort": {"count": -1}},
                            {"$limit": 10}
                        ]
                    }
                }
            ]
            
            result = list(self.collection.aggregate(pipeline))[0]
            
            totales = result["totales"][0] if result["totales"] else {
                "total": 0,
                "completados": 0,
                "cancelados": 0
            }
            
            return {
                "total_registros": totales.get("total", 0),
                "total_completados": totales.get("completados", 0),
                "total_cancelados": totales.get("cancelados", 0),
                "por_periodo": [
                    {"periodo": item["_id"], "count": item["count"]}
                    for item in result["por_periodo"]
                ],
                "por_usuario": [
                    {"usuario": item["_id"], "count": item["count"]}
                    for item in result["por_usuario"]
                ]
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            raise
    
    def delete_historico(self, historico_id: str) -> bool:
        """
        Eliminar un registro histórico (usar con precaución)
        """
        try:
            if not ObjectId.is_valid(historico_id):
                raise ValueError("ID de histórico inválido")
            
            result = self.collection.delete_one({"_id": ObjectId(historico_id)})
            
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error al eliminar histórico: {str(e)}")
            raise