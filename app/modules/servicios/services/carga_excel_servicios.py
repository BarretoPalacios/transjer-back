# app/modules/servicios/services/carga_excel_funcional.py
import pandas as pd
from io import BytesIO
from typing import Dict, List, Any, Optional
from datetime import datetime, time
import logging
import json
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code

logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    """Encoder personalizado para manejar tipos especiales"""
    def default(self, obj):
        if pd.isna(obj):
            return None
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, pd.Series):
            return obj.tolist()
        return super().default(obj)

class CargaExcelFuncionalServicios:
    """
    Servicio funcional para cargar Excel sin errores de JSON
    """
    
    def __init__(self, db):
        self.db = db
        self.servicios_col = db["servicio_principal"]
    
    def cargar_excel_funcional(self, file_content: bytes) -> Dict[str, Any]:
        """
        Carga Excel de manera robusta
        """
        try:
            # 1. Leer Excel
            df = pd.read_excel(
                BytesIO(file_content),
                engine='openpyxl',
                dtype=str,  # Todo como string
                na_values=['', ' ', 'NA', 'N/A', 'null', 'NULL']
            )
            
            logger.info(f"📊 Excel leído: {len(df)} filas")
            
            # 2. Limpiar nombres de columnas
            df.columns = [self._limpiar_nombre_columna(col) for col in df.columns]
            
            # 3. Reemplazar NaN con None
            df = df.where(pd.notnull(df), None)
            
            # 4. Procesar filas
            return self._procesar_filas_funcional(df)
            
        except Exception as e:
            logger.error(f"❌ Error procesando Excel: {str(e)}", exc_info=True)
            raise
    
    def _limpiar_nombre_columna(self, nombre: str) -> str:
        """Limpia nombres de columnas"""
        if pd.isna(nombre):
            return "columna_sin_nombre"
        
        nombre_str = str(nombre).strip().lower()
        
        # Mapeo de nombres
        mapeo = {
            'cuenta': 'cuenta',
            'cliente': 'cliente',
            'm3 / tn': 'm3_tn',
            'm3/tn': 'm3_tn',
            'aux': 'aux',
            'zona': 'zona',
            'mes': 'mes',
            'proveedor': 'proveedor',
            'solicitud': 'solicitud',
            'tipo de servicio': 'tipo_servicio',
            'f. de servicio': 'fecha_servicio',
            'fecha de servicio': 'fecha_servicio',
            'f. de salida': 'fecha_salida',
            'fecha de salida': 'fecha_salida',
            'hora de cita': 'hora_cita',
            'placa': 'placa',
            'tipo de camion': 'tipo_camion',
            'tipo camion': 'tipo_camion',
            'cap.m3.': 'capacidad_m3',
            'cap. m3.': 'capacidad_m3',
            'capacidad m3': 'capacidad_m3',
            'tn.': 'capacidad_tn',
            'capacidad tn': 'capacidad_tn',
            'servicio': 'servicio',
            'conductor': 'conductor',
            'auxiliar': 'auxiliar',
            'origen': 'origen',
            'destino': 'destino',
            'grte': 'grte',
            'cliente destino': 'cliente_destino'
        }
        
        return mapeo.get(nombre_str, nombre_str.replace(' ', '_').replace('.', ''))
    
    def _procesar_filas_funcional(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Procesa filas de manera funcional"""
        servicios_creados = []
        errores = []
        
        total_filas = len(df)
        logger.info(f"🔧 Procesando {total_filas} filas...")
        
        for index in range(total_filas):
            fila_num = index + 2  # +2 por encabezado y base 1
            
            try:
                # Obtener fila como dict limpio
                fila = self._obtener_fila_limpia(df.iloc[index])
                
                # Saltar si está vacía
                if self._es_fila_vacia(fila):
                    logger.debug(f"Fila {fila_num} vacía, saltando...")
                    continue
                
                # Transformar
                servicio_data = self._transformar_fila_funcional(fila, fila_num)
                
                # Insertar
                resultado = self.servicios_col.insert_one(servicio_data)
                
                servicio_id = str(resultado.inserted_id)

                if servicio_data.get("estado") == "Completado":
                    flete_id = self._crear_flete_automatico(servicio_id, servicio_data)
                    if flete_id:
                        # Actualizar el servicio con referencia al flete
                        self.servicios_col.update_one(
                            {"_id": resultado.inserted_id},
                            {"$set": {"flete_asociado_id": flete_id}}
                        )

                servicios_creados.append({
                    "fila_excel": fila_num,
                    "servicio_id": str(resultado.inserted_id),
                    "codigo": servicio_data.get("codigo_servicio_principal"),
                    "cliente": servicio_data.get("cliente", {}).get("nombre", "Sin cliente")
                })
                
                # Log cada 10 filas
                if len(servicios_creados) % 10 == 0:
                    logger.info(f"✅ {len(servicios_creados)} servicios creados...")
                    
            except Exception as e:
                error_info = {
                    "fila_excel": fila_num,
                    "error": str(e),
                    "datos_fila": self._limpiar_para_json(fila) if 'fila' in locals() else {}
                }
                errores.append(error_info)
                logger.error(f"❌ Error fila {fila_num}: {str(e)[:100]}...")
        
        # Retornar resultado seguro para JSON
        return self._limpiar_resultado_json({
            "total_filas_excel": total_filas,
            "servicios_creados": len(servicios_creados),
            "errores": len(errores),
            "detalle_creados": servicios_creados[:5],  # Solo primeros 5
            "detalle_errores": errores[:5] if errores else []
        })
    
    def _obtener_fila_limpia(self, fila_series) -> Dict[str, Any]:
        """Convierte una serie de pandas a dict limpio"""
        fila_dict = {}
        for col, val in fila_series.items():
            if pd.isna(val) or val is None:
                fila_dict[col] = None
            else:
                # Convertir a string y limpiar
                fila_dict[col] = str(val).strip()
        return fila_dict
    
    def _es_fila_vacia(self, fila: Dict[str, Any]) -> bool:
        """Verifica si fila está vacía"""
        for val in fila.values():
            if val is not None and str(val).strip():
                return False
        return True
    
    def _transformar_fila_funcional(self, fila: Dict[str, Any], fila_num: int) -> Dict[str, Any]:
        """Transforma fila a servicio"""
        
        # Helper seguro
        def get_val(key, default=""):
            val = fila.get(key)
            return str(val).strip() if val is not None else default
        
        
        MAPEO_SOLICITUD = {
            "DIA": "Dia",
            "TARDE": "Tarde",
            "NOCHE": "Noche"
        }

        MAPEO_ZONA = {
            "LIMA": "Lima",
            "PROVINCIA": "Provincia"
        }
        solicitud_raw = get_val('solicitud', 'Día').upper()
        zona_raw = get_val('servicio', '').upper()

        # Extraer valores
        cuenta = get_val('cuenta', 'SIN_CUENTA')
        cliente = get_val('cliente', cuenta)
        m3_tn_raw = get_val('m3_tn', '0')
        # zona = get_val('servicio', '')
        zona = MAPEO_ZONA.get(zona_raw, zona_raw.capitalize())
        mes = get_val('mes', '')
        proveedor = get_val('proveedor', '')
        # solicitud = get_val('solicitud', 'Día')
        solicitud = MAPEO_SOLICITUD.get(solicitud_raw, solicitud_raw.capitalize())
        tipo_servicio = get_val('tipo_servicio', 'REGULAR')
        fecha_servicio_str = get_val('fecha_servicio')
        fecha_salida_str = get_val('fecha_salida')
        hora_cita_str = get_val('hora_cita')
        placa = get_val('placa', '').upper()
        tipo_camion = get_val('tipo_camion', '')
        capacidad_m3 = get_val('capacidad_m3', '0')
        capacidad_tn = get_val('capacidad_tn', '0')
        servicio_desc = get_val('servicio', '')
        conductor = get_val('conductor', '')
        auxiliar_nombre = get_val('auxiliar', get_val('aux', ''))
        origen = get_val('origen', '')
        destino = get_val('destino', '')
        cliente_destino = get_val('cliente_destino', '')
        grte = get_val('cliente_destino', '')
        
        # Procesar M3/TN
        m3, tn = self._procesar_m3_tn_funcional(m3_tn_raw, capacidad_m3, capacidad_tn)
        
        # Procesar fechas
        fecha_servicio = self._parsear_fecha_funcional(fecha_servicio_str)
        fecha_salida = self._parsear_fecha_funcional(fecha_salida_str) or fecha_servicio
        
        # Procesar hora
        hora_cita = self._parsear_hora_funcional(hora_cita_str)
        
        # Determinar mes
        if not mes and fecha_servicio:
            mes = fecha_servicio.strftime('%B')
        
        # Generar código
        codigo = f"HIST-{fecha_servicio.strftime('%Y%m') if fecha_servicio else 'NODATE'}-{fila_num:04d}"
        
        # Construir servicio
        servicio = {
            "codigo_servicio_principal": codigo,
            "m3": m3,
            "tn": tn,
            "mes": mes,
            "solicitud": solicitud,
            "tipo_servicio": tipo_servicio,
            "modalidad_servicio": self._determinar_modalidad(tipo_servicio),
            "zona": zona,
            "fecha_servicio": fecha_servicio,
            "fecha_salida": fecha_salida,
            "hora_cita": hora_cita,
            "gia_rr": None,
            "gia_rt": grte,
            "descripcion": servicio_desc or f"Servicio de {tipo_servicio}",
            "origen": origen,
            "destino": destino,
            "cliente_destino": cliente_destino if cliente_destino else None,
            "responsable": "sistema",
            "estado": "Completado",
            "servicio_cerrado": True,
            "es_editable": False,
            "es_eliminable": False,
            "pertenece_a_factura": False,
            "fecha_registro": datetime.now(),
            "fecha_ultima_modificacion": None,
            "fecha_completado": fecha_servicio,
            "fecha_cierre": fecha_servicio if fecha_servicio else None,
            "cuenta": {
                "nombre": cuenta,
                "direccion_origen": origen,
                "tipo_pago": "Contado",
                "dias_credito": 0,
                "limite_credito": 0,
                "estado": "activa",
                "es_principal": True,
                "nombre_conductor": conductor
            },
            "cliente": {
                "id": None,
                "nombre": cliente,
                "razon_social": cliente,
                "ruc": "",
                "numero_documento": ""
            },
            "proveedor": {
                "id": None,
                "nombre": proveedor,
                "razon_social": proveedor,
                "ruc": "",
                "numero_documento": ""
            },
            "flota": {
                "id": None,
                "placa": placa,
                "marca": "",
                "modelo": "",
                "tipo_vehiculo": tipo_camion.lower() if tipo_camion else "",
                "capacidad_m3": float(capacidad_m3) if capacidad_m3 and capacidad_m3.replace('.', '', 1).isdigit() else 0,
                "nombre": placa or "Sin placa",
                "nombre_conductor": conductor
            },
            "conductor": [],
            "auxiliar": [],
            "historial_estados": [
    {
        "estado_anterior": "Programado",  # ← AGREGAR
        "estado_nuevo": "Completado",     # ← AGREGAR
        "estado": "Completado",           # ← MANTENER si lo necesitas
        "fecha": fecha_servicio or datetime.now(),
        "usuario": "sistema",
        "justificacion": f"Importado desde Excel - Fila {fila_num}"
    }
],
            "metadata_origen": {
                "importado_desde": "excel_historico",
                "fila_excel": fila_num,
                "fecha_importacion": datetime.now(),
                "cliente_original": cliente,
                "proveedor_original": proveedor
            }
        }
        
        # Agregar conductor
        if conductor:
            servicio["conductor"].append({
                "id": None,
                "nombres_completos": conductor,
                "nombres": conductor,
                "dni": "",
                "licencia_conducir": "",
                "tipo": "Conductor",
                "nombre": conductor,
                "licencia": ""
            })
        
        # Agregar auxiliar
        if auxiliar_nombre:
            servicio["auxiliar"].append({
                "id": None,
                "nombres_completos": auxiliar_nombre,
                "dni": "",
                "tipo": "Auxiliar",
                "nombre": auxiliar_nombre
            })
        
        return servicio
    
    def _procesar_m3_tn_funcional(self, m3_tn_raw: str, cap_m3: str, cap_tn: str) -> tuple:
        """Procesa M3/TN"""
        m3 = "0"
        tn = "0"
        
        if m3_tn_raw:
            if '/' in m3_tn_raw:
                partes = m3_tn_raw.split('/')
                m3 = partes[0].strip() if len(partes) > 0 else "0"
                tn = partes[1].strip() if len(partes) > 1 else "0"
            elif m3_tn_raw.replace('.', '', 1).isdigit():
                m3 = m3_tn_raw
        
        # Usar capacidades si están disponibles
        if m3 == "0" and cap_m3 and cap_m3 != "0":
            m3 = cap_m3
        if tn == "0" and cap_tn and cap_tn != "0":
            tn = cap_tn
        
        return m3, tn
    
    def _parsear_fecha_funcional(self, fecha_str: Optional[str]) -> Optional[datetime]:
        """Parse fecha segura"""
        if not fecha_str:
            return datetime.now()
        
        try:
            # Si ya es datetime
            if isinstance(fecha_str, (datetime, pd.Timestamp)):
                return fecha_str if isinstance(fecha_str, datetime) else fecha_str.to_pydatetime()
            
            fecha_limpia = str(fecha_str).strip()
            
            # Lista de formatos
            formatos = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%d-%m-%Y",
                "%Y/%m/%d",
                "%d %b %Y",
                "%d %B %Y",
                "%Y%m%d"
            ]
            
            for fmt in formatos:
                try:
                    return datetime.strptime(fecha_limpia, fmt)
                except:
                    continue
            
            # Último intento
            try:
                return pd.to_datetime(fecha_limpia).to_pydatetime()
            except:
                return datetime.now()
                
        except Exception as e:
            logger.warning(f"No se pudo parsear fecha '{fecha_str}': {e}")
            return datetime.now()
    
    def _parsear_hora_funcional(self, hora_str: Optional[str]) -> Optional[datetime]:
        """Parse hora segura"""
        if not hora_str:
            return None
        
        try:
            hora_limpia = str(hora_str).strip().upper()
            
            formatos = [
                "%H:%M:%S",
                "%H:%M",
                "%I:%M:%S %p",
                "%I:%M %p",
                "%H.%M",
                "%I.%M %p"
            ]
            
            ahora = datetime.now() 
    
            for fmt in formatos:
                try:
                    # 2. Convertir el texto a objeto datetime
                    dt = datetime.strptime(hora_limpia, fmt)
                    
                    # 3. Retornar la fecha de HOY con la HORA del texto
                    return ahora.replace(
                        hour=dt.hour, 
                        minute=dt.minute, 
                        second=dt.second if dt.second else 0,
                        microsecond=0
                    )
                except ValueError:
                    continue
                    
            return None
        except Exception:
            return None
    
    def _determinar_modalidad(self, tipo_servicio: str) -> str:
        """Determina modalidad"""
        tipo = tipo_servicio.upper()
        
        if "LOCAL" in tipo:
            return "LOCAL"
        elif "EXPRESS" in tipo:
            return "EXPRESS"
        elif "FRIGORIFICO" in tipo or "REFRIGERADO" in tipo:
            return "FRIGORIFICO"
        elif "PELIGROSO" in tipo:
            return "PELIGROSO"
        
        return "REGULAR"
    
    def _limpiar_para_json(self, obj):
        """Limpia objeto para JSON"""
        if isinstance(obj, dict):
            return {k: self._limpiar_para_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._limpiar_para_json(v) for v in obj]
        elif pd.isna(obj):
            return None
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        else:
            return obj
    
    def _limpiar_resultado_json(self, resultado: Dict[str, Any]) -> Dict[str, Any]:
        """Limpia resultado final para JSON"""
        return json.loads(json.dumps(resultado, cls=JSONEncoder, ensure_ascii=False))

    # En app/modules/servicios/services/carga_excel_funcional.py
# Agrega este método a la clase CargaExcelFuncionalServicios:

    def _crear_flete_automatico(self, servicio_id: str, servicio_data: Dict[str, Any]) -> Optional[str]:
        """
        Crea un flete automático para un servicio importado
        """
        try:
            # Generar código de flete
            codigo_flete = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.db["fletes"],
                sequence_name="fletes",
                field_name="codigo_flete",
                prefix="FLT-",
                length=10
            )
            
            flete_data = {
                "codigo_flete": codigo_flete,
                "servicio_id": servicio_id,
                "codigo_servicio": servicio_data.get("codigo_servicio_principal", ""),
                "estado_flete": "PENDIENTE",
                "monto_flete": 0.0,
                "pertenece_a_factura": False,
                "factura_id": None,
                "codigo_factura": None,
                "fecha_pago": None,
                "observaciones": f"Generado automáticamente para servicio histórico {servicio_data.get('codigo_servicio_principal', '')}",
                "fecha_creacion": datetime.now(),
                "fecha_actualizacion": None,
                "usuario_creador": "sistema_importacion"
            }
            
            result = self.db["fletes"].insert_one(flete_data)
            logger.info(f"✅ Flete creado: {codigo_flete} para servicio {servicio_data.get('codigo_servicio_principal')}")
            
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"❌ Error creando flete para servicio {servicio_id}: {str(e)}")
            return None