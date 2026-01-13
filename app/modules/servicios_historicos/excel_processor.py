# app/services/excel_processor.py - Versión corregida para fechas

import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Any
from io import BytesIO

from app.modules.servicios_historicos.models import ErrorDetalle


class ExcelProcessor:
    """
    Servicio para procesar archivos Excel de servicios de transporte.
    Versión corregida para extraer correctamente las fechas.
    """
    
    def __init__(self, archivo_nombre: str, usuario_carga: str = None):
        self.archivo_nombre = archivo_nombre
        self.usuario_carga = usuario_carga
        self.errores: List[ErrorDetalle] = []
        self.advertencias: List[ErrorDetalle] = []
    
    def procesar_excel(self, contenido: bytes) -> Tuple[List[Dict], List[ErrorDetalle], List[ErrorDetalle]]:
        """
        Procesa el archivo Excel y retorna registros limpios y errores
        """
        try:
            # Leer Excel preservando tipos de datos
            df = pd.read_excel(BytesIO(contenido), engine='openpyxl')
            
            print(f"📊 Excel cargado: {len(df)} filas, {len(df.columns)} columnas")
            
            # DEBUG: Mostrar información sobre las columnas
            print("\n🔍 ANALISIS DE COLUMNAS:")
            for i, col in enumerate(df.columns):
                # Mostrar primeros valores no nulos
                valores_no_nulos = df[col].dropna()
                if len(valores_no_nulos) > 0:
                    ejemplo = valores_no_nulos.iloc[0]
                    tipo = type(ejemplo).__name__
                    print(f"  {i+1:2d}. {col:30} -> Tipo: {tipo:15} | Ejemplo: {ejemplo}")
            
            # Identificar columnas de fecha automáticamente
            columnas_fecha = self._identificar_columnas_fecha(df)
            print(f"\n📅 Columnas identificadas como fechas: {columnas_fecha}")
            
            # Normalizar nombres de columnas
            df_original_columns = list(df.columns)
            df.columns = self._normalizar_columnas(df.columns)
            
            print(f"\n🔄 Columnas normalizadas: {list(df.columns)}")
            
            # Convertir columnas de fecha a datetime
            for col in df.columns:
                if 'fecha' in col.lower() and col in columnas_fecha:
                    print(f"  Convirtiendo columna '{col}' a datetime...")
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            
            registros_limpios = []
            
            for idx, row in df.iterrows():
                try:
                    registro = self._procesar_fila(row, idx + 2)
                    
                    # Validar registro
                    advertencias_validacion = self._validar_registro(registro, idx + 2)
                    if advertencias_validacion:
                        self.advertencias.extend(advertencias_validacion)
                    
                    registros_limpios.append(registro)
                    
                except Exception as e:
                    error_msg = f"Error en fila {idx + 2}: {str(e)[:100]}"
                    print(f"❌ {error_msg}")
                    self.advertencias.append(ErrorDetalle(
                        fila=idx + 2,
                        mensaje=error_msg,
                        tipo="ADVERTENCIA"
                    ))
                    
                    # Crear registro mínimo
                    registro_minimo = self._crear_registro_minimo(row, idx + 2)
                    registros_limpios.append(registro_minimo)
            
            print(f"\n✅ Resultado: {len(registros_limpios)} registros procesados, {len(self.advertencias)} advertencias")
            
            # DEBUG: Mostrar ejemplo de registro procesado
            if registros_limpios:
                print(f"\n📋 Ejemplo de registro procesado (fila 1):")
                ejemplo = registros_limpios[0]
                print(f"  Cuenta: {ejemplo.get('cuenta')}")
                print(f"  Cliente: {ejemplo.get('cliente')}")
                print(f"  Fecha Servicio: {ejemplo.get('fecha_servicio')}")
                print(f"  Fecha Salida: {ejemplo.get('fecha_salida')}")
                print(f"  Placa: {ejemplo.get('placa')}")
                print(f"  Conductor: {ejemplo.get('conductor')}")
            
            return registros_limpios, self.errores, self.advertencias
            
        except Exception as e:
            raise ValueError(f"Error al leer el archivo Excel: {str(e)}")
    
    def _identificar_columnas_fecha(self, df):
        """Identifica automáticamente columnas que contienen fechas"""
        columnas_fecha = []
        
        for col in df.columns:
            # Verificar si el nombre sugiere que es fecha
            col_lower = str(col).lower()
            if any(patron in col_lower for patron in ['fecha', 'f.', 'date', 'dia', 'hora']):
                # Verificar contenido de la columna
                try:
                    # Tomar muestra de valores no nulos
                    muestra = df[col].dropna().head(5)
                    if len(muestra) > 0:
                        # Intentar convertir a datetime
                        for valor in muestra:
                            # Si es string que parece fecha
                            if isinstance(valor, str):
                                if any(sep in valor for sep in ['/', '-', '.']) and any(str(year) in valor for year in range(2020, 2030)):
                                    columnas_fecha.append(col)
                                    break
                            # Si ya es datetime o timestamp
                            elif isinstance(valor, (datetime, pd.Timestamp)):
                                columnas_fecha.append(col)
                                break
                except:
                    continue
        
        return columnas_fecha
    
    def _normalizar_columnas(self, columnas) -> List[str]:
        """Normaliza nombres de columnas a formato estándar"""
        # Primero hacer una pasada para contar cuántas columnas "CLIENTE" hay
        columnas_str = [str(col).strip().upper() for col in columnas]
        
        # Contar ocurrencias de "CLIENTE"
        cliente_indices = [i for i, col in enumerate(columnas_str) if 'CLIENTE' in col]
        
        columnas_normalizadas = []
        
        for i, col_str in enumerate(columnas_str):
            # Para columnas CLIENTE, distinguir entre primera y segunda
            if 'CLIENTE' in col_str:
                if i == cliente_indices[0]:  # Primer cliente
                    columnas_normalizadas.append('cliente')
                elif len(cliente_indices) > 1 and i == cliente_indices[1]:  # Segundo cliente
                    columnas_normalizadas.append('cliente_destino')
                else:
                    columnas_normalizadas.append('cliente')
            
            # Mapeo para columnas de fecha
            elif any(x in col_str for x in ['FECHA SERVICIO', 'F. SERVICIO', 'FECHA DE SERVICIO',"F. DE SERVICIO"]):
                columnas_normalizadas.append('fecha_servicio')
            elif any(x in col_str for x in ['FECHA SALIDA', 'F. SALIDA', 'FECHA DE SALIDA', 'F. DE SALIDA']):
                columnas_normalizadas.append('fecha_salida')
            elif any(x in col_str for x in ['FECHA EMISION', 'F. EMISION', 'FECHA DE EMISION']):
                columnas_normalizadas.append('fecha_emision')
            
            # Mapeo para otras columnas
            elif col_str == 'CUENTA':
                columnas_normalizadas.append('cuenta')
            elif col_str in ['M3 / TN', 'M3/TN', 'M3', 'TN', 'M3 TN']:
                columnas_normalizadas.append('m3_tn')
            elif col_str == 'AUX':
                columnas_normalizadas.append('aux')
            elif col_str == 'ZONA':
                columnas_normalizadas.append('zona')
            elif col_str == 'MES':
                columnas_normalizadas.append('mes')
            elif 'PROVEEDOR' in col_str:
                columnas_normalizadas.append('proveedor')
            elif 'SOLICITUD' in col_str:
                columnas_normalizadas.append('solicitud')
            elif 'TIPO' in col_str and 'SERVICIO' in col_str:
                columnas_normalizadas.append('tipo_servicio')
            elif 'HORA' in col_str:
                columnas_normalizadas.append('hora_cita')
            elif 'PLACA' in col_str:
                columnas_normalizadas.append('placa')
            elif 'CAMION' in col_str:
                columnas_normalizadas.append('tipo_camion')
            elif any(x in col_str for x in ['CAP. M3', 'CAP M3', 'CAPACIDAD M3']):
                columnas_normalizadas.append('capacidad_m3')
            elif any(x in col_str for x in ['TN.', 'CAP. TN.', 'CAPACIDAD TN']):
                columnas_normalizadas.append('capacidad_tn')
            elif col_str == 'SERVICIO':
                columnas_normalizadas.append('servicio')
            elif 'CONDUCTOR' in col_str:
                columnas_normalizadas.append('conductor')
            elif 'AUXILIAR' in col_str:
                columnas_normalizadas.append('auxiliar')
            elif 'ORIGEN' in col_str:
                columnas_normalizadas.append('origen')
            elif 'DESTINO' in col_str:
                columnas_normalizadas.append('destino')
            elif 'GRTE' in col_str:
                columnas_normalizadas.append('grte')
            elif 'FACTURA' in col_str:
                columnas_normalizadas.append('factura_numero')
            elif 'ESTADO' in col_str:
                columnas_normalizadas.append('estado_factura')
            else:
                columnas_normalizadas.append(col_str.lower().replace(' ', '_').replace('.', ''))
        
        return columnas_normalizadas
    
    def _procesar_fila(self, row, numero_fila: int) -> Dict[str, Any]:
        """Procesa una fila del Excel y la convierte en un registro"""
        
        # DEBUG: Verificar valores de fecha en esta fila
        print(f"\n🔍 Procesando fila {numero_fila}:")
        
        # EXTRAER FECHAS DIRECTAMENTE - SIN FALLBACK
        fecha_servicio = self._extraer_fecha_directamente(row, 'fecha_servicio', numero_fila)
        fecha_salida = self._extraer_fecha_directamente(row, 'fecha_salida', numero_fila)
        fecha_emision = self._extraer_fecha_directamente(row, 'fecha_emision', numero_fila)
        
        print(f"  Fecha Servicio: {fecha_servicio}")
        print(f"  Fecha Salida: {fecha_salida}")
        print(f"  Fecha Emisión: {fecha_emision}")
        
        # Determinar estado de factura
        factura_data = self._procesar_factura(row, fecha_emision)
        
        # Determinar estado del servicio
        estado_servicio = self._determinar_estado_servicio(fecha_servicio, factura_data)
        
        # Generar observaciones
        observaciones = self._generar_observaciones(row, fecha_servicio)
        
        # Procesar todos los campos
        registro = {
            # Información básica
            "cuenta": self._limpiar_texto(row.get('cuenta')) or "DESCONOCIDO",
            "cliente": self._limpiar_texto(row.get('cliente')) or "DESCONOCIDO",
            "m3_tn": self._convertir_numero(row.get('m3_tn')),
            "aux": self._convertir_entero(row.get('aux')),
            "zona": self._limpiar_texto(row.get('zona')),
            "mes": self._limpiar_texto(row.get('mes')) or self._obtener_mes_de_fecha(fecha_servicio),
            "proveedor": self._limpiar_texto(row.get('proveedor')) or "DESCONOCIDO",
            
            # Solicitud
            "solicitud": self._limpiar_texto(row.get('solicitud')),
            "tipo_servicio": self._limpiar_texto(row.get('tipo_servicio')) or "FLETE",
            
            # FECHAS - usar las extraídas directamente
            "fecha_servicio": fecha_servicio,
            "fecha_salida": fecha_salida,
            "hora_cita": self._limpiar_texto(row.get('hora_cita')),
            
            # Transporte
            "placa": self._limpiar_texto(row.get('placa')) or "SIN PLACA",
            "tipo_camion": self._limpiar_texto(row.get('tipo_camion')) or "FURGON",
            "capacidad_m3": self._convertir_numero(row.get('capacidad_m3')),
            "capacidad_tn": self._convertir_numero(row.get('capacidad_tn')),
            
            # Servicio
            "servicio": self._limpiar_texto(row.get('servicio')) or "SIN SERVICIO",
            "servicio_descripcion": self._limpiar_texto(row.get('servicio')),
            
            # Personal
            "conductor": self._limpiar_texto(row.get('conductor')) or "DESCONOCIDO",
            "auxiliar": self._limpiar_texto(row.get('auxiliar')),
            
            # Ubicaciones
            "origen": self._limpiar_texto(row.get('origen')) or "SIN ORIGEN",
            "destino": self._limpiar_texto(row.get('destino')) or "SIN DESTINO",
            
            # Cliente destino
            "cliente_destino": self._limpiar_texto(row.get('cliente_destino')),
            "cliente_grte": self._limpiar_texto(row.get('cliente_grte')),
            "grte": self._limpiar_texto(row.get('grte')) or "SIN GRTE",
            
            # Factura
            "factura": factura_data,
            
            # Estado servicio
            "estado_servicio": estado_servicio,
            
            # Metadata
            "metadata": {
                "archivo_origen": self.archivo_nombre,
                "fecha_carga": datetime.utcnow(),
                "usuario_carga": self.usuario_carga,
                "fila_original": numero_fila,
                "observaciones": observaciones
            },
            
            "historial_cambios": [],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        return registro
    
    def _extraer_fecha_directamente(self, row, campo: str, fila: int):
        """Extrae fecha directamente del DataFrame ya convertido"""
        valor = row.get(campo)
        
        if pd.isna(valor) or valor is None:
            print(f"    ⚠️  Campo '{campo}' está vacío o es NaN")
            return None
        
        print(f"    🔍 Campo '{campo}': valor={valor}, tipo={type(valor).__name__}")
        
        # Si ya es datetime
        if isinstance(valor, datetime):
            return valor
        
        # Si es pd.Timestamp
        if isinstance(valor, pd.Timestamp):
            return valor.to_pydatetime()
        
        # Si es string, intentar parsear
        if isinstance(valor, str):
            valor = valor.strip()
            if not valor:
                return None
            
            formatos = [
                '%d/%m/%Y',    # 31/12/2024
                '%d-%m-%Y',    # 31-12-2024
                '%Y-%m-%d',    # 2024-12-31
                '%Y/%m/%d',    # 2024/12/31
                '%d/%m/%y',    # 31/12/24
                '%d.%m.%Y',    # 31.12.2024
                '%m/%d/%Y',    # 12/31/2024
            ]
            
            for formato in formatos:
                try:
                    fecha = datetime.strptime(valor, formato)
                    print(f"    ✅ Parseado como '{formato}': {fecha}")
                    return fecha
                except:
                    continue
        
        # Si es número (fecha Excel)
        if isinstance(valor, (int, float)):
            try:
                # Convertir desde número serial de Excel
                fecha = pd.Timestamp('1899-12-30') + pd.Timedelta(days=float(valor))
                print(f"    ✅ Convertido desde número Excel: {fecha}")
                return fecha.to_pydatetime()
            except Exception as e:
                print(f"    ❌ Error convirtiendo número Excel: {e}")
        
        print(f"    ❌ No se pudo convertir fecha: {valor} (tipo: {type(valor)})")
        return None
    
    def _obtener_mes_de_fecha(self, fecha: datetime):
        """Obtiene el nombre del mes en español"""
        if not fecha:
            return None
        
        meses_es = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        return meses_es.get(fecha.month)
    
    def _crear_registro_minimo(self, row, numero_fila: int) -> Dict[str, Any]:
        """Crea un registro mínimo cuando hay errores"""
        
        # Extraer fechas
        fecha_servicio = self._extraer_fecha_directamente(row, 'fecha_servicio', numero_fila)
        fecha_salida = self._extraer_fecha_directamente(row, 'fecha_salida', numero_fila)
        fecha_emision = self._extraer_fecha_directamente(row, 'fecha_emision', numero_fila)
        
        return {
            "cuenta": self._limpiar_texto(row.get('cuenta')) or "DESCONOCIDO",
            "cliente": self._limpiar_texto(row.get('cliente')) or "DESCONOCIDO",
            "m3_tn": self._convertir_numero(row.get('m3_tn')),
            "aux": self._convertir_entero(row.get('aux')),
            "mes": self._limpiar_texto(row.get('mes')) or self._obtener_mes_de_fecha(fecha_servicio),
            "proveedor": self._limpiar_texto(row.get('proveedor')) or "DESCONOCIDO",
            
            # FECHAS REALES
            "fecha_servicio": fecha_servicio,
            "fecha_salida": fecha_salida,
            
            # Transporte
            "placa": self._limpiar_texto(row.get('placa')) or "SIN PLACA",
            "tipo_camion": self._limpiar_texto(row.get('tipo_camion')) or "FURGON",
            
            # Servicio
            "servicio": self._limpiar_texto(row.get('servicio')) or "SIN SERVICIO",
            
            # Personal
            "conductor": self._limpiar_texto(row.get('conductor')) or "DESCONOCIDO",
            "auxiliar": self._limpiar_texto(row.get('auxiliar')),
            
            # Ubicaciones
            "origen": self._limpiar_texto(row.get('origen')) or "SIN ORIGEN",
            "destino": self._limpiar_texto(row.get('destino')) or "SIN DESTINO",
            
            # Factura
            "factura": {
                "numero": self._limpiar_texto(row.get('factura_numero')),
                "fecha_emision": fecha_emision,
                "estado": "PENDIENTE",
                "monto": None,
                "moneda": "PEN"
            },
            
            "estado_servicio": "PROGRAMADO",
            "metadata": {
                "archivo_origen": self.archivo_nombre,
                "fecha_carga": datetime.utcnow(),
                "usuario_carga": self.usuario_carga,
                "fila_original": numero_fila,
                "observaciones": "Registro procesado con error parcial"
            },
            "historial_cambios": [],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    def _procesar_factura(self, row, fecha_emision) -> Dict[str, Any]:
        """Procesa los datos de factura"""
        numero_factura = self._limpiar_texto(row.get('factura_numero'))
        estado_raw = self._limpiar_texto(row.get('estado_factura'))
        
        # Determinar estado
        if not numero_factura and not fecha_emision:
            estado = "PENDIENTE"
        elif numero_factura and fecha_emision and not estado_raw:
            estado = "EMITIDA"
        elif estado_raw:
            estado_map = {
                'FACTURADO': 'FACTURADO',
                'PENDIENTE': 'PENDIENTE',
                'EMITIDA': 'EMITIDA',
                'POR COBRAR': 'POR_COBRAR',
                'POR_COBRAR': 'POR_COBRAR',
                'OBSERVADO': 'OBSERVADO',
                'ANULADO': 'ANULADO'
            }
            estado = estado_map.get(estado_raw.upper(), 'PENDIENTE')
        else:
            estado = "PENDIENTE"
        
        return {
            "numero": numero_factura,
            "fecha_emision": fecha_emision,
            "estado": estado,
            "monto": None,
            "moneda": "PEN"
        }
    
    def _determinar_estado_servicio(self, fecha_servicio: datetime, factura_data: Dict) -> str:
        """Determina el estado del servicio"""
        if not fecha_servicio:
            return "PROGRAMADO"
        
        if factura_data.get('numero'):
            return "COMPLETADO"
        
        if fecha_servicio and fecha_servicio < datetime.now():
            return "PENDIENTE_FACTURACION"
        
        return "PROGRAMADO"
    
    def _validar_registro(self, registro: Dict, fila: int) -> List[ErrorDetalle]:
        """Valida un registro y retorna advertencias"""
        advertencias = [] 
        
        if not registro.get('fecha_servicio'):
            advertencias.append(ErrorDetalle(
                fila=fila,
                campo='fecha_servicio',
                mensaje='Fecha de servicio no especificada',
                tipo='ADVERTENCIA'
            ))
        
        return advertencias
    
    def _generar_observaciones(self, row, fecha_servicio) -> str:
        """Genera observaciones"""
        observaciones = []
        
        if not fecha_servicio:
            observaciones.append("Fecha de servicio no especificada")
        
        return '; '.join(observaciones) if observaciones else "Registro procesado correctamente"
    
    # ========== FUNCIONES AUXILIARES ==========
    
    def _limpiar_texto(self, valor) -> str:
        """Limpia y normaliza texto"""
        if valor is None or pd.isna(valor):
            return None
        
        if isinstance(valor, (int, float)):
            if valor == int(valor):
                return str(int(valor))
            return str(valor)
        
        valor_str = str(valor).strip()
        return valor_str if valor_str else None
    
    def _convertir_numero(self, valor) -> float:
        """Convierte a número float"""
        if valor is None or pd.isna(valor):
            return None
        
        try:
            if isinstance(valor, str):
                valor = valor.strip().replace(',', '.')
            return float(valor)
        except:
            return None
    
    def _convertir_entero(self, valor) -> int:
        """Convierte a número entero"""
        if valor is None or pd.isna(valor):
            return None
        
        try:
            if isinstance(valor, str):
                valor = valor.replace(',', '.')
            num_float = float(valor)
            return int(num_float)
        except:
            return None