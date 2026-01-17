# import os
import requests
from typing import Dict, Optional
from app.core.config import settings


class RucService:
    def __init__(self):
        # El token se lee desde el archivo .env
        self.token = settings.WEBNOVA_TOKEN
        self.base_url = 'https://api.webnova.pe/api/v1/buscar'

    def consultar_ruc(self, ruc: str) -> Dict:
        """
        Consulta un RUC individual en la API de Webnova.
        """
        # 1. Validaciones básicas de entrada
        if not ruc or not ruc.isdigit() or len(ruc) != 11:
            return {
                'success': False,
                'error': 'RUC inválido. Debe contener 11 dígitos numéricos.',
                'ruc': ruc
            }

        if not self.token:
            return {'success': False, 'error': 'Token no configurado en el archivo .env'}

        try:
            # 2. Configuración de la petición
            url = f'{self.base_url}/{ruc}'
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }

            # 3. Ejecución de la consulta
            response = requests.get(url, headers=headers, timeout=15)
            
            # Manejo específico de errores HTTP comunes
            if response.status_code == 401:
                return {'success': False, 'error': 'Token no válido o expirado'}
            if response.status_code == 404:
                return {'success': False, 'error': 'RUC no encontrado'}
            if response.status_code == 429:
                return {'success': False, 'error': 'Límite de peticiones alcanzado (Rate Limit)'}

            response.raise_for_status()
            data = response.json()

            return {
                'success': True,
                'data': data
            }

        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': f'Error de conexión: {str(e)}',
                'ruc': ruc
            }