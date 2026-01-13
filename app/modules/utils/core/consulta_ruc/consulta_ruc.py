import requests
from typing import Dict, List, Optional


class RucService:
    def __init__(self):
        self.tokens = [
            'sk_12523.QJKV81AThlaFxoAJC1LVCrXwyEr5Xtet',
            'sk_12527.6EK5Kpv3e3Hkds3iyJDc8rGsVHao65Kz'
        ]
        self.current_token_index = 0
        self.token_usage = [0, 0]
        self.max_requests_per_token = 100
        self.base_url = 'https://api.decolecta.com/v1/sunat/ruc'

    def get_current_token(self) -> str:
        if self.token_usage[self.current_token_index] >= self.max_requests_per_token:
            self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
            
            if self.token_usage[self.current_token_index] >= self.max_requests_per_token:
                raise Exception('Todos los tokens han alcanzado el límite de peticiones')
        
        return self.tokens[self.current_token_index]

    def increment_token_usage(self):
        self.token_usage[self.current_token_index] += 1

    def reset_counters(self):
        self.token_usage = [0, 0]
        self.current_token_index = 0

    def get_tokens_status(self) -> List[Dict]:
        return [
            {
                'token': token[:10] + '...',
                'usado': self.token_usage[index],
                'restante': self.max_requests_per_token - self.token_usage[index],
                'activo': index == self.current_token_index
            }
            for index, token in enumerate(self.tokens)
        ]

    def consultar_ruc(self, ruc: str) -> Dict:
        if not ruc or not ruc.isdigit() or len(ruc) != 11:
            return {
                'success': False,
                'error': 'RUC inválido. Debe contener 11 dígitos',
                'ruc': ruc
            }

        try:
            token = self.get_current_token()
            url = f'{self.base_url}?numero={ruc}&token={token}'

            response = requests.get(
                url,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {token}'
                },
                timeout=30
            )

            if response.status_code == 429:
                self.token_usage[self.current_token_index] = self.max_requests_per_token
                return self.consultar_ruc(ruc)

            response.raise_for_status()
            data = response.json()
            self.increment_token_usage()

            return {
                'success': True,
                'data': data,
                'token_utilizado': self.current_token_index + 1,
                'peticiones_restantes': self.max_requests_per_token - self.token_usage[self.current_token_index]
            }

        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': str(e),
                'ruc': ruc
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'ruc': ruc
            }

    def consultar_ruc_lote(self, rucs: List[str]) -> List[Dict]:
        return [self.consultar_ruc(ruc) for ruc in rucs]


# ruc_service = RucService()


# if __name__ == '__main__':
#     resultado = ruc_service.consultar_ruc('20601030013')
    
#     if resultado['success']:
#         print('✅ Consulta exitosa:')
#         print(f"Razón Social: {resultado['data']['razon_social']}")
#         print(f"Estado: {resultado['data']['estado']}")
#         print(f"Condición: {resultado['data']['condicion']}")
#         print(f"Dirección: {resultado['data']['direccion']}")
#         print(f"\nToken utilizado: {resultado['token_utilizado']}")
#         print(f"Peticiones restantes: {resultado['peticiones_restantes']}")
#     else:
#         print(f"❌ Error: {resultado['error']}")
    
#     print('\n📊 Estado de tokens:')
#     for status in ruc_service.get_tokens_status():
#         print(status)