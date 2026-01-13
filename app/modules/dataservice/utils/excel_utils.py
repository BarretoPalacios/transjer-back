import pandas as pd
from io import BytesIO
from typing import List, Dict, Any
from datetime import datetime

class ExcelUtils:
    @staticmethod
    def create_excel(data: List[Dict[str, Any]], sheet_name: str = "Data") -> BytesIO:
        """
        Crea un archivo Excel en memoria a partir de una lista de diccionarios
        """
        df = pd.DataFrame(data)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        output.seek(0)
        return output
    
    @staticmethod
    def read_excel(file_content: bytes) -> pd.DataFrame:
        """
        Lee un archivo Excel desde bytes
        """
        return pd.read_excel(BytesIO(file_content))
    
    @staticmethod
    def format_date(value) -> str:
        """
        Formatea fechas para Excel
        """
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value) if value is not None else ""