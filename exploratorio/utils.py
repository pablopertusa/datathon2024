import pandas as pd
from datetime import datetime


def limpiar_string_lote(string: str) -> int:
    if isinstance(string, str):
        if ',' in string:
            string = string.replace(',', '')
        if '/' in string:
            string = string.replace('/', '')
        if 'P' in string:
            string = string.replace('P', '') # hay un lote que tiene una P al principio
        return int(string)
    return string


def encontrar_subdataset_entre_fechas(df: pd.DataFrame, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    result = {}
    result['DateTime'] = df['DateTime'].map(lambda x: datetime.strptime(x[:-4], '%Y-%m-%d %H:%M:%S'))
    col_bool = (result['DateTime'] >= datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')) & (result['DateTime'] <= datetime.strptime(fecha_fin, '%Y-%m-%d %H:%M:%S'))
    return df[col_bool]