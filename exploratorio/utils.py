import pandas as pd
from datetime import datetime
import numpy as np


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


def buscar_registros_biorreactor(df_biorreactores: pd.DataFrame, fecha_inicio: str, fecha_fin: str) -> list:
    resul = []
    subdataset = encontrar_subdataset_entre_fechas(df_biorreactores, fecha_inicio, fecha_fin)
    cols = list(subdataset.columns)[1:] # quitamos la columna DateTime porque no nos interesa
    for _, row in subdataset.iterrows():
        insert = []
        for col in cols:
            insert.append(row[col])
        resul.append(insert)
    return resul

def buscar_registros_biorreactor_por_id(info: dict, biorreactor: int, fecha_inicio: str, fecha_fin: str, n_registros: int) -> list:
    df_bio = info['biorreactores'][str(biorreactor)]
    resul = buscar_registros_biorreactor(df_bio, fecha_inicio, fecha_fin)
    if len(resul) > n_registros:
        raise ValueError('El subdataset contiene demasiados registros') # n_registros es el número máximo de registros que puede tener un lote, es decir, el tiempo máximo que ha estado cualquier lote en un reactor
    
    añadir_ceros = n_registros - len(resul)
    for i in range(añadir_ceros):
        resul.append(list(np.zeros(len(df_bio.columns)-1)))
    
    return resul