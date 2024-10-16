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


def encontrar_subdataset_entre_fechas(df: pd.DataFrame, fecha_inicio: str, fecha_fin: str, esBiorreactor: bool) -> pd.DataFrame:
    if fecha_inicio == 'NaN' or fecha_fin == 'NaN' or fecha_inicio == 'NaT' or fecha_fin == 'NaT':
        return
    result = {}
    if esBiorreactor:
        result['DateTime'] = df['DateTime'].map(lambda x: datetime.strptime(x[:-4], '%Y-%m-%d %H:%M:%S'))
        col_bool = (result['DateTime'] >= datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')) & (result['DateTime'] <= datetime.strptime(fecha_fin, '%Y-%m-%d %H:%M:%S'))
    else:
        result['DateTime'] = df['DateTime'].map(lambda x: datetime.strptime(x[:-4], '%Y-%m-%d %H:%M:%S'))
        col_bool = (result['DateTime'] >= datetime.strptime(fecha_inicio, '%m-%d-%Y %H:%M:%S')) & (result['DateTime'] <= datetime.strptime(fecha_fin, '%m-%d-%Y %H:%M:%S'))
    return df[col_bool]


def buscar_registros(df_biorreactores: pd.DataFrame, fecha_inicio: str, fecha_fin: str, id_biorreactor: str, esBiorreactor: bool) -> list:
    resul = []
    subdataset = encontrar_subdataset_entre_fechas(df_biorreactores, fecha_inicio, fecha_fin, esBiorreactor)
    if subdataset is None:
        return
    if esBiorreactor:
        cols = ['FERM0101.Agitation_PV',
        'FERM0101.Air_Sparge_PV',
        'FERM0101.Biocontainer_Pressure_PV', 'FERM0101.DO_1_PV',
        'FERM0101.DO_2_PV', 'FERM0101.Gas_Overlay_PV',
        'FERM0101.Load_Cell_Net_PV', 'FERM0101.pH_1_PV',
        'FERM0101.pH_2_PV', 'FERM0101.PUMP_1_PV',
        'FERM0101.PUMP_1_TOTAL', 'FERM0101.PUMP_2_PV',
        'FERM0101.PUMP_2_TOTAL', 'FERM0101.Single_Use_DO_PV',
        'FERM0101.Single_Use_pH_PV', 'FERM0101.Temperatura_PV'] # quitamos la columna DateTime porque no nos interesa y fijamos el orden en el que se acceden porque no todos los biorreactores tienen el mismo orden en las columnas                  
    else:
        cols = ['CTF0101.EN_Parcial', 'CTF0101.EN_Total',
       'D01780551.PV', 'D01906041.PV', 'D01916047.PV',
       'D01916503.PV', 'D01919022.PV']
        
        
    for _, row in subdataset.iterrows():
        insert = []
        for col in cols:
            insert.append(row[id_biorreactor+'_'+col]) # se le añade el id del biorreactor delante porque las columnas de cada dataset son distintas
        resul.append(insert)
    return resul

def buscar_registros_biorreactor(info: dict, biorreactor: int, fecha_inicio: str, fecha_fin: str, n_registros: int) -> list:
    if biorreactor == 'NaN':
        return
    df_bio = info['biorreactores'][str(biorreactor)]
    resul = buscar_registros(df_bio, fecha_inicio, fecha_fin, str(biorreactor), True)
    if resul is None:
        return
    if len(resul) > n_registros:
        raise ValueError('El subdataset contiene demasiados registros') # n_registros es el número máximo de registros que puede tener un lote, es decir, el tiempo máximo que ha estado cualquier lote en un reactor
    
    añadir_ceros = n_registros - len(resul)
    for i in range(añadir_ceros):
        resul.append(list(np.zeros(len(df_bio.columns)-1)))
    
    return resul

def buscar_registros_centrifuga(info: dict, centrifuga: int, fecha_inicio: str, fecha_fin: str, n_registros: int) -> list:
    if centrifuga == 'NaN':
        return
    df_centrifuga = info['centrifugas'][str(centrifuga)]
    resul = buscar_registros(df_centrifuga, fecha_inicio, fecha_fin, str(centrifuga), False)
    if resul is None:
        return
    if len(resul) > n_registros:
        raise ValueError('El subdataset contiene demasiados registros') # n_registros es el número máximo de registros que puede tener un lote, es decir, el tiempo máximo que ha estado cualquier lote en un reactor
    
    añadir_ceros = n_registros - len(resul)
    for i in range(añadir_ceros):
        resul.append(list(np.zeros(len(df_centrifuga.columns)-1)))
    
    return resul


def buscar_registros_temperaturas(info: dict, fecha_inicio: str, fecha_fin: str, n_registros: int) -> list:
    df_temperaturas = info['Temperaturas y humedades']
    resul = buscar_registros(df_temperaturas, fecha_inicio, fecha_fin)
    if len(resul) > n_registros:
        raise ValueError('El subdataset contiene demasiados registros') # n_registros es el número máximo de registros que puede tener un lote, es decir, el tiempo máximo que ha estado cualquier lote en un reactor
    
    añadir_ceros = n_registros - len(resul)
    for i in range(añadir_ceros):
        resul.append(list(np.zeros(len(df_temperaturas.columns)-1)))
    
    return resul