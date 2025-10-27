import pandas as pd
import numpy as np
from src.utilidades.logger import configurar_logger

# Logger común para todos los transformadores
logger = configurar_logger('finnovarisk.transformadores')

def convertir_columnas_minusculas(df: pd.DataFrame, nombre_dataset: str) -> pd.DataFrame:
    """Convierte nombres de columnas a minúsculas"""
    df_proc = df.copy()
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info(f"Nombres de columnas de {nombre_dataset} convertidos a minúsculas.")
    return df_proc

def crear_llave_cedula_numero(df: pd.DataFrame, col_cedula: str, col_numero: str) -> pd.DataFrame:
    """Crea la llave cedula_numero de manera segura"""
    df_proc = df.copy()
    
    if col_cedula in df_proc.columns and col_numero in df_proc.columns:
        # Convertir explícitamente a string
        df_proc[col_cedula] = df_proc[col_cedula].astype(str).fillna('').str.strip()
        df_proc[col_numero] = df_proc[col_numero].astype(str).fillna('').str.strip()
        
        df_proc['cedula_numero'] = df_proc[col_cedula] + '-' + df_proc[col_numero]
        logger.info(f"Creada la columna 'cedula_numero' usando {col_cedula} y {col_numero}.")
    else:
        logger.warning(f"No se encontraron {col_cedula} y/o {col_numero} para crear la llave.")
    
    return df_proc

def eliminar_columnas(df: pd.DataFrame, columnas_a_eliminar: list, nombre_dataset: str) -> pd.DataFrame:
    """Elimina columnas especificadas del DataFrame"""
    df_proc = df.copy()
    columnas_encontradas = [col for col in columnas_a_eliminar if col in df_proc.columns]
    
    if columnas_encontradas:
        df_proc.drop(columns=columnas_encontradas, inplace=True)
        logger.info(f"Se eliminaron {len(columnas_encontradas)} columnas de {nombre_dataset}: {', '.join(columnas_encontradas)}")
    
    return df_proc