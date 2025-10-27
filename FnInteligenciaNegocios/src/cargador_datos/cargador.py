"""
Módulo para la carga de datos desde los archivos originales.
Usa configuración JSON externa.
"""

import pandas as pd
from pathlib import Path


from config.config_loader import ConfigLoader
from src.utilidades.logger import configurar_logger 

# Inicializar logger principal del proyecto
logger = configurar_logger("finnovarisk.carga_datos")

# Cargar configuración desde JSON (REEMPLAZA CONFIG_CARGA)
CONFIG_LOADER = ConfigLoader()
CONFIG_CARGA = CONFIG_LOADER.get_config()

def cargar_excel_con_config(ruta: Path, hojas_config: dict) -> list[pd.DataFrame]:
    """Carga las hojas especificadas de un archivo Excel."""
    dataframes = []
    if not ruta.exists():
        logger.error(f"Archivo no encontrado: {ruta}")
        return dataframes

    try:
        xls = pd.ExcelFile(ruta)
        for hoja, fecha_str in hojas_config.items():
            if hoja not in xls.sheet_names:
                logger.warning(f"No se encontró hoja '{hoja}' en {ruta.name}")
                continue
            df = pd.read_excel(xls, sheet_name=hoja)
            # Solo agregar fecha de corte si no es None (para auxiliares)
            if fecha_str is not None:
                df["corte"] = pd.to_datetime(fecha_str)
            dataframes.append(df)
            logger.info(f"Cargada hoja '{hoja}' ({fecha_str}) desde {ruta.name}")
    except Exception as e:
        logger.exception(f"Error cargando {ruta}: {e}")
    return dataframes


def concatenar_dataframes(dataframes: list[pd.DataFrame], nombre: str) -> pd.DataFrame:
    """Concatena un conjunto de DataFrames y lo loguea."""
    if not dataframes:
        logger.warning(f"Sin datos cargados para {nombre}")
        return pd.DataFrame()
    df_total = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Concatenación de {nombre} completada: {len(df_total)} registros")
    return df_total


def cargar_dataset(nombre: str) -> pd.DataFrame:
    """
    Carga un dataset completo según la configuración global.

    Args:
        nombre: nombre del dataset (ej. 'FNZ007')

    Returns:
        DataFrame concatenado.
    """
    if nombre not in CONFIG_CARGA:
        logger.error(f"No existe configuración para '{nombre}'")
        return pd.DataFrame()

    cfg = CONFIG_CARGA[nombre]
    dataframes = []

    for clave_archivo, ruta in cfg["archivos"].items():
        hojas = cfg["hojas"].get(clave_archivo, {})
        if not hojas:
            logger.warning(f"No hay hojas definidas para {clave_archivo}")
            continue
        dataframes.extend(cargar_excel_con_config(ruta, hojas))

    return concatenar_dataframes(dataframes, nombre)


def cargar_auxiliares() -> dict[str, pd.DataFrame]:
    """
    Carga archivos auxiliares (sin fecha de corte).
    
    Returns:
        dict[str, pd.DataFrame]: Diccionario con DataFrames auxiliares
    """
    logger.info("Cargando archivos auxiliares...")
    auxiliares = {}
    
    if "AUXILIARES" not in CONFIG_CARGA:
        logger.error("No hay configuración para AUXILIARES")
        return auxiliares
        
    cfg = CONFIG_CARGA["AUXILIARES"]
    
    for clave_archivo, ruta in cfg["archivos"].items():
        if not ruta.exists():
            logger.error(f"Archivo auxiliar no encontrado: {ruta}")
            continue
        try:
            # Para auxiliares, cargamos sin fecha de corte
            df = pd.read_excel(ruta)
            auxiliares[clave_archivo] = df
            logger.info(f"Cargado auxiliar '{clave_archivo}': {len(df)} registros")
        except Exception as e:
            logger.exception(f"Error cargando auxiliar {clave_archivo}: {e}")
    
    return auxiliares


# Funciones de compatibilidad (opcionales - para mantener main.py funcionando)
def cargar_fnz007() -> pd.DataFrame:
    """Carga FNZ007 (función de compatibilidad)."""
    return cargar_dataset("FNZ007")


def cargar_analisis_cartera() -> pd.DataFrame:
    """Carga Análisis de Cartera (función de compatibilidad)."""
    return cargar_dataset("ANALISIS_CARTERA")


def cargar_fnz001() -> pd.DataFrame:
    """Carga FNZ001 (función de compatibilidad)."""
    return cargar_dataset("FNZ001")