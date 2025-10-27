import pandas as pd
from .base import logger, convertir_columnas_minusculas, crear_llave_cedula_numero

def procesar_edades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe de Edades.
    """
    if df is None:
        logger.warning("El dataframe de Edades es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de Edades...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "Edades")

    # 2. Crear la llave 'cedula_numero'
    df_proc = crear_llave_cedula_numero(df_proc, 'cc_nit', 'numero')

    # 3. Filtrar por LINEA
    if 'linea' in df_proc.columns:
        lineas_a_mantener = ["[01]CREDITO ARPESOD", "[03]CREDITO RETANQUEO"]
        df_proc = df_proc[df_proc['linea'].isin(lineas_a_mantener)]
        logger.info(f"Filtrado de Edades por línea de crédito: {registros_antes:,} -> {len(df_proc):,} registros.")
    else:
        logger.warning("No se encontró la columna 'linea' en Edades. No se aplicó el filtro.")

    logger.info("Limpieza y procesamiento de Edades completado.")
    return df_proc