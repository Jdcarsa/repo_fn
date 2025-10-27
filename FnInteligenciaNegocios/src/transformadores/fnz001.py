import pandas as pd
from .base import logger, convertir_columnas_minusculas, crear_llave_cedula_numero

def procesar_fnz001(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe FNZ001.
    """
    if df is None:
        logger.warning("El dataframe de FNZ001 es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de FNZ001...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "FNZ001")

    # 2. Renombrar columnas
    renames = {
        'fecha': 'corte',
        'cc_nit': 'cedula',
        'dsm_num': 'numero',
        'vlr_fnz': 'valor'
    }
    
    df_proc.rename(columns={k: v for k, v in renames.items() if k in df_proc.columns}, inplace=True)
    logger.info("Columnas de FNZ001 renombradas.")

    # 3. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
    
    if 'valor' in df_proc.columns:
        df_proc['valor'] = pd.to_numeric(df_proc['valor'], errors='coerce')

    # 4. Crear la llave 'cedula_numero'
    df_proc = crear_llave_cedula_numero(df_proc, 'cedula', 'numero')

    # 5. Eliminar duplicados por 'numero'
    if 'numero' in df_proc.columns:
        df_proc.drop_duplicates(subset=['numero'], keep='first', inplace=True)
        logger.info(f"Eliminados duplicados en FNZ001 por 'numero': {registros_antes:,} -> {len(df_proc):,} registros.")

    logger.info("Limpieza y procesamiento de FNZ001 completado.")
    return df_proc