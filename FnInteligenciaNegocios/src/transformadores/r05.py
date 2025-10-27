import pandas as pd
from .base import logger, convertir_columnas_minusculas

def procesar_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe R05.
    """
    if df is None:
        logger.warning("El dataframe de R05 es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de R05...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "R05")
    logger.info(f"Columnas disponibles en R05: {list(df_proc.columns)}")

    # 2. Identificar y renombrar columnas específicas para R05
    renames = {}
    
    # Buscar columnas específicas de R05
    if 'nit' in df_proc.columns:
        renames['nit'] = 'cedula'
    elif any('nit' in col for col in df_proc.columns):
        col_nit = [col for col in df_proc.columns if 'nit' in col][0]
        renames[col_nit] = 'cedula'
    
    # Buscar mcnnumcru2 que es el número de obligación en R05
    if 'mcnnumcru2' in df_proc.columns:
        renames['mcnnumcru2'] = 'numero'
    elif any('numcru2' in col for col in df_proc.columns):
        col_numero = [col for col in df_proc.columns if 'numcru2' in col][0]
        renames[col_numero] = 'numero'
    
    # Buscar mcnfecha para el corte
    if 'mcnfecha' in df_proc.columns:
        renames['mcnfecha'] = 'corte'
    elif any('fecha' in col for col in df_proc.columns):
        col_fecha = [col for col in df_proc.columns if 'fecha' in col][0]
        renames[col_fecha] = 'corte'

    if renames:
        df_proc.rename(columns=renames, inplace=True)
        logger.info(f"Columnas renombradas en R05: {renames}")

    logger.info(f"Columnas disponibles después del renombrado en R05: {list(df_proc.columns)}")
    
    # 3. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
    
    if 'abono' in df_proc.columns:
        df_proc['abono'] = pd.to_numeric(df_proc['abono'], errors='coerce')

    # 4. Crear la llave 'cedula_numero' (igual que en R)
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        df_proc['cedula_numero'] = df_proc['cedula'].astype(str) + "_" + df_proc['numero'].astype(str)
        logger.info(f"Llave cedula_numero creada para {len(df_proc)} registros")
    else:
        logger.warning("No se pudieron encontrar las columnas para crear cedula_numero")
        logger.warning(f"Columnas disponibles: {list(df_proc.columns)}")
        return None

    # 5. Filtrar por abono > 0 (igual que en R)
    if 'abono' in df_proc.columns:
        registros_antes_filtro = len(df_proc)
        df_proc = df_proc[df_proc['abono'] > 0].copy()
        logger.info(f"Filtrado de R05 por abono > 0: {registros_antes_filtro:,} -> {len(df_proc):,} registros.")


    if 'cedula_numero' in df_proc.columns and 'corte' in df_proc.columns and 'abono' in df_proc.columns:
        registros_antes_agg = len(df_proc)
        df_proc = df_proc.groupby(['cedula_numero', 'corte'], as_index=False)['abono'].sum()

        df_proc.rename(columns={'abono': 'ABONO1'}, inplace=True)
        logger.info(f"Agregación de duplicados en R05: {registros_antes_agg:,} -> {len(df_proc):,} registros.")

    logger.info(f"Limpieza y procesamiento de R05 completado: {registros_antes:,} -> {len(df_proc):,} registros.")
    return df_proc