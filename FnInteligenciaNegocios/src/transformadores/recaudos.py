import pandas as pd
from .base import logger, convertir_columnas_minusculas, crear_llave_cedula_numero

def procesar_recaudos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe Recaudos.
    """
    if df is None:
        logger.warning("El dataframe de Recaudos es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de Recaudos...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "Recaudos")
    logger.info(f"Columnas disponibles en Recaudos: {list(df_proc.columns)}")

    # 2. Identificar y renombrar columnas dinámicamente
    columna_cedula = None
    columna_numero = None
    columna_fecha = None
    columna_capital = None
    
    for col in df_proc.columns:
        if 'cedula' in col or 'identificacion' in col or 'nit' in col or 'vinculado' in col:
            columna_cedula = col
        elif 'numero' in col or 'obligacion' in col or 'ds_numero' in col:
            columna_numero = col
        elif 'fecha' in col or 'corte' in col or 'rc_fecha' in col:
            columna_fecha = col
        elif 'capital' in col or 'capitalrec' in col or 'valor' in col:
            columna_capital = col
    
    # Renombrar columnas identificadas
    renames = {}
    if columna_cedula:
        renames[columna_cedula] = 'cedula'
    if columna_numero:
        renames[columna_numero] = 'numero'
    if columna_fecha:
        renames[columna_fecha] = 'corte'
    if columna_capital:
        renames[columna_capital] = 'capitalrec'
    
    if renames:
        df_proc.rename(columns=renames, inplace=True)
        logger.info(f"Columnas renombradas en Recaudos: {renames}")

    from .base import limpiar_columnas_numericas_como_string
    df_proc = limpiar_columnas_numericas_como_string(df_proc, ['cedula', 'numero'])
    
    # 3. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
    
    if 'capitalrec' in df_proc.columns:
        df_proc['capitalrec'] = pd.to_numeric(df_proc['capitalrec'], errors='coerce')

    # 4. Crear la llave 'cedula_numero'
    df_proc = crear_llave_cedula_numero(df_proc, 'cedula', 'numero')

    # 5. Filtrar por capitalrec > 0
    if 'capitalrec' in df_proc.columns:
        registros_antes_filtro = len(df_proc)
        df_proc = df_proc[df_proc['capitalrec'] > 0].copy()
        logger.info(f"Filtrado de Recaudos por capitalrec > 0: {registros_antes_filtro:,} -> {len(df_proc):,} registros.")

    # 6. Agrupar duplicados
    if 'cedula_numero' in df_proc.columns and 'corte' in df_proc.columns and 'capitalrec' in df_proc.columns:
        registros_antes_agg = len(df_proc)
        df_proc = df_proc.groupby(['cedula_numero', 'corte'], as_index=False)['capitalrec'].sum()
        logger.info(f"Agregación de duplicados en Recaudos: {registros_antes_agg:,} -> {len(df_proc):,} registros.")

    logger.info(f"Limpieza y procesamiento de Recaudos completado: {registros_antes:,} -> {len(df_proc):,} registros.")
    return df_proc