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

    # 2. ✅ SOLUCIÓN: Eliminar mcnfecha si existe, porque 'corte' ya viene del cargador
    if 'mcnfecha' in df_proc.columns:
        df_proc = df_proc.drop(columns=['mcnfecha'])
        logger.info("✅ Eliminada columna 'mcnfecha' (ya existe 'corte' del cargador)")
    
    # 3. Identificar y renombrar otras columnas específicas para R05
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

    if renames:
        df_proc.rename(columns=renames, inplace=True)
        logger.info(f"Columnas renombradas en R05: {renames}")

    logger.info(f"Columnas finales en R05: {list(df_proc.columns)}")
    
    # 4. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
        nulos_corte = df_proc['corte'].isnull().sum()
        if nulos_corte > 0:
            logger.warning(f"⚠️ {nulos_corte:,} registros con 'corte' nulo")
    
    if 'abono' in df_proc.columns:
        df_proc['abono'] = pd.to_numeric(df_proc['abono'], errors='coerce')
        nulos_abono = df_proc['abono'].isnull().sum()
        if nulos_abono > 0:
            logger.warning(f"⚠️ {nulos_abono:,} registros con 'abono' nulo")

    # 5. Crear la llave 'cedula_numero'
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        df_proc['cedula'] = df_proc['cedula'].astype(str).fillna('').str.strip()
        df_proc['numero'] = df_proc['numero'].astype(str).fillna('').str.strip()
        df_proc['cedula_numero'] = df_proc['cedula'] + '-' + df_proc['numero']
        logger.info(f"✅ Llave cedula_numero creada para {len(df_proc):,} registros")
    else:
        logger.error("❌ No se pudieron encontrar las columnas para crear cedula_numero")
        logger.error(f"Columnas disponibles: {list(df_proc.columns)}")
        return None

    # 6. Filtrar por abono > 0
    if 'abono' in df_proc.columns:
        registros_antes_filtro = len(df_proc)
        df_proc = df_proc[df_proc['abono'] > 0].copy()
        eliminados = registros_antes_filtro - len(df_proc)
        logger.info(f"✅ Filtrado por abono > 0: {registros_antes_filtro:,} → {len(df_proc):,} ({eliminados:,} eliminados)")

    # 7. Agrupar duplicados
    if 'cedula_numero' in df_proc.columns and 'corte' in df_proc.columns and 'abono' in df_proc.columns:
        registros_antes_agg = len(df_proc)
        
        # Verificar duplicados
        duplicados = df_proc.duplicated(subset=['cedula_numero', 'corte']).sum()
        logger.info(f"📊 Encontrados {duplicados:,} registros duplicados")
        
        if duplicados > 0:
            df_proc = df_proc.groupby(['cedula_numero', 'corte'], as_index=False)['abono'].sum()
            df_proc.rename(columns={'abono': 'ABONO1'}, inplace=True)
            logger.info(f"✅ Duplicados agrupados: {registros_antes_agg:,} → {len(df_proc):,}")
        else:
            df_proc.rename(columns={'abono': 'ABONO1'}, inplace=True)
            logger.info(f"✅ Sin duplicados, renombrada columna a 'ABONO1'")

    logger.info(f"✅ R05 completado: {registros_antes:,} → {len(df_proc):,} registros")
    return df_proc