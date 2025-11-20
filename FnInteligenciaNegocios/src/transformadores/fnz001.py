"""
Transformador para FNZ001.
Basado en el script R original (líneas de carga y procesamiento básico).
"""

import pandas as pd
from .base import logger, convertir_columnas_minusculas


def procesar_fnz001(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe FNZ001.
    
    Según el R:
    1. Renombrar columnas: CC_NIT → cedula, DSM_NUM → numero, VLR_FNZ → valor
    2. Convertir 'numero' a string
    3. FNZ001 NO tiene eliminación de duplicados en su procesamiento inicial
    
    Nota: Los duplicados se manejan después en el join con BaseFNZ,
    donde se usa distinct(numero, .keep_all = TRUE)
    """
    if df is None or len(df) == 0:
        logger.warning("El dataframe de FNZ001 es None o vacío. Se omite el procesamiento.")
        return None

    logger.info("="*70)
    logger.info("🔄 TRANSFORMACIÓN FNZ001")
    logger.info("="*70)
    
    df_proc = df.copy()
    registros_iniciales = len(df_proc)
    
    # PASO 1: Convertir nombres de columnas a minúsculas
    logger.info("\n📋 PASO 1: Convertir columnas a minúsculas")
    df_proc = convertir_columnas_minusculas(df_proc, "FNZ001")
    
    # PASO 2: Renombrar columnas según R
    logger.info("\n📋 PASO 2: Renombrar columnas")
    df_proc = renombrar_columnas_fnz001(df_proc)
    
    # PASO 3: Convertir tipos de datos
    logger.info("\n📋 PASO 3: Convertir tipos de datos")
    df_proc = convertir_tipos_fnz001(df_proc)
    
    # Resumen final
    logger.info("")
    logger.info("="*70)
    logger.info("✅ FNZ001 TRANSFORMADO")
    logger.info("="*70)
    logger.info(f"Registros: {registros_iniciales:,} (sin cambios - no se eliminan duplicados aquí)")
    logger.info(f"Columnas finales: {len(df_proc.columns)}")
    logger.info(f"Columnas: {list(df_proc.columns)}")
    logger.info("="*70)
    logger.info("")
    
    return df_proc


def renombrar_columnas_fnz001(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas según el script R:
    - CC_NIT → cedula
    - DSM_NUM → numero  
    - VLR_FNZ → valor
    """
    df_proc = df.copy()
    
    # Mapeo exacto del R
    renames = {
        'cc_nit': 'cedula',
        'dsm_num': 'numero',
        'vlr_fnz': 'valor'
    }
    
    # Aplicar solo las que existan
    renames_aplicables = {k: v for k, v in renames.items() if k in df_proc.columns}
    
    if renames_aplicables:
        df_proc.rename(columns=renames_aplicables, inplace=True)
        logger.info(f"   ✅ Columnas renombradas: {renames_aplicables}")
    else:
        logger.warning("   ⚠️  No se encontraron las columnas esperadas para renombrar")
        logger.info(f"   📋 Columnas disponibles: {list(df_proc.columns)}")
    
    return df_proc


def convertir_tipos_fnz001(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte tipos de datos según R, LIMPIANDO el .0
    """
    from .base import limpiar_columna_identificador
    
    df_proc = df.copy()
    
    # Convertir 'numero' - LIMPIAR .0
    if 'numero' in df_proc.columns:
        df_proc['numero'] = limpiar_columna_identificador(df_proc['numero'])
        logger.info(f"   ✅ 'numero' limpiado y convertido a string")
    
    # Convertir 'cedula' - LIMPIAR .0
    if 'cedula' in df_proc.columns:
        df_proc['cedula'] = limpiar_columna_identificador(df_proc['cedula'])
        logger.info(f"   ✅ 'cedula' limpiado y convertido a string")
    
    # Verificar 'corte' (ya debería ser datetime del cargador)
    if 'corte' in df_proc.columns:
        if not pd.api.types.is_datetime64_any_dtype(df_proc['corte']):
            df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
            logger.info(f"   ✅ 'corte' convertido a datetime")
        else:
            logger.info(f"   ✅ 'corte' ya es datetime")
    
    # Convertir 'valor' a numérico
    if 'valor' in df_proc.columns:
        df_proc['valor'] = pd.to_numeric(df_proc['valor'], errors='coerce')
        nulos = df_proc['valor'].isnull().sum()
        if nulos > 0:
            logger.warning(f"   ⚠️  {nulos:,} valores nulos en 'valor'")
        else:
            logger.info(f"   ✅ 'valor' convertido a numérico")
    
    return df_proc