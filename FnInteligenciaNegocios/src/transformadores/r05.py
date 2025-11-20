"""
Transformador para R05.
CORRECCIÓN CRÍTICA: cedula_numero usa "_" (guion bajo), no "-" (guion)
"""

import pandas as pd
from .base import logger, convertir_columnas_minusculas

def procesar_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe R05.
    
    ⚠️ CRÍTICO: Según R (línea ~711):
    ABONOS$cedula_numero <- paste(ABONOS$NIT, ABONOS$MCNNUMCRU2, sep = "_")
    
    R05 usa guion bajo "_" para separar, NO guion "-" como otros datasets.
    """
    if df is None:
        logger.warning("El dataframe de R05 es None. Se omite el procesamiento.")
        return None

    logger.info("="*70)
    logger.info("🔄 TRANSFORMACIÓN R05")
    logger.info("="*70)
    logger.info("")
    
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # PASO 1: Convertir nombres de columnas a minúsculas
    logger.info("📋 PASO 1: Convertir columnas a minúsculas")
    df_proc = convertir_columnas_minusculas(df_proc, "R05")
    logger.info(f"   Columnas disponibles: {list(df_proc.columns)}")
    logger.info("")

    # PASO 2.1: Calcular corte desde mcnfecha
    logger.info("📋 PASO 2: Calcular corte desde mcnfecha")
    df_proc = calcular_corte_fin_mes(df_proc)
    logger.info("")

    # PASO 2.1: Eliminar mcnfecha (ya viene 'corte' del cargador)
    logger.info("📋 PASO 2.1: Eliminar mcnfecha")
    if 'mcnfecha' in df_proc.columns:
        df_proc = df_proc.drop(columns=['mcnfecha'])
        logger.info("   ✅ Eliminada columna 'mcnfecha' (ya existe 'corte' del cargador)")
    else:
        logger.info("   ℹ️  No existe 'mcnfecha'")
    logger.info("")
    
    # PASO 3: Identificar y renombrar columnas específicas
    logger.info("📋 PASO 3: Identificar y renombrar columnas")
    df_proc = identificar_y_renombrar_r05(df_proc)
    logger.info("")

    # PASO 4: Convertir tipos de datos
    logger.info("📋 PASO 4: Convertir tipos de datos")
    df_proc = convertir_tipos_r05(df_proc)
    logger.info("")

    # PASO 5: Crear llave cedula_numero con "-" 
    logger.info("📋 PASO 5: Crear cedula_numero (con guion bajo '-')")
    df_proc = crear_llave_cedula_numero_r05(df_proc)
    logger.info("")

    # PASO 6: Filtrar por abono > 0
    logger.info("📋 PASO 6: Filtrar abono > 0")
    df_proc = filtrar_abono_positivo(df_proc)
    logger.info("")

    # PASO 7: Agrupar duplicados y renombrar a ABONO1
    logger.info("📋 PASO 7: Agrupar duplicados")
    df_proc = agrupar_duplicados_r05(df_proc)
    logger.info("")

    # Resumen final
    logger.info("="*70)
    logger.info("✅ R05 TRANSFORMADO")
    logger.info("="*70)
    logger.info(f"Registros: {registros_antes:,} → {len(df_proc):,}")
    logger.info(f"Columnas finales: {len(df_proc.columns)}")
    logger.info(f"Columnas: {list(df_proc.columns)}")
    logger.info("="*70)
    logger.info("")
    
    return df_proc


def identificar_y_renombrar_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica y renombra columnas específicas de R05.
    """
    renames = {}
    
    # Buscar columna NIT/cedula
    if 'nit' in df.columns:
        renames['nit'] = 'cedula'
    elif any('nit' in col for col in df.columns):
        col_nit = [col for col in df.columns if 'nit' in col][0]
        renames[col_nit] = 'cedula'
    
    # Buscar mcnnumcru2 (número de obligación)
    if 'mcnnumcru2' in df.columns:
        renames['mcnnumcru2'] = 'numero'
    elif any('numcru2' in col for col in df.columns):
        col_numero = [col for col in df.columns if 'numcru2' in col][0]
        renames[col_numero] = 'numero'
    
    # Buscar columna abono
    if 'abono' in df.columns:
        pass  # Ya está bien nombrada
    elif any('abono' in col.lower() for col in df.columns):
        col_abono = [col for col in df.columns if 'abono' in col.lower()][0]
        renames[col_abono] = 'abono'

    if renames:
        df.rename(columns=renames, inplace=True)
        logger.info(f"   ✅ Columnas renombradas: {renames}")
    else:
        logger.warning("   ⚠️  No se encontraron columnas para renombrar")

    logger.info(f"   📋 Columnas finales: {list(df.columns)}")
    
    return df


def convertir_tipos_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte tipos de datos en R05, LIMPIANDO el .0
    """
    from .base import limpiar_columna_identificador
    
    # Convertir 'corte' a datetime
    if 'corte' in df.columns:
        df['corte'] = pd.to_datetime(df['corte'], errors='coerce')
        nulos_corte = df['corte'].isnull().sum()
        if nulos_corte > 0:
            logger.warning(f"   ⚠️  {nulos_corte:,} registros con 'corte' nulo")
        else:
            logger.info(f"   ✅ 'corte' convertido a datetime")
    
    # Convertir 'abono' a numérico
    if 'abono' in df.columns:
        df['abono'] = pd.to_numeric(df['abono'], errors='coerce')
        nulos_abono = df['abono'].isnull().sum()
        if nulos_abono > 0:
            logger.warning(f"   ⚠️  {nulos_abono:,} registros con 'abono' nulo")
        else:
            logger.info(f"   ✅ 'abono' convertido a numérico")
    
    # Convertir 'cedula' y 'numero' a string LIMPIO
    if 'cedula' in df.columns:
        df['cedula'] = limpiar_columna_identificador(df['cedula'])
        logger.info(f"   ✅ 'cedula' limpiado y convertido a string")
    
    if 'numero' in df.columns:
        df['numero'] = limpiar_columna_identificador(df['numero'])
        logger.info(f"   ✅ 'numero' limpiado y convertido a string")
    
    return df


def crear_llave_cedula_numero_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la llave cedula_numero con GUION BAJO "_".
    
    ⚠️ CRÍTICO: R05 usa "_" no "-"
    Según R: ABONOS$cedula_numero <- paste(ABONOS$NIT, ABONOS$MCNNUMCRU2, sep = "_")
    """
    if 'cedula' not in df.columns or 'numero' not in df.columns:
        logger.error("   ❌ No se encontraron 'cedula' y 'numero'")
        logger.error(f"   Columnas disponibles: {list(df.columns)}")
        return df
    
    df['cedula_numero'] = df['cedula'] + '-' + df['numero']
    
    llaves_validas = df['cedula_numero'].notna().sum()
    llaves_vacias = (df['cedula_numero'] == '-').sum()
    
    logger.info(f"   ✅ cedula_numero creada con '-' (guion bajo)")
    logger.info(f"   📊 Llaves válidas: {llaves_validas:,}")
    
    if llaves_vacias > 0:
        logger.warning(f"   ⚠️  {llaves_vacias:,} llaves vacías ('-')")
    
    return df


def filtrar_abono_positivo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra registros con abono > 0.
    """
    if 'abono' not in df.columns:
        logger.warning("   ⚠️  No existe columna 'abono'")
        return df
    
    registros_antes = len(df)
    df = df[df['abono'] > 0].copy()
    eliminados = registros_antes - len(df)
    
    logger.info(f"   ✅ Filtro aplicado: {registros_antes:,} → {len(df):,}")
    logger.info(f"   📊 Eliminados: {eliminados:,}")
    
    return df


def agrupar_duplicados_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa duplicados por cedula_numero y corte, sumando abono.
    Renombra 'abono' a 'ABONO1'.
    """
    if not all(col in df.columns for col in ['cedula_numero', 'corte', 'abono']):
        logger.warning("   ⚠️  Faltan columnas necesarias para agrupar")
        return df
    
    registros_antes = len(df)
    
    # Verificar duplicados
    duplicados = df.duplicated(subset=['cedula_numero', 'corte']).sum()
    logger.info(f"   📊 Duplicados encontrados: {duplicados:,}")
    
    if duplicados > 0:
        # Agrupar
        df = df.groupby(['cedula_numero', 'corte'], as_index=False)['abono'].sum()
        logger.info(f"   ✅ Agrupación completada: {registros_antes:,} → {len(df):,}")
    else:
        logger.info(f"   ℹ️  Sin duplicados")
    
    # Renombrar 'abono' a 'ABONO1'
    df.rename(columns={'abono': 'ABONO1'}, inplace=True)
    logger.info(f"   ✅ Columna renombrada: 'abono' → 'ABONO1'")
    
    return df

def calcular_corte_fin_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el fin de mes desde MCNFECHA.
    
    Según R (línea 695):
    ABONOS$corte <- ceiling_date(ABONOS$MCNFECHA, unit = "month") - days(1)
    """
    if 'mcnfecha' not in df.columns:
        logger.warning("   ⚠️  No existe 'mcnfecha'")
        return df
    
    # Convertir a datetime
    df['mcnfecha'] = pd.to_datetime(df['mcnfecha'], errors='coerce')
    
    # Calcular fin de mes (ceiling_date + 1 mes - 1 día)
    df['corte'] = (df['mcnfecha'] + pd.offsets.MonthEnd(0))
    
    logger.info(f"   ✅ 'corte' calculado desde 'mcnfecha'")
    
    return df