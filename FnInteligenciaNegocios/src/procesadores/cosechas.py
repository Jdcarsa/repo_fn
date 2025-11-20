"""
Módulo para crear el DataFrame de Cosechas.
Replica la lógica del script R original para análisis de cohortes.
"""

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.cosechas')


def crear_cosechas(df_fnz007: pd.DataFrame, 
                   df_ac: pd.DataFrame,
                   df_edades: pd.DataFrame = None,
                   df_r05: pd.DataFrame = None,
                   df_recaudos: pd.DataFrame = None) -> pd.DataFrame:
    """
    Crea el DataFrame de Cosechas uniendo múltiples fuentes.
    
    Según R (líneas ~830-900):
    1. Seleccionar columnas de BaseFNZ: cedula_numero, corte, corte2, valor, fecha
    2. Full join con BaseAC (diasatras, fechafac, vlrini, valatras, saldofac)
    3. Full join con Edades (CAPITAL, INTERES, OTROS, TOTALPAGO)
    4. Full join con R05 (ABONO1)
    5. Full join con Recaudos (CAPITALREC)
    6. Corregir fechafac vacías
    7. Crear fechafac_ajustada (un mes antes)
    8. Filtrar registros con "NA-\d+"
    
    Returns:
        DataFrame de Cosechas procesado
    """
    logger.info("="*70)
    logger.info("🌾 CREANDO COSECHAS")
    logger.info("="*70)
    logger.info("")
    
    # PASO 1: Seleccionar columnas de BaseFNZ
    logger.info("1️⃣ Seleccionando columnas base de FNZ007...")
    
    columnas_base = ['cedula_numero', 'corte', 'valor', 'fecha']
    
    # Verificar qué columnas existen
    columnas_disponibles = [col for col in columnas_base if col in df_fnz007.columns]
    columnas_faltantes = set(columnas_base) - set(columnas_disponibles)
    
    if columnas_faltantes:
        logger.warning(f"   ⚠️  Columnas faltantes en FNZ007: {columnas_faltantes}")
        # Usar solo las disponibles
        columnas_base = columnas_disponibles
    
    # Crear Cosechas inicial
    Cosechas = df_fnz007[columnas_base].copy()

    # Agregar corte2 si existe (es corte.x en el R)
    if 'corte2' in df_fnz007.columns:
        Cosechas['corte2'] = df_fnz007['corte2']
    
    logger.info(f"   ✅ Cosechas inicial: {len(Cosechas):,} registros, {len(Cosechas.columns)} columnas")
    logger.info("")
    
    # PASO 2: Full join con BaseAC
    logger.info("2️⃣ JOIN con Análisis de Cartera...")
    Cosechas = unir_con_ac(Cosechas, df_ac)
    logger.info("")
    
    # PASO 3: Full join con Edades
    if df_edades is not None and len(df_edades) > 0:
        logger.info("3️⃣ JOIN con Edades...")
        Cosechas = unir_con_edades(Cosechas, df_edades)
        logger.info("")
    else:
        logger.warning("3️⃣ Edades no disponible - JOIN omitido")
        logger.info("")
    
    # PASO 4: Full join con R05
    if df_r05 is not None and len(df_r05) > 0:
        logger.info("4️⃣ JOIN con R05...")
        Cosechas = unir_con_r05(Cosechas, df_r05)
        logger.info("")
    else:
        logger.warning("4️⃣ R05 no disponible - JOIN omitido")
        logger.info("")
    
    # PASO 5: Full join con Recaudos
    if df_recaudos is not None and len(df_recaudos) > 0:
        logger.info("5️⃣ JOIN con Recaudos...")
        Cosechas = unir_con_recaudos(Cosechas, df_recaudos)
        logger.info("")
    else:
        logger.warning("5️⃣ Recaudos no disponible - JOIN omitido")
        logger.info("")
    
    # PASO 6: Corregir fechafac vacías
    logger.info("6️⃣ Corrigiendo fechas de facturación...")
    Cosechas = corregir_fechafac(Cosechas)
    logger.info("")
    
    # PASO 7: Crear fechafac_ajustada
    logger.info("7️⃣ Creando fechafac_ajustada (un mes antes)...")
    Cosechas = crear_fechafac_ajustada(Cosechas)
    logger.info("")
    
    # PASO 8: Filtrar registros NA-\d+
    logger.info("8️⃣ Filtrando registros 'NA-'...")
    Cosechas, eliminados = filtrar_registros_na(Cosechas)
    logger.info("")
    logger.info(f"   📊 Registros eliminados: {len(eliminados):,}")
    # Resumen final
    logger.info("="*70)
    logger.info("✅ COSECHAS COMPLETADO")
    logger.info("="*70)
    logger.info(f"📊 Registros finales: {len(Cosechas):,}")
    logger.info(f"📊 Columnas: {len(Cosechas.columns)}")
    logger.info(f"📊 Memoria: {Cosechas.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    logger.info("="*70)
    logger.info("")
    Cosechas = Cosechas.drop_duplicates().reset_index(drop=True)
    return Cosechas, eliminados


def unir_con_ac(cosechas: pd.DataFrame, df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Full join con Análisis de Cartera.
    
    Columnas a unir: diasatras, fechafac, vlrini, valatras, saldofac
    """
    registros_antes = len(cosechas)
    
    # Seleccionar columnas específicas de AC
    columnas_ac = ['cedula_numero', 'corte', 'diasatras', 'fechafac', 
                   'vlrini', 'valatras', 'saldofac']
    
    # Verificar qué columnas existen
    columnas_disponibles = [col for col in columnas_ac if col in df_ac.columns]
    
    if 'cedula_numero' not in columnas_disponibles or 'corte' not in columnas_disponibles:
        logger.error("   ❌ BaseAC no tiene cedula_numero o corte")
        return cosechas
    
    df_ac_filtrado = df_ac[columnas_disponibles].copy()
    
    # Full join (outer)
    cosechas = cosechas.merge(
        df_ac_filtrado,
        on=['cedula_numero', 'corte'],
        how='outer',
        suffixes=('', '_ac')
    )
    
    matches = cosechas['diasatras'].notna().sum() if 'diasatras' in cosechas.columns else 0
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(cosechas):,} registros")
    logger.info(f"   Matches con AC: {matches:,}")
    
    return cosechas


def unir_con_edades(cosechas: pd.DataFrame, df_edades: pd.DataFrame) -> pd.DataFrame:
    """
    Full join con Edades.
    
    Columnas a unir: CAPITAL, INTERES, OTROS, TOTALPAGO
    """
    registros_antes = len(cosechas)
    
    # Seleccionar columnas específicas (mayúsculas según R)
    columnas_edades = ['cedula_numero', 'corte', 'capital', 'interes', 'otros', 'totalpago']
    
    # Verificar qué columnas existen (case-insensitive)
    columnas_disponibles = []
    for col_target in columnas_edades:
        col_encontrada = next((c for c in df_edades.columns if c.lower() == col_target.lower()), None)
        if col_encontrada:
            columnas_disponibles.append(col_encontrada)
    
    if len(columnas_disponibles) < 2:  # Al menos cedula_numero y corte
        logger.warning("   ⚠️  Edades no tiene suficientes columnas")
        return cosechas
    
    df_edades_filtrado = df_edades[columnas_disponibles].copy()
    
    # Normalizar nombres a mayúsculas (como en R)
    df_edades_filtrado.columns = [col.upper() for col in df_edades_filtrado.columns]
    
    # Full join
    cosechas = cosechas.merge(
        df_edades_filtrado,
        left_on=['cedula_numero', 'corte'],
        right_on=['CEDULA_NUMERO', 'CORTE'],
        how='outer',
        suffixes=('', '_edades')
    )
    
    # Eliminar columnas duplicadas del join
    if 'CEDULA_NUMERO' in cosechas.columns and 'cedula_numero' in cosechas.columns:
        cosechas = cosechas.drop(columns=['CEDULA_NUMERO', 'CORTE'])
    
    matches = cosechas['CAPITAL'].notna().sum() if 'CAPITAL' in cosechas.columns else 0
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(cosechas):,} registros")
    logger.info(f"   Matches con Edades: {matches:,}")
    
    return cosechas


def unir_con_r05(cosechas: pd.DataFrame, df_r05: pd.DataFrame) -> pd.DataFrame:
    """
    Full join con R05.
    
    Columna a unir: ABONO1
    """
    registros_antes = len(cosechas)
    
    # R05 debe tener cedula_numero, corte, ABONO1
    if 'ABONO1' not in df_r05.columns:
        # Buscar columna de abono (puede estar como 'abono')
        col_abono = next((c for c in df_r05.columns if 'abono' in c.lower()), None)
        if col_abono:
            df_r05 = df_r05.rename(columns={col_abono: 'ABONO1'})
        else:
            logger.warning("   ⚠️  R05 no tiene columna ABONO1")
            return cosechas
    
    # Seleccionar solo las columnas necesarias
    columnas_r05 = ['cedula_numero', 'corte', 'ABONO1']
    columnas_disponibles = [col for col in columnas_r05 if col in df_r05.columns]
    
    df_r05_filtrado = df_r05[columnas_disponibles].copy()
    
    # Full join
    cosechas = cosechas.merge(
        df_r05_filtrado,
        on=['cedula_numero', 'corte'],
        how='outer',
        suffixes=('', '_r05')
    )
    
    matches = cosechas['ABONO1'].notna().sum()
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(cosechas):,} registros")
    logger.info(f"   Matches con R05: {matches:,}")
    
    return cosechas


def unir_con_recaudos(cosechas: pd.DataFrame, df_recaudos: pd.DataFrame) -> pd.DataFrame:
    """
    Full join con Recaudos.
    
    Columna a unir: CAPITALREC
    """
    registros_antes = len(cosechas)
    
    # Recaudos debe tener cedula_numero, corte, capitalrec
    if 'capitalrec' not in df_recaudos.columns:
        col_capital = next((c for c in df_recaudos.columns if 'capital' in c.lower()), None)
        if col_capital:
            df_recaudos = df_recaudos.rename(columns={col_capital: 'capitalrec'})
        else:
            logger.warning("   ⚠️  Recaudos no tiene columna capitalrec")
            return cosechas
    
    # Seleccionar solo las columnas necesarias
    columnas_recaudos = ['cedula_numero', 'corte', 'capitalrec']
    columnas_disponibles = [col for col in columnas_recaudos if col in df_recaudos.columns]
    
    df_recaudos_filtrado = df_recaudos[columnas_disponibles].copy()
    
    # Convertir a mayúsculas para consistencia (CAPITALREC)
    df_recaudos_filtrado = df_recaudos_filtrado.rename(columns={'capitalrec': 'CAPITALREC'})
    
    # Full join
    cosechas = cosechas.merge(
        df_recaudos_filtrado,
        on=['cedula_numero', 'corte'],
        how='outer',
        suffixes=('', '_recaudos')
    )
    
    matches = cosechas['CAPITALREC'].notna().sum()
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(cosechas):,} registros")
    logger.info(f"   Matches con Recaudos: {matches:,}")
    
    return cosechas


def corregir_fechafac(cosechas: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige fechas de facturación vacías.
    
    Según R (línea ~858):
    Si fechafac es NA, asignar: fin del mes siguiente a 'corte'
    """
    if 'fechafac' not in cosechas.columns:
        logger.warning("   ⚠️  Columna 'fechafac' no existe")
        return cosechas
    
    # Contar nulos antes
    nulos_antes = cosechas['fechafac'].isnull().sum()
    
    if nulos_antes == 0:
        logger.info("   ✅ Sin fechas faltantes")
        return cosechas
    
    # Convertir fechafac a datetime
    cosechas['fechafac'] = pd.to_datetime(cosechas['fechafac'], errors='coerce')
    
    # Crear máscara de nulos
    mask_nulos = cosechas['fechafac'].isnull()
    
    # Calcular fin del mes siguiente
    # ceiling_date(corte, unit = "month") %m+% months(1) - days(1)
    cosechas.loc[mask_nulos, 'fechafac'] = cosechas.loc[mask_nulos, 'corte'].apply(
        lambda x: (x + relativedelta(months=2)).replace(day=1) - pd.Timedelta(days=1)
        if pd.notna(x) else pd.NaT
    )
    
    nulos_despues = cosechas['fechafac'].isnull().sum()
    corregidos = nulos_antes - nulos_despues
    
    logger.info(f"   ✅ Fechas corregidas: {corregidos:,}")
    if nulos_despues > 0:
        logger.warning(f"   ⚠️  Aún quedan {nulos_despues:,} nulos (sin corte válido)")
    
    return cosechas


def crear_fechafac_ajustada(cosechas: pd.DataFrame) -> pd.DataFrame:
    """
    Crea columna fechafac_ajustada (un mes antes de fechafac).
    
    Según R (línea ~867):
    fechafac_ajustada = fechafac %m-% months(1)
    """
    if 'fechafac' not in cosechas.columns:
        logger.warning("   ⚠️  Columna 'fechafac' no existe")
        return cosechas
    
    # Convertir a datetime si no lo es
    cosechas['fechafac'] = pd.to_datetime(cosechas['fechafac'], errors='coerce')
    
    # Restar un mes
    cosechas['fechafac_ajustada'] = cosechas['fechafac'].apply(
        lambda x: x - relativedelta(months=1) if pd.notna(x) else pd.NaT
    )
    
    validos = cosechas['fechafac_ajustada'].notna().sum()
    logger.info(f"   ✅ fechafac_ajustada creada: {validos:,} valores válidos")
    
    return cosechas


def filtrar_registros_na(cosechas: pd.DataFrame) -> tuple:
    """
    Filtra registros que tienen "NA-" seguido de números en cedula_numero.
    
    Según R (líneas ~876-880):
    datos_con_NA <- Cosechas %>% filter(str_detect(cedula_numero, "^NA-\\d+"))
    Cosechas <- Cosechas %>% filter(!str_detect(cedula_numero, "^NA-\\d+"))
    """
    if 'cedula_numero' not in cosechas.columns:
        logger.warning("   ⚠️  Columna 'cedula_numero' no existe")
        return cosechas, pd.DataFrame()
    
    registros_antes = len(cosechas)
    
    # Identificar registros con "NA-\d+"
    mask_na = cosechas['cedula_numero'].astype(str).str.match(r'^NA-\d+', na=False)
    
    # Separar eliminados
    eliminados = cosechas[mask_na].copy()
    
    # Filtrar Cosechas
    cosechas = cosechas[~mask_na].copy()
    
    num_eliminados = len(eliminados)
    logger.info(f"   ✅ Filtro aplicado: {registros_antes:,} → {len(cosechas):,}")
    logger.info(f"   Eliminados con 'NA-': {num_eliminados:,}")
    
    return cosechas, eliminados