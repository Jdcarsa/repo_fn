"""
Módulo para crear el DataFrame de Comportamiento.
Pivota datos de mora por fecha de corte (versión horizontal).
"""
#SIRVE
import pandas as pd
import numpy as np
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.comportamiento')


def crear_comportamiento(df_ac: pd.DataFrame, 
                        df_fnz007: pd.DataFrame,
                        dict_auxiliares: dict = None) -> pd.DataFrame:
    """
    Crea el DataFrame de Comportamiento (versión horizontal).
    
    Según R (líneas ~990-1050):
    1. Seleccionar columnas de BaseAC
    2. Crear columna 'Mora' basada en diasatras
    3. Pivotar (pivot_wider) por corte
    4. Unir con valor desembolsado de BaseFNZ
    5. Unir con categorías DV/PT
    6. Llenar NA iniciales con puntos
    7. Reemplazar NA restantes con 'calificación'
    8. Convertir puntos a NA final
    
    Returns:
        DataFrame Comportamiento pivotado
    """
    logger.info("="*70)
    logger.info("📊 CREANDO COMPORTAMIENTO (Versión Horizontal)")
    logger.info("="*70)
    logger.info("")
    
    # PASO 1: Seleccionar columnas base de AC
    logger.info("1️⃣ Seleccionando columnas de Análisis de Cartera...")
    Comportamiento = seleccionar_columnas_ac(df_ac)
    logger.info("")
    
    # PASO 2: Crear columna Mora
    logger.info("2️⃣ Creando categorías de mora...")
    Comportamiento = crear_columna_mora(Comportamiento)
    logger.info("")
    
    # PASO 3: Pivotar datos
    logger.info("3️⃣ Pivotando datos por fecha de corte...")
    ComportamientoH = pivotar_comportamiento(Comportamiento)
    logger.info("")
    
    # PASO 4: Unir con valor desembolsado
    logger.info("4️⃣ Uniendo con valor desembolsado de FNZ007...")
    ComportamientoH = unir_valor_desembolsado(ComportamientoH, df_fnz007)
    logger.info("")
    
    # PASO 5: Unir con categorías
    if dict_auxiliares and 'categorias' in dict_auxiliares:
        logger.info("5️⃣ Uniendo con categorías DV/PT...")
        ComportamientoH = unir_categorias(ComportamientoH, dict_auxiliares['categorias'])
        logger.info("")
    else:
        logger.warning("5️⃣ Categorías no disponibles - Unión omitida")
        logger.info("")
    
    # PASO 6-8: Procesar columnas de mora
    logger.info("6️⃣ Procesando columnas de mora (llenar NAs)...")
    ComportamientoH = procesar_columnas_mora(ComportamientoH)
    logger.info("")
    
    # Resumen final
    logger.info("="*70)
    logger.info("✅ COMPORTAMIENTO COMPLETADO")
    logger.info("="*70)
    logger.info(f"📊 Registros: {len(ComportamientoH):,}")
    logger.info(f"📊 Columnas: {len(ComportamientoH.columns)}")
    
    # Contar columnas pivotadas
    cols_diasatras = [c for c in ComportamientoH.columns if c.startswith('diasatras_')]
    cols_mora = [c for c in ComportamientoH.columns if c.startswith('Mora_')]
    
    logger.info(f"📊 Columnas pivotadas:")
    logger.info(f"   • diasatras: {len(cols_diasatras)}")
    logger.info(f"   • Mora: {len(cols_mora)}")
    logger.info("="*70)
    logger.info("")
    
    fecha_cols = [c for c in ComportamientoH.columns if c.startswith('fechafac_')]
    for col in fecha_cols:
        if ComportamientoH[col].dtype == 'object' and isinstance(ComportamientoH[col].iloc[0], list):
            ComportamientoH[col] = ComportamientoH[col].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
            )

    columnas_r_orden = [
        'cedula_numero'
    ] + sorted([c for c in ComportamientoH.columns if c.startswith('diasatras_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('vlrini_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('fechafac_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('valatras_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('saldofac_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('valorcuota_')]) + \
    sorted([c for c in ComportamientoH.columns if c.startswith('Mora_')]) + \
    ['valor', 'calificacion']

    # Filtrar columnas que existen
    columnas_finales = [c for c in columnas_r_orden if c in ComportamientoH.columns]
    ComportamientoH = ComportamientoH[columnas_finales]

    logger.info(f"✅ Columnas ordenadas como R: {len(ComportamientoH.columns)} columnas")
    return ComportamientoH


def seleccionar_columnas_ac(df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Selecciona columnas específicas de Análisis de Cartera.
    
    Según R (línea ~992):
    Comportamiento <- BaseAC %>% select("cedula_numero", "diasatras", "corte", 
                                        "fechafac", "vlrini", "valatras", "saldofac", "valorcuota")
    """
    columnas_requeridas = [
        'cedula_numero', 'diasatras', 'corte', 'fechafac', 
        'vlrini', 'valatras', 'saldofac', 'valorcuota'
    ]
    
    # Verificar qué columnas existen
    columnas_disponibles = [col for col in columnas_requeridas if col in df_ac.columns]
    columnas_faltantes = set(columnas_requeridas) - set(columnas_disponibles)
    
    if columnas_faltantes:
        logger.warning(f"   ⚠️  Columnas faltantes: {columnas_faltantes}")
    
    # Seleccionar columnas disponibles
    Comportamiento = df_ac[columnas_disponibles].copy()
    
    logger.info(f"   ✅ Comportamiento base: {len(Comportamiento):,} registros, {len(Comportamiento.columns)} columnas")
    
    return Comportamiento


def crear_columna_mora(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna 'Mora' basada en días de atraso.
    
    Según R (líneas ~994-1005):
    A1: diasatras == 0
    A2: 0 < diasatras <= 30
    B1: 30 < diasatras <= 60
    B2: 60 < diasatras <= 90
    C1: 90 < diasatras <= 120
    C2: 120 < diasatras <= 150
    D1: 150 < diasatras <= 180
    D2: 180 < diasatras <= 210
    EE: diasatras > 210
    """
    if 'diasatras' not in df.columns:
        logger.warning("   ⚠️  Columna 'diasatras' no existe")
        return df
    
    # Asegurar que diasatras es numérico
    df['diasatras'] = pd.to_numeric(df['diasatras'], errors='coerce')
    
    # Crear condiciones
    conditions = [
        (df['diasatras'] == 0),
        (df['diasatras'] > 0) & (df['diasatras'] <= 30),
        (df['diasatras'] > 30) & (df['diasatras'] <= 60),
        (df['diasatras'] > 60) & (df['diasatras'] <= 90),
        (df['diasatras'] > 90) & (df['diasatras'] <= 120),
        (df['diasatras'] > 120) & (df['diasatras'] <= 150),
        (df['diasatras'] > 150) & (df['diasatras'] <= 180),
        (df['diasatras'] > 180) & (df['diasatras'] <= 210),
        (df['diasatras'] > 210)
    ]
    
    choices = ['A1', 'A2', 'B1', 'B2', 'C1', 'C2', 'D1', 'D2', 'EE']
    
    df['Mora'] = np.select(conditions, choices, default=None)
    
    # Estadísticas
    distribucion = df['Mora'].value_counts().sort_index()
    logger.info(f"   ✅ Columna 'Mora' creada")
    logger.info(f"   📊 Distribución:")
    for categoria, cantidad in distribucion.items():
        logger.info(f"      {categoria}: {cantidad:,} ({cantidad/len(df)*100:.1f}%)")
    
    return df


def pivotar_comportamiento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivota el DataFrame por fecha de corte.
    
    Según R (líneas ~1010-1017):
    ComportamientoH <- Comportamiento %>%
      pivot_wider(
        id_cols = c(cedula_numero),
        names_from = corte,
        values_from = c(diasatras, vlrini, fechafac, valatras, saldofac, valorcuota, Mora)
      )
    """
    if 'cedula_numero' not in df.columns or 'corte' not in df.columns:
        logger.error("   ❌ Faltan columnas requeridas para pivotar")
        return df
    
    registros_antes = len(df['cedula_numero'].unique())
    
    # Convertir corte a string para nombres de columnas
    df['corte'] = pd.to_datetime(df['corte']).dt.strftime('%Y-%m-%d')
    
    # Columnas a pivotar
    columnas_pivotar = ['diasatras', 'vlrini', 'fechafac', 'valatras', 'saldofac', 'valorcuota', 'Mora']
    columnas_disponibles = [col for col in columnas_pivotar if col in df.columns]
    
    logger.info(f"   📋 Pivotando {len(columnas_disponibles)} variables por corte...")
    
    # Pivotar
    df_pivotado = df.pivot_table(
        index='cedula_numero',
        columns='corte',
        values=columnas_disponibles,
        aggfunc='first'  # Si hay duplicados, tomar el primero
    )
    
    # Aplanar nombres de columnas
    if isinstance(df_pivotado.columns, pd.MultiIndex):
        df_pivotado.columns = ['_'.join(col).strip() for col in df_pivotado.columns.values]
    
    # Reset index
    df_pivotado = df_pivotado.reset_index()
    
    # ── formatear fechas pivoteadas ---------------------------------
    fecha_cols = [c for c in df_pivotado.columns if c.startswith('fechafac_')]
    for col in fecha_cols:
        df_pivotado[col] = (
            pd.to_datetime(df_pivotado[col], errors='coerce')
            .dt.strftime('%d/%m/%Y')        
        )
    
    registros_despues = len(df_pivotado)
    
    logger.info(f"   ✅ Pivote completado: {registros_antes:,} → {registros_despues:,} registros")
    logger.info(f"   📊 Columnas generadas: {len(df_pivotado.columns)}")
    
    return df_pivotado


def unir_valor_desembolsado(comportamiento_h: pd.DataFrame, 
                            df_fnz007: pd.DataFrame) -> pd.DataFrame:
    """
    Une con el valor desembolsado de BaseFNZ.
    
    Según R (líneas ~1020-1023):
    ComportamientoH <- ComportamientoH %>%
      left_join(BaseFNZ %>% select(cedula_numero, valor), by = "cedula_numero")
    """
    if 'cedula_numero' not in df_fnz007.columns:
        logger.warning("   ⚠️  FNZ007 no tiene cedula_numero")
        return comportamiento_h
    
    # Buscar columna de valor
    col_valor = None
    for col in df_fnz007.columns:
        if col.lower() == 'valor':
            col_valor = col
            break
    
    if col_valor is None:
        logger.warning("   ⚠️  FNZ007 no tiene columna 'valor'")
        return comportamiento_h
    
    # Seleccionar solo cedula_numero y valor (eliminar duplicados)
    df_valor = df_fnz007[['cedula_numero', col_valor]].drop_duplicates(subset=['cedula_numero'])
    
    registros_antes = len(comportamiento_h)
    
    # Left join
    comportamiento_h = comportamiento_h.merge(
        df_valor,
        on='cedula_numero',
        how='left',
        suffixes=('', '_fnz')
    )
    
    matches = comportamiento_h[col_valor].notna().sum()
    porcentaje = (matches / len(comportamiento_h)) * 100
    
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(comportamiento_h):,} registros")
    logger.info(f"   Matches con valor: {matches:,} ({porcentaje:.1f}%)")
    
    return comportamiento_h


def unir_categorias(comportamiento_h: pd.DataFrame, 
                   df_categorias: pd.DataFrame) -> pd.DataFrame:
    """
    Une con las categorías de calificación (DV/PT).
    
    Según R (líneas ~1034-1036):
    ComportamientoH <- ComportamientoH %>%
      left_join(Categorias %>% select("cedula_numero", "calificación"),
                by = c("cedula_numero"))
    """
    if 'cedula_numero' not in df_categorias.columns:
        logger.warning("   ⚠️  Categorías no tiene cedula_numero")
        return comportamiento_h
    
    # Buscar columna de calificación (case-insensitive)
    col_calificacion = None
    for col in df_categorias.columns:
        if 'calificacion' in col.lower() or 'calificación' in col.lower():
            col_calificacion = col
            break
    
    if col_calificacion is None:
        logger.warning("   ⚠️  Categorías no tiene columna de calificación")
        return comportamiento_h
    
    # Seleccionar columnas y eliminar duplicados
    df_cat = df_categorias[['cedula_numero', col_calificacion]].drop_duplicates(subset=['cedula_numero'])
    
    # Renombrar a 'calificacion' (sin tilde)
    df_cat = df_cat.rename(columns={col_calificacion: 'calificacion'})
    
    registros_antes = len(comportamiento_h)
    
    # Left join
    comportamiento_h = comportamiento_h.merge(
        df_cat,
        on='cedula_numero',
        how='left',
        suffixes=('', '_cat')
    )
    
    matches = comportamiento_h['calificacion'].notna().sum()
    porcentaje = (matches / len(comportamiento_h)) * 100
    
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(comportamiento_h):,} registros")
    logger.info(f"   Matches con categoría: {matches:,} ({porcentaje:.1f}%)")
    
    return comportamiento_h


def procesar_columnas_mora(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa columnas de mora según lógica del R.
    
    Según R (líneas ~1039-1059):
    1. Identificar columnas Mora_*
    2. Llenar NA iniciales (antes del primer valor) con '.'
    3. Reemplazar NA restantes con valor de 'calificacion'
    4. Convertir '.' de vuelta a NA
    """
    # Identificar columnas de Mora
    mora_columns = [col for col in df.columns if col.startswith('Mora_')]
    
    if not mora_columns:
        logger.warning("   ⚠️  No se encontraron columnas de Mora")
        return df
    
    logger.info(f"   📋 Procesando {len(mora_columns)} columnas de Mora...")
    
    # Ordenar columnas por fecha (alfabéticamente funciona porque formato es YYYY-MM-DD)
    mora_columns = sorted(mora_columns)
    
    # PASO 1: Llenar NA iniciales con '.'
    logger.info("   📝 Llenando NA iniciales con '.'...")
    df = llenar_na_iniciales(df, mora_columns)
    
    # PASO 2: Reemplazar NA restantes con calificación
    if 'calificacion' in df.columns:
        logger.info("   📝 Reemplazando NA restantes con calificación...")
        df = reemplazar_na_con_calificacion(df, mora_columns)
    else:
        logger.warning("   ⚠️  No hay columna 'calificacion' - Omitiendo reemplazo")
    
    # PASO 3: Convertir '.' de vuelta a NA
    logger.info("   📝 Convirtiendo '.' a NA...")
    df = convertir_puntos_a_na(df, mora_columns)
    
    logger.info("   ✅ Columnas de mora procesadas")
    
    return df


def llenar_na_iniciales(df: pd.DataFrame, mora_columns: list) -> pd.DataFrame:
    """
    Llena los NA que aparecen ANTES del primer valor no-NA con '.'.
    
    Según R (líneas ~1042-1051):
    for (i in 1:nrow(ComportamientoH)) {
      first_non_na_index <- min(which(!is.na(ComportamientoH[i, mora_columns])), na.rm = TRUE)
      if (!is.na(first_non_na_index) && first_non_na_index > 1) {
        ComportamientoH[i, mora_columns[1:(first_non_na_index - 1)]] <- "."
      }
    }
    """
    for idx in df.index:
        # Obtener valores de Mora para esta fila
        valores = df.loc[idx, mora_columns]
        
        # Encontrar índice del primer no-NA
        no_na_indices = valores.notna()
        
        if no_na_indices.any():
            primer_no_na = no_na_indices.idxmax()  # Primera posición con True
            posicion = mora_columns.index(primer_no_na)
            
            if posicion > 0:
                # Llenar columnas anteriores con '.'
                columnas_a_llenar = mora_columns[:posicion]
                df.loc[idx, columnas_a_llenar] = '.'
    
    return df


def reemplazar_na_con_calificacion(df: pd.DataFrame, mora_columns: list) -> pd.DataFrame:
    """
    Reemplaza NA restantes con el valor de 'calificacion'.
    
    Según R (líneas ~1054-1058):
    ComportamientoH <- ComportamientoH %>%
      mutate(across(all_of(mora_columns), as.character)) %>%
      rowwise() %>%
      mutate(across(all_of(mora_columns), ~ coalesce(., as.character(calificación)))) %>%
      ungroup()
    """
    # Convertir columnas de Mora a string
    df[mora_columns] = df[mora_columns].astype(str)
    
    # Convertir calificacion a string
    df['calificacion'] = df['calificacion'].astype(str)
    
    # Reemplazar 'nan' con calificación
    for col in mora_columns:
        mask = df[col].isin(['nan', 'None', ''])
        df.loc[mask, col] = df.loc[mask, 'calificacion']
    
    return df


def convertir_puntos_a_na(df: pd.DataFrame, mora_columns: list) -> pd.DataFrame:
    """
    Convierte puntos ('.') de vuelta a NA.
    
    Según R (línea ~1061):
    ComportamientoH <- ComportamientoH %>%
      mutate(across(all_of(mora_columns), ~ na_if(., ".")))
    """
    for col in mora_columns:
        df[col] = df[col].replace('.', np.nan)
    
    return df

