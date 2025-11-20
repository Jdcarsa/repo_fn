"""
Módulo para crear el DataFrame CRM (Customer Relationship Management).
Genera análisis de clientes con rangos y segmentaciones.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.crm')


def crear_crm(df_fnz007: pd.DataFrame, df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Crea el DataFrame CRM con análisis de cliente.
    
    Según R (líneas ~900-980):
    1. Partir de BaseFNZ
    2. Left join con BaseAC (primer registro por cedula_numero)
    3. Crear rangos de variables numéricas
    4. Calcular edad desde fs1nacfec
    
    Returns:
        DataFrame CRM procesado
    """
    logger.info("="*70)
    logger.info("👥 CREANDO CRM (Customer Relationship Management)")
    logger.info("="*70)
    logger.info("")
    
    # PASO 1: Copiar BaseFNZ
    logger.info("1️⃣ Copiando base FNZ007...")
    logger.info(f"   Columnas BaseFNZ007: {len(df_fnz007.columns)}")
    logger.info(f"   Muestra BaseFNZ007:\n{df_fnz007.head(10)}")
    CRM = df_fnz007.copy()
    registros_inicial = len(CRM)
    logger.info(f"Columnas en CRM inicial:{CRM.columns.tolist()}")
    logger.info(f"   ✅ CRM inicial: {registros_inicial:,} registros")
    
    # PASO 2: Join con BaseAC (primer registro por cedula_numero)
    logger.info("2️⃣ JOIN con Análisis de Cartera (primer registro)...")
    CRM = unir_con_ac_primer_registro(CRM, df_ac)
    logger.info("")
    
    # PASO 3: Calcular edad
    logger.info("3️⃣ Calculando edad desde fecha de nacimiento...")
    CRM = calcular_edad(CRM)
    logger.info("")
    
    # PASO 4: Crear rangos
    logger.info("4️⃣ Creando rangos de variables...")
    CRM = crear_todos_los_rangos(CRM)
    logger.info("")
    
    # Resumen final
    logger.info("="*70)
    logger.info("✅ CRM COMPLETADO")
    logger.info("="*70)
    logger.info(f"📊 Registros: {len(CRM):,}")
    logger.info(f"📊 Columnas: {len(CRM.columns)}")
    logger.info(f"📊 Nuevas columnas de rango: rango_ingresos, rango_avaluo, rango_monto, rango_gastos, rango_edad, rango_cuotas")
    logger.info("="*70)
    logger.info("")
    
    # --- Seleccionar solo las columnas que aparecen en el Excel del R ---
    columnas_r = [
        'numero', 'analista', 'fecha', 'ciudad', 'nomciudad', 'fs0vende', 'vennombre',
        'ccosto', 'cconombre', 'fs0montoap', 'cuotas', 'valor_tota', 'fs1sexo',
        'fs1nacfec', 'fs1estcvil', 'npercargo', 'vvdatipo', 'vvdaaval', 'ingresos',
        'gastos', 'nvescolar', 'corte2', 'act_lab', 'empresa', 'cargos', 'cedula',
        'corte', 'cedula_numero', 'valor', 'fechapag', 'valatras', 'saldofac',
        'cuotaatras', 'valorcuota', 'totcuotas', 'cuotaspag', 'rango_ingresos',
        'rango_avaluo', 'rango_monto', 'rango_gastos', 'edad', 'rango_edad',
        'rango_cuotas'
    ]

    # ✅ Usar CRM (mayúsculas)
    CRM = CRM.rename(columns={'nomciudad': 'ciudad'})

    if 'corte2' not in CRM.columns and 'corte' in CRM.columns:
        CRM['corte2'] = CRM['corte']

    cols_ok = [c for c in columnas_r if c in CRM.columns]
    CRM = CRM[cols_ok]

    logger.info(f"✅ CRM ajustado a columnas del R: {len(CRM.columns)} columnas")
    
    CRM = CRM.drop_duplicates()
    logger.info(f"   🗑️  Duplicados eliminados: {registros_inicial - len(CRM):,} filas")
    
    return CRM


def unir_con_ac_primer_registro(crm: pd.DataFrame, df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Une con BaseAC tomando el primer registro (fecha más antigua) por cedula_numero.
    
    Según R (líneas ~904-912):
    CRM <- CRM %>%
      left_join(
        BaseAC %>%
          group_by(cedula_numero) %>%
          slice_min(corte) %>%
          ungroup() %>%
          select(cedula_numero, fechapag, valatras, saldofac, cuotaatras, valorcuota, totcuotas, cuotaspag),
        by = "cedula_numero"
      )
    """
    if 'cedula_numero' not in df_ac.columns:
        logger.warning("   ⚠️  BaseAC no tiene cedula_numero")
        return crm
    
    registros_antes = len(crm)
    
    # Seleccionar columnas específicas
    columnas_ac = ['cedula_numero', 'corte', 'fechapag', 'valatras', 'saldofac', 
                   'cuotaatras', 'valorcuota', 'totcuotas', 'cuotaspag']
    
    # Verificar qué columnas existen
    columnas_disponibles = [col for col in columnas_ac if col in df_ac.columns]
    
    if 'cedula_numero' not in columnas_disponibles:
        logger.error("   ❌ BaseAC no tiene cedula_numero")
        return crm
    
    # Tomar primer registro por cedula_numero (fecha más antigua)
    df_ac_primer = (df_ac[columnas_disponibles]
                    .sort_values('corte')
                    .groupby('cedula_numero', as_index=False)
                    .first())
    
    # Eliminar columna corte (solo la usamos para ordenar)
    if 'corte' in df_ac_primer.columns:
        df_ac_primer = df_ac_primer.drop(columns=['corte'])
    
    # Left join
    crm = crm.merge(
        df_ac_primer,
        on='cedula_numero',
        how='left',
        suffixes=('', '_ac')
    )
    
    matches = crm['fechapag'].notna().sum() if 'fechapag' in crm.columns else 0
    porcentaje = (matches / len(crm)) * 100
    
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(crm):,} registros")
    logger.info(f"   Matches con AC: {matches:,} ({porcentaje:.1f}%)")
    
    return crm


def calcular_edad(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la edad a partir de la fecha de nacimiento.
    IGUAL QUE R: Usa semanas/52.25 en lugar de días/365.25
    """
    col_nacimiento = None
    for col in crm.columns:
        if 'nacfec' in col.lower() or 'fecha_nacimiento' in col.lower():
            col_nacimiento = col
            break

    if col_nacimiento is None:
        logger.warning("   ⚠️  No se encontró columna de fecha de nacimiento")
        return crm

    crm[col_nacimiento] = pd.to_datetime(crm[col_nacimiento], errors='coerce')

    fecha_actual = datetime.now()
    
    # ========================================
    # CAMBIO: Calcular edad usando SEMANAS (IGUAL QUE R)
    # ========================================
    # R: as.numeric(difftime(Sys.Date(), CRM$fs1nacfec, units = "weeks")) %/% 52.25
    
    diferencia_dias = (fecha_actual - crm[col_nacimiento]).dt.days
    diferencia_semanas = diferencia_dias / 7.0  # Convertir días a semanas
    edad_decimal = diferencia_semanas / 52.25    # Dividir por semanas en un año
    
    # Convertir a enteros (edad cumplida)
    crm['edad'] = np.floor(edad_decimal).astype('Int64')
    
    logger.debug(f"   Muestra de edades calculadas:\n{crm['edad'].head(50)}")

    validas = crm['edad'].notna().sum()
    edad_promedio = crm['edad'].mean()
    logger.info(f"   ✅ Edad calculada (usando semanas/52.25): {validas:,} registros válidos")
    logger.info(f"   📊 Edad promedio: {edad_promedio:.1f} años")
    
    return crm


def crear_todos_los_rangos(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea todos los rangos de variables numéricas.
    """
    # Desactivar notación científica
    pd.options.display.float_format = '{:.0f}'.format
    
    # 1. Rango de ingresos (intervalos de 1,000,000)
    crm = crear_rango_ingresos(crm)
    
    # 2. Rango de avalúo (intervalos de 50,000,000)
    crm = crear_rango_avaluo(crm)
    
    # 3. Rango de monto (intervalos de 1,000,000)
    crm = crear_rango_monto(crm)
    
    # 4. Rango de gastos (intervalos de 500,000)
    crm = crear_rango_gastos(crm)
    
    # 5. Rango de edad (intervalos de 5 años)
    crm = crear_rango_edad(crm)
    
    # 6. Rango de cuotas (intervalos de 3)
    crm = crear_rango_cuotas(crm)
    
    return crm


def crear_rango_ingresos(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_ingresos con intervalos de 1,000,000.
    
    Según R (líneas ~924-932):
    breaks <- seq(0, max(CRM$ingresos) + 1000000, by = 1000000)
    """
    col_ingresos = None
    for col in crm.columns:
        if 'ingreso' in col.lower():
            col_ingresos = col
            break
    
    if col_ingresos is None:
        logger.warning("   ⚠️  No se encontró columna de ingresos")
        return crm
    
    # Convertir a numérico
    crm[col_ingresos] = pd.to_numeric(crm[col_ingresos], errors='coerce')
    
    # Crear breaks
    valor_max = crm[col_ingresos].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Ingresos no tiene valores válidos")
        return crm
    
    breaks = np.arange(0, valor_max + 1000000, 1000000)
    labels = [f"{int(breaks[i]):,} - {int(breaks[i+1]):,}" for i in range(len(breaks)-1)]
    
    crm['rango_ingresos'] = pd.cut(
        crm[col_ingresos],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_ingresos'].nunique()
    logger.info(f"   ✅ rango_ingresos creado: {num_categorias} categorías")
    
    return crm


def crear_rango_avaluo(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_avaluo con intervalos de 50,000,000.
    """
    col_avaluo = None
    for col in crm.columns:
        if 'avaluo' in col.lower() or 'vvdaaval' in col.lower():
            col_avaluo = col
            break
    
    if col_avaluo is None:
        logger.warning("   ⚠️  No se encontró columna de avalúo")
        return crm
    
    crm[col_avaluo] = pd.to_numeric(crm[col_avaluo], errors='coerce')
    
    valor_max = crm[col_avaluo].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Avalúo no tiene valores válidos")
        return crm
    
    breaks = np.arange(0, valor_max + 50000000, 50000000)
    labels = [f"{int(breaks[i]):,} - {int(breaks[i+1]):,}" for i in range(len(breaks)-1)]
    
    crm['rango_avaluo'] = pd.cut(
        crm[col_avaluo],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_avaluo'].nunique()
    logger.info(f"   ✅ rango_avaluo creado: {num_categorias} categorías")
    
    return crm


def crear_rango_monto(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_monto con intervalos de 1,000,000.
    """
    col_monto = None
    for col in crm.columns:
        if 'valor' in col.lower() and 'total' in col.lower():
            col_monto = col
            break
    
    # Si no encontramos valor_total, buscar solo 'valor'
    if col_monto is None:
        for col in crm.columns:
            if col.lower() == 'valor' or 'monto' in col.lower():
                col_monto = col
                break
    
    if col_monto is None:
        logger.warning("   ⚠️  No se encontró columna de monto/valor")
        return crm
    
    crm[col_monto] = pd.to_numeric(crm[col_monto], errors='coerce')
    
    valor_max = crm[col_monto].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Monto no tiene valores válidos")
        return crm
    
    breaks = np.arange(0, valor_max + 1000000, 1000000)
    labels = [f"{int(breaks[i]):,} - {int(breaks[i+1]):,}" for i in range(len(breaks)-1)]
    
    crm['rango_monto'] = pd.cut(
        crm[col_monto],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_monto'].nunique()
    logger.info(f"   ✅ rango_monto creado: {num_categorias} categorías")
    
    return crm


def crear_rango_gastos(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_gastos con intervalos de 500,000.
    """
    col_gastos = None
    for col in crm.columns:
        if 'gasto' in col.lower():
            col_gastos = col
            break
    
    if col_gastos is None:
        logger.warning("   ⚠️  No se encontró columna de gastos")
        return crm
    
    crm[col_gastos] = pd.to_numeric(crm[col_gastos], errors='coerce')
    
    valor_max = crm[col_gastos].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Gastos no tiene valores válidos")
        return crm
    
    breaks = np.arange(0, valor_max + 500000, 500000)
    labels = [f"{int(breaks[i]):,} - {int(breaks[i+1]):,}" for i in range(len(breaks)-1)]
    
    crm['rango_gastos'] = pd.cut(
        crm[col_gastos],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_gastos'].nunique()
    logger.info(f"   ✅ rango_gastos creado: {num_categorias} categorías")
    
    return crm


def crear_rango_edad(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_edad con intervalos de 5 años.
    """
    if 'edad' not in crm.columns:
        logger.warning("   ⚠️  No se encontró columna edad")
        return crm
    
    valor_max = crm['edad'].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Edad no tiene valores válidos")
        return crm
    
    breaks = np.arange(18, valor_max + 5, 5)
    labels = [f"{int(breaks[i])} - {int(breaks[i+1])}" for i in range(len(breaks)-1)]
    
    crm['rango_edad'] = pd.cut(
        crm['edad'],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_edad'].nunique()
    logger.info(f"   ✅ rango_edad creado: {num_categorias} categorías")
    
    return crm


def crear_rango_cuotas(crm: pd.DataFrame) -> pd.DataFrame:
    """
    Crea rango_cuotas con intervalos de 3.
    """
    col_cuotas = None
    for col in crm.columns:
        if 'cuota' in col.lower() and 'total' in col.lower():
            col_cuotas = col
            break
    
    # Si no encontramos totcuotas, buscar solo 'cuotas'
    if col_cuotas is None:
        for col in crm.columns:
            if col.lower() == 'cuotas':
                col_cuotas = col
                break
    
    if col_cuotas is None:
        logger.warning("   ⚠️  No se encontró columna de cuotas")
        return crm
    
    crm[col_cuotas] = pd.to_numeric(crm[col_cuotas], errors='coerce')
    
    valor_max = crm[col_cuotas].max()
    if pd.isna(valor_max):
        logger.warning("   ⚠️  Cuotas no tiene valores válidos")
        return crm
    
    breaks = np.arange(6, valor_max + 3, 3)
    labels = [f"{int(breaks[i])} - {int(breaks[i+1])}" for i in range(len(breaks)-1)]
    
    crm['rango_cuotas'] = pd.cut(
        crm[col_cuotas],
        bins=breaks,
        labels=labels,
        include_lowest=True
    )
    
    num_categorias = crm['rango_cuotas'].nunique()
    logger.info(f"   ✅ rango_cuotas creado: {num_categorias} categorías")
    
    return crm