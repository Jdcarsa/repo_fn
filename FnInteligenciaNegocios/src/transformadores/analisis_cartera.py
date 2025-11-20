import pandas as pd
import numpy as np
from .base import logger, convertir_columnas_minusculas, crear_llave_cedula_numero, eliminar_columnas
#NOMBRE DIAS ATRAS ESTA MAL
def filtrar_finansuenos_ac(df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra el DataFrame de Análisis de Cartera para incluir solo registros
    donde la columna 'reg' es 'FINANSUEÑOS'.
    """
    logger.info("Aplicando filtro 'FINANSUEÑOS' al DataFrame de Análisis de Cartera...")
    registros_antes = len(df_ac)
    
    if 'reg' in df_ac.columns:
        df_filtrado = df_ac[df_ac['reg'] == 'FINANSUEÑOS'].copy()
        logger.info(f"Registros antes: {registros_antes:,}, Registros después: {len(df_filtrado):,}")
        return df_filtrado
    else:
        logger.warning("La columna 'reg' no se encontró. No se aplicó el filtro.")
        return df_ac

def procesar_analisis_cartera(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y preparación inicial del dataframe de Análisis de Cartera.
    """
    logger.info("Iniciando limpieza y procesamiento de Análisis de Cartera...")
    df_proc = df.copy()
    
    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "Análisis de Cartera")
    logger.info(f"Columnas después de convertir a minúsculas: {df_proc.columns.tolist()}")
    # 2. LIMPIAR cedula y numero ANTES de crear la llave
    from .base import limpiar_columnas_numericas_como_string
    df_proc = limpiar_columnas_numericas_como_string(df_proc, ['cedula', 'numero'])

    # 3. Eliminar columnas innecesarias
    columnas_a_eliminar = [
        'reg', 'clase', 'tipo', 'nombre', 'telefono', 'ultpago', 'fechanov', 
        'fechacom', 'totfactura', 'intepagado', 'interespdte', 'direccion', 
        'barrio', 'generar', 'cobra', 'empresa', 'solnum', 'mcncuenta',
        'peripago', 'vinempres', 'soltip', 'tiponov', 'codnov'
    ]
    
    df_proc = eliminar_columnas(df_proc, columnas_a_eliminar, "Análisis de Cartera")

    # 4. Crear la llave 'cedula_numero'
    df_proc = crear_llave_cedula_numero(df_proc, 'cedula', 'numero')

    logger.info("Limpieza y procesamiento de Análisis de Cartera completado.")
    return df_proc

def manejar_duplicados_ac(df: pd.DataFrame) -> pd.DataFrame:
    """
    Maneja los registros duplicados en el DataFrame de Análisis de Cartera.
    IGUAL QUE R: Solo agrupa los duplicados, mantiene únicos intactos.
    """
    logger.info("Manejando duplicados en Análisis de Cartera...")
    if 'cedula_numero' not in df.columns or 'corte' not in df.columns:
        logger.warning("No se encontraron 'cedula_numero' o 'corte'. No se aplicará manejo de duplicados.")
        return df

    registros_antes = len(df)
    
    # ========================================
    # PASO 1: Identificar duplicados (IGUAL QUE R)
    # ========================================
    duplicados_info = df.groupby(['cedula_numero', 'corte']).size().reset_index(name='n')
    duplicados_info = duplicados_info[duplicados_info['n'] > 1]
    
    num_duplicados = len(duplicados_info)
    logger.info(f"   📊 Grupos duplicados encontrados: {num_duplicados:,}")
    
    if num_duplicados == 0:
        logger.info("   ✅ No hay duplicados - DataFrame sin cambios")
        return df
    
    # ========================================
    # PASO 2: Separar duplicados de únicos
    # ========================================
    # Crear máscara de duplicados
    df_con_marcador = df.merge(
        duplicados_info[['cedula_numero', 'corte']],
        on=['cedula_numero', 'corte'],
        how='left',
        indicator=True
    )
    
    # Registros únicos (NO están en duplicados)
    df_unicos = df_con_marcador[df_con_marcador['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    # Registros duplicados (SÍ están en duplicados)
    df_duplicados = df_con_marcador[df_con_marcador['_merge'] == 'both'].drop(columns=['_merge'])
    
    logger.info(f"   📊 Registros únicos: {len(df_unicos):,}")
    logger.info(f"   📊 Registros duplicados a agrupar: {len(df_duplicados):,}")
    
    # ========================================
    # PASO 3: Agrupar SOLO los duplicados
    # ========================================
    # Identificar columnas numéricas y no numéricas
    columnas_numericas = df_duplicados.select_dtypes(include=np.number).columns.tolist()
    columnas_no_numericas = df_duplicados.select_dtypes(exclude=np.number).columns.tolist()

    # Crear diccionario de agregación
    agg_dict = {}
    for col in columnas_numericas:
        agg_dict[col] = 'sum'
    
    for col in columnas_no_numericas:
        if col not in ['cedula_numero', 'corte']:
            agg_dict[col] = 'first'

    # Agrupar duplicados
    df_duplicados_agrupados = df_duplicados.groupby(['cedula_numero', 'corte'], as_index=False).agg(agg_dict)
    
    logger.info(f"   📊 Duplicados agrupados: {len(df_duplicados):,} → {len(df_duplicados_agrupados):,}")
    
    # ========================================
    # PASO 4: Reconstruir DataFrame (IGUAL QUE R)
    # ========================================
    # Combinar únicos + duplicados agrupados
    df_final = pd.concat([df_unicos, df_duplicados_agrupados], ignore_index=True)
    
    # Reordenar columnas para que coincidan con el original
    columnas_finales = [col for col in df.columns if col in df_final.columns]
    df_final = df_final[columnas_finales]
    
    registros_despues = len(df_final)
    logger.info(f"   ✅ DataFrame reconstruido: {registros_antes:,} → {registros_despues:,}")
    
    return df_final


def crear_columna_mora_ac(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna 'mora' en el DataFrame de Análisis de Cartera
    basado en los valores de la columna 'diasatras'.
    """
    logger.info("Creando la columna 'mora'...")
    if 'diasatras' not in df.columns:
        logger.warning("La columna 'diasatras' no se encuentra. No se puede crear 'mora'.")
        return df

    df_mora = df.copy()
    df_mora['diasatras'] = pd.to_numeric(df_mora['diasatras'], errors='coerce')
    
    conditions = [
        (df_mora['diasatras'] == 0),
        (df_mora['diasatras'] > 0) & (df_mora['diasatras'] <= 30),
        (df_mora['diasatras'] > 30) & (df_mora['diasatras'] <= 60),
        (df_mora['diasatras'] > 60) & (df_mora['diasatras'] <= 90),
        (df_mora['diasatras'] > 90) & (df_mora['diasatras'] <= 120),
        (df_mora['diasatras'] > 120) & (df_mora['diasatras'] <= 150),
        (df_mora['diasatras'] > 150) & (df_mora['diasatras'] <= 180),
        (df_mora['diasatras'] > 180) & (df_mora['diasatras'] <= 210),
        (df_mora['diasatras'] > 210)
    ]

    choices = ['A1', 'A2', 'B1', 'B2', 'C1', 'C2', 'D1', 'D2', 'EE']
    df_mora['mora'] = np.select(conditions, choices, default=None)

    no_categorizados = df_mora['mora'].isnull().sum()
    if no_categorizados > 0:
        logger.warning(f"{no_categorizados} registros no pudieron ser categorizados en 'mora'.")

    logger.info("Columna 'mora' creada exitosamente.")
    return df_mora