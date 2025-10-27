"""
Transformador completo para FNZ007.
Incluye todas las limpiezas y correcciones del script R.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from .base import logger, convertir_columnas_minusculas, eliminar_columnas


def procesar_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación completa del dataframe FNZ007.
    
    Pasos:
    1. Dividir columna DESEMBOLSO y filtrar
    2. Limpiar outliers (fecha nacimiento, gastos, ingresos, avalúo)
    3. Convertir columnas a minúsculas
    4. Unificar columnas de empleo
    5. Categorizar variables
    6. Eliminar columnas innecesarias
    7. Renombrar columnas
    """
    logger.info("="*70)
    logger.info("🔄 TRANSFORMACIÓN FNZ007")
    logger.info("="*70)
    
    df_proc = df.copy()
    registros_iniciales = len(df_proc)
    
    # PASO 1: Dividir DESEMBOLSO y filtrar
    df_proc = dividir_y_filtrar_desembolso(df_proc)
    
    # PASO 2: Limpiar outliers ANTES de otras transformaciones
    df_proc = limpiar_outliers_fnz007(df_proc)
    
    # PASO 3: Convertir a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "FNZ007")
    
    # PASO 4: Unificar columnas de empleo
    df_proc = unificar_columnas_empleo(df_proc)
    
    # PASO 5: Categorizar variables
    df_proc = categorizar_estado_civil(df_proc)
    df_proc = categorizar_nivel_escolar(df_proc)
    
    # PASO 6: Eliminar columnas innecesarias
    df_proc = eliminar_columnas_fnz007(df_proc)
    
    # PASO 7: Renombrar variables
    df_proc = renombrar_columnas_fnz007(df_proc)
    
    # Resumen final
    logger.info("")
    logger.info("="*70)
    logger.info("✅ FNZ007 TRANSFORMADO")
    logger.info("="*70)
    logger.info(f"Registros: {registros_iniciales:,} → {len(df_proc):,}")
    logger.info(f"Columnas: {len(df_proc.columns)}")
    logger.info("="*70)
    logger.info("")
    
    return df_proc


def dividir_y_filtrar_desembolso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Divide la columna DESEMBOLSO en DF y NUMERO, luego filtra por DF == "DF".
    
    Código R:
    BaseFNZ <- separate(BaseFNZ, DESEMBOLSO, into = c("DF", "NUMERO"), sep = "-")
    BaseFNZ <- BaseFNZ %>% filter(DF == "DF")
    """
    logger.info("\n📋 PASO 1: División y filtro de DESEMBOLSO")
    
    if 'DESEMBOLSO' not in df.columns:
        logger.warning("⚠️  Columna 'DESEMBOLSO' no encontrada. Saltando este paso.")
        return df
    
    registros_antes = len(df)
    df_proc = df.copy()
    
    # Dividir columna DESEMBOLSO
    try:
        # Convertir a string y dividir por '-'
        df_proc['DESEMBOLSO'] = df_proc['DESEMBOLSO'].astype(str)
        
        # Dividir en dos columnas
        split_data = df_proc['DESEMBOLSO'].str.split('-', n=1, expand=True)
        df_proc['DF'] = split_data[0] if 0 in split_data.columns else None
        df_proc['NUMERO'] = split_data[1] if 1 in split_data.columns else None
        
        logger.info(f"✅ Columna DESEMBOLSO dividida en 'DF' y 'NUMERO'")
        
        # Filtrar solo donde DF == "DF"
        df_proc = df_proc[df_proc['DF'] == 'DF'].copy()
        
        registros_despues = len(df_proc)
        eliminados = registros_antes - registros_despues
        
        logger.info(f"✅ Filtro aplicado (DF == 'DF'): {registros_antes:,} → {registros_despues:,}")
        logger.info(f"   Eliminados: {eliminados:,}")
        
    except Exception as e:
        logger.error(f"❌ Error dividiendo DESEMBOLSO: {e}")
    
    return df_proc


def limpiar_outliers_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia outliers en variables numéricas críticas usando el método IQR.
    Genera valores aleatorios dentro de rangos válidos para reemplazar outliers.
    """
    logger.info("\n🔧 PASO 2: Limpieza de OUTLIERS")
    
    df_proc = df.copy()
    
    # 1. Fecha de Nacimiento
    df_proc = corregir_outliers_fecha_nacimiento(df_proc)
    
    # 2. Gastos
    df_proc = corregir_outliers_gastos(df_proc)
    
    # 3. Ingresos
    df_proc = corregir_outliers_ingresos(df_proc)
    
    # 4. Avalúo de Vivienda
    df_proc = corregir_outliers_avaluo(df_proc)
    
    return df_proc


def corregir_outliers_fecha_nacimiento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige outliers en fecha de nacimiento.
    Rango válido: 1944-01-01 a 2005-12-31
    """
    columna = 'FS1NACFEC'
    
    if columna not in df.columns:
        logger.warning(f"⚠️  Columna '{columna}' no encontrada")
        return df
    
    df_proc = df.copy()
    registros_antes = len(df_proc)
    
    # Convertir a datetime
    df_proc[columna] = pd.to_datetime(df_proc[columna], errors='coerce')
    
    # Definir rangos
    fecha_min = pd.Timestamp('1944-01-01')
    fecha_max = pd.Timestamp('2005-12-31')
    
    # Calcular IQR
    Q1 = df_proc[columna].quantile(0.25)
    Q3 = df_proc[columna].quantile(0.75)
    IQR = Q3 - Q1
    
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR
    
    # Ajustar límites al rango válido
    limite_inferior = max(limite_inferior, fecha_min)
    limite_superior = min(limite_superior, fecha_max)
    
    # Identificar outliers y nulos
    mask_outliers = (df_proc[columna] < limite_inferior) | (df_proc[columna] > limite_superior)
    mask_nulos = df_proc[columna].isnull()
    mask_corregir = mask_outliers | mask_nulos
    
    num_outliers = mask_outliers.sum()
    num_nulos = mask_nulos.sum()
    total_corregir = mask_corregir.sum()
    
    if total_corregir > 0:
        # Generar fechas aleatorias dentro del rango válido
        fechas_aleatorias = pd.to_datetime(
            np.random.randint(
                limite_inferior.value,
                limite_superior.value,
                size=total_corregir
            )
        )
        
        df_proc.loc[mask_corregir, columna] = fechas_aleatorias
        
        logger.info(f"  ✅ {columna}: {total_corregir:,} valores corregidos")
        logger.info(f"     Outliers: {num_outliers:,}, Nulos: {num_nulos:,}")
    else:
        logger.info(f"  ✅ {columna}: Sin outliers")
    
    return df_proc


def corregir_outliers_numericos(df: pd.DataFrame, columna: str, 
                                min_val: float, max_val: float) -> pd.DataFrame:
    """
    Función genérica para corregir outliers en columnas numéricas.
    """
    if columna not in df.columns:
        logger.warning(f"⚠️  Columna '{columna}' no encontrada")
        return df
    
    df_proc = df.copy()
    
    # Convertir a numérico
    df_proc[columna] = pd.to_numeric(df_proc[columna], errors='coerce')
    
    # Calcular IQR
    Q1 = df_proc[columna].quantile(0.25)
    Q3 = df_proc[columna].quantile(0.75)
    IQR = Q3 - Q1
    
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR
    
    # Ajustar a rangos válidos del negocio
    limite_inferior = max(limite_inferior, min_val)
    limite_superior = min(limite_superior, max_val)
    
    # Identificar outliers y nulos
    mask_outliers = (df_proc[columna] < limite_inferior) | (df_proc[columna] > limite_superior)
    mask_nulos = df_proc[columna].isnull()
    mask_corregir = mask_outliers | mask_nulos
    
    total_corregir = mask_corregir.sum()
    
    if total_corregir > 0:
        # Generar valores aleatorios dentro del rango
        valores_aleatorios = np.random.uniform(
            limite_inferior,
            limite_superior,
            size=total_corregir
        )
        
        df_proc.loc[mask_corregir, columna] = valores_aleatorios
        
        logger.info(f"  ✅ {columna}: {total_corregir:,} valores corregidos")
        logger.info(f"     Rango: ${limite_inferior:,.0f} - ${limite_superior:,.0f}")
    else:
        logger.info(f"  ✅ {columna}: Sin outliers")
    
    return df_proc


def corregir_outliers_gastos(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige outliers en GASTOS. Rango: 100,000 - 1,000,000"""
    return corregir_outliers_numericos(df, 'GASTOS', 100000, 1000000)


def corregir_outliers_ingresos(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige outliers en INGRESOS. Rango: 1,300,000 - 3,000,000"""
    return corregir_outliers_numericos(df, 'INGRESOS', 1300000, 3000000)


def corregir_outliers_avaluo(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige outliers en VVDAAVAL. Rango: 50,000,000 - 300,000,000"""
    return corregir_outliers_numericos(df, 'VVDAAVAL', 50000000, 300000000)


def unificar_columnas_empleo(df: pd.DataFrame) -> pd.DataFrame:
    """Unifica columnas de información laboral."""
    logger.info("\n📋 PASO 4: Unificación de columnas de empleo")
    
    df_proc = df.copy()
    
    # ACT_LAB
    if 'ocupacion' in df_proc.columns and 'indpactivi' in df_proc.columns:
        df_proc['act_lab'] = (
            df_proc['ocupacion'].fillna('').astype(str) + ' ' + 
            df_proc['indpactivi'].fillna('').astype(str)
        ).str.strip()
        logger.info("  ✅ Creada 'act_lab'")
    
    # EMPRESA
    if 'lbempresa' in df_proc.columns and 'indprzsoci' in df_proc.columns:
        df_proc['empresa'] = (
            df_proc['lbempresa'].fillna('').astype(str) + ' ' + 
            df_proc['indprzsoci'].fillna('').astype(str)
        ).str.strip()
        logger.info("  ✅ Creada 'empresa'")
    
    # CARGOS
    if 'cargo' in df_proc.columns and 'indpnombre' in df_proc.columns:
        df_proc['cargos'] = (
            df_proc['cargo'].fillna('').astype(str) + ' ' + 
            df_proc['indpnombre'].fillna('').astype(str)
        ).str.strip()
        logger.info("  ✅ Creada 'cargos'")
    
    return df_proc


def categorizar_estado_civil(df: pd.DataFrame) -> pd.DataFrame:
    """Categoriza estado civil agrupando variantes."""
    logger.info("\n📋 PASO 5a: Categorización de estado civil")
    
    if 'fs1estcvil' not in df.columns:
        logger.warning("  ⚠️  Columna 'fs1estcvil' no encontrada")
        return df
    
    df_proc = df.copy()
    
    replacements = {
        'Divorciado': 'Soltero',
        'Separado': 'Soltero',
        'Viudo': 'Soltero',
        '.': 'Soltero'
    }
    
    registros_antes = df_proc['fs1estcvil'].value_counts()
    df_proc['fs1estcvil'] = df_proc['fs1estcvil'].replace(replacements)
    registros_despues = df_proc['fs1estcvil'].value_counts()
    
    logger.info("  ✅ Estado civil categorizado")
    logger.info(f"     Categorías finales: {list(registros_despues.index)}")
    
    return df_proc


def categorizar_nivel_escolar(df: pd.DataFrame) -> pd.DataFrame:
    """Categoriza nivel escolar agrupando similares."""
    logger.info("\n📋 PASO 5b: Categorización de nivel escolar")
    
    if 'nvescolar' not in df.columns:
        logger.warning("  ⚠️  Columna 'nvescolar' no encontrada")
        return df
    
    df_proc = df.copy()
    
    replacements = {
        'Especialización': 'Educacion superior',
        'Especializaci¾n': 'Educacion superior',
        'Maestría': 'Educacion superior',
        'MaestrÝa': 'Educacion superior',
        'Postdoctorado': 'Educacion superior',
        'Universitario': 'Educacion superior',
        'Doctorado': 'Educacion superior',
        'Técnico': 'Tecnico o Tecnologo',
        'TÚcnico': 'Tecnico o Tecnologo',
        'Tecnólogo': 'Tecnico o Tecnologo',
        'Tecn¾logo': 'Tecnico o Tecnologo'
    }
    
    df_proc['nvescolar'] = df_proc['nvescolar'].replace(replacements)
    
    categorias_finales = df_proc['nvescolar'].value_counts()
    logger.info("  ✅ Nivel escolar categorizado")
    logger.info(f"     Categorías finales: {len(categorias_finales)}")
    
    return df_proc


def eliminar_columnas_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas innecesarias de FNZ007."""
    logger.info("\n📋 PASO 6: Eliminación de columnas innecesarias")
    
    # Columnas unificadas (ya creamos act_lab, empresa, cargos)
    columnas_unificadas = [
        'ocupacion', 'indpactivi', 'lbempresa', 
        'indprzsoci', 'cargo', 'indpnombre'
    ]
    
    # Otras columnas a eliminar
    otras_columnas = [
        'clase', 'tipo', 'estado', 'df', 'pagare', 
        'apellidos', 'nombres', 'telefono1', 'movil', 
        'direccion', 'codbarrio', 'barrio', 'fs0nota', 'fs1email',
        'desembolso'  # Ya la dividimos
    ]
    
    todas_columnas = columnas_unificadas + otras_columnas
    
    df_proc = eliminar_columnas(df, todas_columnas, "FNZ007")
    
    return df_proc


def renombrar_columnas_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas según lógica de R."""
    logger.info("\n📋 PASO 7: Renombrado de columnas")
    
    df_proc = df.copy()
    
    renames = {
        'ciudad': 'nomciudad',
        'codciudad': 'ciudad'
    }
    
    columnas_renombradas = {k: v for k, v in renames.items() if k in df_proc.columns}
    
    if columnas_renombradas:
        df_proc.rename(columns=columnas_renombradas, inplace=True)
        logger.info(f"  ✅ Columnas renombradas: {columnas_renombradas}")
    else:
        logger.info("  ℹ️  No se encontraron columnas para renombrar")
    
    return df_proc