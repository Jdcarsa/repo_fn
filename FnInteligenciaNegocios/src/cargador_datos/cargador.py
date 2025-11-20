"""
Módulo para la carga de datos desde los archivos originales.
Incluye correcciones y limpiezas necesarias durante la carga.
"""

import pandas as pd
from pathlib import Path
from config.config_loader import ConfigLoader
from src.utilidades.logger import configurar_logger

logger = configurar_logger("finnovarisk.carga_datos")
CONFIG_LOADER = ConfigLoader()
CONFIG_CARGA = CONFIG_LOADER.get_config()


def limpiar_identificadores_al_cargar(df: pd.DataFrame, nombre_dataset: str) -> pd.DataFrame:
    """
    Limpia columnas de identificadores al momento de cargar.
    Convierte floats como 1007518.0 a strings "1007518"
    """
    # Columnas que deben ser limpiadas en TODOS los datasets
    columnas_identificadores = [
        'cedula', 'cc_nit', 'nit', 'vinculado', 'identificacion',
        'numero', 'dsm_num', 'ds_numero', 'mcnnumcru2', 'obligacion'
    ]
    
    columnas_limpiadas = []
    
    for col in df.columns:
        # Buscar columnas que coincidan (case-insensitive)
        col_lower = col.lower()
        
        if any(identificador in col_lower for identificador in columnas_identificadores):
            # Esta columna necesita limpieza
            if df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                # Convertir a string limpio
                df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) and x == int(x) else str(x) if pd.notna(x) else "")
                columnas_limpiadas.append(col)
    
    if columnas_limpiadas:
        logger.info(f"  🧹 Limpiadas {len(columnas_limpiadas)} columnas de identificadores")
        logger.debug(f"     Columnas: {columnas_limpiadas}")
    
    return df

def cargar_excel_con_config(ruta: Path, hojas_config: dict, 
                            nombre_dataset: str = "") -> list[pd.DataFrame]:
    """
    Carga las hojas especificadas de un archivo Excel.
    Aplica correcciones específicas según el dataset y la hoja.
    
    Args:
        ruta: Ruta al archivo Excel
        hojas_config: Dict con {nombre_hoja: fecha_corte}
        nombre_dataset: Nombre del dataset para aplicar correcciones específicas
        
    Returns:
        Lista de DataFrames cargados
    """
    dataframes = []
    if not ruta.exists():
        logger.error(f"❌ Archivo no encontrado: {ruta}")
        return dataframes

    try:
        xls = pd.ExcelFile(ruta)
        
        for hoja, fecha_str in hojas_config.items():
            if hoja not in xls.sheet_names:
                logger.warning(f"⚠️  Hoja '{hoja}' no encontrada en {ruta.name}")
                continue
            
            # Cargar la hoja
            df = pd.read_excel(xls, sheet_name=hoja)
            logger.info(f"📄 Cargando hoja: {hoja} - {len(df):,} registros")
            # LIMPIAR COLUMNAS QUE DEBERÍAN SER STRING (cedula, numero, nit, etc)
            df = limpiar_identificadores_al_cargar(df, nombre_dataset)
            # === CORRECCIONES ESPECÍFICAS DURANTE LA CARGA ===
            
            # CORRECCIÓN 1: FNZ007 - Eliminar columnas 43-88 en ciertos cortes
            if nombre_dataset == "FNZ007":
                df = corregir_columnas_fnz007(df, hoja, ruta.name)
            
            # CORRECCIÓN 2: FNZ007 - Corregir formato de fecha de nacimiento
            if nombre_dataset == "FNZ007":
                df = corregir_fecha_nacimiento_fnz007(df, hoja)
            
            # CORRECCIÓN 3: ANALISIS_CARTERA - Eliminar 'fechadoc' en ciertos cortes
            if nombre_dataset == "ANALISIS_CARTERA":
                df = corregir_fechadoc_ac(df, hoja)
            
            # Agregar fecha de corte si existe
            if fecha_str is not None:
                df["corte"] = pd.to_datetime(fecha_str)
            
            dataframes.append(df)
            logger.info(f"  ✅ Hoja procesada: {len(df):,} registros, {len(df.columns)} columnas")
    
    except Exception as e:
        logger.exception(f"❌ Error cargando {ruta}: {e}")
    
    return dataframes


def corregir_columnas_fnz007(df: pd.DataFrame, hoja: str, archivo: str) -> pd.DataFrame:
    """
    Elimina columnas extras (43-88) en ciertos cortes de FNZ007.
    
    Según R:
    abr24, jul24, jun24, may24 tienen columnas extras que deben eliminarse.
    """
    # Identificar cortes problemáticos
    cortes_con_columnas_extras = [
        "ABRIL 24", "MAYO 24", "JUNIO 24", "JULIO 24"
    ]
    
    if hoja in cortes_con_columnas_extras:
        num_columnas_antes = len(df.columns)
        
        # Verificar si hay más de 88 columnas
        if num_columnas_antes > 88:
            # Eliminar columnas 43-88 (índices 42-87 en Python)
            columnas_a_mantener = list(range(0, 42)) + list(range(88, num_columnas_antes))
            df = df.iloc[:, columnas_a_mantener]
            
            num_columnas_despues = len(df.columns)
            columnas_eliminadas = num_columnas_antes - num_columnas_despues
            
            logger.info(f"  🔧 {hoja}: Eliminadas {columnas_eliminadas} columnas extras (43-88)")
            logger.info(f"     Columnas: {num_columnas_antes} → {num_columnas_despues}")
        else:
            logger.info(f"  ℹ️  {hoja}: No tiene columnas extras para eliminar")
    
    return df


def corregir_fecha_nacimiento_fnz007(df: pd.DataFrame, hoja: str) -> pd.DataFrame:
    """
    Corrige el formato de la columna FS1NACFEC en ciertos cortes.
    
    Según R:
    dic23$FS1NACFEC <- as.POSIXct(dic23$FS1NACFEC, format = "%Y-%m-%d %H:%M:%S")
    ago24$FS1NACFEC <- as.POSIXct(ago24$FS1NACFEC, format = "%Y-%m-%d %H:%M:%S")
    """
    cortes_con_formato_diferente = ["DICIEMBRE 23", "AGOSTO 24"]
    
    if hoja in cortes_con_formato_diferente:
        if 'FS1NACFEC' in df.columns:
            try:
                # Convertir a datetime con formato específico
                df['FS1NACFEC'] = pd.to_datetime(
                    df['FS1NACFEC'], 
                    format='%Y-%m-%d %H:%M:%S',
                    errors='coerce'
                )
                
                nulos = df['FS1NACFEC'].isnull().sum()
                logger.info(f"  🔧 {hoja}: Fecha nacimiento corregida ({nulos} nulos)")
            except Exception as e:
                logger.warning(f"  ⚠️  {hoja}: No se pudo corregir fecha nacimiento: {e}")
    
    return df


def corregir_fechadoc_ac(df: pd.DataFrame, hoja: str) -> pd.DataFrame:
    """
    Corrige columnas según el corte temporal en Análisis de Cartera.
    
    Correcciones:
    1. Elimina 'fechadoc' en ciertos cortes específicos
    2. Normaliza nombres de columna de días de atraso (busca todas las variaciones)
    
    Maneja variaciones:
    - 'DIAS ATRASO', 'DIAS ATRASOS', 'diasatras', 'Dias Atraso', etc.
    - Búsqueda case-insensitive
    - Con/sin tildes, con/sin espacios
    """
    # Cortes que requieren eliminación de fechadoc
    cortes_con_fechadoc = ["AGOSTO 24", "FEBRERO 25", "MARZO 25", "SEPTIEMBRE 25"]
    cortes_con_FACTURA = ['OCTUBRE 25']
    # === CORRECCIÓN 1: ELIMINAR FECHADOC ===
    hoja_upper = hoja.upper().strip()
    
    if hoja_upper in [corte.upper() for corte in cortes_con_fechadoc]:
        # Buscar columna fechadoc (case-insensitive)
        cols_fechadoc = [col for col in df.columns if col.lower() == 'fechadoc']
        
        if cols_fechadoc:
            df = df.drop(columns=cols_fechadoc)
            logger.info(f"  🔧 {hoja}: Eliminada columna '{cols_fechadoc[0]}'")
        else:
            logger.info(f"  ℹ️  {hoja}: No tiene columna 'fechadoc'")
    
    if hoja_upper in [corte.upper() for corte in cortes_con_FACTURA]:
        
        # Buscar columna FACTURA (case-insensitive)
        cols_factura = [col for col in df.columns if col.lower() == 'factura']
        
        if cols_factura:
            df = df.drop(columns=cols_factura)
            logger.info(f"  🔧 {hoja}: Eliminada columna '{cols_factura[0]}'")
        else:
            logger.info(f"  ℹ️  {hoja}: No tiene columna 'FACTURA'")
    # === CORRECCIÓN 2: NORMALIZAR DIAS ATRASO ===
    # Buscar TODAS las variaciones posibles (case-insensitive, con/sin espacio)
    col_dias_atraso = None
    
    # Patrones a buscar (de más específico a más general)
    patrones_busqueda = [
        'DIAS ATRASO',      # Con espacio (más común en cortes antiguos)
        'DIAS ATRASOS',     # Plural
        'DIASATRASO',       # Sin espacio
        'DIASATRAS',        # Abreviado
        'diasatras',        # Ya normalizado
        'DIAS_ATRASO',      # Con guion bajo
        'dias atraso',      # Minúsculas con espacio
        'Dias Atraso',      # Title case
    ]
    
    # Buscar la columna (case-insensitive, ignorando espacios extra)
    for col in df.columns:
        col_normalizado = col.upper().strip().replace('_', ' ')
        
        for patron in patrones_busqueda:
            patron_normalizado = patron.upper().strip().replace('_', ' ')
            
            if col_normalizado == patron_normalizado:
                col_dias_atraso = col
                break
        
        if col_dias_atraso:
            break
    
    # Renombrar si se encontró y no es el nombre correcto
    if col_dias_atraso:
        if col_dias_atraso != 'diasatras':
            df = df.rename(columns={col_dias_atraso: 'diasatras'})
            logger.info(f"  🔧 {hoja}: Renombrada '{col_dias_atraso}' → 'diasatras'")
        else:
            logger.info(f"  ✅ {hoja}: Ya tiene 'diasatras'")
    else:
        # No se encontró - ADVERTENCIA
        logger.warning(f"  ⚠️  {hoja}: No se encontró columna de días de atraso")
        logger.warning(f"      Columnas disponibles: {list(df.columns)[:15]}")
    
    return df


def concatenar_dataframes(dataframes: list[pd.DataFrame], nombre: str) -> pd.DataFrame:
    """
    Concatena un conjunto de DataFrames y lo loguea.
    """
    if not dataframes:
        logger.warning(f"⚠️  Sin datos cargados para {nombre}")
        return pd.DataFrame()
    
    # Verificar que todas las hojas tengan las mismas columnas
    if len(dataframes) > 1:
        cols_primera = set(dataframes[0].columns)
        
        for i, df in enumerate(dataframes[1:], 1):
            cols_actual = set(df.columns)
            
            # Columnas faltantes y extras
            faltantes = cols_primera - cols_actual
            extras = cols_actual - cols_primera
            
            if faltantes or extras:
                logger.warning(f"  ⚠️  Hoja {i+1} tiene diferencias en columnas:")
                if faltantes:
                    logger.warning(f"     Faltantes: {list(faltantes)[:5]}")
                if extras:
                    logger.warning(f"     Extras: {list(extras)[:5]}")
    
    # Concatenar
    df_total = pd.concat(dataframes, ignore_index=True)
    
    logger.info("")
    logger.info("="*70)
    logger.info(f"📊 CONCATENACIÓN COMPLETA: {nombre}")
    logger.info("="*70)
    logger.info(f"Total hojas concatenadas: {len(dataframes)}")
    logger.info(f"Total registros: {len(df_total):,}")
    logger.info(f"Total columnas: {len(df_total.columns)}")
    logger.info(f"Memoria: {df_total.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Rango de fechas si existe columna 'corte'
    if 'corte' in df_total.columns:
        fecha_min = df_total['corte'].min()
        fecha_max = df_total['corte'].max()
        logger.info(f"Rango de cortes: {fecha_min} → {fecha_max}")
    
    logger.info("="*70)
    logger.info("")
    
    return df_total


def cargar_dataset(nombre: str) -> pd.DataFrame:
    """
    Carga un dataset completo según la configuración global.
    Aplica correcciones específicas durante la carga.

    Args:
        nombre: nombre del dataset (ej. 'FNZ007')

    Returns:
        DataFrame concatenado y corregido
    """
    logger.info("")
    logger.info("="*70)
    logger.info(f"🚀 INICIANDO CARGA: {nombre}")
    logger.info("="*70)
    
    if nombre not in CONFIG_CARGA:
        logger.error(f"❌ No existe configuración para '{nombre}'")
        return pd.DataFrame()

    cfg = CONFIG_CARGA[nombre]
    dataframes = []

    for clave_archivo, ruta in cfg["archivos"].items():
        logger.info(f"\n📁 Archivo: {ruta.name}")
        
        hojas = cfg["hojas"].get(clave_archivo, {})
        if not hojas:
            logger.warning(f"⚠️  No hay hojas definidas para {clave_archivo}")
            continue
        
        # Pasar el nombre del dataset para aplicar correcciones específicas
        dataframes.extend(
            cargar_excel_con_config(ruta, hojas, nombre_dataset=nombre)
        )

    return concatenar_dataframes(dataframes, nombre)


def cargar_auxiliares() -> dict[str, pd.DataFrame]:
    """
    Carga archivos auxiliares (sin fecha de corte).
    
    Returns:
        dict[str, pd.DataFrame]: Diccionario con DataFrames auxiliares
    """
    logger.info("")
    logger.info("="*70)
    logger.info("🚀 CARGANDO ARCHIVOS AUXILIARES")
    logger.info("="*70)
    
    auxiliares = {}
    
    if "AUXILIARES" not in CONFIG_CARGA:
        logger.error("❌ No hay configuración para AUXILIARES")
        return auxiliares
        
    cfg = CONFIG_CARGA["AUXILIARES"]
    
    for clave_archivo, ruta in cfg["archivos"].items():
        logger.info(f"\n📄 Cargando: {clave_archivo}")
        logger.info(f"   Archivo: {ruta.name}")
        
        if not ruta.exists():
            logger.error(f"   ❌ No encontrado: {ruta}")
            continue
        
        try:
            df = pd.read_excel(ruta)
            
            # Normalizar nombres de columnas
            df.columns = [col.lower().strip() for col in df.columns]
            
            auxiliares[clave_archivo] = df
            
            logger.info(f"   ✅ Cargado: {len(df):,} registros, {len(df.columns)} columnas")
            logger.info(f"   Columnas: {list(df.columns)[:5]}...")
            
        except Exception as e:
            logger.exception(f"   ❌ Error: {e}")
    
    logger.info("")
    logger.info("="*70)
    logger.info(f"✅ AUXILIARES CARGADOS: {len(auxiliares)} archivos")
    logger.info("="*70)
    logger.info("")
    
    return auxiliares


# === Funciones de compatibilidad ===

def cargar_fnz007() -> pd.DataFrame:
    """Carga FNZ007 con todas las correcciones."""
    return cargar_dataset("FNZ007")


def cargar_analisis_cartera() -> pd.DataFrame:
    """Carga Análisis de Cartera."""
    return cargar_dataset("ANALISIS_CARTERA")


def cargar_fnz001() -> pd.DataFrame:
    """Carga FNZ001."""
    return cargar_dataset("FNZ001")


def cargar_edades() -> pd.DataFrame:
    """Carga Edades."""
    return cargar_dataset("EDADES")


def cargar_r05() -> pd.DataFrame:
    """Carga R05."""
    return cargar_dataset("R05")


def cargar_recaudos() -> pd.DataFrame:
    """Carga Recaudos."""
    return cargar_dataset("RECAUDOS")


# === FUNCIÓN DE VALIDACIÓN POST-CARGA ===

def validar_carga_completa() -> dict:
    """
    Valida que todos los datasets críticos se hayan cargado correctamente.
    
    Returns:
        dict con resultados de validación
    """
    logger.info("")
    logger.info("="*70)
    logger.info("🔍 VALIDACIÓN DE CARGA COMPLETA")
    logger.info("="*70)
    
    datasets_criticos = ["FNZ007", "ANALISIS_CARTERA", "FNZ001"]
    datasets_opcionales = ["EDADES", "R05", "RECAUDOS"]
    
    resultados = {
        "criticos_ok": True,
        "problemas": [],
        "advertencias": []
    }
    
    # Validar críticos
    for dataset in datasets_criticos:
        try:
            df = cargar_dataset(dataset)
            if df is None or len(df) == 0:
                resultados["criticos_ok"] = False
                resultados["problemas"].append(f"{dataset}: Vacío o None")
                logger.error(f"   ❌ {dataset}: FALLO")
            else:
                logger.info(f"   ✅ {dataset}: {len(df):,} registros")
        except Exception as e:
            resultados["criticos_ok"] = False
            resultados["problemas"].append(f"{dataset}: {str(e)}")
            logger.error(f"   ❌ {dataset}: ERROR - {e}")
    
    # Validar opcionales
    for dataset in datasets_opcionales:
        try:
            df = cargar_dataset(dataset)
            if df is None or len(df) == 0:
                resultados["advertencias"].append(f"{dataset}: Vacío o None")
                logger.warning(f"   ⚠️  {dataset}: Sin datos")
            else:
                logger.info(f"   ✅ {dataset}: {len(df):,} registros")
        except Exception as e:
            resultados["advertencias"].append(f"{dataset}: {str(e)}")
            logger.warning(f"   ⚠️  {dataset}: ERROR - {e}")
    
    # Validar auxiliares
    try:
        aux = cargar_auxiliares()
        if len(aux) == 0:
            resultados["advertencias"].append("No se cargaron auxiliares")
            logger.warning(f"   ⚠️  AUXILIARES: Sin archivos")
        else:
            logger.info(f"   ✅ AUXILIARES: {len(aux)} archivos")
    except Exception as e:
        resultados["advertencias"].append(f"Auxiliares: {str(e)}")
        logger.warning(f"   ⚠️  AUXILIARES: ERROR - {e}")
    
    logger.info("="*70)
    
    if resultados["criticos_ok"]:
        logger.info("✅ VALIDACIÓN EXITOSA - Todos los datasets críticos cargados")
    else:
        logger.error("❌ VALIDACIÓN FALLIDA - Hay problemas en datasets críticos")
    
    if resultados["advertencias"]:
        logger.warning(f"⚠️  {len(resultados['advertencias'])} advertencias")
    
    logger.info("")
    
    return resultados