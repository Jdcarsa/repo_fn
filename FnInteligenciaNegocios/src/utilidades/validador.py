"""
Módulo de validación de DataFrames.
Verifica integridad y calidad de los datos cargados.
"""

import pandas as pd
from typing import Dict, List
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.transformador')

def validar_dataframe(df: pd.DataFrame, nombre: str) -> Dict:
    """
    Valida un DataFrame y retorna un reporte de validación.
    
    Args:
        df: DataFrame a validar
        nombre: Nombre descriptivo del DataFrame
        
    Returns:
        Diccionario con resultado de validación
    """
    resultado = {
        "valido": True,
        "mensaje": "",
        "advertencias": [],
        "estadisticas": {}
    }
    
    # Verificar si está vacío
    if df is None or len(df) == 0:
        resultado["valido"] = False
        resultado["mensaje"] = "DataFrame vacío o None"
        return resultado
    
    # Estadísticas básicas
    resultado["estadisticas"] = {
        "registros": len(df),
        "columnas": len(df.columns),
        "memoria_mb": df.memory_usage(deep=True).sum() / 1024**2
    }
    
    # Verificar valores nulos
    porcentaje_nulos = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    
    if porcentaje_nulos > 50:
        resultado["advertencias"].append(
            f"Alto porcentaje de valores nulos: {porcentaje_nulos:.1f}%"
        )
    
    # Verificar duplicados completos
    duplicados = df.duplicated().sum()
    if duplicados > 0:
        porcentaje_dup = (duplicados / len(df)) * 100
        resultado["advertencias"].append(
            f"Se encontraron {duplicados:,} filas duplicadas ({porcentaje_dup:.1f}%)"
        )
    
    # Verificar columna 'corte' si existe
    if 'corte' in df.columns:
        if df['corte'].isnull().any():
            resultado["advertencias"].append(
                "La columna 'corte' tiene valores nulos"
            )
        
        # Verificar rango de fechas
        try:
            fecha_min = pd.to_datetime(df['corte']).min()
            fecha_max = pd.to_datetime(df['corte']).max()
            resultado["estadisticas"]["fecha_min"] = fecha_min.strftime('%Y-%m-%d')
            resultado["estadisticas"]["fecha_max"] = fecha_max.strftime('%Y-%m-%d')
        except:
            resultado["advertencias"].append(
                "No se pudo procesar la columna 'corte' como fecha"
            )
    
    # Verificar columnas con todos valores nulos
    columnas_todas_nulas = df.columns[df.isnull().all()].tolist()
    if columnas_todas_nulas:
        resultado["advertencias"].append(
            f"Columnas completamente vacías: {', '.join(columnas_todas_nulas[:5])}"
        )
    
    # Consolidar mensaje
    if resultado["advertencias"]:
        resultado["mensaje"] = f"{len(resultado['advertencias'])} advertencias encontradas"
    else:
        resultado["mensaje"] = "Sin problemas detectados"
    
    return resultado


def validar_columnas_requeridas(df: pd.DataFrame, columnas_requeridas: List[str]) -> Dict:
    """
    Verifica que el DataFrame tenga las columnas requeridas.
    
    Args:
        df: DataFrame a validar
        columnas_requeridas: Lista de nombres de columnas que deben existir
        
    Returns:
        Diccionario con resultado de validación
    """
    columnas_presentes = set(df.columns)
    columnas_requeridas_set = set(columnas_requeridas)
    
    columnas_faltantes = columnas_requeridas_set - columnas_presentes
    columnas_extra = columnas_presentes - columnas_requeridas_set
    
    resultado = {
        "valido": len(columnas_faltantes) == 0,
        "columnas_faltantes": list(columnas_faltantes),
        "columnas_extra": list(columnas_extra),
        "mensaje": ""
    }
    
    if columnas_faltantes:
        resultado["mensaje"] = f"Faltan columnas: {', '.join(columnas_faltantes)}"
    else:
        resultado["mensaje"] = "Todas las columnas requeridas están presentes"
    
    return resultado


def validar_tipos_datos(df: pd.DataFrame, esquema: Dict[str, type]) -> Dict:
    """
    Valida que las columnas tengan los tipos de datos esperados.
    
    Args:
        df: DataFrame a validar
        esquema: Diccionario {nombre_columna: tipo_esperado}
        
    Returns:
        Diccionario con resultado de validación
    """
    resultado = {
        "valido": True,
        "problemas": [],
        "mensaje": ""
    }
    
    for columna, tipo_esperado in esquema.items():
        if columna not in df.columns:
            resultado["problemas"].append(f"Columna '{columna}' no existe")
            resultado["valido"] = False
            continue
        
        tipo_actual = df[columna].dtype
        
        # Mapeo de tipos de Pandas a tipos de Python
        if tipo_esperado == str and not pd.api.types.is_string_dtype(tipo_actual):
            resultado["problemas"].append(
                f"Columna '{columna}': esperado string, obtenido {tipo_actual}"
            )
            resultado["valido"] = False
        
        elif tipo_esperado == int and not pd.api.types.is_integer_dtype(tipo_actual):
            resultado["problemas"].append(
                f"Columna '{columna}': esperado int, obtenido {tipo_actual}"
            )
            resultado["valido"] = False
        
        elif tipo_esperado == float and not pd.api.types.is_float_dtype(tipo_actual):
            resultado["problemas"].append(
                f"Columna '{columna}': esperado float, obtenido {tipo_actual}"
            )
            resultado["valido"] = False
    
    if resultado["problemas"]:
        resultado["mensaje"] = f"{len(resultado['problemas'])} problemas de tipos"
    else:
        resultado["mensaje"] = "Tipos de datos correctos"
    
    return resultado


def generar_reporte_calidad(df: pd.DataFrame, nombre: str) -> str:
    """
    Genera un reporte detallado de calidad de datos.
    
    Args:
        df: DataFrame a analizar
        nombre: Nombre del DataFrame
        
    Returns:
        String con el reporte formateado
    """
    reporte = []
    reporte.append(f"\n{'='*70}")
    reporte.append(f"REPORTE DE CALIDAD: {nombre}")
    reporte.append(f"{'='*70}")
    
    # Información general
    reporte.append(f"\n📊 Información General:")
    reporte.append(f"  • Registros: {len(df):,}")
    reporte.append(f"  • Columnas: {len(df.columns)}")
    reporte.append(f"  • Memoria: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Valores nulos por columna
    reporte.append(f"\n❓ Valores Nulos:")
    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0].sort_values(ascending=False)
    
    if len(nulos) > 0:
        for col, cantidad in nulos.head(10).items():
            porcentaje = (cantidad / len(df)) * 100
            reporte.append(f"  • {col}: {cantidad:,} ({porcentaje:.1f}%)")
        if len(nulos) > 10:
            reporte.append(f"  ... y {len(nulos) - 10} columnas más con nulos")
    else:
        reporte.append(f"  ✅ No hay valores nulos")
    
    # Duplicados
    reporte.append(f"\n🔄 Duplicados:")
    duplicados = df.duplicated().sum()
    if duplicados > 0:
        porcentaje = (duplicados / len(df)) * 100
        reporte.append(f"  ⚠️ {duplicados:,} filas duplicadas ({porcentaje:.1f}%)")
    else:
        reporte.append(f"  ✅ No hay duplicados")
    
    # Tipos de datos
    reporte.append(f"\n📋 Tipos de Datos:")
    tipos = df.dtypes.value_counts()
    for tipo, cantidad in tipos.items():
        reporte.append(f"  • {tipo}: {cantidad} columnas")
    
    reporte.append(f"\n{'='*70}\n")
    
    return '\n'.join(reporte)


def diagnosticar_dataframe(df: pd.DataFrame, nombre: str) -> None:
    """
    Función de diagnóstico para verificar la estructura de un DataFrame.
    """
    logger.info(f"=== DIAGNÓSTICO {nombre} ===")
    if df is None:
        logger.info("DataFrame es None")
        return
    
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columnas: {list(df.columns)}")
    logger.info(f"Tipos de datos:\n{df.dtypes}")
    if len(df) > 0:
        logger.info(f"Primeras filas:\n{df.head(2)}")
    logger.info("=== FIN DIAGNÓSTICO ===")