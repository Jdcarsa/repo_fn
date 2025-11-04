"""
Módulo para exportar DataFrames a Excel con formato y timestamp
"""
from pathlib import Path
from datetime import datetime
import pandas as pd
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.exportador')


def guardar_excel(df: pd.DataFrame, nombre: str, carpeta: str = "salidas"):
    """
    Guarda un DataFrame en Excel con timestamp.
    
    Args:
        df: DataFrame a guardar
        nombre: Nombre base del archivo
        carpeta: Carpeta destino (por defecto 'salidas')
        
    Returns:
        Path del archivo guardado
    """
    # Crear carpeta si no existe
    ruta_carpeta = Path(carpeta)
    ruta_carpeta.mkdir(parents=True, exist_ok=True)
    
    # Crear nombre con timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    nombre_archivo = f"{nombre}_{timestamp}.xlsx"
    ruta_completa = ruta_carpeta / nombre_archivo
    
    # Guardar
    logger.info(f"💾 Guardando: {nombre_archivo}")
    logger.info(f"   Registros: {len(df):,}")
    logger.info(f"   Columnas: {len(df.columns)}")
    
    try:
        df.to_excel(ruta_completa, index=False, engine='openpyxl')
        
        # Tamaño del archivo
        tamaño_mb = ruta_completa.stat().st_size / 1024**2
        logger.info(f"   ✅ Guardado exitosamente ({tamaño_mb:.2f} MB)")
        logger.info(f"   📁 Ruta: {ruta_completa}")
        
        return ruta_completa
        
    except Exception as e:
        logger.error(f"   ❌ Error guardando {nombre}: {e}")
        raise


def guardar_multiples_hojas(dict_dataframes: dict, nombre_archivo: str, carpeta: str = "salidas"):
    """
    Guarda múltiples DataFrames en un solo Excel con diferentes hojas.
    
    Args:
        dict_dataframes: Diccionario {nombre_hoja: DataFrame}
        nombre_archivo: Nombre del archivo Excel
        carpeta: Carpeta destino
        
    Returns:
        Path del archivo guardado
    """
    ruta_carpeta = Path(carpeta)
    ruta_carpeta.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    nombre_con_timestamp = f"{nombre_archivo}_{timestamp}.xlsx"
    ruta_completa = ruta_carpeta / nombre_con_timestamp
    
    logger.info(f"💾 Guardando múltiples hojas en: {nombre_con_timestamp}")
    
    try:
        with pd.ExcelWriter(ruta_completa, engine='openpyxl') as writer:
            for nombre_hoja, df in dict_dataframes.items():
                # Excel no permite nombres de hojas > 31 caracteres
                nombre_hoja_limpio = nombre_hoja[:31]
                
                df.to_excel(writer, sheet_name=nombre_hoja_limpio, index=False)
                logger.info(f"   ✅ Hoja '{nombre_hoja_limpio}': {len(df):,} registros")
        
        tamaño_mb = ruta_completa.stat().st_size / 1024**2
        logger.info(f"   📁 Archivo guardado ({tamaño_mb:.2f} MB): {ruta_completa}")
        
        return ruta_completa
        
    except Exception as e:
        logger.error(f"   ❌ Error guardando {nombre_archivo}: {e}")
        raise