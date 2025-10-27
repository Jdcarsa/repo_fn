"""
Sistema de logging para el proyecto Finnovarisk.
Registra todos los eventos importantes en archivos y consola.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
import json



class ColoresConsola:
    """Códigos de color para consola."""
    VERDE = '\033[92m'
    AMARILLO = '\033[93m'
    ROJO = '\033[91m'
    AZUL = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class FormateadorColor(logging.Formatter):
    """Formateador que añade colores a los logs en consola."""
    
    COLORES = {
        'DEBUG': ColoresConsola.AZUL,
        'INFO': ColoresConsola.VERDE,
        'WARNING': ColoresConsola.AMARILLO,
        'ERROR': ColoresConsola.ROJO,
        'CRITICAL': ColoresConsola.ROJO + ColoresConsola.BOLD,
    }
    
    def format(self, record):
        # Añadir color al nivel
        color = self.COLORES.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname}{ColoresConsola.RESET}"
        return super().format(record)


def configurar_logger(nombre: str = "finnovarisk", nivel: int = logging.INFO) -> logging.Logger:
    """
    Configura el sistema de logging con salida a consola y archivo.
    
    Args:
        nombre: Nombre del logger
        nivel: Nivel de logging (por defecto INFO)
        
    Returns:
        Logger configurado
    """
    # Crear carpeta de logs si no existe
    carpeta_logs = Path("logs")
    carpeta_logs.mkdir(exist_ok=True)
    
    # Nombre del archivo log con fecha y hora
    archivo_log = carpeta_logs / f"logger.log"
    
    # Crear logger
    logger = logging.getLogger(nombre)
    logger.setLevel(nivel)
    
    # Limpiar handlers anteriores si existen
    if logger.handlers:
        logger.handlers.clear()
    
    # === HANDLER PARA ARCHIVO ===
    handler_archivo = logging.FileHandler(archivo_log, encoding='utf-8')
    handler_archivo.setLevel(nivel)
    
    formato_archivo = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler_archivo.setFormatter(formato_archivo)
    
    # === HANDLER PARA CONSOLA ===
    handler_consola = logging.StreamHandler(sys.stdout)
    handler_consola.setLevel(nivel)
    
    # En Windows, usar formato sin colores si no está en terminal compatible
    if sys.platform == "win32":
        try:
            import colorama
            colorama.init()
            formato_consola = FormateadorColor(
                fmt='%(levelname)-8s | %(message)s'
            )
        except ImportError:
            formato_consola = logging.Formatter(
                fmt='%(levelname)-8s | %(message)s'
            )
    else:
        formato_consola = FormateadorColor(
            fmt='%(levelname)-8s | %(message)s'
        )
    
    handler_consola.setFormatter(formato_consola)
    
    # Añadir handlers
    logger.addHandler(handler_archivo)
    logger.addHandler(handler_consola)
    
    # Log inicial
    logger.info(f"📝 Log guardado en: {archivo_log}")
    
    return logger


def guardar_resumen_ejecucion(resumen: dict):
    """
    Guarda un resumen JSON de la ejecución.
    
    Args:
        resumen: Diccionario con información de la ejecución
    """
    carpeta_logs = Path("logs")
    carpeta_logs.mkdir(exist_ok=True)
    
    archivo_resumen = carpeta_logs / f"resumen_ejecucion.json"
    
    with open(archivo_resumen, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Resumen guardado en: {archivo_resumen}")


def obtener_ultimo_log() -> Path:
    """
    Obtiene la ruta del último archivo de log.
    
    Returns:
        Path del último log o None si no hay logs
    """
    carpeta_logs = Path("logs")
    
    if not carpeta_logs.exists():
        return None
    
    archivos_log = sorted(carpeta_logs.glob("ejecucion_*.log"), reverse=True)
    
    if archivos_log:
        return archivos_log[0]
    
    return None

