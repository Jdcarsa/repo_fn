"""
Configuración del proyecto Finnovarisk.

Contiene todas las rutas y parámetros necesarios para la ejecución del ETL.
"""

from pathlib import Path

# --- Rutas Principales ---
# Raíz del proyecto (calculada a partir de la ubicación de este archivo)
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

# Carpetas principales
CARPETA_DATOS = RAIZ_PROYECTO / "datos"
CARPETA_SALIDAS = RAIZ_PROYECTO / "salidas"
CARPETA_LOGS = RAIZ_PROYECTO / "logs"
CARPETA_TEMP = RAIZ_PROYECTO / "temp"

# --- Rutas de Datos Originales ---
DATOS_ORIGINALES = CARPETA_DATOS / "originales"

# Rutas específicas de archivos de entrada
ARCHIVOS_FNZ007 = {
    "base_completa": DATOS_ORIGINALES / "FNZ007" / "FNZ 007 BASE COMPLETA.xlsx",
    "2025": DATOS_ORIGINALES / "FNZ007" / "FNZ007 2025.xlsx"
}

ARCHIVOS_ANALISIS_CARTERA = {
    "completo": DATOS_ORIGINALES / "Analisis_Cartera" / "ANALISIS DE CARTERA COMPLETO.xlsx",
    "julio_31": DATOS_ORIGINALES / "Analisis_Cartera" / "ANÁLISIS GENERAL JULIO 31.xlsx",
    "agosto_31": DATOS_ORIGINALES / "Analisis_Cartera" / "ANALISIS GENERAL AGOSTO 31.xlsx",
    "septiembre_30": DATOS_ORIGINALES / "Analisis_Cartera" / "ANALISIS DE CARTERA CIERRE 30 SEPT.xlsx",

    # Se pueden añadir más archivos si es necesario
}

ARCHIVOS_FNZ001 = {
    "2023": DATOS_ORIGINALES / "FNZ001" / "FNZ001 2023.XLSX", # Contiene hojas desde JULIO 23 hasta MAYO 25
    "2024": DATOS_ORIGINALES / "FNZ001" / "FNZ001 2024.XLSX", # Contiene hojas JULIO y AGOSTO 25
    "2025": DATOS_ORIGINALES / "FNZ001" / "FNZ001 2025.XLSX", # Contiene hoja SEPT 25
}

ARCHIVOS_EDADES = {
    "2023": DATOS_ORIGINALES / "Informe_Edades" / "INFORME POR EDADES 2023.xlsx",
    "2024": DATOS_ORIGINALES / "Informe_Edades" / "INFORME POR EDADES 2024.XLSX",
    "2025": DATOS_ORIGINALES / "Informe_Edades" / "INFORME POR EDADES 2025.xlsx",
}

ARCHIVOS_R05 = {
    "2023_2024": DATOS_ORIGINALES / "R05" / "R05 FNS 2023 - 2024 completo.xlsx",
    "2025": DATOS_ORIGINALES / "R05" / "R05 FS 2025.xlsx",
}

ARCHIVOS_RECAUDOS = {
    "2023": DATOS_ORIGINALES / "Recaudos" / "RECAUDOS2023.xlsx",
    "2024": DATOS_ORIGINALES / "Recaudos" / "RECAUDOS2024.xlsx",
    "2025": DATOS_ORIGINALES / "Recaudos" / "FNZ010 2025.XLSX",
}

ARCHIVOS_AUXILIARES = {
    "estado_desembolsos": DATOS_ORIGINALES / "Auxiliares" / "ESTADO DESEMBOLSOS (1).xlsx",
    "categorias": DATOS_ORIGINALES / "Auxiliares" / "Base_categorias.xlsx",
}

# --- Rutas de Datos Procesados ---
DATOS_PROCESADOS = CARPETA_DATOS / "procesados"
DATOS_CACHE = CARPETA_DATOS / "cache"

# --- Rutas de Salidas ---
SALIDAS_COSECHAS = CARPETA_SALIDAS / "Cosechas"
SALIDAS_CRM = CARPETA_SALIDAS / "CRM"
SALIDAS_COMPORTAMIENTO = CARPETA_SALIDAS / "Comportamiento"
SALIDAS_REPORTES = CARPETA_SALIDAS / "Reportes"

# --- Parámetros del Modelo ---
# Ejemplo de un parámetro que podría estar en el script de R
TASA_INTERES_EJEMPLO = 0.05

