import pandas as pd
import numpy as np
from src.utilidades.logger import configurar_logger

# Logger común para todos los transformadores
logger = configurar_logger('finnovarisk.transformadores')

def convertir_columnas_minusculas(df: pd.DataFrame, nombre_dataset: str) -> pd.DataFrame:
    """Convierte nombres de columnas a minúsculas"""
    df_proc = df.copy()
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info(f"Nombres de columnas de {nombre_dataset} convertidos a minúsculas.")
    return df_proc


def limpiar_columna_identificador(serie: pd.Series) -> pd.Series:
    """
    Limpia una serie completa de identificadores (cédulas, números).
    
    Convierte:
    - 1007518.0 → "1007518"
    - "1007518.0" → "1007518" 
    - 1007518 → "1007518"
    - None/NaN → ""
    - "  123  " → "123"
    
    Args:
        serie: Serie de pandas a limpiar
        
    Returns:
        Serie limpia como strings
    """
    def limpiar_valor(valor):
        # Caso 1: None o NaN
        if pd.isna(valor):
            return ""
        
        # Caso 2: Es numérico (int o float)
        if isinstance(valor, (int, np.integer)):
            return str(int(valor))
        
        if isinstance(valor, (float, np.floating)):
            # Convertir a int si no tiene decimales significativos
            if valor == int(valor):
                return str(int(valor))
            else:
                return str(valor)
        
        # Caso 3: Es string
        valor_str = str(valor).strip()
        
        # Si es un número en formato string con .0, quitarlo
        try:
            valor_float = float(valor_str)
            if valor_float == int(valor_float):
                return str(int(valor_float))
            else:
                return str(valor_float)
        except (ValueError, OverflowError):
            # No es un número, devolver como está (limpio)
            return valor_str
    
    # Aplicar limpieza a toda la serie
    return serie.apply(limpiar_valor)


def limpiar_columnas_numericas_como_string(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """
    Limpia múltiples columnas que deben ser strings sin .0
    
    Args:
        df: DataFrame
        columnas: Lista de nombres de columnas a limpiar
        
    Returns:
        DataFrame con columnas limpias
    """
    df_proc = df.copy()
    
    for col in columnas:
        if col in df_proc.columns:
            antes = df_proc[col].dtype
            df_proc[col] = limpiar_columna_identificador(df_proc[col])
            
            # Contar cuántos se limpiaron
            if antes in ['float64', 'float32']:
                logger.info(f"  🧹 Limpiada columna '{col}': {antes} → string (sin .0)")
    
    return df_proc


def crear_llave_cedula_numero(df: pd.DataFrame, col_cedula: str, col_numero: str, 
                               separador: str = '-') -> pd.DataFrame:
    """
    Crea la llave cedula_numero limpiando PRIMERO los valores.
    
    Args:
        df: DataFrame
        col_cedula: Nombre de la columna de cédula
        col_numero: Nombre de la columna de número
        separador: Separador a usar (por defecto '-', R05 usa '_')
        
    Returns:
        DataFrame con columna cedula_numero creada
    """
    df_proc = df.copy()
    
    if col_cedula not in df_proc.columns:
        logger.error(f"❌ Columna '{col_cedula}' no encontrada")
        logger.info(f"   Columnas disponibles: {list(df_proc.columns)[:10]}")
        return df_proc
    
    if col_numero not in df_proc.columns:
        logger.error(f"❌ Columna '{col_numero}' no encontrada")
        logger.info(f"   Columnas disponibles: {list(df_proc.columns)[:10]}")
        return df_proc
    
    # 🔧 PASO 1: LIMPIAR ambas columnas
    logger.info(f"  🧹 Limpiando '{col_cedula}' y '{col_numero}'...")
    
    df_proc[col_cedula] = limpiar_columna_identificador(df_proc[col_cedula])
    df_proc[col_numero] = limpiar_columna_identificador(df_proc[col_numero])
    
    # 🔧 PASO 2: Crear llave
    df_proc['cedula_numero'] = (
        df_proc[col_cedula] + separador + df_proc[col_numero]
    )
    
    # 🔧 PASO 3: Verificar resultados
    llaves_vacias = (df_proc['cedula_numero'] == separador).sum()
    llaves_validas = (
        (df_proc['cedula_numero'] != separador) & 
        (df_proc['cedula_numero'].notna())
    ).sum()
    
    logger.info(f"  ✅ Creada 'cedula_numero' con separador '{separador}'")
    logger.info(f"     Llaves válidas: {llaves_validas:,}")
    
    if llaves_vacias > 0:
        logger.warning(f"     ⚠️  {llaves_vacias:,} llaves vacías ('{separador}')")
    
    # 🔧 PASO 4: Mostrar ejemplos ANTES y DESPUÉS
    ejemplos = df_proc[['cedula_numero']].head(5)
    logger.info(f"     📋 Ejemplos:")
    for i, val in enumerate(ejemplos['cedula_numero'], 1):
        logger.info(f"        {i}. {val}")
    
    return df_proc


def eliminar_columnas(df: pd.DataFrame, columnas_a_eliminar: list, nombre_dataset: str) -> pd.DataFrame:
    """Elimina columnas especificadas del DataFrame"""
    df_proc = df.copy()
    columnas_encontradas = [col for col in columnas_a_eliminar if col in df_proc.columns]
    
    if columnas_encontradas:
        df_proc.drop(columns=columnas_encontradas, inplace=True)
        logger.info(f"Se eliminaron {len(columnas_encontradas)} columnas de {nombre_dataset}: {', '.join(columnas_encontradas)}")
    
    return df_proc