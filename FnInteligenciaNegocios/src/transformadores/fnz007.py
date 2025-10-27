import pandas as pd
from .base import logger, convertir_columnas_minusculas, eliminar_columnas

def procesar_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe FNZ007.
    """
    logger.info("Iniciando limpieza y procesamiento de FNZ007...")
    df_proc = df.copy()

    # 1. Convertir nombres de columnas a minúsculas
    df_proc = convertir_columnas_minusculas(df_proc, "FNZ007")

    # 2. Unificar columnas de empleo
    df_proc['act_lab'] = df_proc['ocupacion'].fillna('').astype(str) + ' ' + df_proc['indpactivi'].fillna('').astype(str)
    df_proc['act_lab'] = df_proc['act_lab'].str.strip()

    df_proc['empresa'] = df_proc['lbempresa'].fillna('').astype(str) + ' ' + df_proc['indprzsoci'].fillna('').astype(str)
    df_proc['empresa'] = df_proc['empresa'].str.strip()

    df_proc['cargos'] = df_proc['cargo'].fillna('').astype(str) + ' ' + df_proc['indpnombre'].fillna('').astype(str)
    df_proc['cargos'] = df_proc['cargos'].str.strip()
    logger.info("Columnas de empleo unificadas en 'act_lab', 'empresa', 'cargos'.")

    # 3. Categorizar Estado Civil
    replacements_estcvil = {
        'Divorciado': 'Soltero',
        'Separado': 'Soltero',
        'Viudo': 'Soltero',
        '.': 'Soltero'
    }
    df_proc['fs1estcvil'] = df_proc['fs1estcvil'].replace(replacements_estcvil)
    logger.info("Columna 'fs1estcvil' categorizada.")

    # 4. Categorizar Nivel Escolar
    replacements_escolar = {
        'Especialización': 'Educacion superior', 'Especializaci¾n': 'Educacion superior',
        'Maestría': 'Educacion superior', 'MaestrÝa': 'Educacion superior',
        'Postdoctorado': 'Educacion superior',
        'Universitario': 'Educacion superior',
        'Doctorado': 'Educacion superior',
        'Técnico': 'Tecnico o Tecnologo', 'TÚcnico': 'Tecnico o Tecnologo',
        'Tecnólogo': 'Tecnico o Tecnologo', 'Tecn¾logo': 'Tecnico o Tecnologo'
    }
    df_proc['nvescolar'] = df_proc['nvescolar'].replace(replacements_escolar)
    logger.info("Columna 'nvescolar' categorizada.")

    # 5. Eliminar columnas innecesarias
    columnas_a_eliminar_fnz = [
        'ocupacion', 'indpactivi', 'lbempresa', 'indprzsoci', 'cargo', 'indpnombre',
        'clase', 'tipo', 'estado', 'df', 'pagare', 'apellidos', 'nombres',
        'telefono1', 'movil', 'direccion', 'codbarrio', 'barrio', 'fs0nota', 'fs1email'
    ]
    
    df_proc = eliminar_columnas(df_proc, columnas_a_eliminar_fnz, "FNZ007")

    # 6. Renombrar variables
    renames = {
        'ciudad': 'nomciudad',
        'codciudad': 'ciudad'
    }
    df_proc.rename(columns=renames, inplace=True)
    logger.info("Columnas de FNZ007 renombradas.")

    logger.info("Limpieza y procesamiento de FNZ007 completado.")
    return df_proc