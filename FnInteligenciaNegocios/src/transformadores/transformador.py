import pandas as pd
import numpy as np
from src.utilidades.logger import configurar_logger

# Configurar un logger específico para este módulo
logger = configurar_logger('finnovarisk.transformador')

def filtrar_finansuenos_ac(df_ac: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra el DataFrame de Análisis de Cartera para incluir solo registros
    donde la columna 'reg' es 'FINANSUEÑOS'.

    Args:
        df_ac (pd.DataFrame): DataFrame de Análisis de Cartera sin filtrar.

    Returns:
        pd.DataFrame: DataFrame de Análisis de Cartera filtrado.
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
    - Convierte nombres de columnas a minúsculas.
    - Elimina columnas innecesarias.
    - Crea la llave primaria 'cedula_numero'.
    """
    logger.info("Iniciando limpieza y procesamiento de Análisis de Cartera...")
    df_proc = df.copy()
    
    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info("Nombres de columnas convertidos a minúsculas.")

    # 2. Eliminar columnas innecesarias (según script R)
    columnas_a_eliminar = [
        'reg', 'clase', 'tipo', 'nombre', 'telefono', 'ultpago', 'fechanov', 
        'fechacom', 'totfactura', 'intepagado', 'interespdte', 'direccion', 
        'barrio', 'generar', 'cobra', 'empresa', 'solnum', 'mcncuenta',
        # Columnas con muchos NAs identificadas en R
        'peripago', 'vinempres', 'soltip', 'tiponov', 'codnov'
    ]
    
    columnas_encontradas = [col for col in columnas_a_eliminar if col in df_proc.columns]
    df_proc.drop(columns=columnas_encontradas, inplace=True)
    logger.info(f"Se eliminaron {len(columnas_encontradas)} columnas: {', '.join(columnas_encontradas)}")

    # 3. Crear la llave 'cedula_numero'
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        # Asegurar que las columnas son strings antes de concatenar
        df_proc['cedula'] = df_proc['cedula'].astype(str)
        df_proc['numero'] = df_proc['numero'].astype(str)
        df_proc['cedula_numero'] = df_proc['cedula'] + '-' + df_proc['numero']
        logger.info("Creada la columna 'cedula_numero'.")
    else:
        logger.error("No se pudieron encontrar las columnas 'cedula' y 'numero' para crear la llave.")
        # Opcional: podrías lanzar un error aquí si esta llave es crítica
        # raise ValueError("Faltan columnas 'cedula' o 'numero'")

    logger.info("Limpieza y procesamiento de Análisis de Cartera completado.")
    return df_proc

def manejar_duplicados_ac(df: pd.DataFrame) -> pd.DataFrame:
    """
    Maneja los registros duplicados en el DataFrame de Análisis de Cartera.
    Agrupa por 'cedula_numero' y 'corte', sumando valores numéricos y
    tomando el primer valor para los demás.
    """
    logger.info("Manejando duplicados en Análisis de Cartera...")
    if 'cedula_numero' not in df.columns or 'corte' not in df.columns:
        logger.warning("No se encontraron 'cedula_numero' o 'corte'. No se aplicará manejo de duplicados.")
        return df

    registros_antes = len(df)
    
    # Identificar columnas numéricas y no numéricas
    columnas_numericas = df.select_dtypes(include=np.number).columns.tolist()
    columnas_no_numericas = df.select_dtypes(exclude=np.number).columns.tolist()

    # Crear el diccionario de agregación
    agg_dict = {}
    for col in columnas_numericas:
        agg_dict[col] = 'sum'
    
    # Para las no numéricas, tomar el primer valor, excluyendo las claves de agrupación
    for col in columnas_no_numericas:
        if col not in ['cedula_numero', 'corte']:
            agg_dict[col] = 'first'

    # Realizar la agregación
    df_agrupado = df.groupby(['cedula_numero', 'corte'], as_index=False).agg(agg_dict)
    
    # Reordenar las columnas para que coincidan con el original (opcional pero buena práctica)
    # Asegurarse de que todas las columnas originales están en el resultado
    columnas_finales = [col for col in df.columns if col in df_agrupado.columns]
    df_agrupado = df_agrupado[columnas_finales]

    registros_despues = len(df_agrupado)
    logger.info(f"Manejo de duplicados: {registros_antes:,} -> {registros_despues:,} registros.")
    
    return df_agrupado

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

    # Asegurarse que diasatras es numérico, convirtiendo errores en NaT/NaN
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

    # Loggear cuántos valores no pudieron ser categorizados
    no_categorizados = df_mora['mora'].isnull().sum()
    if no_categorizados > 0:
        logger.warning(f"{no_categorizados} registros no pudieron ser categorizados en 'mora' (posiblemente diasatras es negativo o no numérico).")

    logger.info("Columna 'mora' creada exitosamente.")
    return df_mora

def procesar_fnz007(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe FNZ007.
    - Convierte nombres de columnas a minúsculas.
    - Unifica columnas de empleo.
    - Categoriza variables como estado civil y nivel escolar.
    - Elimina columnas innecesarias.
    - Renombra columnas.
    """
    logger.info("Iniciando limpieza y procesamiento de FNZ007...")
    df_proc = df.copy()

    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info("Nombres de columnas de FNZ007 convertidos a minúsculas.")

    # 2. Unificar columnas de empleo
    # Convertir todas las columnas a string antes de concatenar
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
        # Unificadas
        'ocupacion', 'indpactivi', 'lbempresa', 'indprzsoci', 'cargo', 'indpnombre',
        # Otras según script R
        'clase', 'tipo', 'estado', 'df', 'pagare', 'apellidos', 'nombres',
        'telefono1', 'movil', 'direccion', 'codbarrio', 'barrio', 'fs0nota', 'fs1email'
    ]
    columnas_encontradas_fnz = [col for col in columnas_a_eliminar_fnz if col in df_proc.columns]
    df_proc.drop(columns=columnas_encontradas_fnz, inplace=True)
    logger.info(f"Se eliminaron {len(columnas_encontradas_fnz)} columnas de FNZ007.")

    # 6. Renombrar variables
    renames = {
        'ciudad': 'nomciudad',
        'codciudad': 'ciudad'
    }
    df_proc.rename(columns=renames, inplace=True)
    logger.info("Columnas de FNZ007 renombradas.")

    logger.info("Limpieza y procesamiento de FNZ007 completado.")
    return df_proc

def procesar_edades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe de Edades.
    - Convierte nombres de columnas a minúsculas.
    - Crea la llave 'cedula_numero'.
    - Filtra por tipo de línea de crédito.
    """
    if df is None:
        logger.warning("El dataframe de Edades es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de Edades...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info("Nombres de columnas de Edades convertidos a minúsculas.")

    # 2. Crear la llave 'cedula_numero'
    if 'cc_nit' in df_proc.columns and 'numero' in df_proc.columns:
        df_proc['cc_nit'] = df_proc['cc_nit'].astype(str)
        df_proc['numero'] = df_proc['numero'].astype(str)
        df_proc['cedula_numero'] = df_proc['cc_nit'] + '-' + df_proc['numero']
        logger.info("Creada la columna 'cedula_numero' en Edades.")
    else:
        logger.error("No se pudieron encontrar 'cc_nit' y 'numero' en Edades para crear la llave.")

    # 3. Filtrar por LINEA
    if 'linea' in df_proc.columns:
        lineas_a_mantener = ["[01]CREDITO ARPESOD", "[03]CREDITO RETANQUEO"]
        df_proc = df_proc[df_proc['linea'].isin(lineas_a_mantener)]
        logger.info(f"Filtrado de Edades por línea de crédito: {registros_antes:,} -> {len(df_proc):,} registros.")
    else:
        logger.warning("No se encontró la columna 'linea' en Edades. No se aplicó el filtro.")

    logger.info("Limpieza y procesamiento de Edades completado.")
    return df_proc

def procesar_r05(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe R05.
    - Convierte nombres de columnas a minúsculas.
    - Renombra columnas.
    - Convierte tipos de datos.
    - Crea la llave 'cedula_numero'.
    - Filtra por abono > 0.
    - Agrega duplicados por 'cedula_numero' y 'corte'.
    """
    if df is None:
        logger.warning("El dataframe de R05 es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de R05...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info("Nombres de columnas de R05 convertidos a minúsculas.")
    logger.info(f"Columnas disponibles en R05: {list(df_proc.columns)}")

    # 2. Renombrar columnas - VERIFICAR LOS NOMBRES EXACTOS
    renames = {
        'fecha': 'corte',
        'identificacion': 'cedula',
        'numero obligacion': 'numero',
        'valor abono': 'abono'
    }
    
    # Verificar qué columnas existen realmente
    columnas_existentes = {k: v for k, v in renames.items() if k in df_proc.columns}
    logger.info(f"Columnas a renombrar: {columnas_existentes}")
    
    df_proc.rename(columns=columnas_existentes, inplace=True)
    logger.info("Columnas de R05 renombradas.")
    logger.info(f"Columnas después del renombrado: {list(df_proc.columns)}")

    # 3. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
        # Verificar conversión
        nulos_corte = df_proc['corte'].isnull().sum()
        if nulos_corte > 0:
            logger.warning(f"{nulos_corte} registros no pudieron convertir 'corte' a fecha")
    
    if 'cedula' in df_proc.columns:
        df_proc['cedula'] = df_proc['cedula'].astype(str)
    
    if 'numero' in df_proc.columns:
        df_proc['numero'] = df_proc['numero'].astype(str)
    
    if 'abono' in df_proc.columns:
        df_proc['abono'] = pd.to_numeric(df_proc['abono'], errors='coerce')
        # Verificar conversión
        nulos_abono = df_proc['abono'].isnull().sum()
        if nulos_abono > 0:
            logger.warning(f"{nulos_abono} registros no pudieron convertir 'abono' a numérico")
    
    logger.info("Tipos de datos de R05 convertidos.")

    # 4. Crear la llave 'cedula_numero'
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        df_proc['cedula_numero'] = df_proc['cedula'] + '-' + df_proc['numero']
        logger.info("Creada la columna 'cedula_numero' en R05.")
        
        # Verificar que no hay valores nulos en la llave
        nulos_llave = df_proc['cedula_numero'].isnull().sum()
        if nulos_llave > 0:
            logger.warning(f"{nulos_llave} registros tienen llave 'cedula_numero' nula")
    else:
        columnas_faltantes = []
        if 'cedula' not in df_proc.columns:
            columnas_faltantes.append('cedula')
        if 'numero' not in df_proc.columns:
            columnas_faltantes.append('numero')
        logger.error(f"No se pudieron encontrar {columnas_faltantes} en R05 para crear la llave.")
        return df_proc  # Retornar sin procesar más si no hay llave

    # 5. Filtrar por abono > 0
    if 'abono' in df_proc.columns:
        registros_antes_filtro = len(df_proc)
        df_proc = df_proc[df_proc['abono'] > 0].copy()
        logger.info(f"Filtrado de R05 por abono > 0: {registros_antes_filtro:,} -> {len(df_proc):,} registros.")
    else:
        logger.warning("La columna 'abono' no se encuentra en R05. No se aplicó el filtro.")

    # 6. Agrupar duplicados - CON MANEJO MEJORADO DE DUPLICADOS
    if 'cedula_numero' in df_proc.columns and 'corte' in df_proc.columns and 'abono' in df_proc.columns:
        registros_antes_agg = len(df_proc)
        
        # Verificar duplicados antes de agrupar
        duplicados = df_proc.duplicated(subset=['cedula_numero', 'corte']).sum()
        logger.info(f"Encontrados {duplicados} registros duplicados por 'cedula_numero' y 'corte'")
        
        # Agrupar asegurando que no hay claves duplicadas
        try:
            df_proc = df_proc.groupby(['cedula_numero', 'corte'], as_index=False, observed=True)['abono'].sum()
            logger.info(f"Agregación de duplicados en R05: {registros_antes_agg:,} -> {len(df_proc):,} registros.")
        except Exception as e:
            logger.error(f"Error al agrupar duplicados en R05: {e}")
            # Alternativa: usar drop_duplicates si groupby falla
            df_proc = df_proc.sort_values('abono', ascending=False).drop_duplicates(subset=['cedula_numero', 'corte'], keep='first')
            logger.info(f"Usando drop_duplicates como alternativa: {registros_antes_agg:,} -> {len(df_proc):,} registros.")
    else:
        columnas_faltantes = []
        if 'cedula_numero' not in df_proc.columns:
            columnas_faltantes.append('cedula_numero')
        if 'corte' not in df_proc.columns:
            columnas_faltantes.append('corte')
        if 'abono' not in df_proc.columns:
            columnas_faltantes.append('abono')
        logger.warning(f"No se pudieron encontrar {columnas_faltantes} en R05 para agregar duplicados.")

    logger.info("Limpieza y procesamiento de R05 completado.")
    return df_proc

def procesar_recaudos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe Recaudos.
    - Convierte nombres de columnas a minúsculas.
    - Renombra columnas.
    - Convierte tipos de datos.
    - Crea la llave 'cedula_numero'.
    - Filtra por capitalrec > 0.
    - Agrega duplicados por 'cedula_numero' y 'corte'.
    """
    if df is None:
        logger.warning("El dataframe de Recaudos es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de Recaudos...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info(f"Columnas disponibles en Recaudos: {list(df_proc.columns)}")

    # 2. IDENTIFICAR COLUMNAS REALES (similar a R05)
    # Buscar columnas candidatas basadas en nombres comunes
    columna_cedula = None
    columna_numero = None
    columna_fecha = None
    columna_capital = None
    
    # Buscar posibles columnas para cada concepto
    for col in df_proc.columns:
        if 'cedula' in col or 'identificacion' in col or 'nit' in col or 'vinculado' in col:
            columna_cedula = col
        elif 'numero' in col or 'obligacion' in col or 'ds_numero' in col:
            columna_numero = col
        elif 'fecha' in col or 'corte' in col or 'rc_fecha' in col:
            columna_fecha = col
        elif 'capital' in col or 'capitalrec' in col or 'valor' in col:
            columna_capital = col
    
    logger.info(f"Columnas identificadas - Cédula: {columna_cedula}, Número: {columna_numero}, Fecha: {columna_fecha}, Capital: {columna_capital}")

    # 3. Renombrar columnas
    renames = {}
    if columna_cedula:
        renames[columna_cedula] = 'cedula'
    if columna_numero:
        renames[columna_numero] = 'numero'
    if columna_fecha:
        renames[columna_fecha] = 'corte'
    if columna_capital:
        renames[columna_capital] = 'capitalrec'
    
    logger.info(f"Renombrando columnas en Recaudos: {renames}")
    df_proc.rename(columns=renames, inplace=True)

    # 4. CONVERTIR TIPOS DE DATOS ANTES DE CREAR LA LLAVE
    if 'cedula' in df_proc.columns:
        # Convertir a string, manejando valores nulos
        df_proc['cedula'] = df_proc['cedula'].astype(str).fillna('')
        # Limpiar espacios
        df_proc['cedula'] = df_proc['cedula'].str.strip()
    
    if 'numero' in df_proc.columns:
        # Convertir a string, manejando valores nulos
        df_proc['numero'] = df_proc['numero'].astype(str).fillna('')
        # Limpiar espacios
        df_proc['numero'] = df_proc['numero'].str.strip()
    
    if 'corte' in df_proc.columns:
        # Convertir fecha
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
        nulos_corte = df_proc['corte'].isnull().sum()
        if nulos_corte > 0:
            logger.warning(f"{nulos_corte} registros no pudieron convertir 'corte' a fecha")
    
    if 'capitalrec' in df_proc.columns:
        # Convertir a numérico
        df_proc['capitalrec'] = pd.to_numeric(df_proc['capitalrec'], errors='coerce')
        nulos_capital = df_proc['capitalrec'].isnull().sum()
        if nulos_capital > 0:
            logger.warning(f"{nulos_capital} registros no pudieron convertir 'capitalrec' a numérico")

    # 5. Crear la llave 'cedula_numero' - SOLUCIÓN AL ERROR
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        # VERIFICAR: asegurarse de que ambas son strings
        logger.info(f"Tipo de 'cedula': {df_proc['cedula'].dtype}")
        logger.info(f"Tipo de 'numero': {df_proc['numero'].dtype}")
        
        # Crear la llave de manera segura
        df_proc['cedula_numero'] = df_proc['cedula'].astype(str) + '-' + df_proc['numero'].astype(str)
        logger.info("Creada la columna 'cedula_numero' en Recaudos.")
        
        # Verificar que no hay valores vacíos en la llave
        llaves_vacias = (df_proc['cedula_numero'] == '-').sum()
        if llaves_vacias > 0:
            logger.warning(f"{llaves_vacias} registros tienen llave vacía ('-')")
    else:
        columnas_faltantes = []
        if 'cedula' not in df_proc.columns:
            columnas_faltantes.append('cedula')
        if 'numero' not in df_proc.columns:
            columnas_faltantes.append('numero')
        logger.warning(f"No se pudieron encontrar {columnas_faltantes} en Recaudos para crear la llave.")

    # 6. Filtrar por capitalrec > 0
    if 'capitalrec' in df_proc.columns:
        registros_antes_filtro = len(df_proc)
        df_proc = df_proc[df_proc['capitalrec'] > 0].copy()
        logger.info(f"Filtrado de Recaudos por capitalrec > 0: {registros_antes_filtro:,} -> {len(df_proc):,} registros.")
    else:
        logger.warning("La columna 'capitalrec' no se encuentra en Recaudos. No se aplicó el filtro.")

    # 7. Agrupar duplicados
    if 'cedula_numero' in df_proc.columns and 'corte' in df_proc.columns and 'capitalrec' in df_proc.columns:
        registros_antes_agg = len(df_proc)
        
        # Verificar duplicados antes de agrupar
        duplicados = df_proc.duplicated(subset=['cedula_numero', 'corte']).sum()
        logger.info(f"Encontrados {duplicados} registros duplicados por 'cedula_numero' y 'corte'")
        
        try:
            df_proc = df_proc.groupby(['cedula_numero', 'corte'], as_index=False, observed=True)['capitalrec'].sum()
            logger.info(f"Agregación de duplicados en Recaudos: {registros_antes_agg:,} -> {len(df_proc):,} registros.")
        except Exception as e:
            logger.error(f"Error al agrupar duplicados en Recaudos: {e}")
            # Alternativa: eliminar duplicados manteniendo el mayor capitalrec
            df_proc = df_proc.sort_values('capitalrec', ascending=False).drop_duplicates(
                subset=['cedula_numero', 'corte'], 
                keep='first'
            )
            logger.info(f"Usando drop_duplicates como alternativa: {registros_antes_agg:,} -> {len(df_proc):,} registros.")
    else:
        columnas_faltantes = []
        if 'cedula_numero' not in df_proc.columns:
            columnas_faltantes.append('cedula_numero')
        if 'corte' not in df_proc.columns:
            columnas_faltantes.append('corte')
        if 'capitalrec' not in df_proc.columns:
            columnas_faltantes.append('capitalrec')
        logger.warning(f"No se pudieron encontrar {columnas_faltantes} en Recaudos para agregar duplicados.")

    logger.info(f"Limpieza y procesamiento de Recaudos completado: {registros_antes:,} -> {len(df_proc):,} registros.")
    return df_proc

def procesar_fnz001(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la limpieza y transformación del dataframe FNZ001.
    - Convierte nombres de columnas a minúsculas.
    - Renombra columnas.
    - Convierte tipos de datos.
    - Crea la llave 'cedula_numero'.
    - Elimina duplicados por 'numero'.
    """
    if df is None:
        logger.warning("El dataframe de FNZ001 es None. Se omite el procesamiento.")
        return None

    logger.info("Iniciando limpieza y procesamiento de FNZ001...")
    df_proc = df.copy()
    registros_antes = len(df_proc)

    # 1. Convertir nombres de columnas a minúsculas
    df_proc.columns = [col.lower() for col in df_proc.columns]
    logger.info("Nombres de columnas de FNZ001 convertidos a minúsculas.")

    # 2. Renombrar columnas
    renames = {
        'fecha': 'corte',
        'cc_nit': 'cedula',
        'dsm_num': 'numero',
        'vlr_fnz': 'valor'
    }
    df_proc.rename(columns={k: v for k, v in renames.items() if k in df_proc.columns}, inplace=True)
    logger.info("Columnas de FNZ001 renombradas.")

    # 3. Convertir tipos de datos
    if 'corte' in df_proc.columns:
        df_proc['corte'] = pd.to_datetime(df_proc['corte'], errors='coerce')
    if 'cedula' in df_proc.columns:
        df_proc['cedula'] = df_proc['cedula'].astype(str)
    if 'numero' in df_proc.columns:
        df_proc['numero'] = df_proc['numero'].astype(str)
    if 'valor' in df_proc.columns:
        df_proc['valor'] = pd.to_numeric(df_proc['valor'], errors='coerce')
    logger.info("Tipos de datos de FNZ001 convertidos.")

    # 4. Crear la llave 'cedula_numero'
    if 'cedula' in df_proc.columns and 'numero' in df_proc.columns:
        df_proc['cedula_numero'] = df_proc['cedula'] + '-' + df_proc['numero']
        logger.info("Creada la columna 'cedula_numero' en FNZ001.")
    else:
        logger.warning("No se pudieron encontrar 'cedula' y 'numero' en FNZ001 para crear la llave.")

    # 5. Eliminar duplicados por 'numero'
    if 'numero' in df_proc.columns:
        df_proc.drop_duplicates(subset=['numero'], keep='first', inplace=True)
        logger.info(f"Eliminados duplicados en FNZ001 por 'numero': {registros_antes:,} -> {len(df_proc):,} registros.")
    else:
        logger.warning("La columna 'numero' no se encuentra en FNZ001. No se eliminaron duplicados.")

    logger.info("Limpieza y procesamiento de FNZ001 completado.")
    return df_proc
