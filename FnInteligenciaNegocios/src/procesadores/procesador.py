"""
Módulo de procesamiento - Joins y creación de BaseFNZ final
"""
import pandas as pd
from src.utilidades.logger import configurar_logger

logger = configurar_logger('finnovarisk.procesador')


def unir_datasets(df_fnz007, df_ac, df_fnz001, df_edades, df_r05, df_recaudos, dict_auxiliares):
    """
    Realiza todos los joins según el script R original.
    
    Secuencia de joins:
    1. BaseFNZ (FNZ007 + FNZ001)
    2. BaseFNZ + Análisis de Cartera
    3. BaseFNZ + Edades
    4. BaseFNZ + R05
    5. BaseFNZ + Recaudos
    6. Filtro por Estado Desembolsos
    
    Returns:
        DataFrame BaseFNZ final con todos los datos unidos
    """
    logger.info("="*70)
    logger.info("🔗 INICIANDO UNIÓN DE DATASETS")
    logger.info("="*70)
    logger.info("")
    
    # ========================================
    # JOIN 1: FNZ007 + FNZ001
    # ========================================
    logger.info("1️⃣ JOIN: FNZ007 + FNZ001")
    logger.info("-"*40)
    
    # Preparar FNZ001 para el join
    fnz001_para_join = df_fnz001[['numero', 'cedula', 'corte','valor']].copy()
    
    # Eliminar duplicados por 'numero' (keeping first)
    registros_fnz001_antes = len(fnz001_para_join)
    fnz001_para_join = fnz001_para_join.drop_duplicates(subset=['numero'], keep='first')
    logger.info(f"   FNZ001 duplicados eliminados: {registros_fnz001_antes:,} → {len(fnz001_para_join):,}")
    
    # Left join
    BaseFNZ = df_fnz007.merge(
        fnz001_para_join,
        on='numero',
        how='left',
        suffixes=('', '_fnz001')
    )
    
    logger.info(f"   ✅ JOIN completado: {len(BaseFNZ):,} registros")
    logger.info(f"   Columnas añadidas: cedula, corte (de FNZ001)")
    logger.info("")
    
    # ========================================
    # Crear cedula_numero para BaseFNZ
    # ========================================
    logger.info("📝 Creando llave cedula_numero en BaseFNZ")
    logger.info("-"*40)
    
    if 'cedula' in BaseFNZ.columns and 'numero' in BaseFNZ.columns:
        BaseFNZ['cedula'] = BaseFNZ['cedula'].astype(str).fillna('').str.strip()
        BaseFNZ['numero'] = BaseFNZ['numero'].astype(str).fillna('').str.strip()
        BaseFNZ['cedula_numero'] = BaseFNZ['cedula'] + '-' + BaseFNZ['numero']
        
        # Verificar llaves creadas
        llaves_validas = BaseFNZ['cedula_numero'].notna().sum()
        logger.info(f"   ✅ Llave cedula_numero creada: {llaves_validas:,} registros válidos")
    else:
        logger.error("   ❌ No se pudo crear cedula_numero")
        raise ValueError("Faltan columnas 'cedula' o 'numero' en BaseFNZ")
    
    logger.info("")
    
    # ========================================
    # JOIN 2: BaseFNZ + Análisis de Cartera
    # ========================================
    logger.info("2️⃣ JOIN: BaseFNZ + Análisis de Cartera")
    logger.info("-"*40)
    
    registros_antes = len(BaseFNZ)
    
    BaseFNZ = BaseFNZ.merge(
        df_ac,
        on=['cedula_numero', 'corte'],
        how='left',
        suffixes=('', '_ac')
    )
    
    # Contar cuántos registros hicieron match
    matches = BaseFNZ[df_ac.columns[0]].notna().sum()  # Primera columna de AC como indicador
    porcentaje_match = (matches / len(BaseFNZ)) * 100
    
    logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(BaseFNZ):,} registros")
    logger.info(f"   Matches encontrados: {matches:,} ({porcentaje_match:.1f}%)")
    logger.info(f"   Columnas añadidas: {len(df_ac.columns)} de Análisis de Cartera")
    logger.info("")
    
    # ========================================
    # JOIN 3: BaseFNZ + Edades
    # ========================================
    logger.info("3️⃣ JOIN: BaseFNZ + Edades")
    logger.info("-"*40)
    
    if df_edades is not None and len(df_edades) > 0:
        registros_antes = len(BaseFNZ)
        
        BaseFNZ = BaseFNZ.merge(
            df_edades,
            on='cedula_numero',
            how='left',
            suffixes=('', '_edades')
        )
        
        matches = BaseFNZ[df_edades.columns[0]].notna().sum()
        porcentaje_match = (matches / len(BaseFNZ)) * 100
        
        logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(BaseFNZ):,} registros")
        logger.info(f"   Matches encontrados: {matches:,} ({porcentaje_match:.1f}%)")
    else:
        logger.warning("   ⚠️ Edades no disponible - JOIN omitido")
    
    logger.info("")
    
    # ========================================
    # JOIN 4: BaseFNZ + R05
    # ========================================
    logger.info("4️⃣ JOIN: BaseFNZ + R05")
    logger.info("-"*40)
    
    if df_r05 is not None and len(df_r05) > 0:
        registros_antes = len(BaseFNZ)
        
        BaseFNZ = BaseFNZ.merge(
            df_r05,
            on=['cedula_numero', 'corte'],
            how='left',
            suffixes=('', '_r05')
        )
        
        matches = BaseFNZ['ABONO1'].notna().sum() if 'ABONO1' in BaseFNZ.columns else 0
        porcentaje_match = (matches / len(BaseFNZ)) * 100
        
        logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(BaseFNZ):,} registros")
        logger.info(f"   Matches encontrados: {matches:,} ({porcentaje_match:.1f}%)")
    else:
        logger.warning("   ⚠️ R05 no disponible - JOIN omitido")
    
    logger.info("")
    
    # ========================================
    # JOIN 5: BaseFNZ + Recaudos
    # ========================================
    logger.info("5️⃣ JOIN: BaseFNZ + Recaudos")
    logger.info("-"*40)
    
    if df_recaudos is not None and len(df_recaudos) > 0:
        registros_antes = len(BaseFNZ)
        
        BaseFNZ = BaseFNZ.merge(
            df_recaudos,
            on=['cedula_numero', 'corte'],
            how='left',
            suffixes=('', '_recaudos')
        )
        
        matches = BaseFNZ['capitalrec'].notna().sum() if 'capitalrec' in BaseFNZ.columns else 0
        porcentaje_match = (matches / len(BaseFNZ)) * 100
        
        logger.info(f"   ✅ JOIN completado: {registros_antes:,} → {len(BaseFNZ):,} registros")
        logger.info(f"   Matches encontrados: {matches:,} ({porcentaje_match:.1f}%)")
    else:
        logger.warning("   ⚠️ Recaudos no disponible - JOIN omitido")
    
    logger.info("")
    
    # ========================================
    # FILTRO 6: Eliminar Estado Desembolsos
    # ========================================
    logger.info("6️⃣ FILTRO: Eliminando Estado Desembolsos")
    logger.info("-"*40)
    
    if 'estado_desembolsos' in dict_auxiliares:
        estado_desemp = dict_auxiliares['estado_desembolsos']
        
        # Crear llave en estado_desembolsos si no existe
        if 'cedula_numero' not in estado_desemp.columns:
            if 'cedula' in estado_desemp.columns and 'numero' in estado_desemp.columns:
                estado_desemp['cedula'] = estado_desemp['cedula'].astype(str).fillna('').str.strip()
                estado_desemp['numero'] = estado_desemp['numero'].astype(str).fillna('').str.strip()
                estado_desemp['cedula_numero'] = estado_desemp['cedula'] + '-' + estado_desemp['numero']
        
        registros_antes = len(BaseFNZ)
        
        # Filtrar
        BaseFNZ = BaseFNZ[~BaseFNZ['cedula_numero'].isin(estado_desemp['cedula_numero'])]
        
        eliminados = registros_antes - len(BaseFNZ)
        porcentaje_eliminado = (eliminados / registros_antes) * 100
        
        logger.info(f"   ✅ Filtro aplicado: {registros_antes:,} → {len(BaseFNZ):,} registros")
        logger.info(f"   Eliminados: {eliminados:,} ({porcentaje_eliminado:.1f}%)")
    else:
        logger.warning("   ⚠️ Estado Desembolsos no disponible - Filtro omitido")
    
    logger.info("")


    logger.info("7️⃣ Eliminando duplicados más recientes de BaseFNZ")
    logger.info("-"*40)
    BaseFNZ = eliminar_duplicados_mas_recientes(BaseFNZ)
    logger.info(f"   ✅ Duplicados eliminados: {registros_antes:,} → {len(BaseFNZ):,}")
    # ========================================
    # RESUMEN FINAL
    # ========================================
    logger.info("="*70)
    logger.info("✅ UNIÓN DE DATASETS COMPLETADA")
    logger.info("="*70)
    logger.info(f"📊 BaseFNZ Final:")
    logger.info(f"   • Registros: {len(BaseFNZ):,}")
    logger.info(f"   • Columnas: {len(BaseFNZ.columns)}")
    logger.info(f"   • Memoria: {BaseFNZ.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    logger.info("")
    
    return BaseFNZ



# FALTA AGREGAR en procesador.py o fnz007.py

def eliminar_duplicados_mas_recientes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina el registro más reciente de cada cedula_numero duplicada.
    """
    df = df.copy()
    
    # Contar duplicados
    df['total'] = df.groupby('cedula_numero')['cedula_numero'].transform('count')
    
    # Ordenar por corte dentro de cada grupo
    df = df.sort_values(['cedula_numero', 'corte'])
    
    # Marcar el más reciente
    df['es_mas_reciente'] = df.groupby('cedula_numero').cumcount() == (df['total'] - 1)
    
    # Filtrar: eliminar SOLO si es duplicado Y es el más reciente
    df = df[~((df['total'] > 1) & df['es_mas_reciente'])]
    
    # Limpiar columnas auxiliares
    df = df.drop(columns=['total', 'es_mas_reciente'])
    
    return df