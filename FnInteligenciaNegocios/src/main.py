"""
Módulo principal para la ejecución del ETL de Finnovarisk.
Versión actualizada con correcciones implementadas
"""
import time
import sys
from datetime import datetime
from pathlib import Path

from src.cargador_datos import cargador
from src.transformadores import (
    filtrar_finansuenos_ac,
    procesar_analisis_cartera,
    manejar_duplicados_ac,
    crear_columna_mora_ac,
    procesar_fnz007,  
    procesar_edades,
    procesar_r05,
    procesar_recaudos,
    procesar_fnz001
)
from src.utilidades.logger import configurar_logger, guardar_resumen_ejecucion
from src.utilidades.validador import diagnosticar_dataframe, validar_dataframe, generar_reporte_calidad


def main():
    """Función principal del ETL - Versión con correcciones completas."""
    logger = configurar_logger()
    inicio = time.time()
    errores = []
    advertencias = []
    
    # ============================================
    # ENCABEZADO
    # ============================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("🚀 ETL FINNOVARISK - INICIO DE EJECUCIÓN")
    logger.info("=" * 70)
    logger.info(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 Directorio: {Path.cwd()}")
    logger.info("=" * 70)
    logger.info("")
    
    try:
        # ============================================
        # FASE 1: CARGA DE DATOS
        # ============================================
        logger.info("=" * 70)
        logger.info("📂 FASE 1: CARGA DE DATOS")
        logger.info("=" * 70)
        logger.info("")
        
        # --- Datasets Críticos (obligatorios) ---
        logger.info("🔴 Cargando datasets CRÍTICOS...")
        logger.info("-" * 40)
        
        # FNZ007 - Con correcciones de columnas y formato de fecha
        logger.info("1️⃣  Cargando FNZ007...")
        df_fnz007 = cargador.cargar_dataset("FNZ007")
        resultado_fnz007 = validar_dataframe(df_fnz007, "FNZ007")
        if not resultado_fnz007["valido"]:
            error_msg = f"FNZ007 no válido: {resultado_fnz007['mensaje']}"
            logger.error(f"❌ {error_msg}")
            errores.append(error_msg)
            raise Exception(error_msg)
        logger.info(f"   ✅ {len(df_fnz007):,} registros | {len(df_fnz007.columns)} columnas")
        if resultado_fnz007["advertencias"]:
            for adv in resultado_fnz007["advertencias"]:
                logger.warning(f"   ⚠️  {adv}")
                advertencias.append(f"FNZ007: {adv}")
        
        # Análisis de Cartera
        logger.info("2️⃣  Cargando Análisis de Cartera...")
        df_ac = cargador.cargar_dataset("ANALISIS_CARTERA")
        resultado_ac = validar_dataframe(df_ac, "Análisis de Cartera")
        if not resultado_ac["valido"]:
            error_msg = f"Análisis de Cartera no válido: {resultado_ac['mensaje']}"
            logger.error(f"❌ {error_msg}")
            errores.append(error_msg)
            raise Exception(error_msg)
        logger.info(f"   ✅ {len(df_ac):,} registros | {len(df_ac.columns)} columnas")
        if resultado_ac["advertencias"]:
            for adv in resultado_ac["advertencias"]:
                logger.warning(f"   ⚠️  {adv}")
                advertencias.append(f"ANALISIS_CARTERA: {adv}")
        
        # FNZ001
        logger.info("3️⃣  Cargando FNZ001...")
        df_fnz001 = cargador.cargar_dataset("FNZ001")
        resultado_fnz001 = validar_dataframe(df_fnz001, "FNZ001")
        if not resultado_fnz001["valido"]:
            error_msg = f"FNZ001 no válido: {resultado_fnz001['mensaje']}"
            logger.error(f"❌ {error_msg}")
            errores.append(error_msg)
            raise Exception(error_msg)
        logger.info(f"   ✅ {len(df_fnz001):,} registros | {len(df_fnz001.columns)} columnas")
        if resultado_fnz001["advertencias"]:
            for adv in resultado_fnz001["advertencias"]:
                logger.warning(f"   ⚠️  {adv}")
                advertencias.append(f"FNZ001: {adv}")
        
        logger.info("")
        logger.info("✅ Datasets críticos cargados exitosamente")
        logger.info("")
        
        # --- Datasets Secundarios (opcionales) ---
        logger.info("🟡 Cargando datasets SECUNDARIOS...")
        logger.info("-" * 40)
        
        # Edades
        logger.info("4️⃣  Cargando Edades...")
        try:
            df_edades = cargador.cargar_dataset("EDADES")
            if df_edades is not None and len(df_edades) > 0:
                logger.info(f"   ✅ {len(df_edades):,} registros | {len(df_edades.columns)} columnas")
            else:
                logger.warning("   ⚠️  Edades cargado pero sin registros")
                advertencias.append("EDADES: Sin datos")
                df_edades = None
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo cargar Edades: {str(e)}")
            advertencias.append(f"EDADES: {str(e)}")
            df_edades = None
        
        # R05
        logger.info("5️⃣  Cargando R05...")
        try:
            df_r05 = cargador.cargar_dataset("R05")
            if df_r05 is not None and len(df_r05) > 0:
                logger.info(f"   ✅ {len(df_r05):,} registros | {len(df_r05.columns)} columnas")
            else:
                logger.warning("   ⚠️  R05 cargado pero sin registros")
                advertencias.append("R05: Sin datos")
                df_r05 = None
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo cargar R05: {str(e)}")
            advertencias.append(f"R05: {str(e)}")
            df_r05 = None
        
        # Recaudos
        logger.info("6️⃣  Cargando Recaudos...")
        try:
            df_recaudos = cargador.cargar_dataset("RECAUDOS")
            if df_recaudos is not None and len(df_recaudos) > 0:
                logger.info(f"   ✅ {len(df_recaudos):,} registros | {len(df_recaudos.columns)} columnas")
            else:
                logger.warning("   ⚠️  Recaudos cargado pero sin registros")
                advertencias.append("RECAUDOS: Sin datos")
                df_recaudos = None
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo cargar Recaudos: {str(e)}")
            advertencias.append(f"RECAUDOS: {str(e)}")
            df_recaudos = None
        
        # Auxiliares
        logger.info("7️⃣  Cargando Auxiliares...")
        try:
            dict_auxiliares = cargador.cargar_auxiliares()
            if dict_auxiliares:
                total = sum(len(df) for df in dict_auxiliares.values())
                logger.info(f"   ✅ {len(dict_auxiliares)} archivos | {total:,} registros totales")
            else:
                logger.warning("   ⚠️  No se cargaron auxiliares")
                advertencias.append("AUXILIARES: Sin datos")
        except Exception as e:
            logger.warning(f"   ⚠️  Auxiliares no cargados: {str(e)}")
            advertencias.append(f"AUXILIARES: {str(e)}")
            dict_auxiliares = {}
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("")
        
        # ============================================
        # FASE 2: TRANSFORMACIONES
        # ============================================
        logger.info("=" * 70)
        logger.info("🔄 FASE 2: TRANSFORMACIONES Y LIMPIEZA")
        logger.info("=" * 70)
        logger.info("")
        
        # --- Análisis de Cartera ---
        logger.info("1️⃣  Transformando Análisis de Cartera...")
        logger.info("-" * 40)
        registros_ac_inicial = len(df_ac)
        
        logger.info("   • Filtrando FINANSUEÑOS...")
        df_ac = filtrar_finansuenos_ac(df_ac)
        logger.info(f"     {registros_ac_inicial:,} → {len(df_ac):,} registros")
        
        logger.info("   • Procesando columnas...")
        df_ac = procesar_analisis_cartera(df_ac)
        
        logger.info("   • Manejando duplicados...")
        registros_antes_dup = len(df_ac)
        df_ac = manejar_duplicados_ac(df_ac)
        logger.info(f"     {registros_antes_dup:,} → {len(df_ac):,} registros")
        
        logger.info("   • Creando columna MORA...")
        df_ac = crear_columna_mora_ac(df_ac)
        
        logger.info(f"   ✅ COMPLETO: {registros_ac_inicial:,} → {len(df_ac):,} registros finales")
        logger.info("")
        
        # --- FNZ007 ---
        logger.info("2️⃣  Transformando FNZ007...")
        logger.info("-" * 40)
        registros_fnz_inicial = len(df_fnz007)
        
        logger.info("   • Dividiendo columna DESEMBOLSO...")
        logger.info("   • Filtrando DF == 'DF'...")
        logger.info("   • Limpiando outliers (fecha nacimiento, gastos, ingresos, avalúo)...")
        logger.info("   • Unificando columnas de empleo...")
        logger.info("   • Categorizando estado civil y nivel escolar...")
        logger.info("   • Eliminando columnas innecesarias...")
        logger.info("   • Renombrando columnas...")
        
        df_fnz007 = procesar_fnz007(df_fnz007)
        
        logger.info(f"   ✅ COMPLETO: {registros_fnz_inicial:,} → {len(df_fnz007):,} registros finales")
        logger.info("")
        
        # --- Edades ---
        if df_edades is not None:
            logger.info("3️⃣  Transformando Edades...")
            logger.info("-" * 40)
            registros_edades_inicial = len(df_edades)
            
            logger.info("   • Limpiando columnas...")
            logger.info("   • Creando llave cedula_numero...")
            logger.info("   • Filtrando por línea de crédito...")
            
            df_edades = procesar_edades(df_edades)
            
            logger.info(f"   ✅ COMPLETO: {registros_edades_inicial:,} → {len(df_edades):,} registros finales")
            logger.info("")
        else:
            logger.warning("3️⃣  Edades no se procesó (DataFrame vacío o None)")
            logger.info("")
        
        # --- R05 ---
        if df_r05 is not None:
            logger.info("4️⃣  Transformando R05...")
            logger.info("-" * 40)
            registros_r05_inicial = len(df_r05)
            
            logger.info("   • Renombrando columnas...")
            logger.info("   • Creando llave cedula_numero...")
            logger.info("   • Filtrando abono > 0...")
            logger.info("   • Agrupando duplicados...")
            
            df_r05 = procesar_r05(df_r05)
            
            if df_r05 is not None:
                logger.info(f"   ✅ COMPLETO: {registros_r05_inicial:,} → {len(df_r05):,} registros finales")
            else:
                logger.warning("   ⚠️  R05 retornó None tras procesamiento")
                advertencias.append("R05: Error en procesamiento")
            logger.info("")
        else:
            logger.warning("4️⃣  R05 no se procesó (DataFrame vacío o None)")
            logger.info("")
        
        # --- Recaudos ---
        if df_recaudos is not None:
            logger.info("5️⃣  Transformando Recaudos...")
            logger.info("-" * 40)
            registros_recaudos_inicial = len(df_recaudos)
            
            logger.info("   • Identificando columnas dinámicamente...")
            logger.info("   • Creando llave cedula_numero...")
            logger.info("   • Filtrando capitalrec > 0...")
            logger.info("   • Agrupando duplicados...")
            
            df_recaudos = procesar_recaudos(df_recaudos)
            
            if df_recaudos is not None:
                logger.info(f"   ✅ COMPLETO: {registros_recaudos_inicial:,} → {len(df_recaudos):,} registros finales")
            else:
                logger.warning("   ⚠️  Recaudos retornó None tras procesamiento")
                advertencias.append("RECAUDOS: Error en procesamiento")
            logger.info("")
        else:
            logger.warning("5️⃣  Recaudos no se procesó (DataFrame vacío o None)")
            logger.info("")
        
        # --- FNZ001 ---
        if df_fnz001 is not None:
            logger.info("6️⃣  Transformando FNZ001...")
            logger.info("-" * 40)
            registros_fnz001_inicial = len(df_fnz001)
            
            logger.info("   • Limpiando columnas...")
            logger.info("   • Creando llave cedula_numero...")
            logger.info("   • Eliminando duplicados...")
            
            df_fnz001 = procesar_fnz001(df_fnz001)
            
            if df_fnz001 is not None:
                logger.info(f"   ✅ COMPLETO: {registros_fnz001_inicial:,} → {len(df_fnz001):,} registros finales")
            else:
                logger.warning("   ⚠️  FNZ001 retornó None tras procesamiento")
                advertencias.append("FNZ001: Error en procesamiento")
            logger.info("")
        else:
            logger.warning("6️⃣  FNZ001 no se procesó (DataFrame vacío o None)")
            logger.info("")
        
        logger.info("=" * 70)
        logger.info("")
        
        # ============================================
        # FASE 3: RESUMEN Y GUARDADO
        # ============================================
        logger.info("=" * 70)
        logger.info("📊 FASE 3: RESUMEN DE RESULTADOS")
        logger.info("=" * 70)
        logger.info("")
        
        # Preparar datos para resumen
        datos = {
            "fnz007": df_fnz007,
            "ac": df_ac,
            "fnz001": df_fnz001,
            "edades": df_edades,
            "r05": df_r05,
            "recaudos": df_recaudos
        }
        
        # Mostrar tabla de resumen
        logger.info("📋 Tabla de Registros:")
        logger.info("-" * 70)
        logger.info(f"{'Dataset':<20} | {'Registros':<15} | {'Columnas':<10} | {'Estado':<10}")
        logger.info("-" * 70)
        for nombre, df in datos.items():
            if df is not None and len(df) > 0:
                registros = f"{len(df):,}"
                columnas = f"{len(df.columns)}"
                estado = "✅"
            else:
                registros = "0"
                columnas = "0"
                estado = "⚠️"
            logger.info(f"{nombre.upper():<20} | {registros:<15} | {columnas:<10} | {estado:<10}")
        logger.info("-" * 70)
        logger.info("")
        
        # Calcular tiempo de ejecución
        fin = time.time()
        tiempo_total = fin - inicio
        minutos = tiempo_total / 60
        
        # Preparar resumen para JSON
        resumen = {
            "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tiempo_segundos": round(tiempo_total, 2),
            "tiempo_minutos": round(minutos, 2),
            "registros_cargados": {
                nombre: len(df) if df is not None else 0 
                for nombre, df in datos.items()
            },
            "errores": errores,
            "advertencias": advertencias,
            "exitoso": len(errores) == 0
        }
        
        # Guardar resumen en JSON
        guardar_resumen_ejecucion(resumen)
        
        # ============================================
        # RESULTADO FINAL
        # ============================================
        logger.info("=" * 70)
        if errores:
            logger.error("❌ PROCESO COMPLETADO CON ERRORES")
        else:
            logger.info("✅ PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 70)
        logger.info("")
        
        logger.info(f"⏱️  Tiempo de ejecución: {minutos:.2f} minutos ({tiempo_total:.1f} segundos)")
        logger.info("")
        
        if advertencias:
            logger.warning(f"⚠️  Se generaron {len(advertencias)} advertencias:")
            for i, adv in enumerate(advertencias[:10], 1):
                logger.warning(f"   {i}. {adv}")
            if len(advertencias) > 10:
                logger.warning(f"   ... y {len(advertencias) - 10} advertencias más")
            logger.info("")
        
        if errores:
            logger.error(f"❌ Se encontraron {len(errores)} errores críticos:")
            for i, err in enumerate(errores, 1):
                logger.error(f"   {i}. {err}")
            logger.info("")
        else:
            logger.info("🎉 ¡Sin errores críticos!")
            logger.info("")
        
        logger.info("=" * 70)
        logger.info("📁 Archivos generados:")
        logger.info(f"   • Log: logs/logger.log")
        logger.info(f"   • Resumen: logs/resumen_ejecucion.json")
        logger.info("=" * 70)
        logger.info("")
        
        return datos
        
    except KeyboardInterrupt:
        logger.error("")
        logger.error("=" * 70)
        logger.error("⛔ PROCESO INTERRUMPIDO POR EL USUARIO")
        logger.error("=" * 70)
        logger.error("")
        sys.exit(1)
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 70)
        logger.error("💥 ERROR CRÍTICO EN LA EJECUCIÓN")
        logger.error("=" * 70)
        logger.error(f"Error: {str(e)}")
        logger.error("")
        
        # Guardar resumen de error
        fin = time.time()
        resumen_error = {
            "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tiempo_segundos": round(fin - inicio, 2),
            "error_critico": str(e),
            "errores": errores + [str(e)],
            "advertencias": advertencias,
            "exitoso": False
        }
        guardar_resumen_ejecucion(resumen_error)
        
        logger.error("=" * 70)
        logger.error("📁 Log guardado en: logs/logger.log")
        logger.error("=" * 70)
        logger.error("")
        
        sys.exit(1)


if __name__ == "__main__":
    main()