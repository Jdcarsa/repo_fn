"""
Módulo principal para la ejecución del ETL de Finnovarisk.
Versión simplificada y modular
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
from src.utilidades.validador import diagnosticar_dataframe, validar_dataframe


def main():
    """Función principal del ETL - Versión simplificada."""
    logger = configurar_logger()
    inicio = time.time()
    errores = []
    advertencias = []
    
    # Configuración inicial
    logger.info("🚀 INICIANDO ETL FINNOVARISK")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 {Path.cwd()}\n")
    
    try:
        # ============================================
        # 1. CARGA DE DATOS
        # ============================================
        logger.info("📂 CARGA DE DATOS")
        logger.info("-" * 40)
        
        # Datasets principales (críticos)
        logger.info("Cargando FNZ007...")
        df_fnz007 = cargador.cargar_dataset("FNZ007")
        resultado_fnz007 = validar_dataframe(df_fnz007, "FNZ007")
        if resultado_fnz007["valido"]:
            logger.info(f"✅ FNZ007 cargado: {len(df_fnz007):,} registros")
        else:
            logger.warning(f"⚠️ FNZ007 con advertencias: {resultado_fnz007['mensaje']}")
            advertencias.append(f"FNZ007: {resultado_fnz007['mensaje']}")

        logger.info("Cargando Análisis de Cartera...")
        df_ac = cargador.cargar_dataset("ANALISIS_CARTERA")
        resultado_ac = validar_dataframe(df_ac, "Análisis de Cartera")
        if resultado_ac["valido"]:
            logger.info(f"✅ Análisis de Cartera cargado: {len(df_ac):,} registros")
        else:
            logger.warning(f"⚠️ Análisis de Cartera con advertencias: {resultado_ac['mensaje']}")
            advertencias.append(f"ANALISIS_CARTERA: {resultado_ac['mensaje']}")

        logger.info("Cargando FNZ001...")
        df_fnz001 = cargador.cargar_dataset("FNZ001")
        resultado_fnz001 = validar_dataframe(df_fnz001, "FNZ001")
        if resultado_fnz001["valido"]:
            logger.info(f"✅ FNZ001 cargado: {len(df_fnz001):,} registros")
        else:
            logger.warning(f"⚠️ FNZ001 con advertencias: {resultado_fnz001['mensaje']}")
            advertencias.append(f"FNZ001: {resultado_fnz001['mensaje']}")
        
        # Verificar datasets críticos
        if any(df is None for df in [df_fnz007, df_ac, df_fnz001]):
            logger.error("❌ Faltan datasets críticos - Proceso detenido")
            raise Exception("Error en carga de datos críticos")
        
        # Datasets secundarios
        logger.info("Cargando Edades...")
        try:
            df_edades = cargador.cargar_dataset("EDADES")
            if df_edades is not None and len(df_edades) > 0:
                logger.info(f"✅ Edades cargado: {len(df_edades):,} registros")
            else:
                logger.warning("⚠️ Edades cargado pero sin registros")
                df_edades = None
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar Edades: {str(e)}")
            df_edades = None

        logger.info("Cargando R05...")
        try:
            df_r05 = cargador.cargar_dataset("R05")
            if df_r05 is not None and len(df_r05) > 0:
                logger.info(f"✅ R05 cargado: {len(df_r05):,} registros")
            else:
                logger.warning("⚠️ R05 cargado pero sin registros")
                df_r05 = None
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar R05: {str(e)}")
            df_r05 = None

        logger.info("Cargando Recaudos...")
        try:
            df_recaudos = cargador.cargar_dataset("RECAUDOS")
            if df_recaudos is not None and len(df_recaudos) > 0:
                logger.info(f"✅ Recaudos cargado: {len(df_recaudos):,} registros")
            else:
                logger.warning("⚠️ Recaudos cargado pero sin registros")
                df_recaudos = None
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar Recaudos: {str(e)}")
            df_recaudos = None
        
        # Auxiliares
        try:
            dict_auxiliares = cargador.cargar_auxiliares()
            if dict_auxiliares:
                total = sum(len(df) for df in dict_auxiliares.values())
                logger.info(f"✅ Auxiliares: {total:,} registros totales")
        except Exception as e:
            logger.warning(f"⚠️ Auxiliares no cargados: {str(e)}")
            dict_auxiliares = {}
        
        # ============================================
        # 2. TRANSFORMACIONES
        # ============================================
        logger.info("\n🔄 TRANSFORMACIONES")
        logger.info("-" * 40)
        
        # Transformar Análisis de Cartera
        registros_antes = len(df_ac)
        df_ac = filtrar_finansuenos_ac(df_ac)
        df_ac = procesar_analisis_cartera(df_ac)
        df_ac = manejar_duplicados_ac(df_ac)
        df_ac = crear_columna_mora_ac(df_ac)
        logger.info(f"✅ Análisis de Cartera: {registros_antes:,} → {len(df_ac):,} registros tras transformaciones completas (limpieza, duplicados, mora).")

        # Transformar FNZ007
        registros_antes_fnz = len(df_fnz007)
        df_fnz007 = procesar_fnz007(df_fnz007)
        logger.info(f"✅ FNZ007: {registros_antes_fnz:,} → {len(df_fnz007):,} registros tras limpieza y transformación.")

        # Transformar Edades
        if df_edades is not None:
            registros_antes_edades = len(df_edades)
            df_edades = procesar_edades(df_edades)
            logger.info(f"✅ Edades: {registros_antes_edades:,} → {len(df_edades):,} registros tras limpieza y transformación.")
        else:
            logger.warning("⚠️ Edades no se transformó porque el DataFrame es None.")

        # Transformar R05
        if df_r05 is not None:
            registros_antes_r05 = len(df_r05)
            df_r05 = procesar_r05(df_r05)
            logger.info(f"✅ R05: {registros_antes_r05:,} → {len(df_r05):,} registros tras limpieza y transformación.")
        else:
            logger.warning("⚠️ R05 no se transformó porque el DataFrame es None.")

        # Transformar Recaudos
        if df_recaudos is not None:
            registros_antes_recaudos = len(df_recaudos)
            df_recaudos = procesar_recaudos(df_recaudos)
            logger.info(f"✅ Recaudos: {registros_antes_recaudos:,} → {len(df_recaudos):,} registros tras limpieza y transformación.")
        else:
            logger.warning("⚠️ Recaudos no se transformó porque el DataFrame es None.")

        # Transformar FNZ001
        if df_fnz001 is not None:

            registros_antes_fnz001 = len(df_fnz001)
            df_fnz001 = procesar_fnz001(df_fnz001)
            logger.info(f"✅ FNZ001: {registros_antes_fnz001:,} → {len(df_fnz001):,} registros tras limpieza y transformación.")
        else:
            logger.warning("⚠️ FNZ001 no se transformó porque el DataFrame es None.")
        
        # ============================================
        # 3. RESULTADOS FINALES
        # ============================================
        logger.info("\n📊 RESULTADOS")
        logger.info("-" * 40)
        
        # Preparar datos para resumen
        datos = {
            "fnz007": df_fnz007,
            "ac": df_ac,
            "fnz001": df_fnz001,
            "edades": df_edades,
            "r05": df_r05,
            "recaudos": df_recaudos
        }

        # Guardar resumen
        fin = time.time()
        tiempo_total = fin - inicio
        
        resumen = {
            "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tiempo_segundos": tiempo_total,
            "registros_cargados": {
                nombre: len(df) if df is not None else 0 
                for nombre, df in datos.items()
            },
            "errores": errores,
            "advertencias": advertencias,
            "exitoso": len(errores) == 0
        }
        
        guardar_resumen_ejecucion(resumen)
        
        # Mostrar resultado final
        minutos = tiempo_total / 60
        
        logger.info("=" * 50)
        if errores:
            logger.error("❌ PROCESO COMPLETADO CON ERRORES")
        else:
            logger.info("✅ PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 50)
        
        logger.info(f"⏱️  Tiempo total: {minutos:.2f} minutos ({tiempo_total:.1f} segundos)")
        
        if advertencias:
            logger.warning(f"\n⚠️  Advertencias ({len(advertencias)}):")
            for adv in advertencias:
                logger.warning(f"   • {adv}")
        
        if errores:
            logger.error(f"\n❌ Errores ({len(errores)}):")
            for err in errores:
                logger.error(f"   • {err}")
        else:
            logger.info("\n🎉 ¡Sin errores críticos!")
        
        return datos
        
    except KeyboardInterrupt:
        logger.error("\n❌ Interrumpido por usuario")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ Error crítico: {str(e)}")
        
        # Guardar resumen de error
        fin = time.time()
        resumen_error = {
            "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tiempo_segundos": fin - inicio,
            "error_critico": str(e),
            "errores": errores,
            "advertencias": advertencias,
            "exitoso": False
        }
        guardar_resumen_ejecucion(resumen_error)
        sys.exit(1)

if __name__ == "__main__":
    main()