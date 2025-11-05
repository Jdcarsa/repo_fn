"""
Módulo principal para la ejecución del ETL de Finnovarisk.
Versión COMPLETA con todos los módulos implementados.
"""
import time
import sys
from datetime import datetime
from pathlib import Path

from src.procesadores import unir_datasets, crear_cosechas, crear_crm, crear_comportamiento
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
from src.utilidades.validador import validar_dataframe
from src.utilidades.exportador import guardar_excel, guardar_multiples_hojas


def main():
    """Función principal del ETL - Versión COMPLETA."""
    logger = configurar_logger()
    inicio = time.time()
    errores = []
    advertencias = []
    
    # ============================================
    # ENCABEZADO
    # ============================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("🚀 ETL FINNOVARISK - VERSIÓN COMPLETA")
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
        
        logger.info("🔴 Cargando datasets CRÍTICOS...")
        logger.info("-" * 40)
        
        # FNZ007
        logger.info("1️⃣  Cargando FNZ007...")
        df_fnz007 = cargador.cargar_dataset("FNZ007")
        resultado_fnz007 = validar_dataframe(df_fnz007, "FNZ007")
        if not resultado_fnz007["valido"]:
            raise Exception(f"FNZ007 no válido: {resultado_fnz007['mensaje']}")
        logger.info(f"   ✅ {len(df_fnz007):,} registros | {len(df_fnz007.columns)} columnas")
        
        # Análisis de Cartera
        logger.info("2️⃣  Cargando Análisis de Cartera...")
        df_ac = cargador.cargar_dataset("ANALISIS_CARTERA")
        resultado_ac = validar_dataframe(df_ac, "Análisis de Cartera")
        if not resultado_ac["valido"]:
            raise Exception(f"AC no válido: {resultado_ac['mensaje']}")
        logger.info(f"   ✅ {len(df_ac):,} registros | {len(df_ac.columns)} columnas")
        
        # FNZ001
        logger.info("3️⃣  Cargando FNZ001...")
        df_fnz001 = cargador.cargar_dataset("FNZ001")
        resultado_fnz001 = validar_dataframe(df_fnz001, "FNZ001")
        if not resultado_fnz001["valido"]:
            raise Exception(f"FNZ001 no válido: {resultado_fnz001['mensaje']}")
        logger.info(f"   ✅ {len(df_fnz001):,} registros | {len(df_fnz001.columns)} columnas")
        
        logger.info("")
        
        logger.info("🟡 Cargando datasets SECUNDARIOS...")
        logger.info("-" * 40)
        
        # Edades
        logger.info("4️⃣  Cargando Edades...")
        try:
            df_edades = cargador.cargar_dataset("EDADES")
            if df_edades is not None and len(df_edades) > 0:
                logger.info(f"   ✅ {len(df_edades):,} registros")
            else:
                df_edades = None
                advertencias.append("EDADES: Sin datos")
        except Exception as e:
            df_edades = None
            advertencias.append(f"EDADES: {str(e)}")
        
        # R05
        logger.info("5️⃣  Cargando R05...")
        try:
            df_r05 = cargador.cargar_dataset("R05")
            if df_r05 is not None and len(df_r05) > 0:
                logger.info(f"   ✅ {len(df_r05):,} registros")
            else:
                df_r05 = None
                advertencias.append("R05: Sin datos")
        except Exception as e:
            df_r05 = None
            advertencias.append(f"R05: {str(e)}")
        
        # Recaudos
        logger.info("6️⃣  Cargando Recaudos...")
        try:
            df_recaudos = cargador.cargar_dataset("RECAUDOS")
            if df_recaudos is not None and len(df_recaudos) > 0:
                logger.info(f"   ✅ {len(df_recaudos):,} registros")
            else:
                df_recaudos = None
                advertencias.append("RECAUDOS: Sin datos")
        except Exception as e:
            df_recaudos = None
            advertencias.append(f"RECAUDOS: {str(e)}")
        
        # Auxiliares
        logger.info("7️⃣  Cargando Auxiliares...")
        try:
            dict_auxiliares = cargador.cargar_auxiliares()
            if dict_auxiliares:
                logger.info(f"   ✅ {len(dict_auxiliares)} archivos")
            else:
                advertencias.append("AUXILIARES: Sin datos")
        except Exception as e:
            dict_auxiliares = {}
            advertencias.append(f"AUXILIARES: {str(e)}")
        
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
        
        # Análisis de Cartera
        logger.info("1️⃣  Transformando Análisis de Cartera...")
        logger.info("-" * 40)
        df_ac = filtrar_finansuenos_ac(df_ac)
        df_ac = procesar_analisis_cartera(df_ac)
        df_ac = manejar_duplicados_ac(df_ac)
        df_ac = crear_columna_mora_ac(df_ac)
        logger.info(f"   ✅ COMPLETO: {len(df_ac):,} registros")
        logger.info("")
        
        # FNZ007
        logger.info("2️⃣  Transformando FNZ007...")
        logger.info("-" * 40)
        df_fnz007 = procesar_fnz007(df_fnz007)
        logger.info(f"   ✅ COMPLETO: {len(df_fnz007):,} registros")
        logger.info("")
        
        # Edades
        if df_edades is not None:
            logger.info("3️⃣  Transformando Edades...")
            logger.info("-" * 40)
            df_edades = procesar_edades(df_edades)
            logger.info(f"   ✅ COMPLETO: {len(df_edades):,} registros")
            logger.info("")
        
        # R05
        if df_r05 is not None:
            logger.info("4️⃣  Transformando R05...")
            logger.info("-" * 40)
            df_r05 = procesar_r05(df_r05)
            if df_r05 is not None:
                logger.info(f"   ✅ COMPLETO: {len(df_r05):,} registros")
            logger.info("")
        
        # Recaudos
        if df_recaudos is not None:
            logger.info("5️⃣  Transformando Recaudos...")
            logger.info("-" * 40)
            df_recaudos = procesar_recaudos(df_recaudos)
            if df_recaudos is not None:
                logger.info(f"   ✅ COMPLETO: {len(df_recaudos):,} registros")
            logger.info("")
        
        # FNZ001
        if df_fnz001 is not None:
            logger.info("6️⃣  Transformando FNZ001...")
            logger.info("-" * 40)
            df_fnz001 = procesar_fnz001(df_fnz001)
            if df_fnz001 is not None:
                logger.info(f"   ✅ COMPLETO: {len(df_fnz001):,} registros")
            logger.info("")
        
        logger.info("=" * 70)
        logger.info("")
        
        # ============================================
        # FASE 3: CREACIÓN DE DATASETS FINALES
        # ============================================
        logger.info("=" * 70)
        logger.info("🏗️  FASE 3: CREACIÓN DE DATASETS FINALES")
        logger.info("=" * 70)
        logger.info("")
        
        # 3.1 BaseFNZ (unión de todos los datasets)
        logger.info("📊 3.1 Creando BaseFNZ (unión de datasets)...")
        logger.info("")
        BaseFNZ_final = unir_datasets(
            df_fnz007=df_fnz007,
            df_ac=df_ac,
            df_fnz001=df_fnz001,
            df_edades=df_edades,
            df_r05=df_r05,
            df_recaudos=df_recaudos,
            dict_auxiliares=dict_auxiliares
        )
        logger.info(f"✅ BaseFNZ creado: {len(BaseFNZ_final):,} registros")
        logger.info("")
        
        # 3.2 Cosechas
        logger.info("🌾 3.2 Creando Cosechas...")
        logger.info("")
        Cosechas, Cosechas_eliminados = crear_cosechas(
            df_fnz007=BaseFNZ_final,  # Usar BaseFNZ ya unido
            df_ac=df_ac,
            df_edades=df_edades,
            df_r05=df_r05,
            df_recaudos=df_recaudos
        )
        logger.info(f"✅ Cosechas creado: {len(Cosechas):,} registros")
        logger.info(f"   📄 Eliminados: {len(Cosechas_eliminados):,} registros")
        logger.info("")
        
        # 3.3 CRM
        logger.info("👥 3.3 Creando CRM...")
        logger.info("")
        CRM = crear_crm(
            df_fnz007=BaseFNZ_final,  # Usar BaseFNZ ya unido
            df_ac=df_ac
        )
        logger.info(f"✅ CRM creado: {len(CRM):,} registros")
        logger.info("")
        
        # 3.4 Comportamiento
        logger.info("📊 3.4 Creando Comportamiento...")
        logger.info("")
        Comportamiento = crear_comportamiento(
            df_ac=df_ac,
            df_fnz007=BaseFNZ_final,  # Usar BaseFNZ ya unido
            dict_auxiliares=dict_auxiliares
        )
        logger.info(f"✅ Comportamiento creado: {len(Comportamiento):,} registros")
        logger.info("")
        
        logger.info("=" * 70)
        logger.info("")
        
        # ============================================
        # FASE 4: EXPORTACIÓN
        # ============================================
        logger.info("=" * 70)
        logger.info("💾 FASE 4: EXPORTACIÓN DE RESULTADOS")
        logger.info("=" * 70)
        logger.info("")
        
        # Crear carpeta de salidas
        carpeta_salidas = Path("salidas")
        carpeta_salidas.mkdir(exist_ok=True)
        
        # 4.1 Guardar Cosechas
        logger.info("1️⃣  Guardando Cosechas...")
        ruta_cosechas = guardar_excel(Cosechas, "Cosechas", "salidas")
        logger.info("")
        
        # Guardar Cosechas eliminados
        if len(Cosechas_eliminados) > 0:
            logger.info("   📄 Guardando Cosechas eliminados...")
            ruta_eliminados = guardar_excel(Cosechas_eliminados, "Cosechas_Eliminados", "salidas")
            logger.info("")
        
        # 4.2 Guardar CRM
        logger.info("2️⃣  Guardando CRM...")
        ruta_crm = guardar_excel(CRM, "CRM", "salidas")
        logger.info("")
        
        # 4.3 Guardar Comportamiento
        logger.info("3️⃣  Guardando Comportamiento...")
        ruta_comportamiento = guardar_excel(Comportamiento, "Comportamiento", "salidas")
        logger.info("")
        
        # 4.4 Guardar BaseFNZ (opcional - si es necesario)
        logger.info("4️⃣  Guardando BaseFNZ (completo)...")
        ruta_basefnz = guardar_excel(BaseFNZ_final, "BaseFNZ_Completo", "salidas")
        logger.info("")
        
        # 4.5 Guardar BaseEdades (si existe)
        if df_edades is not None and len(df_edades) > 0:
            logger.info("5️⃣  Guardando BaseEdades...")
            ruta_edades = guardar_excel(df_edades, "BaseEdades", "salidas")
            logger.info("")
        
        logger.info("=" * 70)
        logger.info("🎉 EXPORTACIÓN COMPLETADA")
        logger.info("=" * 70)
        logger.info("")
        logger.info("📁 Archivos generados:")
        logger.info(f"   1. {ruta_cosechas.name}")
        logger.info(f"   2. {ruta_crm.name}")
        logger.info(f"   3. {ruta_comportamiento.name}")
        logger.info(f"   4. {ruta_basefnz.name}")
        if len(Cosechas_eliminados) > 0:
            logger.info(f"   5. Cosechas_Eliminados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx")
        if df_edades is not None:
            logger.info(f"   6. BaseEdades_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx")
        logger.info("")
        
        # ============================================
        # RESUMEN FINAL
        # ============================================
        fin = time.time()
        tiempo_total = fin - inicio
        minutos = tiempo_total / 60
        
        logger.info("=" * 70)
        logger.info("✅ PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 70)
        logger.info(f"⏱️  Tiempo de ejecución: {minutos:.2f} minutos ({tiempo_total:.1f} segundos)")
        logger.info("")
        
        logger.info("📊 RESUMEN DE RESULTADOS:")
        logger.info(f"   • Cosechas: {len(Cosechas):,} registros")
        logger.info(f"   • CRM: {len(CRM):,} registros")
        logger.info(f"   • Comportamiento: {len(Comportamiento):,} registros")
        logger.info(f"   • BaseFNZ: {len(BaseFNZ_final):,} registros")
        logger.info("")
        
        if advertencias:
            logger.warning(f"⚠️  Se generaron {len(advertencias)} advertencias (ver log)")
        else:
            logger.info("🎉 ¡Sin advertencias!")
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("📁 Archivos generados:")
        logger.info(f"   • Salidas: {carpeta_salidas.absolute()}")
        logger.info(f"   • Log: logs/logger.log")
        logger.info(f"   • Resumen: logs/resumen_ejecucion.json")
        logger.info("=" * 70)
        logger.info("")
        
        # Guardar resumen JSON
        resumen = {
            "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tiempo_segundos": round(tiempo_total, 2),
            "tiempo_minutos": round(minutos, 2),
            "registros_generados": {
                "cosechas": len(Cosechas),
                "crm": len(CRM),
                "comportamiento": len(Comportamiento),
                "basefnz": len(BaseFNZ_final)
            },
            "errores": errores,
            "advertencias": advertencias,
            "exitoso": True
        }
        guardar_resumen_ejecucion(resumen)
        
        return {
            "cosechas": Cosechas,
            "crm": CRM,
            "comportamiento": Comportamiento,
            "basefnz": BaseFNZ_final
        }
        
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
        logger.exception(f"Error: {str(e)}")
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