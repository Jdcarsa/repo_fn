import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import openpyxl
from typing import Dict, List

def comparar_excels(
    ruta_r: str,
    ruta_python: str,
    hoja: str = "Sheet1",
    generar_reporte: bool = True,
    ruta_salida: str = "salidas"
) -> Dict:
    """
    Compara dos archivos Excel columna por columna.
    Valida:
    - Columnas (nombre y orden)
    - Tipos de datos
    - Valores (incluyendo NAs)
    
    Args:
        ruta_r: Ruta del archivo Excel generado por R
        ruta_python: Ruta del archivo Excel generado por Python
        hoja: Nombre de la hoja a comparar
        generar_reporte: Si genera archivo de reporte
        ruta_salida: Carpeta para guardar reportes
    
    Returns:
        Dict con resultados de la comparación
    """
    print("🔍 Comparando archivos Excel...")
    print(f"📄 R:     {ruta_r}")
    print(f"📄 Python: {ruta_python}")

    # Cargar archivos
    try:
        df_r = pd.read_excel(ruta_r, sheet_name=hoja)
        df_py = pd.read_excel(ruta_python, sheet_name=hoja)
    except Exception as e:
        error_msg = f"❌ Error cargando archivos: {e}"
        print(error_msg)
        return {
            "error": True,
            "mensaje": error_msg,
            "columnas_ok": False,
            "orden_ok": False,
            "tipos_ok": False,
            "valores_ok": False,
            "diferencias": [error_msg],
            "resumen": "❌ Error en comparación"
        }

    reporte = {
        "error": False,
        "columnas_ok": True,
        "orden_ok": True,
        "tipos_ok": True,
        "valores_ok": True,
        "diferencias": [],
        "resumen": "",
        "estadisticas": {
            "filas_r": len(df_r),
            "filas_python": len(df_py),
            "columnas_r": len(df_r.columns),
            "columnas_python": len(df_py.columns)
        }
    }

    # 1. Comparar número de filas
    if len(df_r) != len(df_py):
        reporte["diferencias"].append(
            f"❌ Diferencia en número de filas: R={len(df_r)} vs Python={len(df_py)}"
        )

    # 2. Comparar columnas (nombre y orden)
    cols_r = df_r.columns.tolist()
    cols_py = df_py.columns.tolist()

    if set(cols_r) != set(cols_py):
        reporte["columnas_ok"] = False
        reporte["diferencias"].append("❌ Columnas distintas")
        solo_en_r = set(cols_r) - set(cols_py)
        solo_en_py = set(cols_py) - set(cols_r)
        if solo_en_r:
            reporte["diferencias"].append(f"   Solo en R: {list(solo_en_r)}")
        if solo_en_py:
            reporte["diferencias"].append(f"   Solo en Python: {list(solo_en_py)}")

    if cols_r != cols_py:
        reporte["orden_ok"] = False
        if reporte["columnas_ok"]:  # Solo orden diferente
            reporte["diferencias"].append("⚠️ Mismas columnas pero orden diferente")

    # 3. Comparar tipos de datos
    columnas_comunes = set(cols_r) & set(cols_py)
    for col in columnas_comunes:
        tipo_r = str(df_r[col].dtype)
        tipo_py = str(df_py[col].dtype)
        if tipo_r != tipo_py:
            reporte["tipos_ok"] = False
            reporte["diferencias"].append(f"❌ Tipo distinto en '{col}': R={tipo_r}, Python={tipo_py}")

    # 4. Comparar valores (solo en columnas comunes)
    for col in columnas_comunes:
        serie_r = df_r[col]
        serie_py = df_py[col]

        # Comparar valores (incluyendo NAs)
        if not serie_r.equals(serie_py):
            reporte["valores_ok"] = False
            
            # Detectar diferencias específicas
            diff_mask = (serie_r != serie_py) & ~(serie_r.isna() & serie_py.isna())
            diff_count = diff_mask.sum()
            
            if diff_count > 0:
                reporte["diferencias"].append(f"❌ {diff_count} valores distintos en '{col}'")
                
                # Muestra primeras diferencias (máximo 3)
                diff_idx = serie_r[diff_mask].index[:3]
                for idx in diff_idx:
                    val_r = serie_r.iloc[idx]
                    val_py = serie_py.iloc[idx]
                    reporte["diferencias"].append(
                        f"   Fila {idx}: R='{val_r}' | Python='{val_py}'"
                    )

    # 5. Generar resumen
    if all([reporte["columnas_ok"], reporte["orden_ok"], reporte["tipos_ok"], reporte["valores_ok"]]):
        reporte["resumen"] = "✅ Archivos idénticos"
    else:
        reporte["resumen"] = "⚠️ Se encontraron diferencias"

    # 6. Guardar reporte detallado
    if generar_reporte:
        _guardar_reporte_completo(reporte, ruta_r, ruta_python, ruta_salida)

    return reporte

def _guardar_reporte_completo(reporte: Dict, ruta_r: str, ruta_python: str, ruta_salida: str):
    """Guarda reporte detallado en archivo de texto"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_reporte = Path(ruta_salida) / f"reporte_diferencias_{timestamp}.txt"
    ruta_reporte.parent.mkdir(exist_ok=True)
    
    with open(ruta_reporte, "w", encoding="utf-8") as f:
        f.write("REPORTE DE COMPARACIÓN - EXCEL R vs PYTHON\n")
        f.write("=" * 60 + "\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Archivo R:     {ruta_r}\n")
        f.write(f"Archivo Python: {ruta_python}\n")
        f.write("=" * 60 + "\n\n")
        
        # Estadísticas
        stats = reporte.get("estadisticas", {})
        f.write("ESTADÍSTICAS:\n")
        f.write(f"- Filas R: {stats.get('filas_r', 'N/A')}\n")
        f.write(f"- Filas Python: {stats.get('filas_python', 'N/A')}\n")
        f.write(f"- Columnas R: {stats.get('columnas_r', 'N/A')}\n")
        f.write(f"- Columnas Python: {stats.get('columnas_python', 'N/A')}\n\n")
        
        # Diferencias
        f.write("DIFERENCIAS ENCONTRADAS:\n")
        f.write("-" * 40 + "\n")
        if reporte["diferencias"]:
            for diff in reporte["diferencias"]:
                f.write(diff + "\n")
        else:
            f.write("No se encontraron diferencias\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write(f"RESUMEN: {reporte['resumen']}\n")
    
    print(f"📄 Reporte guardado en: {ruta_reporte}")

def comparacion_rapida(ruta_r: str, ruta_python: str, hoja: str = "Sheet1") -> bool:
    """
    Versión rápida que solo verifica si los archivos son idénticos
    
    Returns:
        bool: True si son idénticos, False si hay diferencias
    """
    resultado = comparar_excels(ruta_r, ruta_python, hoja, generar_reporte=False)
    return all([
        resultado["columnas_ok"],
        resultado["orden_ok"], 
        resultado["tipos_ok"],
        resultado["valores_ok"]
    ])

# Ejemplo de uso independiente
if __name__ == "__main__":
    # Puedes usar el comparador directamente
    resultado = comparar_excels(
        ruta_r="../../datos/cache/crm.xlsx",
        ruta_python="../../datos/salidas/CRM_2025-11-19_16-48-46.xlsx",
        hoja="Sheet1"
    )
    
    print(resultado["resumen"])