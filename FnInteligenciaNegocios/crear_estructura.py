"""
Script para crear la estructura completa del proyecto Finnovarisk.
Ejecuta este archivo y se crearán todas las carpetas necesarias.
"""

from pathlib import Path
import sys

def crear_estructura_proyecto():
    """Crea todas las carpetas necesarias del proyecto."""
    
    print("\n" + "="*70)
    print("🚀 CREANDO ESTRUCTURA DEL PROYECTO FINNOVARISK")
    print("="*70 + "\n")
    
    # Obtener ubicación actual
    ubicacion_actual = Path.cwd()
    
    # Preguntar dónde crear el proyecto
    print(f"📍 Ubicación actual: {ubicacion_actual}\n")
    print("¿Dónde quieres crear el proyecto?")
    print("1. Aquí mismo (crear carpeta 'finnovarisk_etl')")
    print("2. En otra ubicación (especificar ruta)")
    print("3. Usar carpeta actual como raíz del proyecto")
    
    opcion = input("\nElige opción (1/2/3): ").strip()
    
    if opcion == "1":
        raiz = ubicacion_actual / "finnovarisk_etl"
    elif opcion == "2":
        ruta_personalizada = input("Ingresa la ruta completa: ").strip()
        raiz = Path(ruta_personalizada)
    elif opcion == "3":
        raiz = ubicacion_actual
    else:
        print("❌ Opción inválida. Usando opción 1 por defecto.")
        raiz = ubicacion_actual / "finnovarisk_etl"
    
    print(f"\n📁 Creando proyecto en: {raiz}\n")
    
    # Definir estructura de carpetas
    carpetas = [
        # Configuración
        "config",
        
        # Código fuente
        "src",
        "src/cargador_datos",
        "src/transformadores",
        "src/procesadores",
        "src/utilidades",
        
        # Datos - FNZ007
        "datos/originales/FNZ007",
        
        # Datos - Análisis de Cartera
        "datos/originales/Analisis_Cartera",
        
        # Datos - FNZ001
        "datos/originales/FNZ001",
        
        # Datos - Informe de Edades
        "datos/originales/Informe_Edades",
        
        # Datos - R05
        "datos/originales/R05",
        
        # Datos - Recaudos
        "datos/originales/Recaudos",
        
        # Datos - Auxiliares (Estado desembolsos, categorías, etc.)
        "datos/originales/Auxiliares",
        
        # Datos procesados y caché
        "datos/procesados",
        "datos/cache",
        
        # Salidas
        "salidas/Cosechas",
        "salidas/CRM",
        "salidas/Comportamiento",
        "salidas/Reportes",
        
        # Sistema
        "logs",
        "temp"
    ]
    
    # Crear carpetas
    carpetas_creadas = 0
    carpetas_existentes = 0
    
    for carpeta in carpetas:
        ruta_completa = raiz / carpeta
        
        if not ruta_completa.exists():
            ruta_completa.mkdir(parents=True, exist_ok=True)
            print(f"✅ Creada: {carpeta}")
            carpetas_creadas += 1
        else:
            print(f"⏭️  Ya existe: {carpeta}")
            carpetas_existentes += 1
    
    # Crear archivos README en carpetas clave
    print("\n📝 Creando archivos de ayuda...")
    crear_archivos_readme(raiz)
    
    # Resumen
    print("\n" + "="*70)
    print("✨ ¡ESTRUCTURA CREADA EXITOSAMENTE!")
    print("="*70)
    print(f"\n📊 Resumen:")
    print(f"   • Carpetas creadas: {carpetas_creadas}")
    print(f"   • Carpetas ya existían: {carpetas_existentes}")
    print(f"   • Total: {carpetas_creadas + carpetas_existentes}")
    print(f"\n📂 Ubicación del proyecto:")
    print(f"   {raiz}")
    
    print("\n" + "="*70)
    print("📋 PRÓXIMOS PASOS:")
    print("="*70)
    print("\n1️⃣  Copia tus archivos Excel a las carpetas correspondientes:")
    print(f"    📁 {raiz / 'datos/originales'}")
    print("\n2️⃣  Coloca los archivos en sus carpetas:")
    print("    • FNZ 007 → FNZ007/")
    print("    • Análisis de Cartera → Analisis_Cartera/")
    print("    • FNZ001 → FNZ001/")
    print("    • Y así sucesivamente...")
    print("\n3️⃣  Ejecuta el sistema:")
    print("    python main.py")
    print("\n" + "="*70 + "\n")
    
    return raiz


def crear_archivos_readme(raiz: Path):
    """Crea archivos README con instrucciones en carpetas importantes."""
    
    readmes = {
        "datos/originales/FNZ007/LEEME.txt": """
=== CARPETA FNZ007 ===

📂 COLOCA AQUÍ:
• FNZ 007 BASE COMPLETA.xlsx
• FNZ007 2025.xlsx
• Cualquier otro archivo FNZ007

📝 Estos archivos contienen:
- Información de desembolsos
- Datos de clientes
- Montos y condiciones de crédito
""",
        
        "datos/originales/Analisis_Cartera/LEEME.txt": """
=== CARPETA ANÁLISIS DE CARTERA ===

📂 COLOCA AQUÍ:
• ANALISIS DE CARTERA COMPLETO.xlsx
• ANÁLISIS GENERAL JULIO 31.xlsx
• ANALISIS GENERAL AGOSTO 31.xlsx
• Nuevos cortes mensuales

📝 Estos archivos contienen:
- Estado de la cartera por mes
- Días de atraso
- Saldos pendientes
""",
        
        "datos/originales/Auxiliares/LEEME.txt": """
=== CARPETA AUXILIARES ===

📂 COLOCA AQUÍ:
• ESTADO DESEMBOLSOS (1).xlsx
• Base_categorias.xlsx
• Cualquier otro archivo de apoyo

📝 Estos archivos contienen:
- Estado de desembolsos para filtrar
- Categorías DV y PT
- Información complementaria
""",
        
        "salidas/LEEME.txt": """
=== CARPETA DE RESULTADOS ===

📊 AQUÍ SE GENERAN AUTOMÁTICAMENTE:

• Cosechas/: Base de análisis de cosechas
• CRM/: Base de gestión de clientes
• Comportamiento/: Análisis de comportamiento de pago
• Reportes/: Informes adicionales

⚠️ NO modifiques estos archivos manualmente.
   Se regeneran en cada ejecución del sistema.

💡 Los archivos tienen fecha y hora en el nombre
   para no sobrescribirse.
"""
    }
    
    for ruta_relativa, contenido in readmes.items():
        ruta_completa = raiz / ruta_relativa
        ruta_completa.parent.mkdir(parents=True, exist_ok=True)
        
        with open(ruta_completa, 'w', encoding='utf-8') as f:
            f.write(contenido.strip())
        
        print(f"   📄 Creado: {ruta_relativa}")


if __name__ == "__main__":
    try:
        raiz_proyecto = crear_estructura_proyecto()
        
        # Preguntar si quiere abrir la carpeta
        if sys.platform == "win32":
            abrir = input("\n¿Abrir carpeta del proyecto? (s/n): ").strip().lower()
            if abrir == 's':
                import os
                os.startfile(raiz_proyecto)
    
    except KeyboardInterrupt:
        print("\n\n❌ Operación cancelada por el usuario.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)