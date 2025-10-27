"""
Cargador de configuración para ejecutable
"""

import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    def __init__(self, ruta_config="config/datasets.json"):
        self.ruta_config = Path(ruta_config)
        self.config = self._cargar_configuracion()
    
    def _cargar_configuracion(self):
        """Carga la configuración desde JSON"""
        if not self.ruta_config.exists():
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {self.ruta_config}")
        
        with open(self.ruta_config, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Convertir rutas strings a objetos Path
        for dataset, info in config.items():
            for archivo_clave, ruta_str in info["archivos"].items():
                # Usar rutas relativas al directorio del proyecto
                ruta_completa = Path(ruta_str)
                if not ruta_completa.is_absolute():
                    ruta_completa = Path.cwd() / ruta_completa
                config[dataset]["archivos"][archivo_clave] = ruta_completa
        
        logger.info(f"✅ Configuración cargada: {len(config)} datasets")
        return config
    
    def get_config(self, dataset=None):
        """Obtiene configuración completa o de un dataset específico"""
        if dataset:
            return self.config.get(dataset)
        return self.config
    
    def listar_datasets(self):
        """Lista todos los datasets disponibles"""
        return list(self.config.keys())
    
    def validar_configuracion(self):
        """Valida que todos los archivos existan"""
        errores = []
        
        for dataset, info in self.config.items():
            for archivo_clave, ruta in info["archivos"].items():
                if not ruta.exists():
                    errores.append(f"{dataset}.{archivo_clave}: {ruta} no existe")
            
            # Validar que cada archivo tenga hojas configuradas
            for archivo_clave in info["archivos"]:
                if archivo_clave not in info["hojas"]:
                    errores.append(f"{dataset}.{archivo_clave}: no tiene hojas configuradas")
        
        return errores

    def get_datasets_disponibles(self):
        """Obtiene lista de datasets con archivos existentes"""
        disponibles = []
        for dataset in self.listar_datasets():
            config_dataset = self.get_config(dataset)
            archivos_existen = all(ruta.exists() for ruta in config_dataset["archivos"].values())
            if archivos_existen:
                disponibles.append(dataset)
        return disponibles


# Instancia global para uso fácil
CONFIG_LOADER = ConfigLoader()