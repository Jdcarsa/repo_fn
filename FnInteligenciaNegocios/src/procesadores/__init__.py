"""
Módulo de procesadores para FinnovaRisk.
Incluye funciones para crear datasets finales.
"""

from .procesador import unir_datasets
from .cosechas import crear_cosechas
from .crm import crear_crm
from .comportamiento import crear_comportamiento

__all__ = [
    'unir_datasets',
    'crear_cosechas',
    'crear_crm',
    'crear_comportamiento'
]