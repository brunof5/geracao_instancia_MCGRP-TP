# src\mcgrp_app\core\utils\state.py

import pandas as pd
from dataclasses import dataclass
from typing import Optional

@dataclass
class GraphState:
    """
    Encapsula o estado atual do grafo.
    """
    # DataFrames de Lógica (para processamento)
    data_streets: pd.DataFrame
    data_points: Optional[pd.DataFrame]
    
    # DataFrames de Mapa (para visualização)
    map_streets: pd.DataFrame
    map_points: Optional[pd.DataFrame]
    
    # DataFrames de Referência
    neighborhoods: pd.DataFrame
    
    # CRS padrão
    crs: str = "EPSG:4326"