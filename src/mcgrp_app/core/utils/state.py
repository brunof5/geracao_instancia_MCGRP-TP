# src\mcgrp_app\core\utils\state.py

import geopandas as gpd
from dataclasses import dataclass
from typing import Optional

@dataclass
class GraphState:
    """
    Encapsula todos os GeoDataFrames que definem o estado 
    atual do pipeline de processamento do grafo.
    """
    # GDFs de Lógica (para processamento)
    data_streets: gpd.GeoDataFrame
    data_points: Optional[gpd.GeoDataFrame]
    
    # GDFs de Mapa (para visualização)
    map_streets: gpd.GeoDataFrame
    map_points: Optional[gpd.GeoDataFrame]
    
    # GDFs de Referência
    neighborhoods: gpd.GeoDataFrame