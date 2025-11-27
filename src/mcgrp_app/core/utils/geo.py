# src\mcgrp_app\core\utils\geo.py

import math
import numpy as np
import pandas as pd
import geopandas as gpd
from typing import List, Optional

class GeoCalculator:
    """
    Classe para cálculos geográficos e planos, 
    portada do Google Colab.
    """
    EARTH_RADIUS = 6371000              # metros
    DEFAULT_MAX_SPEED = 20              # km/h
    PROXIMITY_THRESHOLD = 1             # metros
    PRECISION_DIGITS = 6
    BASE_CRS = "EPSG:4326"              # CRS base (WGS84)
    PROJECTED_CRS = "EPSG:3857"         # CRS Métrico (Web Mercator)

    @staticmethod
    def haversine_distance(coord1: tuple[float, float], coord2: tuple[float, float]) -> float:
        """
        Calcula distância geodésica (em metros) entre dois pontos (lon, lat).
        """
        lon1, lat1 = coord1
        lon2, lat2 = coord2

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (math.sin(delta_phi/2)**2 +
             math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return round(GeoCalculator.EARTH_RADIUS * c, GeoCalculator.PRECISION_DIGITS)

    @staticmethod
    def azimuth(coord1: tuple[float, float], coord2: tuple[float, float]) -> float:
        """
        Calcula o ângulo de azimute (0-360) de coord1 para coord2.
        """
        lon1, lat1 = map(math.radians, coord1)
        lon2, lat2 = map(math.radians, coord2)

        delta_lon = lon2 - lon1

        x = math.sin(delta_lon) * math.cos(lat2)
        y = (math.cos(lat1) * math.sin(lat2) -
             math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon))

        azimuth_rad = math.atan2(x, y)
        azimuth_deg = (math.degrees(azimuth_rad) + 360) % 360

        return azimuth_deg

    @staticmethod
    def azimuth_inverse(angle: float) -> float:
        """Calcula o ângulo inverso (oposto) de um ângulo azimuth dado."""
        return (angle + 180) % 360
    
    @staticmethod
    def mean_angle_deg(degrees: List[float]) -> Optional[float]:
        """
        Calcula a média circular de um conjunto de ângulos em graus.
        """
        if not degrees:
            return None

        radians = [math.radians(deg) for deg in degrees]
        x_sum = sum(math.cos(rad) for rad in radians)
        y_sum = sum(math.sin(rad) for rad in radians)

        mean_rad = math.atan2(y_sum, x_sum)
        mean_deg = math.degrees(mean_rad)

        return mean_deg % 360
    
    @staticmethod
    def are_coords_close(c1: tuple[float, float], c2: tuple[float, float]) -> bool:
        """
        Verifica se duas coordenadas (lon, lat) estão dentro do 
        limite de proximidade (PROXIMITY_THRESHOLD).
        """
        if not c1 or not c2:
            return False
        try:
            return GeoCalculator.haversine_distance(c1, c2) < GeoCalculator.PROXIMITY_THRESHOLD
        except (ValueError, TypeError):
            return False
        
    @staticmethod
    def calculate_traversal_cost(dist_km: float, maxspeed_str: str) -> int:
        """
        Cálculo do custo de travessia (tempo) em segundos.
        """
        if dist_km is None or pd.isna(dist_km) or dist_km == 0:
            return 0
            
        try:
            speed_val = "".join(filter(str.isdigit, str(maxspeed_str)))
            maxspeed = int(speed_val)
            if maxspeed == 0:
                maxspeed = GeoCalculator.DEFAULT_MAX_SPEED
        except (ValueError, TypeError):
            maxspeed = GeoCalculator.DEFAULT_MAX_SPEED

        # Limita a velocidade pela padrão
        vel = min(maxspeed, GeoCalculator.DEFAULT_MAX_SPEED)

        return math.ceil(dist_km / vel * 3600)      # segundos
    
    @staticmethod
    def create_map_points(data_points_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Controí o map_points_gdf a partir do data_points_gdf.
        """
        print("  Construindo GDF de pontos de mapa...")
        
        # Cria uma coluna 'coord_tuple' para agrupamento
        data_points_gdf['coord_tuple'] = data_points_gdf.geometry.apply(
            lambda p: tuple(np.round(p.coords[0], GeoCalculator.PRECISION_DIGITS))
        )
        
        # Agrupa pelo 'coord_tuple'
        grouped = data_points_gdf.groupby('coord_tuple')
        
        # Define como agregar cada coluna
        agg_rules = {
            'geometry': 'first',
            'from_line_id': lambda x: list(x),
            'eh_unido': 'first',
            'eh_extremidade': lambda x: list(x),
            'vertex_index': lambda x: list(x),
            'demanda': lambda x: int(any(x)),

            'id_bairro': lambda x: x.dropna().iloc[0] if not x.dropna().empty else None,
            'bairro': lambda x: x.dropna().iloc[0] if not x.dropna().empty else None,

            # Anula campos específicos da rua
            'distance': lambda x: None,
            'angle': lambda x: None,
            'angle_inv': lambda x: None,
            'vertex_to': lambda x: None,
            'name': lambda x: None,
            'alt_name': lambda x: None
        }
        
        if 'node_index' in data_points_gdf.columns:
            agg_rules['node_index'] = 'first'

        # Aplica a agregação
        map_points_data = grouped.agg(agg_rules)
        
        # Converte o resultado de volta para um GeoDataFrame
        map_points_gdf = gpd.GeoDataFrame(
            map_points_data, 
            geometry='geometry', 
            crs=data_points_gdf.crs
        ).reset_index(drop=True)

        data_points_gdf.drop(columns='coord_tuple')

        return map_points_gdf