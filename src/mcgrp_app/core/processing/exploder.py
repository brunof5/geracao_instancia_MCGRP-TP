# src\mcgrp_app\core\processing\exploder.py

import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from ..utils import FieldsManager, GeoCalculator, GraphState

class PointExploder:
    """Trabalhador para explodir e rotular pontos."""
    
    def __init__(self):
        pass

    def explode_and_label(self, state: GraphState) -> GraphState:
        """
        Método orquestrador.
        """
        if state.data_streets is None:
            raise RuntimeError("Ruas de dados não podem ser nulas.")
            
        state.data_points = self._explode_linestrings(state.data_streets)
        
        state = self._label_shared_vertices(state)
        state = self._label_by_line(state)
        
        return state
    
    def _explode_linestrings(self, data_streets_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Método principal: Executa a explosão de LineString para Point.
        """
        print("Executando: explode_linestrings_to_points")
        
        all_points_data = []
        
        # Itera sobre o GDF de dados (lógico)
        for row in data_streets_gdf.itertuples():
            all_points_data.extend(
                self._create_points_from_linestring(row)
            )
            
        # Cria o GDF de dados de pontos
        data_points_gdf = gpd.GeoDataFrame(
            [p[1] for p in all_points_data],                # Lista de dicts de propriedades
            geometry=[p[0] for p in all_points_data],       # Lista de geometrias Point
            crs=data_streets_gdf.crs
        )
        
        return data_points_gdf

    def _create_points_from_linestring(self, street_row: pd.Series) -> list:
        """
        Cria uma lista de (Geometria, Propriedades) para cada vértice
        em uma única rua (linha do GDF).
        """
        points_list = []
        line_props = street_row
        line_id = street_row.id
        
        for idx, coord in enumerate(line_props.geometry.coords):
            # Obtém o dicionário de padrões
            point_props = FieldsManager.get_point_basic_fields()
            
            # Preenche com dados da linha
            point_props["from_line_id"] = line_id
            point_props["vertex_index"] = idx
            
            # Copia propriedades herdadas
            point_props["name"] = line_props.name
            point_props["alt_name"] = line_props.alt_name
            point_props["id_bairro"] = line_props.id_bairro
            point_props["bairro"] = line_props.bairro
            
            point_geom = Point(coord)
            points_list.append((point_geom, point_props))
            
        return points_list

    def _label_shared_vertices(self, state: GraphState) -> GraphState:
        """
        Identifica vértices compartilhados (mesma coordenada) e
        define 'eh_unido' = 'yes'/'no' no state.data_points.
        """
        print("  Rotulando vértices 'unidos'...")
        
        # Cria uma coluna 'coord_tuple' para agrupamento preciso
        state.data_points['coord_tuple'] = state.data_points.geometry.apply(
            lambda p: tuple(np.round(p.coords[0], GeoCalculator.PRECISION_DIGITS))
        )
        
        # Agrupa por coordenada
        grouped = state.data_points.groupby('coord_tuple')
        
        # 'transform' aplica o resultado de volta ao GDF original
        # 'count' conta o número total de pontos em cada grupo de coordenadas
        point_counts = grouped['from_line_id'].transform('count')
        
        # Se mais de 1 linha única compartilha a coordenada, é 'unido'
        state.data_points['eh_unido'] = np.where(point_counts > 1, 'yes', 'no')

        state.data_points = state.data_points.drop(columns='coord_tuple')

        return state

    def _label_by_line(self, state: GraphState) -> GraphState:
        """
        Calcula atributos de segmento (dist, angle) e rotula 'eh_extremidade'
        no state.data_points.
        """
        print("  Rotulando 'extremidades' e calculando ângulos/distâncias...")
        
        # Agrupa os pontos por rua
        grouped = state.data_points.groupby('from_line_id')
        
        # Armazena os dicts das linhas processadas
        processed_rows = []
        # Lista para armazenar as atualizações de 'total_dist' das ruas
        total_dists_map = {}

        # Iteramos sobre os grupos
        for line_id, group in grouped:
            if group.empty:
                continue
                
            # Ordena os pontos da rua
            sorted_points = group.sort_values(by='vertex_index')
            
            # Converte para tuplas (lon, lat) para o GeoCalculator
            coords = [tuple(p.coords[0]) for p in sorted_points.geometry]
            
            # Converte o grupo em uma lista de dicionários
            group_rows = sorted_points.to_dict('records')
            
            total_dist_m = 0.0

            # Iteramos sobre os dicionários
            for idx in range(len(group_rows)):
                row = group_rows[idx]       # 'row' é um dict
                
                if idx > 0:
                    # Distância j -> i (atual -> anterior)
                    dist_m = GeoCalculator.haversine_distance(coords[idx], coords[idx - 1])
                    total_dist_m += dist_m
                    row['distance'] = dist_m
                    row['vertex_to'] = group_rows[idx - 1]['vertex_index']
                else:
                    row['distance'] = 0.0
                    row['vertex_to'] = 0
                
                if idx < len(group_rows) - 1:
                    # Ângulo i -> j (atual -> próximo)
                    angle = GeoCalculator.azimuth(coords[idx], coords[idx + 1])
                    row['angle'] = round(angle, GeoCalculator.PRECISION_DIGITS)
                    row['angle_inv'] = round(GeoCalculator.azimuth_inverse(angle), GeoCalculator.PRECISION_DIGITS)
                    row['eh_extremidade'] = 'no'
                else:
                    # Último ponto não tem ângulo
                    row['angle'] = None
                    row['angle_inv'] = None

            # Rotula extremidades
            if group_rows:
                group_rows[0]['eh_extremidade'] = 'yes'
                group_rows[-1]['eh_extremidade'] = 'yes'

            # Adiciona as linhas processadas à lista principal
            processed_rows.extend(group_rows)
            # Salva a distância total (em metros)
            total_dists_map[line_id] = total_dist_m

        # Reconstrói o GDF
        state.data_points = gpd.GeoDataFrame(
            processed_rows, 
            crs=state.data_points.crs
        )
        
        # Obtém o mapa de distâncias (metros) e converte para km
        total_dists_km = pd.Series(total_dists_map).map(
            lambda m: m / 1000
        ).round(GeoCalculator.PRECISION_DIGITS)
        
        # Mapeia a 'total_dist' de volta para os GDFs de ruas
        state.data_streets['total_dist'] = state.data_streets['id'].map(total_dists_km)
        state.map_streets['total_dist'] = state.map_streets['id'].map(total_dists_km)

        # Também atualizamos a coluna 'distance' dos pontos para km
        state.data_points['distance'] = (
            state.data_points['distance'] / 1000
        ).round(GeoCalculator.PRECISION_DIGITS)

        return state