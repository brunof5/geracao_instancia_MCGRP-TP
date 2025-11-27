# src\mcgrp_app\core\processing\splitter.py

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString

from ..utils import GeoCalculator, GraphState

class LineStringSplitter:
    """
    Trabalhador para dividir LineStrings em segmentos menores com base em vértices especiais.
    Recalcula todos os atributos de pontos e ruas para os novos segmentos.
    Também divide LineStrings em segmentos de exatamente 2 pontos cada.
    """
    
    def __init__(self):
        # Listas para armazenar resultados temporários
        self.new_data_streets_list = []
        self.new_data_points_list = []
        self.new_map_streets_list = []
        self.next_temp_line_id = 1

    def _reset_internal_state(self, starting_id: int):
        """Limpa as listas temporárias antes de uma operação."""
        self.new_data_streets_list = []
        self.new_data_points_list = []
        self.new_map_streets_list = []
        self.next_temp_line_id = starting_id
    
    def split_by_special_vertices(self, state: GraphState, 
                                  split_on_united: bool = False, 
                                  split_on_depot: bool = False, 
                                  split_on_required: bool = False) -> GraphState:
        """
        Método orquestrador. Itera sobre todas as ruas e as divide.
        No final, reconstrói e re-indexa os GDFs.
        """
        if not any([split_on_united, split_on_depot, split_on_required]):
            print("  Splitter: Nenhum critério de divisão fornecido. Pulando.")
            return state

        print("Executando: split_by_special_vertices")

        # Reseta as listas temporárias
        self._reset_internal_state(
            (state.data_streets['id'].max() if not state.data_streets.empty else 0) + 1
        )
        
        # Agrupa os pontos por rua
        points_grouped_by_line = state.data_points.groupby('from_line_id')
        
        # Itera sobre cada rua
        for street_row in state.data_streets.itertuples():
            line_id = street_row.id
            
            # Pega os pontos desta rua
            try:
                points_in_line = points_grouped_by_line.get_group(line_id).sort_values('vertex_index')
            except KeyError:
                # Rua não tem pontos? Adiciona a rua e continua
                self.new_data_streets_list.append(street_row._asdict())
                continue
            
            # Encontra os índices (0, 1, 2..) dos pontos onde devemos dividir
            split_indices = self._find_split_indices(
                points_in_line, split_on_united, split_on_depot, split_on_required
            )

            # Processa a divisão
            self._process_one_line(street_row, points_in_line, split_indices)
            
        # Finalmente, reconstrói e re-indexa tudo
        state = self._rebuild_and_reindex_gdfs(state)

        return state

    def _find_split_indices(self, points_gdf: gpd.GeoDataFrame, united: bool, depot: bool, required: bool) -> list:
        """Encontra os 'vertex_index' onde uma rua deve ser dividida."""
        split_indices = set()
        
        # Itera sobre os pontos intermediários (não o primeiro nem o último)
        for point in points_gdf.itertuples(index=False):
            v_index = point.vertex_index
            if v_index == 0 or v_index == (len(points_gdf) - 1):
                continue
                
            if united and point.eh_unido == 'yes':
                split_indices.add(v_index)
            elif depot and point.depot == 'yes':
                split_indices.add(v_index)
            elif required and point.eh_requerido == 'yes':
                split_indices.add(v_index)
                
        return sorted(list(split_indices))
        
    def _process_one_line(self, street_row: pd.Series, points_gdf: gpd.GeoDataFrame, split_indices: list):
        """Processa uma rua, dividindo-a nos 'split_indices'."""
        
        # Se não há divisões, apenas adiciona os dados originais
        if not split_indices:
            self.new_data_streets_list.append(street_row._asdict())
            self.new_data_points_list.extend(points_gdf.to_dict('records'))
            return

        # Converte para dicts
        points_list = points_gdf.to_dict('records')
        
        # Adiciona o início (0) e o fim (N-1) para criar os segmentos
        segment_indices = [0] + split_indices + [len(points_list) - 1]
        
        # Itera sobre os pares de segmentos (ex: [0, 5], [5, N-1])
        for i in range(len(segment_indices) - 1):
            start_v_index = segment_indices[i]
            end_v_index = segment_indices[i+1]
            
            # Pega a "fatia" de pontos para este novo segmento
            segment_points_dicts = points_list[start_v_index : end_v_index + 1]
            
            # Descarta o ID original e pega um novo ID temporário
            new_temp_id = self.next_temp_line_id
            self.next_temp_line_id += 1
            
            self._create_new_segment(street_row, segment_points_dicts, new_temp_id)
    
    def _create_new_segment(self, original_street: pd.Series, segment_points_dicts: list, new_line_id: int):
        """
        Cria uma nova rua e seus novos pontos a partir de um segmento.
        Recalcula todos os atributos.
        """
        # Cria a nova rua
        new_street_props = original_street._asdict()
        new_street_props['id'] = new_line_id
        
        # Recalcula geometria e 'total_dist'
        new_coords = [Point(p['geometry']).coords[0] for p in segment_points_dicts]
        new_street_props['geometry'] = LineString(new_coords)
        
        # Cria os novos pontos
        total_dist_m = 0.0
        new_segment_points = []
        
        for i, point_dict in enumerate(segment_points_dicts):
            new_point_props = point_dict.copy()     # Copia atributos (ex: eh_unido)
            
            # Define/Sobrescreve atributos do novo segmento
            new_point_props['from_line_id'] = new_line_id
            new_point_props['vertex_index'] = i
            
            if i == 0:
                new_point_props['distance'] = 0.0
                new_point_props['vertex_to'] = 0
                new_point_props['eh_extremidade'] = 'yes'       # É o primeiro ponto
            else:
                # Calcula distância do anterior (em metros)
                prev_coord = new_coords[i-1]
                curr_coord = new_coords[i]
                dist_m = GeoCalculator.haversine_distance(curr_coord, prev_coord)
                
                new_point_props['distance'] = round(dist_m / 1000, GeoCalculator.PRECISION_DIGITS)
                new_point_props['vertex_to'] = i - 1
                total_dist_m += dist_m
            
            if i < len(segment_points_dicts) - 1:
                # Calcula ângulo para o próximo
                curr_coord = new_coords[i]
                next_coord = new_coords[i+1]
                angle = GeoCalculator.azimuth(curr_coord, next_coord)
                new_point_props['angle'] = round(angle, GeoCalculator.PRECISION_DIGITS)
                new_point_props['angle_inv'] = round(GeoCalculator.azimuth_inverse(angle), GeoCalculator.PRECISION_DIGITS)
                
                # Se for 'unido', mantém. Se não, é 'intermediário'
                if i > 0 and new_point_props['eh_unido'] != 'yes':
                    new_point_props['eh_extremidade'] = 'no'
            else:
                new_point_props['angle'] = None
                new_point_props['angle_inv'] = None
                new_point_props['eh_extremidade'] = 'yes'       # É o último ponto
            
            new_segment_points.append(new_point_props)

        # Salva a 'total_dist' (em km) na rua
        new_street_props['total_dist'] = round(total_dist_m / 1000, GeoCalculator.PRECISION_DIGITS)
        
        # Adiciona à lista de resultados
        self.new_data_streets_list.append(new_street_props)
        self.new_data_points_list.extend(new_segment_points)
    
    def _rebuild_and_reindex_gdfs(self, state: GraphState) -> GraphState:
        """
        Reconstrói e re-indexa os GFDs e retorna o novo estado.
        """
        print(f"  Divisão concluída. {len(self.new_data_streets_list)} novos segmentos criados.")
        print("  Reconstruindo e re-indexando GDFs...")
        
        if not self.new_data_streets_list:
            return state        # Nada foi alterado

        # Cria GDF de ruas (com IDs temporários)
        temp_streets_gdf = gpd.GeoDataFrame(
            self.new_data_streets_list, 
            crs=state.data_streets.crs
        )
        
        # Cria mapa de re-indexação (ID_temporário -> ID_final_1_N)
        # Reseta o índice (0 a N-1) e adiciona 1 (1 a N)
        temp_streets_gdf = temp_streets_gdf.reset_index(drop=True)
        temp_streets_gdf['final_id'] = temp_streets_gdf.index + 1
        
        # Cria o mapa: {temp_id: final_id}
        id_map = temp_streets_gdf.set_index('id')['final_id'].to_dict()
        
        # Aplica o ID final
        temp_streets_gdf['id'] = temp_streets_gdf['final_id']
        state.data_streets = temp_streets_gdf.drop(columns='final_id')
        
        # Cria GDF de pontos (com IDs temporários)
        temp_points_gdf = gpd.GeoDataFrame(
            self.new_data_points_list, 
            crs=state.data_points.crs
        )
        
        # Propaga os IDs finais para os pontos
        temp_points_gdf['from_line_id'] = temp_points_gdf['from_line_id'].map(id_map)
        state.data_points = temp_points_gdf

        # Constrói o GDF de mapa (se aplicável)
        if self.new_map_streets_list:
            temp_map_streets_gdf = gpd.GeoDataFrame(
                self.new_map_streets_list, crs=state.map_streets.crs
            )
            temp_map_streets_gdf['id'] = temp_map_streets_gdf['id'].map(id_map)
            state.map_streets = temp_map_streets_gdf
        else:
            # Se não, espelha as ruas de dados
            state.map_streets = state.data_streets.copy()

        return state
    
    def split_into_two_point_segments(self, state: GraphState) -> GraphState:
        """
        Garante que todas as ruas tenham exatamente 2 pontos.
        """
        print("Executando: split_into_two_point_segments")

        # Reseta as listas temporárias
        self._reset_internal_state(
            (state.data_streets['id'].max() if not state.data_streets.empty else 0) + 1
        )
        
        # Agrupa os pontos lógicos
        points_by_line = state.data_points.groupby('from_line_id')
        
        # Mapeia ID da rua para a linha do GDF de mapa
        map_streets_map = {row.id: row for row in state.map_streets.itertuples()}

        # Itera sobre cada RUA DE DADOS
        for street_row in state.data_streets.itertuples():
            line_id = street_row.id
            
            try:
                data_points_gdf = points_by_line.get_group(line_id).sort_values('vertex_index')
            except KeyError:
                continue        # Rua sem pontos

            # Obtém a rua visual correspondente
            map_street_row = map_streets_map.get(line_id)

            # CASO 1: Rua já tem 2 pontos
            if len(data_points_gdf) <= 2:
                self.new_data_streets_list.append(street_row._asdict())
                self.new_data_points_list.extend(data_points_gdf.to_dict('records'))
                if map_street_row:
                    self.new_map_streets_list.append(map_street_row._asdict())
                continue

            # CASO 2: Rua tem > 2 pontos 
            # Converte para dicts para fatiamento
            points_list = data_points_gdf.to_dict('records')
            map_coords = list(map_street_row.geometry.coords) if map_street_row else None
            
            # Itera sobre os novos segmentos
            for i in range(len(points_list) - 1):
                pt1_dict = points_list[i]
                pt2_dict = points_list[i+1]
                
                new_temp_id = self.next_temp_line_id
                self.next_temp_line_id += 1
                
                # Cria nova rua (lógico)
                new_data_street = street_row._asdict()
                new_data_street['id'] = new_temp_id
                new_data_street['geometry'] = LineString([
                    Point(pt1_dict['geometry']).coords[0],
                    Point(pt2_dict['geometry']).coords[0]
                ])
                new_data_street['total_dist'] = pt2_dict['distance'] 
                self.new_data_streets_list.append(new_data_street)

                # Cria nova rua (mapa)
                if map_street_row:
                    new_map_street = map_street_row._asdict()
                    new_map_street['id'] = new_temp_id
                    new_map_street['total_dist'] = pt2_dict['distance']
                    
                    # Encontra as coordenadas visuais para este segmento
                    try:
                        start_idx = pt1_dict['vertex_index']
                        end_idx = pt2_dict['vertex_index']
                        new_map_street['geometry'] = LineString(map_coords[start_idx : end_idx + 1])
                    except Exception:
                        # Fallback: usa a geometria dos dados (sem curvas)
                        new_map_street['geometry'] = new_data_street['geometry']
                        
                    self.new_map_streets_list.append(new_map_street)
                
                # Cria novos pontos
                new_pt1 = pt1_dict.copy()       # cópia o ponto1
                new_pt1.update({
                    'from_line_id': new_temp_id, 
                    'vertex_index': 0, 
                    'vertex_to': 0,
                    'distance': 0.0, 
                    'eh_extremidade': 'yes'
                })
                # (Ângulo é herdado)
                
                new_pt2 = pt2_dict.copy()       # cópia o ponto2
                new_pt2.update({
                    'from_line_id': new_temp_id, 
                    'vertex_index': 1, 
                    'vertex_to': 0,
                    'eh_extremidade': 'yes'
                })
                # (Distância é herdada)
                
                self.new_data_points_list.append(new_pt1)
                self.new_data_points_list.append(new_pt2)

        # Reconstrói e Re-indexa GDFs
        return self._rebuild_and_reindex_gdfs(state)