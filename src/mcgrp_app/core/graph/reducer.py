# src\mcgrp_app\core\graph\reducer.py

import math
import numpy as np
import pandas as pd
import geopandas as gpd
from typing import Optional
from shapely.geometry import LineString

from ..utils import GeoCalculator, GraphState

class ReducedGraphProcessor:
    """
    Trabalhador para reduzir o grafo removendo vértices intermediários 
    e mesclar ruas em fronteiras de bairros.
    """

    def __init__(self, neighborhoods_gdf: gpd.GeoDataFrame):
        self.BASE_CRS = GeoCalculator.BASE_CRS
        self.PROJECTED_CRS = GeoCalculator.PROJECTED_CRS
        self.neighborhoods_gdf = neighborhoods_gdf
        
        # Estruturas auxiliares
        self._points_by_line = {}
        self._lines_by_id = {}
        self._points_by_coord = {}
        self.next_temp_line_id = 1

        # Cache de projeção
        self.neighborhoods_gdf_proj = self.neighborhoods_gdf.to_crs(self.PROJECTED_CRS)

    def _build_auxiliary_structures(self, state: GraphState):
        """Constrói mapas em memória a partir do estado atual."""
        # Limpa estruturas antigas se for chamado novamente
        self._points_by_line = {}
        self._lines_by_id = {}
        self._points_by_coord = {}

        # Agrupa pontos por linha
        self._points_by_line = {
            line_id: group.sort_values('vertex_index')
            for line_id, group in state.data_points.groupby('from_line_id')
        }

        # Indexa as ruas por 'id'
        self._lines_by_id = {
            row.id: row.Index
            for row in state.data_streets.itertuples()
        }

        # Mapeia coord_tuple -> [lista_de_indices_GDF]
        temp_points_by_coord = {}
        
        # Constrói mapa de pontos por coordenada
        for point_row in state.data_points.itertuples():
            coord_tuple = tuple(np.round(point_row.geometry.coords[0], GeoCalculator.PRECISION_DIGITS))
            if coord_tuple not in temp_points_by_coord:
                temp_points_by_coord[coord_tuple] = []
            temp_points_by_coord[coord_tuple].append(point_row.Index) 
        self._points_by_coord = temp_points_by_coord

        self.next_temp_line_id = (state.data_streets['id'].max() if not state.data_streets.empty else 0) + 1
    
    def create_reduced_graph(self, state: GraphState) -> GraphState:
        """
        Método orquestrador: Reduz o grafo lógico (data_gdf).
        """
        self._build_auxiliary_structures(state)
        
        points_to_keep_indices = set()
        
        # Itera sobre cada linha e seus pontos associados
        for line_id, points_gdf in self._points_by_line.items():
            
            # Identifica os índices (0, 1, 2...) dos pontos especiais
            special_indices = self._find_special_indices(points_gdf)

            # Se a linha não pode ser reduzida, marca todos os pontos para manter
            if len(special_indices) < 2:
                points_to_keep_indices.update(points_gdf.index)
                continue

            # Processa os segmentos entre pontos especiais
            kept_indices = self._process_line_segments(points_gdf, special_indices, line_id, state)
            points_to_keep_indices.update(kept_indices)

        # Filtra o GDF de pontos final
        print(f"  Reducer: Grafos de dados reduzidos de {len(state.data_points)} para {len(points_to_keep_indices)} pontos.")
        state.data_points = state.data_points.loc[list(points_to_keep_indices)].copy()
        
        # Reconstrói o GDF de pontos (agora reduzido) para re-indexar 'vertex_index'
        state.data_points = self._reindex_reduced_points(state.data_points)

        return state

    def _find_special_indices(self, points_gdf: gpd.GeoDataFrame) -> list:
        """
        Encontra os índices da linha (0, 1, 2...N-1) dos pontos especiais.
        """
        is_special = (points_gdf['eh_extremidade'] == 'yes') | (points_gdf['eh_unido'] == 'yes')
        
        # Retorna os índices da linha (vertex_index)
        return points_gdf[is_special]['vertex_index'].tolist()
    
    def _process_line_segments(self, points_gdf: gpd.GeoDataFrame, special_indices: list, line_id: int, state: GraphState) -> set:
        """
        Itera sobre os segmentos (entre nós especiais), calcula atributos
        acumulados e atualiza os GDFs de dados.
        """
        kept_indices = set()        # Índices do GDF de pontos a manter
        
        # Converte para dicts
        points_list = points_gdf.to_dict('records')
        # Mapeia vertex_index -> dict (para acesso não sequencial)
        points_map = {p['vertex_index']: p for p in points_list}

        for k in range(len(special_indices) - 1):
            i_v_index = special_indices[k]          # ex: 0
            j_v_index = special_indices[k+1]        # ex: 5

            # Acumula distância e ângulos
            dist_total_km = 0.0
            angles = []
            angles_inv = []
            # Armazena coords dos pontos entre i e j
            intermediate_coordinates = []

            for m in range(i_v_index, j_v_index + 1):
                point_m = points_map[m]

                # Coleta coordenadas intermediárias
                if m > i_v_index and m < j_v_index:
                    intermediate_coordinates.append(point_m['geometry'].coords[0])

                dist_total_km += point_m["distance"]
                angle = point_m.get("angle")
                angle_inv = point_m.get("angle_inv")
                
                if angle is not None and isinstance(angle, (int, float)) and not math.isnan(angle):
                    angles.append(angle)
                if angle_inv is not None and isinstance(angle_inv, (int, float)) and not math.isnan(angle_inv):
                    angles_inv.append(angle_inv)
            
            # Calcula a média dos ângulos
            avg_angle = GeoCalculator.mean_angle_deg(angles)
            avg_angle_inv = GeoCalculator.mean_angle_deg(angles_inv)

            # Atualiza atributos no GDF de pontos
            # Atualiza ponto i (início do segmento)
            i_point_idx = points_gdf.iloc[i_v_index].name
            if avg_angle is not None:
                state.data_points.loc[i_point_idx, 'angle'] = round(avg_angle, GeoCalculator.PRECISION_DIGITS)
            if avg_angle_inv is not None:
                state.data_points.loc[i_point_idx, 'angle_inv'] = round(avg_angle_inv, GeoCalculator.PRECISION_DIGITS)

            # Atualiza ponto j (fim do segmento)
            j_point_idx = points_gdf.iloc[j_v_index].name
            state.data_points.loc[j_point_idx, 'vertex_to'] = i_v_index
            state.data_points.loc[j_point_idx, 'distance'] = round(dist_total_km, GeoCalculator.PRECISION_DIGITS)
            
            # Marca os dois pontos para manter
            kept_indices.add(i_point_idx)
            kept_indices.add(j_point_idx)

        return kept_indices
    
    def _reindex_reduced_points(self, data_points_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Após a redução, os 'vertex_index' dos pontos restantes (ex: 0, 5, 8) 
        são re-indexados para (0, 1, 2) para cada rua.
        """
        print("  Reducer: Re-indexando 'vertex_index' dos pontos reduzidos...")
        
        # Armazena os dicts das linhas processadas
        new_points_list = []
        
        # Itera sobre os grupos
        for line_id, group in data_points_gdf.groupby('from_line_id'):
            # Garante a ordem correta
            group = group.sort_values('vertex_index')
            
            # Converte para dicts
            group_rows = group.to_dict('records')

            # Cria um mapa de (vertex_index ANTIGO -> vertex_index NOVO)
            # ex: {0: 0, 5: 1, 8: 2}
            old_to_new_map = {
                old_v_index['vertex_index']: new_v_index 
                for new_v_index, old_v_index in enumerate(group_rows)
            }
            # Garante que 'vertex_to=0' do ponto inicial mapeie para 0
            old_to_new_map[0] = 0 

            # Itera sobre os dicts e aplica os novos índices
            for i, row in enumerate(group_rows):
                # Aplica o novo 'vertex_index' (0, 1, 2...)
                row['vertex_index'] = i 
                
                # Mapeia o 'vertex_to' para o novo v_index usando o mapa
                row['vertex_to'] = old_to_new_map.get(row['vertex_to'], 0) 
                
                new_points_list.append(row)
        
        # Reconstrói o GDF
        return gpd.GeoDataFrame(
            new_points_list, 
            crs=data_points_gdf.crs
        )
    
    def remove_boundary_vertices(self, state: GraphState) -> GraphState:
        """
        Remove vértices que conectam ruas de bairros diferentes, 
        mesclando as ruas.
        """
        print("Executando: remove_boundary_vertices")
        
        # Reseta as estruturas auxiliares
        self._build_auxiliary_structures(state)

        # Lista de (pt1_row, pt2_row)
        boundary_pairs = []

        # Encontra todos os pares de fronteira
        for coord, point_indices in self._points_by_coord.items():
            if len(point_indices) != 2:
                continue        # Só processa pares exatos
                
            pt1_row = state.data_points.loc[point_indices[0]]
            pt2_row = state.data_points.loc[point_indices[1]]
            
            if self._should_merge(pt1_row, pt2_row):
                boundary_pairs.append((pt1_row, pt2_row))
        
        if not boundary_pairs:
            print("  Nenhum vértice de fronteira para mesclar.")
            return

        # Conjuntos para rastrear o que foi modificado/removido
        lines_to_remove_idx = set()
        points_to_remove_idx = set()
        
        # Listas para novas features (a serem concatenadas no final)
        new_data_streets_list = []
        new_map_streets_list = []
        new_data_points_list = []
        
        # Processa cada par de fronteira
        for pt1_row, pt2_row in boundary_pairs:
            line1_id = pt1_row['from_line_id']
            line2_id = pt2_row['from_line_id']

            # Verifica se alguma dessas linhas/pontos já foi processada
            if (line1_id in self._lines_by_id and 
                line2_id in self._lines_by_id and
                pt1_row.name not in points_to_remove_idx and 
                pt2_row.name not in points_to_remove_idx):
                
                # Executa a lógica de mesclagem
                self._merge_boundary_linestrings(
                    pt1_row, pt2_row, state, 
                    lines_to_remove_idx, points_to_remove_idx,
                    new_data_streets_list, new_map_streets_list, new_data_points_list
                )
        
        print(f"  Mesclando {len(new_data_streets_list)} novas ruas.")

        # Aplica as remoções
        state.data_streets.drop(index=list(lines_to_remove_idx), inplace=True)
        state.map_streets.drop(index=list(lines_to_remove_idx), inplace=True)
        state.data_points.drop(index=list(points_to_remove_idx), inplace=True)

        # Adiciona as novas features
        if new_data_streets_list:
            state.data_streets = pd.concat(
                [state.data_streets, gpd.GeoDataFrame(new_data_streets_list, crs=state.data_streets.crs)],
                ignore_index=True
            )
            state.map_streets = pd.concat(
                [state.map_streets, gpd.GeoDataFrame(new_map_streets_list, crs=state.map_streets.crs)],
                ignore_index=True
            )
            state.data_points = pd.concat(
                [state.data_points, gpd.GeoDataFrame(new_data_points_list, crs=state.data_points.crs)],
                ignore_index=True
            )

        # Reindexa tudo
        if new_data_streets_list:
            print("  Re-indexando GDFs pós-mesclagem...")
            self._reindex_all_gdfs(state)

        return state

    def _should_merge(self, pt1_row: pd.Series, pt2_row: pd.Series) -> bool:
        """Verifica se dois pontos devem ser mesclados."""
        
        # Devem ser de ruas diferentes
        if pt1_row['from_line_id'] == pt2_row['from_line_id']:
            return False
            
        # Devem ser de bairros diferentes
        if pt1_row['id_bairro'] == pt2_row['id_bairro']:
            return False
            
        return True
    
    def _find_other_extreme_point_idx(self, line_id: int, excluded_point_idx: int) -> Optional[int]:
        """Encontra o índice GDF do ponto extremo oposto."""
        points = self._points_by_line.get(line_id)
        
        # Assume-se que as linhas têm 2 pontos (index 0 e 1)
        if points is None or len(points) != 2:
            return None # Erro ou linha já processada

        first_point_idx = points.index[0]
        last_point_idx = points.index[1]

        if excluded_point_idx == first_point_idx:
            return last_point_idx
        if excluded_point_idx == last_point_idx:
            return first_point_idx
            
        return None
    
    def _calculate_new_avg_angles(self, *points_rows) -> tuple:
        """
        Calcula ângulos médios acumulados para um conjunto de 'point rows'.
        """
        angles = []
        angles_inv = []

        for point in points_rows:
            angle = point.get("angle")
            angle_inv = point.get("angle_inv")
            
            # Filtro de segurança para None E nan
            if angle is not None and isinstance(angle, (int, float)) and not math.isnan(angle):
                angles.append(angle)
            if angle_inv is not None and isinstance(angle_inv, (int, float)) and not math.isnan(angle_inv):
                angles_inv.append(angle_inv)

        avg_angle = GeoCalculator.mean_angle_deg(angles)
        avg_angle_inv = GeoCalculator.mean_angle_deg(angles_inv)
        
        # Arredonda
        avg_angle = round(avg_angle, GeoCalculator.PRECISION_DIGITS) if avg_angle is not None else None
        avg_angle_inv = round(avg_angle_inv, GeoCalculator.PRECISION_DIGITS) if avg_angle_inv is not None else None

        return (avg_angle, avg_angle_inv)
    
    def _merge_boundary_linestrings(
        self, pt1_row: pd.Series, pt2_row: pd.Series, state: GraphState, 
        lines_to_remove_idx: set, points_to_remove_idx: set,
        new_data_streets_list: list, new_map_streets_list: list, 
        new_data_points_list: list
    ):
        """Lógica principal de mesclagem."""
        
        line1_id = pt1_row['from_line_id']
        line2_id = pt2_row['from_line_id']
        
        line1_gdf_idx = self._lines_by_id.get(line1_id)
        line2_gdf_idx = self._lines_by_id.get(line2_id)

        # Encontra os outros pontos (A e C)
        other_pt1_idx = self._find_other_extreme_point_idx(line1_id, pt1_row.name)
        other_pt2_idx = self._find_other_extreme_point_idx(line2_id, pt2_row.name)

        if other_pt1_idx is None or other_pt2_idx is None:
            return

        other_pt1_row = state.data_points.loc[other_pt1_idx]
        other_pt2_row = state.data_points.loc[other_pt2_idx]
        
        line1_row = state.data_streets.loc[line1_gdf_idx]
        line2_row = state.data_streets.loc[line2_gdf_idx]
        line1_map_row = state.map_streets.loc[line1_gdf_idx]
        line2_map_row = state.map_streets.loc[line2_gdf_idx]

        # Marca itens antigos para remoção
        lines_to_remove_idx.add(line1_gdf_idx)
        lines_to_remove_idx.add(line2_gdf_idx)
        points_to_remove_idx.add(pt1_row.name)
        points_to_remove_idx.add(pt2_row.name)
        points_to_remove_idx.add(other_pt1_idx)
        points_to_remove_idx.add(other_pt2_idx)

        # Cria novos atributos
        new_id = self.next_temp_line_id
        self.next_temp_line_id += 1
        
        # Soma o 'total_dist' das ruas originais
        new_total_dist = round(line1_row['total_dist'] + line2_row['total_dist'], GeoCalculator.PRECISION_DIGITS)
        
        # Calcula a média dos ângulos acumulados dos 4 pontos envolvidos
        avg_angle, avg_angle_inv = self._calculate_new_avg_angles(
            pt1_row, pt2_row, other_pt1_row, other_pt2_row
        )

        # Determina a orientação correta
        if other_pt1_row['vertex_index'] == 0:
            first_vertex_row = other_pt1_row
            second_vertex_row = other_pt2_row
            
            # Orientação é A -> C
            coords1 = list(line1_map_row.geometry.coords)           # A...B
            coords2 = list(line2_map_row.geometry.coords)           # B...C
            
            # Garante que A->B esteja na ordem correta
            if pt1_row['vertex_index'] == 0: coords1.reverse()      # Era B->A, inverte
            # Garante que B->C esteja na ordem correta
            if pt2_row['vertex_index'] != 0: coords2.reverse()      # Era C->B, inverte
            
        else:
            first_vertex_row = other_pt2_row
            second_vertex_row = other_pt1_row
            
            # Orientação é C -> A. Prepara coords visuais
            coords1 = list(line2_map_row.geometry.coords)           # C...B
            coords2 = list(line1_map_row.geometry.coords)           # B...A

            # Garante que C->B esteja na ordem correta
            if pt2_row['vertex_index'] == 0: coords1.reverse()      # Era B->C, inverte
            # Garante que B->A esteja na ordem correta
            if pt1_row['vertex_index'] != 0: coords2.reverse()      # Era A->B, inverte

        # Cria Novas Ruas

        # Cria rua de DADOS
        new_data_street = line1_row.to_dict()       # Usa line1 como base
        new_data_street['id'] = new_id
        new_data_street['geometry'] = LineString([
            first_vertex_row.geometry.coords[0], 
            second_vertex_row.geometry.coords[0]
        ])
        new_data_street['total_dist'] = new_total_dist
        
        # Cria rua de MAPA
        new_map_street = line1_map_row.to_dict()
        new_map_street['id'] = new_id
        new_map_street['total_dist'] = new_total_dist
        new_map_street['geometry'] = LineString(coords1 + coords2[1:])
        
        # Reatribui Bairro para a nova rua (usando a geometria do MAPA)
        new_bairro_id, new_bairro_name = self._get_dominant_bairro(new_map_street['geometry'])
        new_data_street['id_bairro'] = new_bairro_id
        new_data_street['bairro'] = new_bairro_name
        new_map_street['id_bairro'] = new_bairro_id
        new_map_street['bairro'] = new_bairro_name

        new_data_streets_list.append(new_data_street)
        new_map_streets_list.append(new_map_street)
        
        # Cria Novos Pontos
        new_pt1 = first_vertex_row.to_dict()
        new_pt1.update({
            'from_line_id': new_id, 
            'vertex_index': 0, 
            'vertex_to': 0, 
            'distance': 0.0,
            'angle': avg_angle, 
            'angle_inv': avg_angle_inv
        })
        # (eh_unido, eh_extremidade, etc. são herdados)
        
        new_pt2 = second_vertex_row.to_dict()
        new_pt2.update({
            'from_line_id': new_id, 
            'vertex_index': 1, 
            'vertex_to': 0, 
            'distance': new_total_dist,
            'angle': None, 
            'angle_inv': None
        })
        
        new_data_points_list.append(new_pt1)
        new_data_points_list.append(new_pt2)
        
        # Remove os IDs das linhas antigas das estruturas auxiliares
        del self._lines_by_id[line1_id]
        del self._lines_by_id[line2_id]

    def _get_dominant_bairro(self, line_geom: LineString) -> tuple:
        """Reatribui o bairro de uma rua."""
        try:
            line_gdf_proj = gpd.GeoDataFrame(
                geometry=[line_geom], crs=self.BASE_CRS
            ).to_crs(self.PROJECTED_CRS)

            intersection_gdf = gpd.overlay(
                line_gdf_proj, 
                self.neighborhoods_gdf_proj,
                how='intersection',
                keep_geom_type=False
            )
            intersection_gdf = intersection_gdf[
                intersection_gdf.geom_type == 'LineString'
            ]
            if intersection_gdf.empty: return (None, None)

            intersection_gdf['length_m'] = intersection_gdf.geometry.length
            lengths_by_bairro = intersection_gdf.groupby('id_bairro')['length_m'].sum()
            
            new_bairro_id = lengths_by_bairro.idxmax()
            new_bairro_name = self.neighborhoods_gdf[
                self.neighborhoods_gdf['id_bairro'] == new_bairro_id
            ]['bairro'].iloc[0]
            
            return (new_bairro_id, new_bairro_name)
        except Exception:
            return (None, None)
        
    def _reindex_all_gdfs(self, state: GraphState) -> GraphState:
        """Re-indexa 'id' e propaga para 'from_line_id'."""
        
        # Reseta o índice principal (0 a N-1)
        state.data_streets.reset_index(drop=True, inplace=True)
        state.map_streets.reset_index(drop=True, inplace=True)
        
        # Cria mapa de 'id_antigo' -> 'id_novo' (1 a N)
        new_id_map = pd.Series(
            range(1, len(state.data_streets) + 1), 
            index=state.data_streets['id']
        ).to_dict()

        # Aplica novos IDs
        state.data_streets['id'] = state.data_streets['id'].map(new_id_map)
        state.map_streets['id'] = state.map_streets['id'].map(new_id_map)
        
        # Propaga para os pontos
        state.data_points['from_line_id'] = state.data_points['from_line_id'].map(new_id_map)
        
        # Remove pontos órfãos (se alguma mesclagem falhou)
        valid_line_ids = set(state.data_streets['id'])
        state.data_points = state.data_points[
            state.data_points['from_line_id'].isin(valid_line_ids)
        ].copy()