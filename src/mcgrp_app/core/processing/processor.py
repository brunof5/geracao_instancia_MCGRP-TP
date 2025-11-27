# src\mcgrp_app\core\processing\processor.py

import pandas as pd
import geopandas as gpd
import traceback
import itertools
from shapely.geometry import LineString

from ..utils import FieldsManager, FieldConfigType, GeoCalculator, GraphState

class GeoProcessor:
    """
    Trabalhador para filtrar, normalizar
    e processar fronteiras/extremidades.
    """

    BOUNDARY_DISTANCE_THRESHOLD_M = 1       # Distância (m) para considerar um ponto "na fronteira"
    PROTECTION_DISTANCE_KM = 0.05           # 50m (distância para proteger segmento)

    def __init__(self, neighborhoods_gdf: gpd.GeoDataFrame):
        self.BASE_CRS = GeoCalculator.BASE_CRS
        self.PROJECTED_CRS = GeoCalculator.PROJECTED_CRS
        self.neighborhoods_gdf = neighborhoods_gdf

        # Projeta os bairros (cache)
        self.neighborhoods_gdf_proj = self.neighborhoods_gdf.to_crs(self.PROJECTED_CRS)
        
        # Cria um mapa (dicionário) para consulta rápida de nomes de bairros
        self.bairro_name_map = self.neighborhoods_gdf.set_index('id_bairro')['bairro'].to_dict()
        
        # Obtém TODAS as geometrias de fronteira (ainda em 4326)
        all_boundaries_4326 = self.neighborhoods_gdf.geometry.boundary
        
        # Projeta a GeoSeries inteira
        all_boundaries_proj = all_boundaries_4326.to_crs(self.PROJECTED_CRS)

        # Cria o dicionário (map) usando 'zip', agora temos a fronteira de cada bairro (cache)
        self.bairros_fronteiras_proj = {
            id_bairro: boundary_geom
            for id_bairro, boundary_geom in zip(self.neighborhoods_gdf['id_bairro'], all_boundaries_proj)
        }
    
    def filter_and_normalize(self, state: GraphState) -> GraphState:
        """
        Filtra colunas desnecessárias e normaliza campos.
        """
        print("Executando: filter_and_normalize")
        
        # Obtém a lista de colunas "basic" do FieldsManager
        basic_fields = FieldsManager.get_field_config(
            FieldConfigType.BASIC
        )['LineString']
        
        # Adiciona 'geometry'
        if 'geometry' not in basic_fields:
            basic_fields.append('geometry')
            
        # Filtra colunas em ambos os GDFs
        state.data_streets = self._filter_columns(state.data_streets, basic_fields)
        state.map_streets = self._filter_columns(state.map_streets, basic_fields)
        
        # Normaliza o campo 'name'
        self._normalize_names(state.data_streets)
        self._normalize_names(state.map_streets)

        return state

    def _filter_columns(self, gdf: gpd.GeoDataFrame, keep_cols: list) -> gpd.GeoDataFrame:
        """Mantém apenas as colunas da lista 'keep_cols'."""
        # Colunas que existem no GDF e também estão na lista de 'keep_cols'
        cols_to_keep = [col for col in gdf.columns if col in keep_cols]
        
        # Colunas a serem removidas
        cols_to_drop = [col for col in gdf.columns if col not in cols_to_keep]
        
        if cols_to_drop:
            gdf = gdf.drop(columns=cols_to_drop)
            
        return gdf
    
    def _normalize_names(self, gdf: gpd.GeoDataFrame):
        """Preenche 'name' com 'alt_name' ou 'desconhecida'."""
        if 'name' in gdf.columns:
            if 'alt_name' in gdf.columns:
                gdf.loc[gdf['name'].isnull(), 'name'] = gdf['alt_name']
            gdf.loc[gdf['name'].isnull(), 'name'] = "desconhecida"

    def process_neighborhood_boundaries(self, state: GraphState) -> GraphState:
        """
        Orquestra a correção de vértices em fronteiras de bairros.
        """
        print("Executando: process_neighborhood_boundaries")
        
        # Agrupa por 'osm_id' (identificador da rua)
        grouped = state.data_streets.groupby('osm_id')
        
        # Índices das linhas que foram modificadas
        modified_indices = set()

        for osm_id, group in grouped:
            # Só processa ruas que aparecem em mais de um bairro
            if len(group) > 1:
                # Obtém todos os pares de índices (ex: (idx1, idx2), (idx1, idx3), ...)
                for idx1, idx2 in itertools.combinations(group.index, 2):
                    was_modified = self._adjust_boundary_vertices(
                        idx1, idx2, state.data_streets, state.map_streets
                    )
                    if was_modified:
                        modified_indices.add(idx1)
                        modified_indices.add(idx2)

        # Reatribui bairros
        if modified_indices:
            print(f"  Reatribuindo bairros para {len(modified_indices)} ruas modificadas...")
            self._reassign_neighborhood(
                list(modified_indices), state.data_streets, state.map_streets
            )

        # Remove LineStrings que ficaram inválidas
        state.data_streets, state.map_streets = self._remove_invalid_linestrings(
            state.data_streets, state.map_streets
        )

        return state

    def _adjust_boundary_vertices(self, idx1: int, idx2: int, 
                                  data_gdf: gpd.GeoDataFrame, 
                                  map_gdf: gpd.GeoDataFrame) -> bool:
        """
        Ajusta vértices de fronteira entre dois segmentos de rua (idx1, idx2).
        Retorna True se uma modificação foi feita, False caso contrário.
        """
        # Pega as linhas de ambos os GDFs
        row1_data = data_gdf.loc[idx1]
        row2_data = data_gdf.loc[idx2]
        
        # Se forem do mesmo bairro, não faz nada
        if row1_data['id_bairro'] == row2_data['id_bairro']:
            return False

        # Obtém as coordenadas
        coords1 = list(row1_data.geometry.coords)
        coords2 = list(row2_data.geometry.coords)

        if len(coords1) <= 1 or len(coords2) <= 1:
            return False
            
        modified = False

        # Proximidade início(1) - fim(2)
        if GeoCalculator.are_coords_close(coords1[0], coords2[-1]):
            coords1.pop(0)
            coords2[-1] = coords1[0]
            modified = True

        # Proximidade fim(1) - início(2)
        elif GeoCalculator.are_coords_close(coords1[-1], coords2[0]):
            coords2.pop(0)
            coords1[-1] = coords2[0]
            modified = True
            
        # Se modificado, atualiza a geometria em ambos GDFs
        if modified:
            try:
                # Armazena-se a geometria, se ficou inválida (< 2), cria uma LineString vazia
                geom1 = LineString(coords1) if len(coords1) >= 2 else LineString()
                geom2 = LineString(coords2) if len(coords2) >= 2 else LineString()
                
                # Atualiza os GDFs
                data_gdf.loc[idx1, 'geometry'] = geom1
                data_gdf.loc[idx2, 'geometry'] = geom2
                map_gdf.loc[idx1, 'geometry'] = geom1
                map_gdf.loc[idx2, 'geometry'] = geom2
            except Exception as e:
                print(f"  Aviso: Falha ao recriar geometria para índices {idx1}, {idx2}. {e}")
                return False

        return modified
    
    def _reassign_neighborhood(self, modified_indices: list, data_gdf: gpd.GeoDataFrame, map_gdf: gpd.GeoDataFrame):
        """
        Reatribui bairros usando a regra da DOMINÂNCIA (>50%).
        """
        try:
            # Filtra apenas ruas válidas
            modified_streets_gdf = data_gdf.loc[modified_indices]
            
            # Remove geometrias vazias
            modified_streets_gdf = modified_streets_gdf[~modified_streets_gdf.geometry.is_empty]
            
            # Remove NaNs/None
            modified_streets_gdf = modified_streets_gdf[modified_streets_gdf.geometry.notna()]
            
            # Valida tipo e integridade
            mask_valid = (modified_streets_gdf.geometry.type == 'LineString') & \
                          modified_streets_gdf.geometry.is_valid
            
            modified_streets_gdf = modified_streets_gdf[mask_valid]
            
            if modified_streets_gdf.empty: return

            # Projeta ruas
            modified_streets_gdf['orig_idx'] = modified_streets_gdf.index
            streets_proj = modified_streets_gdf.to_crs(self.PROJECTED_CRS)
            streets_proj['total_len'] = streets_proj.geometry.length

            # Overlay
            # Renomeia 'id_bairro' para 'target_id_bairro' antes do overlay
            neigh_subset = self.neighborhoods_gdf_proj[['id_bairro', 'geometry']].rename(
                columns={'id_bairro': 'target_id_bairro'}
            )

            intersections = gpd.overlay(
                streets_proj, 
                neigh_subset,
                how='intersection', 
                keep_geom_type=False
            )
            
            # Filtra apenas segmentos de linha
            intersections = intersections[intersections.geometry.type.isin(['LineString', 'MultiLineString'])]
            
            if intersections.empty:
                return

            # Calcula comprimento
            intersections['segment_len'] = intersections.geometry.length
            
            # Agregação
            stats = intersections.groupby(['orig_idx', 'target_id_bairro'])['segment_len'].sum().reset_index()
            
            # Junta com total
            stats = stats.merge(streets_proj[['orig_idx', 'total_len']], on='orig_idx')
            stats['coverage_pct'] = stats['segment_len'] / stats['total_len']

            # Determinação do vencedor
            updates_count = 0
            
            for orig_idx in stats['orig_idx'].unique():
                street_stats = stats[stats['orig_idx'] == orig_idx]
                if street_stats.empty: continue

                best_match = street_stats.loc[street_stats['coverage_pct'].idxmax()]
                
                # Regra de Ouro: > 50%
                if best_match['coverage_pct'] > 0.50:
                    new_bairro_id = int(best_match['target_id_bairro'])
                    current_bairro_id = data_gdf.loc[orig_idx, 'id_bairro']
                    
                    if new_bairro_id != current_bairro_id:
                        new_name = self.bairro_name_map.get(new_bairro_id, "DESCONHECIDO")
                        
                        # Atualiza Dados
                        data_gdf.at[orig_idx, 'id_bairro'] = new_bairro_id
                        data_gdf.at[orig_idx, 'bairro'] = new_name
                        
                        # Atualiza Mapa
                        if orig_idx in map_gdf.index:
                            map_gdf.at[orig_idx, 'id_bairro'] = new_bairro_id
                            map_gdf.at[orig_idx, 'bairro'] = new_name
                        
                        updates_count += 1

            if updates_count > 0:
                print(f"  Reatribuídos {updates_count} bairros com base em dominância geométrica > 50%.")

        except Exception as e:
            print(f"  Erro na reatribuição de bairros: {e}")
            traceback.print_exc()

    def _remove_invalid_linestrings(self, data_gdf: gpd.GeoDataFrame, map_gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Remove linhas que ficaram com menos de 2 pontos."""
        print("  Removendo LineStrings inválidas (menos de 2 pontos).")
        
        # Filtra geometrias válidas
        is_valid = data_gdf.geometry.apply(
            lambda geom: geom is not None and not geom.is_empty and geom.geom_type == 'LineString' and len(geom.coords) >= 2
        )
        
        invalid_count = len(is_valid[~is_valid])
        if invalid_count > 0:
            print(f"  {invalid_count} ruas inválidas removidas.")
            data_gdf = data_gdf[is_valid].copy()
            map_gdf = map_gdf[is_valid].copy()

            print("  Re-indexando o campo 'id' das ruas...")
            
            # Reseta o índice principal do DataFrame (0, 1, 2, ... N-1)
            # 'drop=True' impede que o índice antigo vire uma coluna
            data_gdf.reset_index(drop=True, inplace=True)
            map_gdf.reset_index(drop=True, inplace=True)

            # Cria a nova lista de IDs (1, 2, 3, ... N)
            new_ids = range(1, len(data_gdf) + 1)

            # Sobrescreve a coluna 'id' existente com os novos IDs
            data_gdf['id'] = new_ids
            map_gdf['id'] = new_ids

        return data_gdf, map_gdf

    def remove_invalid_endpoints(self, state: GraphState) -> GraphState:
        """
        Remove pontos de extremidade (não unidos) que estão muito próximos
        às fronteiras dos bairros, respeitando um critério de proteção.
        """
        print("Executando: remove_invalid_endpoints")
        
        # Agrupa pontos por 'from_line_id'
        points_by_line_id = {
            line_id: group.sort_values('vertex_index')
            for line_id, group in state.data_points.groupby('from_line_id')
        }

        points_to_remove_idx = set()
        lines_to_remove_ids = set()

        # Itera sobre cada grupo de pontos (de cada rua)
        for line_id, pontos_gdf in points_by_line_id.items():
            vertices_count = len(pontos_gdf)
            if vertices_count < 2:
                continue

            # Verifica o PONTO INICIAL (vertex_index == 0)
            start_point = pontos_gdf.iloc[0]
            if start_point['eh_unido'] == 'no':
                if self._is_point_removable(start_point, pontos_gdf.iloc[1]['distance']):
                    if vertices_count == 2:
                        lines_to_remove_ids.add(line_id)
                        points_to_remove_idx.update(pontos_gdf.index)
                    else:
                        points_to_remove_idx.add(start_point.name)
                        self._shorten_line_start(line_id, pontos_gdf, state)
            
            # Verifica o PONTO FINAL (se a linha ainda for válida)
            if line_id not in lines_to_remove_ids and vertices_count > 0:
                end_point = pontos_gdf.iloc[-1]
                if end_point['eh_unido'] == 'no':
                    if self._is_point_removable(end_point, end_point['distance']):
                        if vertices_count == 2:
                            lines_to_remove_ids.add(line_id)
                            points_to_remove_idx.update(pontos_gdf.index)
                        else:
                            points_to_remove_idx.add(end_point.name)
                            self._shorten_line_end(line_id, pontos_gdf, state)

        print(f"  {len(points_to_remove_idx)} pontos e {len(lines_to_remove_ids)} linhas marcados para remoção.")

        # Aplica as remoções e re-indexações
        if len(points_to_remove_idx) > 0 or len(lines_to_remove_ids) > 0:
            state = self._apply_removals_and_reindex(
                state, points_to_remove_idx, lines_to_remove_ids
            )
        else:
            print("  Não há extremidades inválidas.")

        return state

    def _is_point_removable(self, point_row: pd.Series, segment_distance_km: float) -> bool:
        """
        Helper que verifica se um Ponto de extremidade está perto da
        fronteira E se falha no critério de proteção.
        """
        id_bairro = point_row['id_bairro']
        if id_bairro not in self.bairros_fronteiras_proj:
            return False        # Bairro sem fronteira (??)

        # Pega a fronteira pré-projetada do cache
        fronteira_proj = self.bairros_fronteiras_proj[id_bairro]
        
        # Projeta o ponto para metros
        ponto_gds = gpd.GeoSeries(
            [point_row.geometry], crs=self.BASE_CRS
        )
        ponto_proj = ponto_gds.to_crs(self.PROJECTED_CRS).iloc[0]

        # Verifica proximidade com fronteira
        if ponto_proj.distance(fronteira_proj) <= self.BOUNDARY_DISTANCE_THRESHOLD_M:
            # Verifica critério de proteção
            if segment_distance_km > self.PROTECTION_DISTANCE_KM:
                return False        # Está protegido, NÃO remove
            
            return True             # Está perto E não está protegido, REMOVE
            
        return False                # Não está perto da fronteira
    
    def _shorten_line_start(self, line_id: int, pontos_gdf: gpd.GeoDataFrame, state: GraphState):
        """
        Opera no 'state' para remover o primeiro vértice de uma rua.
        """
        # Encurta a geometria da rua (em ambos os GDFs de ruas)
        new_geom = LineString(pontos_gdf.geometry.iloc[1:].values)
        state.data_streets.loc[state.data_streets['id'] == line_id, 'geometry'] = new_geom
        state.map_streets.loc[state.map_streets['id'] == line_id, 'geometry'] = new_geom
        
        # Re-indexa os pontos restantes (somente no GDF de dados)
        for i, idx in enumerate(pontos_gdf.index[1:]):      # Itera do segundo ponto em diante
            state.data_points.loc[idx, 'vertex_index'] = i
            state.data_points.loc[idx, 'vertex_to'] = i
            
            if i == 0:      # Este é o NOVO ponto inicial
                state.data_points.loc[idx, 'distance'] = 0.0
                state.data_points.loc[idx, 'eh_extremidade'] = 'yes'

    def _shorten_line_end(self, line_id: int, pontos_gdf: gpd.GeoDataFrame, state: GraphState):
        """
        Opera no 'state' para remover o último vértice de uma rua.
        """
        # Encurta a geometria da rua (em ambos os GDFs de ruas)
        new_geom = LineString(pontos_gdf.geometry.iloc[:-1].values)
        state.data_streets.loc[state.data_streets['id'] == line_id, 'geometry'] = new_geom
        state.map_streets.loc[state.map_streets['id'] == line_id, 'geometry'] = new_geom
        
        # Atualiza o NOVO ponto final
        new_end_idx = pontos_gdf.index[-2]      # O penúltimo ponto
        state.data_points.loc[new_end_idx, 'eh_extremidade'] = 'yes'
        # Limpa ângulo (não tem ponto seguinte)
        state.data_points.loc[new_end_idx, ['angle', 'angle_inv']] = None

    def _apply_removals_and_reindex(self, state: GraphState, points_to_remove_idx: set, 
                                    lines_to_remove_ids: set) -> GraphState:
        """
        Aplica as remoções de pontos e linhas e, em seguida,
        re-indexa o campo 'id' (ruas) e propaga para 'from_line_id' (pontos).
        """
        # Remove os pontos individuais (dos encurtamentos)
        if points_to_remove_idx:
            state.data_points.drop(index=list(points_to_remove_idx), inplace=True)
            
        # Remove as linhas inteiras (e seus pontos associados)
        if lines_to_remove_ids:
            # Remove pontos
            state.data_points = state.data_points[
                ~state.data_points['from_line_id'].isin(lines_to_remove_ids)
            ].copy()
            
            # Remove ruas
            state.data_streets = state.data_streets[
                ~state.data_streets['id'].isin(lines_to_remove_ids)
            ].copy()
            state.map_streets = state.map_streets[
                ~state.map_streets['id'].isin(lines_to_remove_ids)
            ].copy()

        # Re-indexa o 'id' das ruas restantes (1 a N)
        # Garante que os GDFs de ruas estejam na mesma ordem
        state.data_streets.sort_index(inplace=True)
        state.map_streets.sort_index(inplace=True)
        
        # Reseta o índice principal (0 a N-1)
        state.data_streets.reset_index(drop=True, inplace=True)
        state.map_streets.reset_index(drop=True, inplace=True)
        
        # Cria um mapa de 'id_antigo' -> 'id_novo'
        # (Nota: 'id' ainda tem os valores antigos aqui)
        new_id_map = pd.Series(
            range(1, len(state.data_streets) + 1),       # Novos IDs (1 a N)
            index=state.data_streets['id']               # Índice é o ID antigo
        ).to_dict()

        # Aplica os novos IDs
        state.data_streets['id'] = state.data_streets['id'].map(new_id_map)
        state.map_streets['id'] = state.map_streets['id'].map(new_id_map)
        
        # Propaga os novos IDs para os pontos
        state.data_points['from_line_id'] = state.data_points['from_line_id'].map(new_id_map)

        return state