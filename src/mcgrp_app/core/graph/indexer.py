# src\mcgrp_app\core\graph\indexer.py

import numpy as np
import pandas as pd

from ..utils import FieldConfigType, FieldsManager, GeoCalculator, GraphState

class GraphIndexer:
    """
    Responsável pela indexação final do grafo (nós, arestas, arcos)
    e pelo cálculo de custos.
    """

    def __init__(self, valid_neighborhoods: set = None):
        self.valid_neighborhoods = valid_neighborhoods

    def assign_indices(self, state: GraphState) -> GraphState:
        """Orquestrador principal."""
        print("Executando: assign_indices")
        
        # Limpa índices/custos antigos
        state = self._reset_indices(state)
        
        # Filtra DataFrames por bairros válidos
        state = self._prepare_valid_features(state)
        
        # Atribui node_index (1-N)
        state = self._assign_node_indices(state)
        
        # Atribui edge_index / arc_index (1-N)
        state = self._assign_edge_and_arc_indices(state)
        
        # Vincula nós (from_node, to_node) às ruas
        state = self._link_nodes_to_edges(state)
        
        # Calcula custos (travessia, serviço)
        state = self._calculate_costs(state)
        
        # Remove itens que falharam na indexação
        state = self._remove_invalid(state)

        return state

    def _reset_indices(self, state: GraphState) -> GraphState:
        """Limpa todas as propriedades de índice e custo."""
        print("  Indexer: Resetando índices e custos...")

        state.data_streets = FieldsManager.ensure_fields_exist(
            state.data_streets, FieldConfigType.EXTENDED
        )
        state.data_points = FieldsManager.ensure_fields_exist(
            state.data_points, FieldConfigType.EXTENDED
        )
        state.map_streets = FieldsManager.ensure_fields_exist(
            state.map_streets, FieldConfigType.EXTENDED
        )
        state.map_points = FieldsManager.ensure_fields_exist(
            state.map_points, FieldConfigType.EXTENDED
        )

        cols_to_reset_streets = ['edge_index', 'arc_index', 'from_node', 'to_node', 'custo_travessia', 'custo_servico']
        cols_to_reset_points = ['node_index']
        
        for col in cols_to_reset_streets:
            state.data_streets[col] = None
            state.map_streets[col] = None
            
        for col in cols_to_reset_points:
            state.data_points[col] = None
            # map_points será reconstruído depois

        return state

    def _prepare_valid_features(self, state: GraphState) -> GraphState:
        """Filtra os DataFrames com base nos bairros válidos."""
        if self.valid_neighborhoods is None:
            print("  Indexer: 'valid_neighborhoods' é None. Todos os bairros são válidos.")
            return state

        print(f"  Indexer: Filtrando DataFrames por {len(self.valid_neighborhoods)} bairros válidos...")
        
        # Filtra ruas e pontos
        state.data_streets = state.data_streets[
            state.data_streets['id_bairro'].isin(self.valid_neighborhoods)
        ].copy()
        state.map_streets = state.map_streets[
            state.map_streets['id_bairro'].isin(self.valid_neighborhoods)
        ].copy()
        state.data_points = state.data_points[
            state.data_points['id_bairro'].isin(self.valid_neighborhoods)
        ].copy()
        
        # Remove pontos que agora são órfãos
        valid_line_ids = set(state.data_streets['id'])
        state.data_points = state.data_points[
            state.data_points['from_line_id'].isin(valid_line_ids)
        ].copy()

        return state

    def _assign_node_indices(self, state: GraphState) -> GraphState:
        """Atribui 'node_index' (1-N) aos pontos."""
        print("  Indexer: Atribuindo 'node_index'...")
        
        # Cria 'coord_tuple' para agrupamento
        state.data_points['coord_tuple'] = state.data_points['geometry'].apply(
            lambda p: tuple(np.round(p.coords[0], GeoCalculator.PRECISION_DIGITS))
        )
        
        # Obtém todas as coordenadas únicas na ordem em que aparecem
        unique_coords = state.data_points['coord_tuple'].unique()
        
        # Cria o mapa (coord -> node_index 1-N)
        coords_to_node_map = {
            coord: idx + 1 for idx, coord in enumerate(unique_coords)
        }
        
        # Mapeia o 'node_index' para o DataFrame de dados
        state.data_points['node_index'] = state.data_points['coord_tuple'].map(coords_to_node_map)

        state.data_points = state.data_points.drop(columns='coord_tuple')
        
        # Reconstrói o DataFrame de mapa (visual)
        print("  Indexer: Reconstruindo DataFrame de pontos de mapa (para 'node_index')...")
        state.map_points = GeoCalculator.create_map_points(state.data_points)

        return state

    def _assign_edge_and_arc_indices(self, state: GraphState) -> GraphState:
        """Atribui 'edge_index' (bidirecional) e 'arc_index' (unidirecional)."""
        print("  Indexer: Atribuindo 'edge_index' e 'arc_index'...")
        
        for df in [state.data_streets, state.map_streets]:
            if df.empty: continue

            # Normaliza 'oneway' (None, NaN, 'não' -> 'no')
            oneway = df['oneway'].fillna('no').astype(str).str.lower()
            is_arc = (oneway == 'yes') | (oneway == '1') | (oneway == 'true')
            
            # Cria máscaras booleanas para atribuição
            n_edges = (~is_arc).sum()
            n_arcs = is_arc.sum()
            
            # Inicializa com NA
            df['edge_index'] = pd.NA
            df['arc_index'] = pd.NA
            
            if n_edges > 0:
                df.loc[~is_arc, 'edge_index'] = range(1, n_edges + 1)
            
            if n_arcs > 0:
                df.loc[is_arc, 'arc_index'] = range(1, n_arcs + 1)
            
            # Converte para Int64
            df['edge_index'] = df['edge_index'].astype('Int64')
            df['arc_index'] = df['arc_index'].astype('Int64')
        
        return state

    def _link_nodes_to_edges(self, state: GraphState) -> GraphState:
        """Define 'from_node' e 'to_node' nas ruas."""
        print("  Indexer: Vinculando 'from_node' e 'to_node'...")

        if state.data_points.empty or state.data_streets.empty:
            return state
        
        # Extrai mapeamento de pontos: from_line_id -> node_index
        # Considera-se vertex_index=0 como INÍCIO e vertex_index=1 como FIM (pós-splitter)

        # Filtra pontos de início
        start_points = state.data_points[state.data_points['vertex_index'] == 0]
        # Cria série: index=from_line_id, value=node_index
        start_map = start_points.set_index('from_line_id')['node_index']
        
        # Filtra pontos de fim (assumindo segmentos de 2 pontos, index 1)
        end_points = state.data_points[state.data_points['vertex_index'] == 1]
        end_map = end_points.set_index('from_line_id')['node_index']
        
        for df in [state.data_streets, state.map_streets]:
            if df.empty: continue
            
            # Mapeia usando o ID da rua
            df['from_node'] = df['id'].map(start_map).astype('Int64')
            df['to_node'] = df['id'].map(end_map).astype('Int64')

        return state

    def _calculate_costs(self, state: GraphState) -> GraphState:
        """Calcula 'custo_travessia' e 'custo_servico'."""
        print("  Indexer: Calculando custos de travessia e serviço...")
        
        for df in [state.data_streets, state.map_streets]:
            if df.empty: continue

            # Extrai números da string de velocidade '30 km/h' -> 30.0
            # Preenche NaNs com DEFAULT
            speeds = df['maxspeed'].astype(str).str.extract(r'(\d+)')[0].astype(float)
            speeds = speeds.fillna(GeoCalculator.DEFAULT_MAX_SPEED)
            speeds = speeds.replace(0, GeoCalculator.DEFAULT_MAX_SPEED)
            
            # Garante que distâncias sejam float
            dists = df['total_dist'].astype(float).fillna(0)
            
            speeds = speeds.clip(upper=GeoCalculator.DEFAULT_MAX_SPEED)
            costs_seconds = (dists / speeds * 3600).fillna(0)
            
            # Arredonda para cima e converte para Int64
            df['custo_travessia'] = np.ceil(costs_seconds).astype('Int64')
            
            # Custo Serviço = 1.5 * Travessia
            df['custo_servico'] = np.ceil(df['custo_travessia'] * 1.5).astype('Int64')

        return state

    def _remove_invalid(self, state: GraphState) -> GraphState:
        """Remove ruas/pontos que falharam na indexação."""
        print("  Indexer: Removendo inválidas (pós-indexação)...")
        
        # Encontra ruas inválidas (sem nó inicial/final ou sem índice)
        invalid_streets_mask = (
            state.data_streets['from_node'].isna() |
            state.data_streets['to_node'].isna() |
            (state.data_streets['edge_index'].isna() & state.data_streets['arc_index'].isna())
        )
        invalid_street_ids = set(state.data_streets[invalid_streets_mask]['id'])
        
        if invalid_street_ids:
            print(f"  Removendo {len(invalid_street_ids)} ruas inválidas.")
            # Remove de ambos os DataFrames de ruas
            state.data_streets = state.data_streets[~invalid_streets_mask].copy()
            state.map_streets = state.map_streets[
                ~state.map_streets['id'].isin(invalid_street_ids)
            ].copy()
            
            # Remove pontos órfãos
            state.data_points = state.data_points[
                ~state.data_points['from_line_id'].isin(invalid_street_ids)
            ].copy()
            state.map_points = state.map_points[
                ~state.map_points['from_line_id'].apply(lambda x: bool(set(x) & invalid_street_ids))
            ].copy()

        # Encontra pontos inválidos (sem 'node_index')
        invalid_points_mask = state.data_points['node_index'].isna()
        if invalid_points_mask.any():
            print(f"  Removendo {invalid_points_mask.sum()} pontos inválidos.")
            state.data_points = state.data_points[~invalid_points_mask].copy()
            # (map_points_gdf é reconstruído)

        return state