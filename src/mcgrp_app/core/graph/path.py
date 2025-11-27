# src\mcgrp_app\core\graph\path.py

import heapq
import pandas as pd
from collections import defaultdict
from typing import Dict, Set, List, Tuple

from ..utils import GraphState

class ShortestPathAnalyzer:
    """Analisa caminhos mínimos e conectividade entre bairros no grafo."""

    def __init__(self, state: GraphState):
        """Inicializa o analisador com o estado atual do grafo."""
        self.state = state
        
        # Verifica se temos os dados necessários
        if self.state.neighborhoods is None:
            raise ValueError("GDF de Bairros é necessário para análise de caminhos.")
        
        self.depot_node = None
        self.graph = defaultdict(list)
        self.neighborhood_connections = {}              # Bairro -> Depósito
        self.neighborhood_connections_return = {}       # Depósito -> Bairro

        # Mapa de fronteiras dos bairros
        self.bairros_boundaries = {
            row["id_bairro"]: row.geometry.boundary
            for _, row in self.state.neighborhoods.iterrows()
        }

    def analyze_neighborhoods(self) -> Set[int]:
        """
        Executa análise completa de conectividade entre bairros com elementos requeridos.
        Usa interseções de fronteira para encontrar rotas.
        """
        print("ShortestPath: Iniciando análise de conectividade...")
        self._build_graph()
        self._find_depot()

        required_neighborhoods = self._identify_required_neighborhoods()

        for neighborhood_id in required_neighborhoods:
            self._process_neighborhood_exits(neighborhood_id)

        # Coleta todos os bairros identificados como passagem
        kept_neighborhoods = set(required_neighborhoods)
        
        # Adiciona o bairro do depósito (sempre necessário)
        kept_neighborhoods.add(self.depot_id_bairro)

        for data in self.neighborhood_connections.values():
            kept_neighborhoods.update(data["neighbors"])
        for data in self.neighborhood_connections_return.values():
            kept_neighborhoods.update(data["neighbors"])
            
        print(f"ShortestPath: {len(kept_neighborhoods)} bairros mantidos após análise.")
        return kept_neighborhoods

    def _build_graph(self) -> None:
        """Constrói grafo de adjacências para Dijkstra."""
        # Limpa grafo anterior
        self.graph = defaultdict(list)
        
        if self.state.data_streets is None: return

        for row in self.state.data_streets.itertuples():
            u = int(getattr(row, 'from_node'))
            v = int(getattr(row, 'to_node'))
            weight = getattr(row, 'custo_travessia', 0)
            line_id = getattr(row, 'id')
            
            # Aresta direcionada u -> v
            self.graph[u].append((v, weight, line_id))
            
            # Se for aresta (bidirecional), adiciona v -> u
            edge_idx = getattr(row, 'edge_index', None)
            if pd.notna(edge_idx) and int(edge_idx) != -1:
                self.graph[v].append((u, weight, line_id))

    def _find_depot(self) -> None:
        """Localiza o nó e o bairro do depósito."""
        if self.state.data_points is None:
            raise ValueError("GDF de Pontos vazio.")
            
        depot_rows = self.state.data_points[self.state.data_points['depot'] == 'yes']
        if depot_rows.empty:
            raise ValueError("Nenhum depósito definido no grafo.")
        
        # Obtém o primeiro encontrado
        depot_row = depot_rows.iloc[0]
        self.depot_node = int(depot_row['node_index'])
        self.depot_id_bairro = int(depot_row['id_bairro']) if pd.notna(depot_row.get('id_bairro')) else -1
        
        print(f"ShortestPath: Depósito identificado no Nó {self.depot_node} (Bairro {self.depot_id_bairro})")

    def _identify_required_neighborhoods(self) -> Set[int]:
        """Retorna IDs de bairros que possuem ruas ou nós requeridos."""
        req_neighs = set()
        
        # Verifica nós requeridos
        if self.state.data_points is not None:
            req_nodes = self.state.data_points[
                (self.state.data_points['eh_requerido'] == 'yes') & 
                (self.state.data_points['depot'] != 'yes')
            ]
            req_neighs.update(req_nodes['id_bairro'].dropna().unique().astype(int))

        # Verifica Ruas Requeridas
        if self.state.data_streets is not None:
            req_streets = self.state.data_streets[self.state.data_streets['eh_requerido'] == 'yes']
            req_neighs.update(req_streets['id_bairro'].dropna().unique().astype(int))
            
        return req_neighs

    def _process_neighborhood_exits(self, neighborhood_id: int) -> None:
        """Encontra interseções (saídas) do bairro e calcula caminhos."""
        # Encontra pontos de saída (interseção de ruas com a fronteira)
        exit_nodes = self._find_boundary_intersection_nodes(neighborhood_id)
        
        if not exit_nodes:
            print(f"  Aviso: Nenhuma saída de fronteira encontrada para Bairro {neighborhood_id}.")
            return

        # Inicializa estruturas de armazenamento
        if neighborhood_id not in self.neighborhood_connections:
            self.neighborhood_connections[neighborhood_id] = {
                "neighbors": set([neighborhood_id]), "min_dist": float('inf')
            }
        if neighborhood_id not in self.neighborhood_connections_return:
            self.neighborhood_connections_return[neighborhood_id] = {
                "neighbors": set([neighborhood_id, self.depot_id_bairro]), "min_dist": float('inf')
            }

        # Para cada saída, calcula Dijkstra até/do Depósito
        for exit_node in exit_nodes:
            # Ida: Depósito -> Saída
            dist1, path1 = self._dijkstra_with_path(self.depot_node, exit_node)
            
            # Volta: Saída -> Depósito
            dist2, path2 = self._dijkstra_with_path(exit_node, self.depot_node)
            
            # Processa caminho de IDA
            if path1 and dist1 < self.neighborhood_connections[neighborhood_id]["min_dist"]:
                # Achou um caminho melhor! Atualiza
                self.neighborhood_connections[neighborhood_id]["min_dist"] = dist1
                self._register_path_neighborhoods(path1, self.neighborhood_connections[neighborhood_id]["neighbors"])

            # Processa caminho de VOLTA
            if path2 and dist2 < self.neighborhood_connections_return[neighborhood_id]["min_dist"]:
                self.neighborhood_connections_return[neighborhood_id]["min_dist"] = dist2
                self._register_path_neighborhoods(path2, self.neighborhood_connections_return[neighborhood_id]["neighbors"])

    def _find_boundary_intersection_nodes(self, neighborhood_id: int) -> List[int]:
        """
        Encontra os nós das ruas que interceptam a fronteira do bairro.
        """
        boundary_geom = self.bairros_boundaries.get(neighborhood_id)
        if boundary_geom is None: return []

        # Filtra ruas que pertencem a este bairro
        streets_in_neigh = self.state.map_streets[self.state.map_streets['id_bairro'] == neighborhood_id]
        
        exit_nodes = set()
        
        # Itera sobre as ruas do bairro
        for row in streets_in_neigh.itertuples():
            # Se a rua cruza a fronteira (ou toca)
            if row.geometry.intersects(boundary_geom):
                exit_nodes.add(int(row.from_node))
                exit_nodes.add(int(row.to_node))
            
        return list(exit_nodes)

    def _dijkstra_with_path(self, start_node: int, end_node: int) -> Tuple[float, List[int]]:
        """Dijkstra padrão. Retorna (distância, lista_de_line_ids)."""
        if start_node == end_node:
            return 0.0, []
        
        distances = {start_node: 0}
        previous = {}                   # {node: (prev_node, line_id)}
        heap = [(0, start_node)]
        visited = set()

        while heap:
            current_dist, current_node = heapq.heappop(heap)

            if current_node in visited: continue
            visited.add(current_node)

            if current_node == end_node:
                return current_dist, self._reconstruct_path_lines(previous, start_node, end_node)

            if current_dist > distances.get(current_node, float('inf')):
                continue

            for neighbor, weight, line_id in self.graph[current_node]:
                new_dist = current_dist + weight
                if new_dist < distances.get(neighbor, float('inf')):
                    distances[neighbor] = new_dist
                    previous[neighbor] = (current_node, line_id)
                    heapq.heappush(heap, (new_dist, neighbor))

        return float('inf'), []

    def _reconstruct_path_lines(self, previous: Dict, start: int, end: int) -> List[int]:
        path_lines = []
        curr = end
        while curr != start:
            prev, line_id = previous[curr]
            path_lines.append(line_id)
            curr = prev
        return list(reversed(path_lines))

    def _register_path_neighborhoods(self, line_ids: List[int], neighbors_set: Set[int]):
        """Adiciona os bairros das ruas do caminho ao conjunto de vizinhos."""
        rows = self.state.data_streets[self.state.data_streets['id'].isin(line_ids)]
        bairros = rows['id_bairro'].dropna().unique().astype(int)
        neighbors_set.update(bairros)

    def prune_dead_ends(self):
        """
        Método público para iniciar a remoção iterativa de extremidades.
        """
        print("ShortestPath: Iniciando poda de extremidades mortas (dead-ends)...")
        self._remove_safe_extremes()
        print("ShortestPath: Poda concluída.")

    def _remove_safe_extremes(self) -> None:
        """
        Remove iterativamente nós extremos de ruas sem saída, 
        desde que não sejam requeridos ou depósitos.
        """
        changed = True
        iteration = 0

        while changed:
            changed = False
            iteration += 1
            
            # Calcular grau de todos os nós (baseado em ruas ativas)
            all_nodes = pd.concat([
                self.state.data_streets['from_node'], 
                self.state.data_streets['to_node']
            ])
            node_counts = all_nodes.value_counts()
            
            # Nós com grau 1 (Extremos/Leafs)
            leaf_nodes = node_counts[node_counts == 1].index
            
            if leaf_nodes.empty:
                break

            # Identificar nós removíveis (Leaf + Não Requerido + Não Depósito)
            unique_points = self.state.data_points.drop_duplicates('node_index').set_index('node_index')
            
            # Intersecção entre folhas e pontos existentes
            candidate_leaves = unique_points.index.intersection(leaf_nodes)
            candidates = unique_points.loc[candidate_leaves]
            
            removable_nodes = candidates[
                (candidates['eh_requerido'] != 'yes') & 
                (candidates['depot'] != 'yes')
            ].index

            if removable_nodes.empty:
                break

            # Identificar Ruas removíveis
            streets = self.state.data_streets
            
            # A rua deve tocar um nó removível
            touch_removable = streets['from_node'].isin(removable_nodes) | \
                              streets['to_node'].isin(removable_nodes)
            
            # A rua NÃO pode ser requerida
            not_required = streets['eh_requerido'] != 'yes'
            
            streets_to_remove = streets[touch_removable & not_required]
            
            if streets_to_remove.empty:
                break
                
            # Executar Remoção
            ids_to_remove = streets_to_remove['id'].tolist()
            
            count_removed = len(ids_to_remove)
            if count_removed > 0:
                # Remove Ruas (Dados e Mapa)
                self.state.data_streets = self.state.data_streets[
                    ~self.state.data_streets['id'].isin(ids_to_remove)
                ].copy()
                
                self.state.map_streets = self.state.map_streets[
                    ~self.state.map_streets['id'].isin(ids_to_remove)
                ].copy()
                
                # Remove Pontos associados às ruas removidas
                self.state.data_points = self.state.data_points[
                    ~self.state.data_points['from_line_id'].isin(ids_to_remove)
                ].copy()
                
                # Atualiza mapa visual de pontos
                valid_nodes = set(self.state.data_points['node_index'])
                self.state.map_points = self.state.map_points[
                    self.state.map_points['node_index'].isin(valid_nodes)
                ].copy()
                
                print(f"   Iter {iteration}: Removidas {count_removed} ruas sem saída.")
                
                changed = True