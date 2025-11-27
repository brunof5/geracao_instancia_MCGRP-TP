# src\mcgrp_app\core\instance\mcgrptp_generator.py

import pandas as pd
from typing import List, Dict, Optional
from collections import defaultdict
from pathlib import Path

from .generator import InstanceGenerator

class MCGRPTPInstanceGenerator(InstanceGenerator):
    """Gerador de instâncias MCGRP-TP (com Turn Penalties)."""

    DEFAULT_VEHICLE_COUNT = 1
    DEFAULT_CAPACITY = 1000
    
    # Custos de Penalidade (segundos)
    UTURN_PENALTY = 60
    DEFAULT_TURN_PENALTY = 0
    DEFAULT_TURN_PENALTY_LEFT_RIGHT = 4
    HARD_TURN_PENALTY_LEFT_RIGHT = 8

    def __init__(self, state):
        """Inicializa o gerador MCGRP-TP."""
        super().__init__(state)
        self.turns = []
        self.node_adjacencies = {}
        self.edge_angles = {}                   # {(from_node, to_node): angle}
        self.edge_angles_inv = {}               # {(from_node, to_node): angle_inv}
        self.line_features_by_nodes = {}        # {(from_node, to_node): row_dict}

    def generate_instance(self, instance_name: str, capacity: int = None, vehicle_count: int = None) -> str:
        """Gera arquivo de instância MCGRP-TP."""
        if capacity is None: capacity = self.DEFAULT_CAPACITY
        if vehicle_count is None: vehicle_count = self.DEFAULT_VEHICLE_COUNT

        # Coletar estatísticas
        self.stats = self._collect_statistics()

        # Gerar turns
        self._generate_turns()

        # Construir linhas
        lines = self._build_header(instance_name, capacity, vehicle_count)
        lines.extend(self._build_nodes())
        lines.extend(self._build_edges())
        lines.extend(self._build_arcs())
        lines.extend(self._build_turns())

        # Salvar arquivo
        root_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
        output_dir = root_dir / "instancias"
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / f"{instance_name}-TP.dat"

        return self._save_instance(output_path, lines)

    def _collect_statistics(self) -> Dict:
        """Coleta estatísticas específicas para formato MCGRP-TP."""
        stats = {
            "total_service_cost": 0,
            "total_demand": 0,
            "depot_node": 0,
            "max_node": 0,
            "max_edge": 0,
            "max_arc": 0,
            "req_nodes": [],
            "req_edges": [],
            "req_arcs": [],
            "nodes": [],
            "edges": [],
            "arcs": []
        }

        def get_val(row, attr, default=None):
            return getattr(row, attr, default)
        
        def safe_int(val):
            if pd.isna(val): return 0
            try: return int(val)
            except: return 0

        # Nós
        if self.state.map_points is not None and not self.state.map_points.empty:
            sorted_nodes = self.state.map_points.sort_values('node_index')
            
            for row in sorted_nodes.itertuples():
                node_idx_raw = get_val(row, "node_index")
                if pd.isna(node_idx_raw): continue
                
                node_idx = int(node_idx_raw)
                
                props = {
                    "node_index": node_idx,
                    "depot": get_val(row, "depot"),
                    "eh_requerido": get_val(row, "eh_requerido"),
                    "custo_servico": safe_int(get_val(row, "custo_servico")),
                    "demanda": safe_int(get_val(row, "demanda"))
                }

                stats["max_node"] = max(stats["max_node"], node_idx)
                stats["nodes"].append(props)

                if props["depot"] == "yes":
                    stats["depot_node"] = node_idx

                if props["eh_requerido"] == "yes" and props["depot"] != "yes":
                    stats["req_nodes"].append(props)
                    stats["total_service_cost"] += props["custo_servico"]
                    stats["total_demand"] += props["demanda"]

        # Ruas
        if self.state.data_streets is not None and not self.state.data_streets.empty:
            sorted_streets = self.state.data_streets.sort_values('id')
            
            for row in sorted_streets.itertuples():
                edge_idx = get_val(row, "edge_index")
                arc_idx = get_val(row, "arc_index")
                
                is_edge = pd.notna(edge_idx) and edge_idx != -1
                is_arc = pd.notna(arc_idx) and arc_idx != -1

                if not is_edge and not is_arc: continue

                props = {
                    "edge_index": safe_int(edge_idx) if is_edge else None,
                    "arc_index": safe_int(arc_idx) if is_arc else None,
                    "from_node": safe_int(get_val(row, "from_node")),
                    "to_node": safe_int(get_val(row, "to_node")),
                    "custo_travessia": safe_int(get_val(row, "custo_travessia")),
                    "custo_servico": safe_int(get_val(row, "custo_servico")),
                    "demanda": safe_int(get_val(row, "demanda")),
                    "eh_requerido": get_val(row, "eh_requerido", "no")
                }

                if is_edge:
                    e_idx = props["edge_index"]
                    stats["max_edge"] = max(stats["max_edge"], e_idx)
                    stats["edges"].append(props)
                    if props["eh_requerido"] == "yes":
                        stats["req_edges"].append(props)
                        stats["total_service_cost"] += props["custo_servico"]
                        stats["total_demand"] += props["demanda"]

                elif is_arc:
                    a_idx = props["arc_index"]
                    stats["max_arc"] = max(stats["max_arc"], a_idx)
                    stats["arcs"].append(props)
                    if props["eh_requerido"] == "yes":
                        stats["req_arcs"].append(props)
                        stats["total_service_cost"] += props["custo_servico"]
                        stats["total_demand"] += props["demanda"]

        return stats

    # --- CÁLCULO DE TURN PENALTIES ---

    def _generate_turns(self):
        """Gera todos os turns (triplets i, j, k)."""
        self._preprocess_data_structures()
        self._build_adjacencies()
        self._generate_and_process_triplets()

    def _preprocess_data_structures(self):
        """Pré-processa estruturas auxiliares a partir dos DataFrames."""
        
        def get_val(row, attr, default=None):
            return getattr(row, attr, default)
        
        # Indexar Ruas por Nós
        if self.state.data_streets is not None and not self.state.data_streets.empty:
            for row in self.state.data_streets.itertuples():
                props = {
                    "from_node": get_val(row, "from_node"),
                    "to_node": get_val(row, "to_node"),
                    "edge_index": get_val(row, "edge_index")
                }
                
                u = int(props["from_node"])
                v = int(props["to_node"])
                
                self.line_features_by_nodes[(u, v)] = props
                
                # Se for aresta (bidirecional), indexa o inverso também
                edge_idx = props.get("edge_index")
                if pd.notna(edge_idx) and int(edge_idx) != -1:
                    self.line_features_by_nodes[(v, u)] = props

        # Indexar Ângulos
        if self.state.data_points is not None and not self.state.data_points.empty:
            # Filtra pontos relevantes
            candidates = self.state.data_points[
                self.state.data_points['vertex_index'].isin([0, 0.0, -1, -1.0])
            ]
            
            if not candidates.empty:
                # Mapa local line_id -> (u, v)
                line_to_nodes = dict(zip(self.state.data_streets['id'], zip(self.state.data_streets['from_node'], self.state.data_streets['to_node'])))

                for row in candidates.itertuples():
                    v_idx = int(get_val(row, 'vertex_index', -999))
                    angle = get_val(row, 'angle')
                    angle_inv = get_val(row, 'angle_inv')
                    
                    # Validação de ângulos para nós inseridos (-1)
                    if v_idx == -1:
                        is_angle_valid = (angle is not None and not pd.isna(angle) and float(angle) != 0.0)
                        is_angle_inv_valid = (angle_inv is not None and not pd.isna(angle_inv) and float(angle_inv) != 0.0)
                        if not (is_angle_valid or is_angle_inv_valid):
                            continue

                    line_id = get_val(row, 'from_line_id')
                    if line_id not in line_to_nodes: continue
                    
                    u, v = line_to_nodes[line_id]
                    u, v = int(u), int(v)

                    if pd.notna(angle):
                        self.edge_angles[(u, v)] = float(angle)
                    if pd.notna(angle_inv):
                        self.edge_angles_inv[(u, v)] = float(angle_inv)

    def _get_precomputed_angle(self, from_node: int, to_node: int) -> Optional[float]:
        """Obtém o azimute da rua saindo de from_node em direção a to_node."""
        
        # Caso 1: A rua existe na direção from->to
        if (from_node, to_node) in self.line_features_by_nodes:
            feat = self.line_features_by_nodes[(from_node, to_node)]
            
            # Se a definição original é from->to, usamos 'angle'
            if int(feat["from_node"]) == from_node and int(feat["to_node"]) == to_node:
                return self.edge_angles.get((from_node, to_node))
            
            # Se a definição original é to->from, usamos o ângulo 'angle_inv'
            elif int(feat["from_node"]) == to_node and int(feat["to_node"]) == from_node:
                return self.edge_angles_inv.get((to_node, from_node))
            
        # Caso 2: A rua existe na direção to->from
        elif (to_node, from_node) in self.line_features_by_nodes:
            feat = self.line_features_by_nodes[(to_node, from_node)]

            # Se é aresta, podemos transitar no sentido inverso
            if feat.get("edge_index") is not None and pd.notna(feat.get("edge_index")) and feat.get("edge_index") != -1:
                return self.edge_angles_inv.get((from_node, to_node))
        
        return None

    def _build_adjacencies(self):
        """Constrói grafo de adjacência."""
        self.node_adjacencies = defaultdict(set)
        
        for (u, v), props in self.line_features_by_nodes.items():
            self.node_adjacencies[u].add(v)

    def _generate_and_process_triplets(self):
        """Gera os movimentos i -> j -> k e calcula penalidades."""
        depot_node = self.stats["depot_node"]
        triplets_data = []

        for i in self.node_adjacencies:
            neighbors_j = list(self.node_adjacencies[i])
            
            for j in neighbors_j:
                neighbors_k = list(self.node_adjacencies.get(j, []))
                
                for k in neighbors_k:
                    # Lógica de Custo
                    if j == depot_node:
                        turn_type, turn_cost = 'O', 0
                    elif i == k:
                        # U-Turn (retorno)
                        turn_type, turn_cost = 'U', self.UTURN_PENALTY
                    else:
                        # Ângulo de chegada (i->j)
                        angle_in = self._get_precomputed_angle(i, j)
                        # Ângulo de saída (j->k)
                        angle_out = self._get_precomputed_angle(j, k)
                        
                        if angle_in is None or angle_out is None:
                            turn_type, turn_cost = 'F', self.DEFAULT_TURN_PENALTY
                        else:
                            # Diferença de ângulo (Azimute de saída - Azimute de entrada)
                            diff = angle_out - angle_in
                            if diff < 0: diff += 360
                            
                            # Classificação baseada na diferença
                            if diff == 180:
                                turn_type, turn_cost = 'U', self.UTURN_PENALTY
                            elif (330 <= diff) or (diff <= 30):
                                # Frente (quase reto)
                                turn_type, turn_cost = 'F', self.DEFAULT_TURN_PENALTY
                            elif 30 < diff <= 135:
                                # Direita (entre suave e acentuada)
                                turn_type, turn_cost = 'R', self.DEFAULT_TURN_PENALTY_LEFT_RIGHT
                            elif 135 < diff < 180:
                                # Direita (bem acentuada)
                                turn_type, turn_cost = 'R', self.HARD_TURN_PENALTY_LEFT_RIGHT
                            elif 180 < diff < 225:
                                # Esquerda (bem acentuada)
                                turn_type, turn_cost = 'L', self.HARD_TURN_PENALTY_LEFT_RIGHT
                            elif 225 <= diff < 330:
                                # Esquerda (entre suave e acentuada)
                                turn_type, turn_cost = 'L', self.DEFAULT_TURN_PENALTY_LEFT_RIGHT
                            else:
                                turn_type, turn_cost = 'F', self.DEFAULT_TURN_PENALTY

                    triplets_data.append({
                        'i': i, 'j': j, 'k': k,
                        'cost': turn_cost, 'type': turn_type
                    })
        
        self.turns = triplets_data

    # --- FORMATAÇÃO ---

    def _build_header(self, name: str, capacity: int, vehicle_count: int) -> List[str]:
        max_node = self.stats['max_node']
        max_edge = self.stats['max_edge']
        max_arc = self.stats['max_arc']

        req_nodes = len(self.stats['req_nodes'])
        req_edges = len(self.stats['req_edges'])
        req_arcs = len(self.stats['req_arcs'])

        return [
            f"Name:\t\t\t{name}",
            f"#Vehicles:\t\t{vehicle_count}",
            f"Capacity:\t\t{capacity}",
            f"Depot:\t\t\t{self.stats['depot_node']}",
            f"#Nodes:\t\t\t{max_node}",
            f"#Edges:\t\t\t{max_edge}",
            f"#Arcs:\t\t\t{max_arc}",
            f"#Required-N:\t{req_nodes}",
            f"#Required-E:\t{req_edges}",
            f"#Required-A:\t{req_arcs}",
            f"#Nb-Turns:\t\t{len(self.turns)}",
            ""
        ]

    def _build_nodes(self) -> List[str]:
        lines = ["----------NODES----------", "INDEX\tQTY\tIS-REQUIRED\tX\tY"]
        
        for props in self.stats['nodes']:
            idx = props["node_index"]
            qty = int(props.get('custo_servico', 0))
            is_req = 1 if props.get('eh_requerido') == 'yes' and props.get('depot') != 'yes' else 0
            
            # X e Y não são usados
            lines.append(f"{idx}\t{qty}\t{is_req}\t-1\t-1")
            
        lines.append("")
        return lines

    def _build_edges(self) -> List[str]:
        lines = ["----------EDGES----------", "INDEX-I\tINDEX-J\tQTY\tIS-REQUIRED\tTR-COST"]
        
        for props in self.stats['edges']:
            u = int(props['from_node'])
            v = int(props['to_node'])
            qty = int(props.get('custo_servico', 0))
            is_req = 1 if props.get('eh_requerido') == 'yes' else 0
            cost = int(props.get('custo_travessia', 0))
            
            lines.append(f"{u}\t{v}\t{qty}\t{is_req}\t{cost}")
            
        lines.append("")
        return lines

    def _build_arcs(self) -> List[str]:
        lines = ["-----------ARCS----------", "INDEX-I\tINDEX-J\tQTY\tIS-REQUIRED\tTR-COST"]
        
        for props in self.stats['arcs']:
            u = int(props['from_node'])
            v = int(props['to_node'])
            qty = int(props.get('custo_servico', 0))
            is_req = 1 if props.get('eh_requerido') == 'yes' else 0
            cost = int(props.get('custo_travessia', 0))
            
            lines.append(f"{u}\t{v}\t{qty}\t{is_req}\t{cost}")
            
        lines.append("")
        return lines

    def _build_turns(self) -> List[str]:
        lines = ["----------TURNS----------", "INDEX-I\tINDEX-J\tINDEX-K\tCOST\tTYPE"]
        
        # Ordena: i, j, k
        sorted_turns = sorted(self.turns, key=lambda t: (t['i'], t['j'], t['k']))
        
        for t in sorted_turns:
            lines.append(f"{t['i']}\t{t['j']}\t{t['k']}\t{t['cost']}\t{t['type']}")
            
        return lines