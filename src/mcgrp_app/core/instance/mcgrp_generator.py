# src\mcgrp_app\core\instance\mcgrp_generator.py

import pandas as pd
from typing import List, Dict
from pathlib import Path

from .generator import InstanceGenerator

class MCGRPInstanceGenerator(InstanceGenerator):
    """Gerador de instâncias MCGRP."""

    DEFAULT_VEHICLE_COUNT = 1
    DEFAULT_CAPACITY = 3_600

    def generate_instance(self, instance_name: str, capacity: int = None, vehicle_count: int = None) -> str:
        """Gera arquivo de instância MCGRP."""
        if capacity is None: capacity = self.DEFAULT_CAPACITY
        if vehicle_count is None: vehicle_count = self.DEFAULT_VEHICLE_COUNT

        # Coletar estatísticas
        self.stats = self._collect_statistics()

        # Construir linhas da instância
        lines = self._build_header(instance_name, capacity, vehicle_count)
        lines.extend(self._build_required_nodes())
        lines.extend(self._build_required_edges())
        lines.extend(self._build_non_required_edges())
        lines.extend(self._build_required_arcs())
        lines.extend(self._build_non_required_arcs())

        # Salvar arquivo
        root_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
        output_dir = root_dir / "instancias"
        output_dir.mkdir(exist_ok=True, parents=True)       # garante que a pasta existe
        output_path = output_dir / f"{instance_name}.dat"

        return self._save_instance(output_path, lines)

    def _collect_statistics(self) -> Dict:
        """Coleta estatísticas completas dos DataFrames para formato MCGRP."""
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
            "non_req_edges": [],
            "non_req_arcs": []
        }

        # Helpers para extração de dados
        def get_val(row, attr, default=None):
            return getattr(row, attr, default)
        
        def safe_int(val):
            if pd.isna(val): return 0
            try:
                return int(val)
            except:
                return 0

        # Processar nós (Points)
        if self.state.map_points is not None and not self.state.map_points.empty:
            for row in self.state.map_points.itertuples():
                node_idx_raw = get_val(row, "node_index")
                if pd.isna(node_idx_raw): continue
                
                node_idx = int(node_idx_raw)
                stats["max_node"] = max(stats["max_node"], node_idx)

                # Verificar depósito
                if get_val(row, "depot") == "yes":
                    stats["depot_node"] = node_idx

                # Coletar nós requeridos (Depósito não entra aqui)
                if get_val(row, "eh_requerido") == "yes" and get_val(row, "depot") != "yes":
                    props = {
                        "node_index": node_idx,
                        "demanda": safe_int(get_val(row, "demanda")),
                        "custo_servico": safe_int(get_val(row, "custo_servico"))
                    }
                    stats["req_nodes"].append(props)
                    stats["total_service_cost"] += props["custo_servico"]
                    stats["total_demand"] += props["demanda"]

        # Processar ruas (Streets)
        if self.state.data_streets is not None and not self.state.data_streets.empty:
            for row in self.state.data_streets.itertuples():
                edge_idx = get_val(row, "edge_index")
                arc_idx = get_val(row, "arc_index")
                
                # Tratamento para Int64/Nullable
                is_edge = pd.notna(edge_idx) and edge_idx != -1
                is_arc = pd.notna(arc_idx) and arc_idx != -1
                
                if not is_edge and not is_arc:
                    continue

                # Propriedades comuns
                props = {
                    "from_node": safe_int(get_val(row, "from_node")),
                    "to_node": safe_int(get_val(row, "to_node")),
                    "custo_travessia": safe_int(get_val(row, "custo_travessia")),
                    "custo_servico": safe_int(get_val(row, "custo_servico")),
                    "demanda": safe_int(get_val(row, "demanda")),
                    "eh_requerido": get_val(row, "eh_requerido", "no")
                }

                if is_edge:
                    e_idx = int(edge_idx)
                    props["edge_index"] = e_idx
                    stats["max_edge"] = max(stats["max_edge"], e_idx)
                    
                    if props["eh_requerido"] == "yes":
                        stats["req_edges"].append(props)
                        stats["total_service_cost"] += props["custo_servico"]
                        stats["total_demand"] += props["demanda"]
                    else:
                        stats["non_req_edges"].append(props)

                elif is_arc:
                    a_idx = int(arc_idx)
                    props["arc_index"] = a_idx
                    stats["max_arc"] = max(stats["max_arc"], a_idx)
                    
                    if props["eh_requerido"] == "yes":
                        stats["req_arcs"].append(props)
                        stats["total_service_cost"] += props["custo_servico"]
                        stats["total_demand"] += props["demanda"]
                    else:
                        stats["non_req_arcs"].append(props)

        # Ordena as listas para garantir consistência e determinismo no arquivo
        stats["req_nodes"].sort(key=lambda x: x["node_index"])
        stats["req_edges"].sort(key=lambda x: x["edge_index"])
        stats["non_req_edges"].sort(key=lambda x: x["edge_index"])
        stats["req_arcs"].sort(key=lambda x: x["arc_index"])
        stats["non_req_arcs"].sort(key=lambda x: x["arc_index"])

        return stats

    def _build_header(self, name: str, capacity: int, vehicle_count: int) -> List[str]:
        """Constrói cabeçalho da instância MCGRP."""
        return [
            f"Name:\t\t{name}",
            f"Optimal value:\t-1",
            f"#Vehicles:\t{vehicle_count}",
            f"Capacity:\t{capacity}",
            f"Depot Node:\t{self.stats['depot_node']}",
            f"#Nodes:\t\t{self.stats['max_node']}",
            f"#Edges:\t\t{self.stats['max_edge']}",
            f"#Arcs:\t\t{self.stats['max_arc']}",
            f"#Required N:\t{len(self.stats['req_nodes'])}",
            f"#Required E:\t{len(self.stats['req_edges'])}",
            f"#Required A:\t{len(self.stats['req_arcs'])}",
            ""
        ]

    def _build_required_nodes(self) -> List[str]:
        """Constrói seção de nós requeridos."""
        lines = ["ReN.\tDEMAND\tS. COST"]
        if self.stats["req_nodes"]:
            for props in self.stats["req_nodes"]:
                lines.append(
                    f"N{props['node_index']}\t"
                    f"{props['demanda']}\t"
                    f"{props['custo_servico']}"
                )
        lines.append("")
        return lines

    def _build_required_edges(self) -> List[str]:
        """Constrói seção de arestas requeridas."""
        lines = ["ReE.\tFROM N.\tTO N.\tT. COST\tDEMAND\tS. COST"]
        if self.stats["req_edges"]:
            for props in self.stats["req_edges"]:
                lines.append(
                    f"E{props['edge_index']}\t"
                    f"{props['from_node']}\t"
                    f"{props['to_node']}\t"
                    f"{props['custo_travessia']}\t"
                    f"{props['demanda']}\t"
                    f"{props['custo_servico']}"
                )
        lines.append("")
        return lines

    def _build_non_required_edges(self) -> List[str]:
        """Constrói seção de arestas não requeridas."""
        lines = ["EDGE\tFROM N.\tTO N.\tT. COST"]
        if self.stats["non_req_edges"]:
            for props in self.stats["non_req_edges"]:
                lines.append(
                    f"NrE{props['edge_index']}\t"
                    f"{props['from_node']}\t"
                    f"{props['to_node']}\t"
                    f"{props['custo_travessia']}"
                )
        lines.append("")
        return lines

    def _build_required_arcs(self) -> List[str]:
        """Constrói seção de arcos requeridos."""
        lines = ["ReA.\tFROM N.\tTO N.\tT. COST\tDEMAND\tS. COST"]
        if self.stats["req_arcs"]:
            for props in self.stats["req_arcs"]:
                lines.append(
                    f"A{props['arc_index']}\t"
                    f"{props['from_node']}\t"
                    f"{props['to_node']}\t"
                    f"{props['custo_travessia']}\t"
                    f"{props['demanda']}\t"
                    f"{props['custo_servico']}"
                )
        lines.append("")
        return lines

    def _build_non_required_arcs(self) -> List[str]:
        """Constrói seção de arcos não requeridos."""
        lines = ["ARC\tFROM N.\tTO N.\tT. COST"]
        if self.stats["non_req_arcs"]:
            for props in self.stats["non_req_arcs"]:
                lines.append(
                    f"NrA{props['arc_index']}\t"
                    f"{props['from_node']}\t"
                    f"{props['to_node']}\t"
                    f"{props['custo_travessia']}"
                )
        return lines