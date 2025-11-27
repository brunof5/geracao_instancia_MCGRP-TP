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
        output_path = output_dir / f"{instance_name}.dat"

        return self._save_instance(output_path, lines)

    def _collect_statistics(self) -> Dict:
        """Coleta estatísticas completas dos GeoDataFrames para formato MCGRP."""
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

        # Processar nós (Points)
        if self.state.map_points is not None:
            for _, row in self.state.map_points.iterrows():
                props = row.to_dict()
                
                node_idx = int(props.get("node_index", 0))
                stats["max_node"] = max(stats["max_node"], node_idx)

                # Verificar depósito
                if props.get("depot") == "yes":
                    stats["depot_node"] = node_idx

                # Coletar nós requeridos
                # Nota: Depósito nunca é requerido como tarefa, mas verificamos 'eh_requerido'
                if props.get("eh_requerido") == "yes" and props.get("depot") != "yes":
                    stats["req_nodes"].append(props)
                    stats["total_service_cost"] += int(props.get("custo_servico", 0))
                    stats["total_demand"] += int(props.get("demanda", 0))

        # Processar ruas (Streets)
        if self.state.data_streets is not None:
            for _, row in self.state.data_streets.iterrows():
                props = row.to_dict()
                
                edge_idx = props.get("edge_index")
                arc_idx = props.get("arc_index")
                
                # Tratamento de NaN/None para índices
                is_edge = pd.notna(edge_idx) and int(edge_idx) != -1
                is_arc = pd.notna(arc_idx) and int(arc_idx) != -1

                if is_edge:
                    e_idx = int(edge_idx)
                    stats["max_edge"] = max(stats["max_edge"], e_idx)
                    if props.get("eh_requerido") == "yes":
                        stats["req_edges"].append(props)
                        stats["total_service_cost"] += int(props.get("custo_servico", 0))
                        stats["total_demand"] += int(props.get("demanda", 0))
                    else:
                        stats["non_req_edges"].append(props)

                elif is_arc:
                    a_idx = int(arc_idx)
                    stats["max_arc"] = max(stats["max_arc"], a_idx)
                    if props.get("eh_requerido") == "yes":
                        stats["req_arcs"].append(props)
                        stats["total_service_cost"] += int(props.get("custo_servico", 0))
                        stats["total_demand"] += int(props.get("demanda", 0))
                    else:
                        stats["non_req_arcs"].append(props)

        # Ordena as listas para garantir consistência no arquivo
        stats["req_nodes"].sort(key=lambda x: int(x["node_index"]))
        stats["req_edges"].sort(key=lambda x: int(x["edge_index"]))
        stats["non_req_edges"].sort(key=lambda x: int(x["edge_index"]))
        stats["req_arcs"].sort(key=lambda x: int(x["arc_index"]))
        stats["non_req_arcs"].sort(key=lambda x: int(x["arc_index"]))

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
                    f"N{int(props['node_index'])}\t"
                    f"{int(props.get('demanda', 0))}\t"
                    f"{int(props.get('custo_servico', 0))}"
                )
        lines.append("")
        return lines

    def _build_required_edges(self) -> List[str]:
        """Constrói seção de arestas requeridas."""
        lines = ["ReE.\tFROM N.\tTO N.\tT. COST\tDEMAND\tS. COST"]
        if self.stats["req_edges"]:
            for props in self.stats["req_edges"]:
                lines.append(
                    f"E{int(props['edge_index'])}\t"
                    f"{int(props['from_node'])}\t"
                    f"{int(props['to_node'])}\t"
                    f"{int(props.get('custo_travessia', 0))}\t"
                    f"{int(props.get('demanda', 0))}\t"
                    f"{int(props.get('custo_servico', 0))}"
                )
        lines.append("")
        return lines

    def _build_non_required_edges(self) -> List[str]:
        """Constrói seção de arestas não requeridas."""
        lines = ["EDGE\tFROM N.\tTO N.\tT. COST"]
        if self.stats["non_req_edges"]:
            for props in self.stats["non_req_edges"]:
                lines.append(
                    f"NrE{int(props['edge_index'])}\t"
                    f"{int(props['from_node'])}\t"
                    f"{int(props['to_node'])}\t"
                    f"{int(props.get('custo_travessia', 0))}"
                )
        lines.append("")
        return lines

    def _build_required_arcs(self) -> List[str]:
        """Constrói seção de arcos requeridos."""
        lines = ["ReA.\tFROM N.\tTO N.\tT. COST\tDEMAND\tS. COST"]
        if self.stats["req_arcs"]:
            for props in self.stats["req_arcs"]:
                lines.append(
                    f"A{int(props['arc_index'])}\t"
                    f"{int(props['from_node'])}\t"
                    f"{int(props['to_node'])}\t"
                    f"{int(props.get('custo_travessia', 0))}\t"
                    f"{int(props.get('demanda', 0))}\t"
                    f"{int(props.get('custo_servico', 0))}"
                )
        lines.append("")
        return lines

    def _build_non_required_arcs(self) -> List[str]:
        """Constrói seção de arcos não requeridos."""
        lines = ["ARC\tFROM N.\tTO N.\tT. COST"]
        if self.stats["non_req_arcs"]:
            for props in self.stats["non_req_arcs"]:
                lines.append(
                    f"NrA{int(props['arc_index'])}\t"
                    f"{int(props['from_node'])}\t"
                    f"{int(props['to_node'])}\t"
                    f"{int(props.get('custo_travessia', 0))}"
                )
        return lines