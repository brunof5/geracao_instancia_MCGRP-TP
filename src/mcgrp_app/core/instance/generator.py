# src\mcgrp_app\core\instance\generator.py

from abc import ABC, abstractmethod
from typing import List, Dict, Union
from pathlib import Path

from ..utils import GraphState

class InstanceGenerator(ABC):
    """
    Classe base abstrata para geradores de instância de problemas de roteamento.
    """

    def __init__(self, state: GraphState):
        """Inicializa o gerador base com o estado atual do grafo."""
        self.state = state
        self.stats = {}

    @abstractmethod
    def generate_instance(self, output_path: Union[str, Path]) -> str:
        """Método abstrato para gerar o arquivo de instância específico."""
        pass

    @abstractmethod
    def _collect_statistics(self) -> Dict:
        """Método abstrato para coletar estatísticas específicas do grafo a partir dos GeoDataFrames."""
        pass

    def _save_instance(self, filepath: Union[str, Path], lines: List[str]) -> str:
        """Salva arquivo de instância com linhas formatadas."""
        # Garante que o diretório pai existe
        path_obj = Path(filepath)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        with open(path_obj, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        
        return str(path_obj)