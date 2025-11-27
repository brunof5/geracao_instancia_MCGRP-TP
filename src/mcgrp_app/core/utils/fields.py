# src\mcgrp_app\core\utils\fields.py

from enum import Enum
from typing import TYPE_CHECKING

# Para evitar importação circular
if TYPE_CHECKING:
    from geopandas import GeoDataFrame

class FieldConfigType(Enum):
    """
    Enum para tipos de configuração de campos.
    """
    BASIC = 1
    EXTENDED = 2

class FieldsManager:
    """
    Classe utilitária para gerenciar valores padrão e configurações de campos.
    """
    # Campos comuns a todos os tipos de geometria
    _COMMON = ["name", "alt_name", "id_bairro", "bairro"]

    # Atributos para geometrias do tipo LineString
    _LINE_FIELDS = ["id", "total_dist", "osm_id", "osm_type", "highway", "maxspeed", "oneway", "lanes", "surface"]
    _LINE_EXTRA = ["edge_index", "arc_index", "from_node", "to_node", "custo_travessia", "eh_requerido", "demanda", "custo_servico", "total_dist_fmt", "tooltip_html"]

    # Atributos para geometrias do tipo Point
    _POINT_FIELDS = ["from_line_id", "vertex_index", "distance", "vertex_to", "angle", "angle_inv", "eh_unido", "eh_extremidade"]
    _POINT_EXTRA = ["node_index", "eh_requerido", "demanda", "custo_servico", "depot", "tooltip_html"]
    
    # Schema de tipos por campo
    _FIELD_TYPES = {
        # Campos comuns
        "name": str,
        "alt_name": str,
        "id_bairro": int,
        "bairro": str,

        # Campos de LineString
        "id": int,
        "total_dist": float,
        "osm_id": str,
        "maxspeed": str,
        "edge_index": int,
        "arc_index": int,
        "from_node": int,
        "to_node": int,
        "custo_travessia": int,
        "demanda": int,
        "custo_servico": int,
        "total_dist_fmt": str,
        "tooltip_html": str,

        # Campos de Point
        "from_line_id": int,
        "vertex_index": int,
        "distance": float,
        "vertex_to": int,
        "angle": float,
        "angle_inv": float,
        "node_index": int
    }
    
    # Dicionário para valores padrão
    _FIELD_DEFAULTS = {
        # Colunas de estado
        "eh_requerido": 'no',
        "depot": 'no',

        # Colunas de demanda/custo
        "demanda": 0,
        "custo_servico": 0,
        "custo_travessia": 0,
        
        # Colunas de índice
        "edge_index": None,
        "arc_index": None,
        "from_node": None,
        "to_node": None,
        "node_index": None,
        "from_line_id": None,
        "vertex_index": None,
        "vertex_to": None,
        "id_bairro": None,

        # Colunas do pipeline
        "distance": 0.0,
        "angle": 0.0,
        "angle_inv": 0.0,
        "eh_unido": 'no',
        "eh_extremidade": 'no',
        
        # Colunas de formatação
        "total_dist_fmt": None,
        "tooltip_html": None,

        # Colunas comuns
        "name": None,
        "alt_name": None,
        "bairro": None
    }

    # --- Métodos de Configuração (Privados) ---

    @classmethod
    def _linestring_basic(cls):
        return cls._LINE_FIELDS + cls._COMMON

    @classmethod
    def _linestring_extended(cls):
        return cls._linestring_basic() + cls._LINE_EXTRA

    @classmethod
    def _point_basic(cls):
        return cls._POINT_FIELDS + cls._COMMON

    @classmethod
    def _point_extended(cls):
        return cls._point_basic() + cls._POINT_EXTRA
    
    # --- Métodos Públicos que Obtém o Schema de Tipos ---

    @classmethod
    def get_point_basic_fields(cls) -> dict:
        """Retorna campos básicos de Point com seus tipos."""
        fields = cls._point_basic()
        return {f: cls._FIELD_TYPES.get(f, object) for f in fields}

    @classmethod
    def get_point_extended_fields(cls) -> dict:
        """Retorna campos estendidos de Point com seus tipos."""
        fields = cls._point_extended()
        return {f: cls._FIELD_TYPES.get(f, object) for f in fields}

    @classmethod
    def get_linestring_basic_fields(cls) -> dict:
        """Retorna campos básicos de LineString com seus tipos."""
        fields = cls._linestring_basic()
        return {f: cls._FIELD_TYPES.get(f, object) for f in fields}

    @classmethod
    def get_linestring_extended_fields(cls) -> dict:
        """Retorna campos estendidos de LineString com seus tipos."""
        fields = cls._linestring_extended()
        return {f: cls._FIELD_TYPES.get(f, object) for f in fields}
    
    # --- Outro ---
    
    @classmethod
    def get_field_config(cls, config_type: FieldConfigType) -> dict:
        """
        Retorna configuração de campos baseada no tipo especificado.
        """
        if config_type == FieldConfigType.BASIC:
            return {
                "Point": cls._point_basic(),
                "LineString": cls._linestring_basic()
            }
        else:       # FieldConfigType.EXTENDED
            return {
                "Point": cls._point_extended(),
                "LineString": cls._linestring_extended()
            }
    
    @classmethod
    def ensure_fields_exist(cls, gdf: "GeoDataFrame", config_type: FieldConfigType) -> "GeoDataFrame":
        """
        Garante que o GeoDataFrame possua todos os campos esperados
        (básicos ou estendidos), criando colunas ausentes com valor None.
        """

        if gdf is None or len(gdf) == 0:
            return gdf

        # Define os conjuntos de campos esperados conforme configuração
        field_config = cls.get_field_config(config_type)

        # Determina quais geometrias existem no GDF
        geom_types_present = set(gdf.geometry.geom_type.unique())

        supported_types = {"Point", "LineString"}
        invalid_types = geom_types_present - supported_types

        if invalid_types:
            raise ValueError(f"Geometrias não suportadas encontradas: {invalid_types}")

        # Junta todos os campos necessários para os tipos presentes
        expected_fields = set()
        for gt in geom_types_present:
            expected_fields.update(field_config[gt])

        # Adiciona campos faltantes com valores padrão
        for field in expected_fields:
            if field not in gdf.columns:
                default_value = cls._FIELD_DEFAULTS.get(field, None)
                gdf[field] = default_value

        return gdf