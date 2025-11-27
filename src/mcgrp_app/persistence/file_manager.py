# src\mcgrp_app\persistence\file_manager.py

import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import Dict, Optional
from shapely.geometry import LineString

from PySide6.QtCore import QObject, Signal

from ..core.utils import FieldsManager, GeoCalculator

class FileManager(QObject):
    """
    Gerencia a leitura (I/O) de arquivos geoespaciais.
    
    Usa Sinais (Signals) para comunicar o sucesso ou falha
    de volta para a GUI de forma assíncrona.
    """
    
    # Sinais que a GUI pode "ouvir"
    streets_loaded = Signal(gpd.GeoDataFrame, str) 
    neighborhoods_loaded = Signal(gpd.GeoDataFrame, str)
    error_occurred = Signal(str)

    TARGET_CRS = "EPSG:4326"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.streets_gdf = None
        self.neighborhoods_gdf = None

    def _check_and_convert_crs(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Garante que o GeoDataFrame esteja no CRS EPSG:4326.
        Tenta converter se for diferente, falha se não for possível.
        
        Retorna: O GeoDataFrame com o CRS correto.
        Levanta: ValueError se o CRS estiver ausente ou a conversão falhar.
        """
        if gdf.crs is None:
            raise ValueError("O arquivo não possui CRS (Sistema de Coordenadas) definido. Impossível validar.")
        
        # Compara usando a representação string
        if gdf.crs.to_string() == self.TARGET_CRS:
            print("CRS está correto (EPSG:4326).")
            return gdf
        
        print(f"CRS detectado: {gdf.crs.to_string()}. Tentando converter para {self.TARGET_CRS}...")
        try:
            gdf_converted = gdf.to_crs(self.TARGET_CRS)
            print("Conversão de CRS realizada com sucesso.")
            return gdf_converted
        except Exception as e:
            raise ValueError(f"Falha ao converter CRS de '{gdf.crs.to_string()}' para '{self.TARGET_CRS}'.\nErro: {e}")

    def load_geopackage_streets(self, file_path: str):
        """
        Carrega a camada de ruas de um GeoPackage.
        Validações:
        1. Deve conter APENAS geometrias 'LineString'.
        2. Deve ter/ser conversível para o CRS 'EPSG:4326'.
        """
        try:
            gdf = gpd.read_file(file_path)
            
            # 1. Validação de Geometria
            if gdf.empty:
                raise ValueError("O arquivo está vazio.")
            
            if not all(gdf.geom_type == 'LineString'):
                invalid_types = gdf[gdf.geom_type != 'LineString'].geom_type.unique()
                raise ValueError(f"O arquivo deve conter APENAS 'LineString'. Tipos inválidos encontrados: {invalid_types}")
            
            # 2. Validação e Conversão de CRS
            self.streets_gdf = self._check_and_convert_crs(gdf)
            
            print(f"GeoPackage de ruas carregado e validado: {file_path}")
            self.streets_loaded.emit(self.streets_gdf, file_path)
            
        except Exception as e:
            print(f"Erro ao carregar GeoPackage: {e}")
            self.error_occurred.emit(f"Erro ao carregar {file_path}:\n{e}")

    def load_gpkg_layer(self, file_path: str, layer_name: str) -> gpd.GeoDataFrame:
        """
        Carrega uma camada específica de um arquivo .gpkg.
        (Versão simplificada de 'load_geopackage_streets')
        """
        try:
            gdf = gpd.read_file(file_path, layer=layer_name)
            
            # Garante o CRS (por segurança)
            return self._check_and_convert_crs(gdf)
        except Exception as e:
            print(f"Erro ao carregar camada '{layer_name}' de '{file_path}': {e}")
            self.error_occurred.emit(f"Erro ao carregar {layer_name}: {e}")
            return None
    
    def load_shapefile_neighborhoods(self, file_path: str):
        """
        Carrega a camada de bairros de um Shapefile (via .zip ou .shp).
        Validações:
        1. Deve conter APENAS geometrias 'Polygon'.
        2. Deve ter/ser conversível para o CRS 'EPSG:4326'.
        """
        try:
            gdf = gpd.read_file(file_path)
            
            # 1. Validação de Geometria
            if gdf.empty:
                raise ValueError("O arquivo está vazio.")
            
            if not all(gdf.geom_type == 'Polygon'):
                invalid_types = gdf[gdf.geom_type != 'Polygon'].geom_type.unique()
                raise ValueError(f"O arquivo deve conter APENAS 'Polygon'. Tipos inválidos encontrados: {invalid_types}")

            # 2. Validação e Conversão de CRS
            self.neighborhoods_gdf = self._check_and_convert_crs(gdf)
            
            print(f"Shapefile de bairros carregado e validado: {file_path}")
            self.neighborhoods_loaded.emit(self.neighborhoods_gdf, file_path)
            
        except Exception as e:
            print(f"Erro ao carregar Shapefile: {e}")
            self.error_occurred.emit(f"Erro ao carregar {file_path}:\n{e}")

    def delete_gpkg_files(self, paths_dict: dict):
        """
        Deleta todos os arquivos .gpkg associados a uma execução.
        """
        print(f"Deletando arquivos GPKG: {paths_dict.values()}")
        for path_str in paths_dict.values():
            if not path_str: continue
            try:
                path_obj = Path(path_str)
                if path_obj.exists():
                    os.remove(path_obj)
                    print(f"  Arquivo deletado: {path_str}")
                else:
                    print(f"  Aviso: Arquivo não encontrado (já deletado?): {path_str}")
            except Exception as e:
                print(f"  Erro ao deletar arquivo {path_str}: {e}")
                self.error_occurred.emit(f"Erro ao deletar arquivo: {e}")
    
    @staticmethod
    def export_to_geopackage(datasets: Dict[str, Optional[gpd.GeoDataFrame]], output_file: str, field_config: Optional[Dict] = None) -> None:
        """
        Salva um dicionário de GeoDataFrames em um único arquivo GeoPackage,
        onde cada GDF (chave do dicionário) é uma camada separada.
        """
        output_filename = f"{output_file}.gpkg"
        print(f"Exportando para: {output_filename}")
        
        # Mapeamento de tipos
        pandas_dtypes = {}
        for col, py_type in FieldsManager._FIELD_TYPES.items():
            if py_type == int:
                pandas_dtypes[col] = 'Int64'
            elif py_type == float:
                pandas_dtypes[col] = 'float64'
            elif py_type == str:
                pandas_dtypes[col] = 'string'
                
        for layer_name, gdf in datasets.items():
            if gdf is None or gdf.empty:
                print(f"  Aviso: Camada '{layer_name}' está vazia ou nula. Pulando...")
                continue
            
            # Extraímos os dados brutos para criar um novo 
            # objeto na thread atual, desvinculado do CRS antigo.
            try:
                # Extrai dados como DataFrame Pandas (remove metadados Geo)
                df_raw = pd.DataFrame(gdf.drop(columns='geometry'))
                
                # Extrai geometria como lista/array de objetos Shapely
                geometry_raw = gdf.geometry.values
                
                # Constrói NOVO GeoDataFrame
                subset = gpd.GeoDataFrame(df_raw, geometry=geometry_raw, crs=GeoCalculator.BASE_CRS)
                
            except Exception as e:
                print(f"  Erro ao reconstruir GDF para exportação: {e}")
                continue
            
            # Força a tipagem
            existing_dtypes = {
                col: dtype for col, dtype in pandas_dtypes.items() if col in subset.columns
            }
            if existing_dtypes:
                try:
                    subset = subset.astype(existing_dtypes, errors='ignore')
                except Exception as e:
                    print(f"  Aviso: Falha ao forçar tipagem na camada '{layer_name}'. {e}")

            # Aplica field_config
            if field_config:
                geom_type = ""
                if "line" in layer_name.lower() or "street" in layer_name.lower():
                    geom_type = "LineString"
                elif "point" in layer_name.lower() or "node" in layer_name.lower():
                    geom_type = "Point"
                
                if geom_type in field_config:
                    fields = field_config[geom_type].copy()
                    if "geometry" not in fields:
                        fields.append("geometry")       # Garantir geometria
                    
                    # Mantém apenas os campos que existem no subset
                    valid_fields = [f for f in fields if f in subset.columns]
                    subset = subset[valid_fields]

            # Sanitização de geometria
            try:
                initial_count = len(subset)
                
                # Remove Nulos/Vazios
                subset = subset[subset.geometry.notna()]
                subset = subset[~subset.geometry.is_empty]
                
                # Make Valid
                subset['geometry'] = subset.geometry.make_valid()
                
                # Filtra Tipos Específicos
                if "line" in layer_name.lower() or "street" in layer_name.lower():
                    subset = subset[subset.geometry.apply(
                        lambda geom: isinstance(geom, LineString) and len(geom.coords) >= 2 and geom.length > 0
                    )]
                
                # is_valid final
                subset = subset[subset.geometry.is_valid]

                if len(subset) < initial_count:
                    print(f"  ALERTA: {initial_count - len(subset)} geometrias inválidas removidas de '{layer_name}'.")
            except Exception as e:
                print(f"  Erro na sanitização: {e}")
                continue
            
            # Exporta a camada
            try:
                # Salva a camada no arquivo GPKG
                subset.to_file(output_filename, layer=layer_name, driver="GPKG")
            except Exception as e:
                print(f"  Erro ao exportar camada '{layer_name}' para '{output_filename}': {e}")
        
        print(f"GeoPackage salvo em: {output_filename}")