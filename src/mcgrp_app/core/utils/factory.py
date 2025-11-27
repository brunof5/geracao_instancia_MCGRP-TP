# src\mcgrp_app\core\utils\factory.py

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry

class GeoFactory:
    """
    Fábrica estática responsável por converter DataFrames (Pandas) 
    em GeoDataFrames (GeoPandas) apenas quando necessário para cálculos.
    """
    
    DEFAULT_CRS = "EPSG:4326"

    @staticmethod
    def create_gdf(data: list[dict], geometry_col: str = 'geometry') -> gpd.GeoDataFrame:
        if not data:
            # Retorna GDF vazio
            return gpd.GeoDataFrame(
                columns=[geometry_col], 
                geometry=geometry_col, 
                crs=GeoFactory.DEFAULT_CRS
            )

        # Cria DataFrame Pandas Puro
        df = pd.DataFrame(data)

        # Validação de Geometria
        if geometry_col not in df.columns:
            raise ValueError(f"Coluna {geometry_col} ausente.")
        
        # Remove Nulos e Tipos Errados
        valid_mask = df[geometry_col].notna() & \
                     df[geometry_col].apply(lambda x: isinstance(x, BaseGeometry))
        
        df_clean = df[valid_mask].copy()

        # Cria a GeoSeries explicitamente
        try:
            gs = gpd.GeoSeries(df_clean[geometry_col])
            gdf = gpd.GeoDataFrame(df_clean.drop(columns=[geometry_col]), geometry=gs)
            gdf.crs = GeoFactory.DEFAULT_CRS

            return gdf
        except Exception as e:
            raise RuntimeError(f"Falha fatal na fábrica de GDF: {e}")
        
    @staticmethod
    def create_empty_gdf(crs: str = DEFAULT_CRS) -> gpd.GeoDataFrame:
        """Cria um GDF vazio com o CRS configurado."""
        gdf = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry')
        gdf.crs = crs
        return gdf
    
    @staticmethod
    def to_gdf(df: pd.DataFrame, crs: str = DEFAULT_CRS) -> gpd.GeoDataFrame:
        """
        Converte um DataFrame Pandas (com coluna 'geometry' contendo objetos Shapely)
        em um GeoDataFrame.
        """
        if df is None:
            return None
            
        if df.empty:
            # Retorna GDF vazio
            return gpd.GeoDataFrame(df, geometry='geometry')        # Sem CRS
        
        local_df = df.copy()
        
        # Validação Básica
        if 'geometry' not in local_df.columns:
            raise ValueError("DataFrame não possui coluna 'geometry'.")
            
        # Sanitização (Remove Nulos e Não-Shapely)
        valid_mask = local_df['geometry'].notna() & \
                     local_df['geometry'].apply(lambda x: isinstance(x, BaseGeometry))
        
        if not valid_mask.all():
            invalid_count = len(local_df) - valid_mask.sum()
            print(f"GeoFactory: Removendo {invalid_count} geometrias inválidas/nulas.")
            local_df = local_df[valid_mask]

        try:
            geo_series = gpd.GeoSeries(local_df['geometry'])
            df_data = local_df.drop(columns=['geometry'])
            gdf = gpd.GeoDataFrame(df_data, geometry=geo_series)
            gdf.crs = crs
            
            return gdf
            
        except Exception as e:
            print(f"GeoFactory CRÍTICO: Erro ao criar GeoDataFrame: {e}")
            return gpd.GeoDataFrame(local_df, geometry='geometry')

    @staticmethod
    def from_gdf(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Converte um GeoDataFrame para um DataFrame Pandas.
        """
        if gdf is None:
            return None
            
        return pd.DataFrame(gdf)