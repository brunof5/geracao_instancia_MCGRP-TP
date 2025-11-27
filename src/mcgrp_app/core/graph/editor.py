# src\mcgrp_app\core\editing.py

from typing import Any, Dict, List
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely.geometry.base import BaseGeometry

from ..utils import FieldConfigType, FieldsManager, GeoCalculator, GraphState

class GraphEditor:
    """
    Contém lógica para modificar interativamente o GraphState,
    como dividir ruas e recalcular métricas.
    """

    def __init__(self):
        pass

    def _sanitize_and_build_gdf(self, df: pd.DataFrame, crs: Any) -> gpd.GeoDataFrame:
        """
        Reconstrói um GeoDataFrame de forma segura.
        """
        if df.empty:
            gdf = gpd.GeoDataFrame(df, geometry='geometry')
            gdf.crs = crs
            return gdf

        df = df.copy()

        if 'geometry' not in df.columns:
            raise ValueError("DataFrame não possui coluna 'geometry'.")

        # Limpeza Bruta
        df = df[df['geometry'].notna()]

        # Validação de Tipo
        valid_geom_mask = df['geometry'].apply(lambda x: isinstance(x, BaseGeometry))
        if not valid_geom_mask.all():
            print(f"  AVISO: Removendo {len(df) - valid_geom_mask.sum()} geometrias inválidas.")
            df = df[valid_geom_mask]

        # Reconstrução Segura
        try:
            # Tenta criar GeoSeries sem CRS
            geo_series = gpd.GeoSeries(df['geometry'])
            df_no_geom = df.drop(columns=['geometry'])
            
            # Cria GDF sem CRS
            gdf = gpd.GeoDataFrame(df_no_geom, geometry=geo_series)
            
            # Atribui CRS com segurança
            gdf.crs = crs
            return gdf

        except Exception:
            # Fallback WKT
            try:
                wkt_series = df['geometry'].apply(lambda x: x.wkt)
                geo_series = gpd.GeoSeries.from_wkt(wkt_series)
                df_no_geom = df.drop(columns=['geometry'])
                gdf = gpd.GeoDataFrame(df_no_geom, geometry=geo_series)
                gdf.crs = crs
                return gdf
            except Exception as e:
                print(f"  ERRO CRÍTICO na reconstrução do GDF: {e}")
                # Último recurso
                gdf = gpd.GeoDataFrame(df, geometry='geometry')
                gdf.crs = crs
                return gdf
    
    def _align_dataframe_structure(self, new_df: pd.DataFrame, template_df: pd.DataFrame) -> pd.DataFrame:
        """
        Garante que new_df tenha as mesmas colunas e tipos compatíveis com template_df
        antes da concatenação.
        """
        # Garante que todas as colunas do template existam no novo
        for col in template_df.columns:
            if col not in new_df.columns:
                new_df[col] = None # Ou pd.NA
        
        # Ordena colunas para igualar ao template
        common_cols = [c for c in template_df.columns if c in new_df.columns]
        new_df = new_df[common_cols].copy()

        # Tenta alinhar tipos para colunas que são totalmente nulas/NA no new_df
        for col in new_df.columns:
            if new_df[col].isna().all() and not template_df[col].isna().all():
                try:
                    # Tenta castar a coluna de Nones para o tipo da coluna original (ex: float64, Int64)
                    new_df[col] = new_df[col].astype(template_df[col].dtype)
                except Exception:
                    pass        # Se falhar, deixa como object/None
        
        return new_df
    
    def _preformat_street_tooltip(self, row_dict: dict) -> str:
        """Helper para formatar o tooltip de uma rua."""
        try:
            oneway = (row_dict.get('oneway', 'no') or 'no').lower()
            if oneway in ['yes', '1', 'true']:
                header = f"<b>Arco:</b> {int(row_dict['arc_index'])} (De: {int(row_dict['from_node'])}, Para: {int(row_dict['to_node'])})"
            else:
                header = f"<b>Aresta:</b> {int(row_dict['edge_index'])}"
            
            rua = row_dict.get('name', 'desconhecida')
            bairro = row_dict.get('bairro', 'N/A')

            dist_km = row_dict.get('total_dist', 0.0)
            dist_fmt = f"{dist_km:.3f} km"
            row_dict['total_dist_fmt'] = dist_fmt

            custo_val = int(row_dict.get('custo_travessia'))
            custo = f"{custo_val} s" if pd.notna(custo_val) and custo_val >= 0 else "N/A"
            
            return (
                f"{header}"
                f"<br><b>Rua:</b> {rua}"
                f"<br><b>Bairro:</b> {bairro}"
                f"<br><b>Comprimento:</b> {dist_fmt}"
                f"<br><b>Custo Travessia:</b> {custo}"
            )
        except Exception:
            return "Erro no Tooltip"
        
    def _preformat_node_tooltip(self, row_dict: dict) -> str:
        """Helper para formatar o tooltip de um nó."""
        try:
            node_idx = int(row_dict.get('node_index', '?'))

            if row_dict.get('depot') == 'yes':
                return f"<b>Depósito:</b> {node_idx}"
            
            custo_serv = int(row_dict.get('custo_servico', 0))
            return (
                f"<b>Nó:</b> {node_idx}" 
                f"<br><b>Custo de serviço:</b> {custo_serv}s"
            )
        except Exception:
            return "Erro no Tooltip"

    def _find_split_index_and_snapped_point(self, line_geom: LineString, click_point: Point) -> tuple[int, Point]:
        """
        Encontra o índice na lista de coordenadas da linha onde o novo
        ponto deve ser inserido e retorna o ponto (projetado) exato.
        """
        # Projeta o ponto na linha inteira para garantir que ele esteja "em cima" da linha
        distance_along_line = line_geom.project(click_point)
        snapped_point = line_geom.interpolate(distance_along_line)
        
        coords = list(line_geom.coords)
        
        # Encontra o segmento correto
        for i in range(len(coords) - 1):
            # Cria uma geometria para o segmento atual
            segment = LineString([coords[i], coords[i+1]])
            
            # Verifica se o ponto projetado está neste segmento
            # Usamos uma tolerância pequena para lidar com imprecisão de float
            if segment.distance(snapped_point) < 1e-8:
                return i + 1, snapped_point
        
        # Fallback: se algo der errado, insere no final
        return len(coords), snapped_point

    def _calculate_line_length(self, coords_list: List[tuple[float, float]]) -> float:
        """Calcula o comprimento total (em metros) de uma lista de coordenadas."""
        total_dist = 0.0
        for i in range(len(coords_list) - 1):
            total_dist += GeoCalculator.haversine_distance(coords_list[i], coords_list[i+1])
        return total_dist

    def _calculate_segment_angles(self, coords_list: List[tuple[float, float]]) -> List[float]:
        """Calcula o azimute de cada segmento em uma lista de coordenadas."""
        angles = []
        for i in range(len(coords_list) - 1):
            angle = GeoCalculator.azimuth(coords_list[i], coords_list[i+1])
            angles.append(angle)
        return angles
        
    def _get_next_index(self, df: pd.DataFrame, column: str) -> int:
        """Retorna o próximo índice disponível para uma coluna."""
        if df.empty or column not in df.columns:
            return 1
        valid_series = pd.to_numeric(df[column], errors='coerce').dropna()
        if valid_series.empty:
            return 1
        return int(valid_series.max()) + 1
    
    def _add_row_from_dict(self, gdf: gpd.GeoDataFrame, row_dict: Dict[str, Any]):
        """
        Adiciona uma nova linha a um GeoDataFrame usando o método .loc[].
        """
        new_index = len(gdf)
        # Define a geometria separadamente para garantir que o CRS seja tratado
        gdf.loc[new_index, 'geometry'] = row_dict['geometry']
        
        # Define os outros valores
        for col, val in row_dict.items():
            if col != 'geometry' and col in gdf.columns:
                gdf.at[new_index, col] = val

    def _rebuild_and_reindex_gdfs(self, data_streets: gpd.GeoDataFrame, data_points: gpd.GeoDataFrame, map_streets: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Re-indexa os IDs das ruas (1 a N) e propaga para os pontos.
        Necessário para manter a integridade referencial após a divisão.
        """
        print("Editor: Re-indexando grafo...")
        
        # Ruas de DADOS
        # Reset index para garantir sequencialidade limpa
        data_streets = data_streets.reset_index(drop=True)
        
        # Cria mapa de ID antigo -> ID novo
        data_streets['final_id'] = data_streets.index + 1
        id_map = dict(zip(data_streets['id'], data_streets['final_id']))
        
        data_streets['id'] = data_streets['final_id']
        data_streets = data_streets.drop(columns='final_id')
        
        # Pontos
        data_points['from_line_id'] = data_points['from_line_id'].map(id_map)
        # Remove órfãos (segurança)
        data_points = data_points.dropna(subset=['from_line_id'])
        data_points = data_points.reset_index(drop=True)

        # Ruas de MAPA
        map_streets['final_id'] = map_streets['id'].map(id_map)
        map_streets = map_streets.dropna(subset=['final_id'])
        map_streets['id'] = map_streets['final_id']
        map_streets = map_streets.drop(columns='final_id')
        map_streets = map_streets.reset_index(drop=True)

        return data_streets, data_points, map_streets
    
    def finalize_reindexing(self, state: GraphState) -> GraphState:
        """
        Realiza a re-indexação final de todos os identificadores do grafo (1 a N).
        Deve ser chamado antes de salvar como 'requerido'.
        """
        print("Editor: Finalizando re-indexação completa (1..N)...")
        
        # Re-indexação de rua
        
        # Ordena para garantir determinismo
        state.data_streets = state.data_streets.sort_values('id').reset_index(drop=True)
        state.map_streets = state.map_streets.sort_values('id').reset_index(drop=True)      # Assume alinhamento
        
        # Cria mapa de IDs antigos -> novos
        old_ids = state.data_streets['id'].tolist()
        new_ids = list(range(1, len(state.data_streets) + 1))
        street_id_map = dict(zip(old_ids, new_ids))
        
        # Aplica novos IDs
        state.data_streets['id'] = new_ids
        state.map_streets['id'] = new_ids       # Assume alinhamento
        
        # Atualiza referências nos pontos
        state.data_points['from_line_id'] = state.data_points['from_line_id'].map(street_id_map)
        
        # Re-indexação de Edge/Arc Index

        # Separa arestas e arcos
        mask_edges = state.data_streets['edge_index'].notna() & (state.data_streets['edge_index'] != -1)
        mask_arcs = state.data_streets['arc_index'].notna() & (state.data_streets['arc_index'] != -1)
        
        # Regenera índices sequenciais
        n_edges = mask_edges.sum()
        n_arcs = mask_arcs.sum()
        
        state.data_streets.loc[mask_edges, 'edge_index'] = range(1, n_edges + 1)
        state.data_streets.loc[mask_arcs, 'arc_index'] = range(1, n_arcs + 1)
        
        # Reflete no mapa
        state.map_streets.loc[mask_edges, 'edge_index'] = state.data_streets.loc[mask_edges, 'edge_index'].values
        state.map_streets.loc[mask_arcs, 'arc_index'] = state.data_streets.loc[mask_arcs, 'arc_index'].values

        # Re-indexação de nós

        # Obtém todos os node_index únicos presentes no data_points
        unique_nodes = sorted(state.data_points['node_index'].unique())
        node_id_map = {old: new for new, old in enumerate(unique_nodes, 1)}     # 1 a N
        
        # Aplica mapa nos Pontos
        state.data_points['node_index'] = state.data_points['node_index'].map(node_id_map)
        
        # Aplica mapa nas Ruas (from_node, to_node)
        state.data_streets['from_node'] = state.data_streets['from_node'].map(node_id_map)
        state.data_streets['to_node'] = state.data_streets['to_node'].map(node_id_map)
        
        state.map_streets['from_node'] = state.map_streets['from_node'].map(node_id_map)
        state.map_streets['to_node'] = state.map_streets['to_node'].map(node_id_map)
        
        # Reconstrói Map Points

        # Salva estado visual antigo com a chave sendo o ID antigo
        old_map_state = state.map_points[['node_index', 'eh_requerido', 'depot', 'custo_servico', 'demanda']].copy()
        old_map_state['node_index'] = old_map_state['node_index'].map(node_id_map)
        old_map_state = old_map_state.set_index('node_index')
        
        # Recria geometria visual
        final_map_points = GeoCalculator.create_map_points(state.data_points)
        final_map_points = FieldsManager.ensure_fields_exist(final_map_points, FieldConfigType.EXTENDED)
        
        # Restaura estado visual
        final_map_points = final_map_points.set_index('node_index')
        final_map_points.update(old_map_state)
        final_map_points = final_map_points.reset_index()
        
        # Gera tooltips atualizados
        final_map_points['tooltip_html'] = final_map_points.apply(lambda row: self._preformat_node_tooltip(row.to_dict()), axis=1)
        
        # Sanitiza e atribui
        state.map_points = self._sanitize_and_build_gdf(final_map_points, state.map_streets.crs)
        
        # Atualiza tooltips das ruas também
        state.map_streets['tooltip_html'] = state.map_streets.apply(lambda row: self._preformat_street_tooltip(row.to_dict()), axis=1)
        
        print("Editor: Re-indexação final concluída.")
        return state
    
    def split_street(self, state: GraphState, original_street_id: int, new_node_row_input: pd.Series, is_depot: bool = False) -> GraphState:
        """
        Divide uma rua (A-B) em duas (A-C, C-B) usando um novo nó (C).
        Retorna um GraphState completamente novo e atualizado.
        """
        
        # --- OBTER PEÇAS ---
        print(f"Editor: Dividindo rua ID {original_street_id}...")

        # Configura flags com base no tipo de inserção
        req_val = 'no' if is_depot else 'yes'
        depot_val = 'yes' if is_depot else 'no'
        demanda_val = 0 if is_depot else 1

        new_node_cost = new_node_row_input.get('custo_servico', 0)

        # Novo Nó (C)
        new_node_row = new_node_row_input.copy()
        new_node_id = new_node_row['node_index']
        new_node_geom = new_node_row['geometry']        # Ponto do clique

        # Rua Original (A-B)
        street_data_row = state.data_streets[state.data_streets['id'] == original_street_id].iloc[0]
        street_map_row = state.map_streets[state.map_streets['id'] == original_street_id].iloc[0]
        
        # Herança de atributos
        original_eh_requerido = street_data_row.get('eh_requerido', 'no')
        maxspeed = street_data_row['maxspeed']
        base_name = street_data_row.get('name')
        base_alt_name = street_data_row.get('alt_name')
        base_bairro = street_data_row.get('bairro')
        base_id_bairro = street_data_row.get('id_bairro')

        # IDs dos Pontos Originais (A e B)
        pt_A_id = street_data_row['from_node']
        pt_B_id = street_data_row['to_node']

        # Linhas dos Pontos Originais (A e B)
        points_for_street = state.data_points[
            state.data_points['from_line_id'] == original_street_id
        ]
        pt_A_row = points_for_street[points_for_street['node_index'] == pt_A_id].iloc[0]
        pt_B_row = points_for_street[points_for_street['node_index'] == pt_B_id].iloc[0]

        # --- ENCAIXAR NÓ C NA GEOMETRIA VISUAL ---
        map_street_geom = street_map_row.geometry
        visual_coords = list(map_street_geom.coords)
        
        split_index, snapped_node_geom = self._find_split_index_and_snapped_point(
            map_street_geom, new_node_geom
        )
        snapped_node_coords = snapped_node_geom.coords[0]
        
        # Atualiza a geometria do novo nó para o ponto exato na linha
        new_node_row['geometry'] = snapped_node_geom
        
        # --- CALCULAR MÉTRICAS ---
        
        # A-C: do início até o índice de corte + o ponto novo
        visual_coords_A_C = visual_coords[:split_index] + [snapped_node_coords]
        # C-B: o ponto novo + do índice de corte até o fim
        visual_coords_C_B = [snapped_node_coords] + visual_coords[split_index:]

        # Geometrias (Dados e Mapa)
        geom_A_C_data = LineString([pt_A_row.geometry, snapped_node_geom])
        geom_C_B_data = LineString([snapped_node_geom, pt_B_row.geometry])
        geom_A_C_map = LineString(visual_coords_A_C)
        geom_C_B_map = LineString(visual_coords_C_B)

        # Distâncias (em metros e km)
        dist_A_C_m = self._calculate_line_length(visual_coords_A_C)
        dist_C_B_m = self._calculate_line_length(visual_coords_C_B)
        dist_A_C_km = round(dist_A_C_m / 1000.0, GeoCalculator.PRECISION_DIGITS)
        dist_C_B_km = round(dist_C_B_m / 1000.0, GeoCalculator.PRECISION_DIGITS)
        
        # Ângulos (Média)
        angles_A_C = self._calculate_segment_angles(visual_coords_A_C)
        angles_C_B = self._calculate_segment_angles(visual_coords_C_B)

        angle_A_C = round(GeoCalculator.mean_angle_deg(angles_A_C), GeoCalculator.PRECISION_DIGITS)
        angle_C_B = round(GeoCalculator.mean_angle_deg(angles_C_B), GeoCalculator.PRECISION_DIGITS)
        inv_angle_A_C = round(GeoCalculator.azimuth_inverse(angle_A_C), GeoCalculator.PRECISION_DIGITS)
        inv_angle_C_B = round(GeoCalculator.azimuth_inverse(angle_C_B), GeoCalculator.PRECISION_DIGITS)
        
        # Custos
        cost_travessia_A_C = GeoCalculator.calculate_traversal_cost(dist_A_C_km, maxspeed)
        cost_travessia_C_B = GeoCalculator.calculate_traversal_cost(dist_C_B_km, maxspeed)

        cost_servico_A_C = int(round(cost_travessia_A_C * 1.5))
        cost_servico_C_B = int(round(cost_travessia_C_B * 1.5))

        # --- OBTER NOVOS IDs DE RUA ---
        id_A_C = int(state.data_streets['id'].max()) + 1
        id_C_B = id_A_C + 1
        
        # Determina Edge ou Arc Index
        edge_idx_A_C, arc_idx_A_C = None, None
        edge_idx_C_B, arc_idx_C_B = None, None
        
        if pd.notna(street_data_row.get('edge_index')):
            # É aresta
            next_edge = self._get_next_index(state.data_streets, 'edge_index')
            edge_idx_A_C = next_edge
            edge_idx_C_B = next_edge + 1
        elif pd.notna(street_data_row.get('arc_index')):
            # É arco
            next_arc = self._get_next_index(state.data_streets, 'arc_index')
            arc_idx_A_C = next_arc
            arc_idx_C_B = next_arc + 1

        print(f"Editor: Nova rua A-C (ID {id_A_C}), Nova rua C-B (ID {id_C_B})")

        # --- PREPARAR NOVAS RUAS (A-C e C-B) ---
        
        # Template comum
        base_street_data = street_data_row.to_dict()
        base_street_data.update({
            'edge_index': edge_idx_A_C if edge_idx_A_C != None else None,
            'arc_index': arc_idx_A_C if arc_idx_A_C != None else None,
            'eh_requerido': original_eh_requerido
        })
        
        # Rua A-C (Dados)
        street_A_C_data = base_street_data.copy()
        street_A_C_data.update({
            'id': id_A_C,
            'geometry': geom_A_C_data,
            'total_dist': dist_A_C_km,
            'custo_travessia': cost_travessia_A_C,
            'custo_servico': cost_servico_A_C,
            'from_node': pt_A_id,
            'to_node': new_node_id,
            'edge_index': edge_idx_A_C,
            'arc_index': arc_idx_A_C,
            'eh_requerido': original_eh_requerido
        })
        
        # Rua C-B (Dados)
        street_C_B_data = base_street_data.copy()
        street_C_B_data.update({
            'id': id_C_B,
            'geometry': geom_C_B_data,
            'total_dist': dist_C_B_km,
            'custo_travessia': cost_travessia_C_B,
            'custo_servico': cost_servico_C_B,
            'from_node': new_node_id,
            'to_node': pt_B_id,
            'edge_index': edge_idx_C_B,
            'arc_index': arc_idx_C_B,
            'eh_requerido': original_eh_requerido
        })

        # Rua A-C (Mapa)
        street_A_C_map = street_map_row.to_dict()
        street_A_C_map.update(street_A_C_data)          # Copia métricas
        street_A_C_map['geometry'] = geom_A_C_map       # Define geometria visual
        street_A_C_map['tooltip_html'] = self._preformat_street_tooltip(street_A_C_map)
        
        # Rua C-B (Mapa)
        street_C_B_map = street_map_row.to_dict()
        street_C_B_map.update(street_C_B_data)          # Copia métricas
        street_C_B_map['geometry'] = geom_C_B_map       # Define geometria visual
        street_C_B_map['tooltip_html'] = self._preformat_street_tooltip(street_C_B_map)

        # --- PREPARAR NOVOS PONTOS (A1, C1, C2, B1) ---
        
        # Ponto A1 (atualizado)
        pt_A1 = pt_A_row.to_dict()
        pt_A1.update({
            'from_line_id': id_A_C,
            'angle': angle_A_C,
            'angle_inv': inv_angle_A_C,
            'distance': 0.0
        })
        
        # Ponto C1 (novo)
        pt_C1 = new_node_row.to_dict()
        pt_C1.update({
            'from_line_id': id_A_C,
            'vertex_index': -1,
            'distance': dist_A_C_km,
            'angle': None,
            'angle_inv': None,
            'eh_extremidade': 'yes',
            'eh_unido': 'yes',
            'eh_requerido': req_val,
            'custo_servico': new_node_cost,
            'depot': depot_val,
            'demanda': demanda_val,
            'name': base_name,
            'alt_name': base_alt_name,
            'bairro': base_bairro,
            'id_bairro': base_id_bairro
        })

        # Ponto C2 (novo)
        pt_C2 = new_node_row.to_dict()
        pt_C2.update({
            'from_line_id': id_C_B,
            'vertex_index': -1,
            'distance': 0.0,
            'angle': angle_C_B,
            'angle_inv': inv_angle_C_B,
            'eh_extremidade': 'yes',
            'eh_unido': 'yes',
            'eh_requerido': req_val,
            'custo_servico': new_node_cost,
            'depot': depot_val,
            'demanda': demanda_val,
            'name': base_name,
            'alt_name': base_alt_name,
            'bairro': base_bairro,
            'id_bairro': base_id_bairro
        })

        # Ponto B1 (atualizado)
        pt_B1 = pt_B_row.to_dict()
        pt_B1.update({
            'from_line_id': id_C_B,
            'distance': dist_C_B_km,
            'angle': None,
            'angle_inv': None
        })
        
        # --- RECONSTRUIR GDFS DO ESTADO ---

        data_streets_clean = state.data_streets[state.data_streets['id'] != original_street_id].copy()
        map_streets_clean = state.map_streets[state.map_streets['id'] != original_street_id].copy()
        data_points_clean = state.data_points[state.data_points['from_line_id'] != original_street_id].copy()
        
        safe_crs_s, safe_crs_m, safe_crs_p = state.data_streets.crs, state.map_streets.crs, state.data_points.crs

        # Cria DFs
        df_new_streets_data = pd.DataFrame([street_A_C_data, street_C_B_data])
        dict_map_A_C = street_A_C_data.copy(); dict_map_A_C.update(street_map_row.to_dict()); dict_map_A_C.update(street_A_C_data); dict_map_A_C['geometry'] = geom_A_C_map
        dict_map_C_B = street_C_B_data.copy(); dict_map_C_B.update(street_map_row.to_dict()); dict_map_C_B.update(street_C_B_data); dict_map_C_B['geometry'] = geom_C_B_map
        df_new_streets_map = pd.DataFrame([dict_map_A_C, dict_map_C_B])
        df_new_points = pd.DataFrame([pt_A1, pt_C1, pt_C2, pt_B1])

        # Alinha estrutura
        df_new_streets_data = self._align_dataframe_structure(df_new_streets_data, data_streets_clean)
        df_new_streets_map = self._align_dataframe_structure(df_new_streets_map, map_streets_clean)
        df_new_points = self._align_dataframe_structure(df_new_points, data_points_clean)
        
        # Cria GDFs temporários
        gdf_temp_s = gpd.GeoDataFrame(df_new_streets_data, geometry='geometry') 
        gdf_temp_m = gpd.GeoDataFrame(df_new_streets_map, geometry='geometry')
        gdf_temp_p = gpd.GeoDataFrame(df_new_points, geometry='geometry')

        # Lobotomia
        data_streets_clean.crs = None; gdf_temp_s.crs = None
        map_streets_clean.crs = None; gdf_temp_m.crs = None
        data_points_clean.crs = None; gdf_temp_p.crs = None

        # Concat
        final_data_streets_df = pd.concat([data_streets_clean, gdf_temp_s], ignore_index=True)
        final_map_streets_df = pd.concat([map_streets_clean, gdf_temp_m], ignore_index=True)
        final_data_points_df = pd.concat([data_points_clean, gdf_temp_p], ignore_index=True)

        # Restaura
        final_data_streets = self._sanitize_and_build_gdf(final_data_streets_df, safe_crs_s)
        final_map_streets = self._sanitize_and_build_gdf(final_map_streets_df, safe_crs_m)
        final_data_points = self._sanitize_and_build_gdf(final_data_points_df, safe_crs_p)

        # Re-indexa
        final_data_streets, final_data_points, final_map_streets = self._rebuild_and_reindex_gdfs(
            final_data_streets, final_data_points, final_map_streets
        )

        # Mapa visual
        print("Editor: Reconstruindo map_points...")
        old_map_points_state = state.map_points[['node_index', 'eh_requerido', 'depot', 'custo_servico', 'demanda']].set_index('node_index')
        
        final_map_points_gdf = GeoCalculator.create_map_points(final_data_points)
        final_map_points_gdf = FieldsManager.ensure_fields_exist(final_map_points_gdf, FieldConfigType.EXTENDED)
        
        final_map_points_gdf = final_map_points_gdf.set_index('node_index')
        final_map_points_gdf.update(old_map_points_state)
        final_map_points_gdf = final_map_points_gdf.reset_index()
        final_map_points_gdf.loc[final_map_points_gdf['node_index'] == new_node_id, 'eh_requerido'] = req_val
        final_map_points_gdf.loc[final_map_points_gdf['node_index'] == new_node_id, 'depot'] = depot_val
        final_map_points_gdf.loc[final_map_points_gdf['node_index'] == new_node_id, 'custo_servico'] = new_node_cost

        final_map_points_gdf['tooltip_html'] = final_map_points_gdf.apply(lambda row: self._preformat_node_tooltip(row.to_dict()), axis=1)
        
        final_map_points_gdf = self._sanitize_and_build_gdf(final_map_points_gdf, safe_crs_m)

        print(f"Editor: Divisão concluída.")
        
        return GraphState(
            data_streets=final_data_streets, data_points=final_data_points,
            map_streets=final_map_streets, map_points=final_map_points_gdf,
            neighborhoods=state.neighborhoods 
        )
    
    def remove_node_and_merge_streets(self, state: GraphState, node_id_C: int) -> GraphState:
        """
        Remove um nó temporário (C) e mescla as duas ruas conectadas (A-C e C-B)
        de volta em uma única rua (A-B).
        """
        print(f"Editor: Removendo nó {node_id_C} e mesclando ruas...")
        
        # Encontra as ruas conectadas a C
        connected_streets = state.data_streets[
            (state.data_streets['from_node'] == node_id_C) | 
            (state.data_streets['to_node'] == node_id_C)
        ]

        if len(connected_streets) != 2:
            print(f"  ERRO: Nó {node_id_C} não conecta exatamente 2 ruas (encontradas: {len(connected_streets)}). Abortando.")
            return state

        # O nó C é 'to_node' de uma rua (A->C) e 'from_node' da outra (C->B)
        street_AC_row = connected_streets[connected_streets['to_node'] == node_id_C]
        street_CB_row = connected_streets[connected_streets['from_node'] == node_id_C]

        # Validação
        if street_AC_row.empty or street_CB_row.empty:
            print("  AVISO: Topologia não identificada")
            return state

        street_AC_row = street_AC_row.iloc[0]
        street_CB_row = street_CB_row.iloc[0]
        
        id_AC = street_AC_row['id']
        id_CB = street_CB_row['id']
        
        # Identifica Nós A e B
        node_id_A = street_AC_row['from_node']
        node_id_B = street_CB_row['to_node']

        # Recupera linhas do mapa correspondentes
        map_AC_row = state.map_streets[state.map_streets['id'] == id_AC].iloc[0]
        map_CB_row = state.map_streets[state.map_streets['id'] == id_CB].iloc[0]

        print(f"  Mesclando: Rua {id_AC} (A->C) + Rua {id_CB} (C->B)")
        
        # Geometria Visual Combinada
        # [c1, c2, ..., C] + [C, c3, c4...] -> Removemos o C duplicado
        coords_AC = list(map_AC_row.geometry.coords)
        coords_CB = list(map_CB_row.geometry.coords)
        
        # Verifica snap
        if coords_AC[-1] != coords_CB[0]:
            print("  Aviso: Coordenada de junção C difere")
        
        merged_coords = coords_AC[:-1] + coords_CB[1:]      # Remove o último de AC e junta com CB
        geom_AB_map = LineString(merged_coords)

        # Geometria Lógica (Dados)
        # [Coord_A, Coord_B]
        pt_A_data = state.data_points[
            (state.data_points['from_line_id'] == id_AC) & 
            (state.data_points['node_index'] == node_id_A)
        ].iloc[0]
        
        pt_B_data = state.data_points[
            (state.data_points['from_line_id'] == id_CB) & 
            (state.data_points['node_index'] == node_id_B)
        ].iloc[0]
        
        # Desidratação (Tuplas)
        coord_A = pt_A_data.geometry.coords[0]
        coord_B = pt_B_data.geometry.coords[0]
        geom_AB_data = LineString([coord_A, coord_B])

        # Métricas
        total_dist_km = street_AC_row['total_dist'] + street_CB_row['total_dist']
        # Recalcula custos
        total_custo_travessia = street_AC_row['custo_travessia'] + street_CB_row['custo_travessia']
        total_custo_servico = int(round(total_custo_travessia * 1.5))

        # Requerido / Demanda
        is_required = 'yes' if (street_AC_row.get('eh_requerido') == 'yes' or street_CB_row.get('eh_requerido') == 'yes') else 'no'
        demanda = 1 if is_required == 'yes' else 0

        # Novo ID e Índices
        new_street_id = int(state.data_streets['id'].max()) + 1
        
        # Gera Edge/Arc Index
        new_edge_index = None
        new_arc_index = None
        
        if pd.notna(street_AC_row.get('edge_index')) and street_AC_row['edge_index'] != -1:
            new_edge_index = self._get_next_index(state.data_streets, 'edge_index')
        else:
            new_arc_index = self._get_next_index(state.data_streets, 'arc_index')

        # Rua Mesclada (Dados)
        dict_AB = street_AC_row.to_dict()       # Herda nome, bairro, etc de AC
        dict_AB.update({
            'id': new_street_id,
            'geometry': geom_AB_data,
            'total_dist': total_dist_km,
            'custo_travessia': total_custo_travessia,
            'custo_servico': total_custo_servico,
            'from_node': node_id_A,
            'to_node': node_id_B,
            'edge_index': new_edge_index,
            'arc_index': new_arc_index,
            'eh_requerido': is_required,
            'demanda': demanda
        })
        
        # Mapa
        dict_map_AB = dict_AB.copy()
        dict_map_AB.update(street_AC_row.to_dict())     # Herda propriedades visuais de AC
        dict_map_AB.update(dict_AB)                     # Sobrescreve métricas
        dict_map_AB['geometry'] = geom_AB_map
        dict_map_AB['tooltip_html'] = self._preformat_street_tooltip(dict_map_AB)
        
        # Recalcula métricas baseadas na nova geometria visual
        dist_AB_m = self._calculate_line_length(merged_coords) # Distância real visual
        angles_AB = self._calculate_segment_angles(merged_coords)
        mean_angle_AB = round(GeoCalculator.mean_angle_deg(angles_AB), GeoCalculator.PRECISION_DIGITS)
        inv_angle_AB = round(GeoCalculator.azimuth_inverse(mean_angle_AB), GeoCalculator.PRECISION_DIGITS)

        # Ponto A (Inicio)
        dict_pt_A = pt_A_data.to_dict()
        dict_pt_A.update({
            'from_line_id': new_street_id,
            'angle': mean_angle_AB,
            'angle_inv': inv_angle_AB,
            'distance': 0.0
        })

        # Ponto B (Fim)
        dict_pt_B = pt_B_data.to_dict()
        dict_pt_B.update({
            'from_line_id': new_street_id,
            'distance': round(dist_AB_m / 1000.0, GeoCalculator.PRECISION_DIGITS),
            'angle': 0.0,
            'angle_inv': 0.0
        })

        # Remove IDs antigos (AC, CB) e pontos C (e A, B antigos)
        ids_to_remove = [id_AC, id_CB]
        
        # Limpeza
        data_streets_clean = state.data_streets[~state.data_streets['id'].isin(ids_to_remove)].copy()
        map_streets_clean = state.map_streets[~state.map_streets['id'].isin(ids_to_remove)].copy()
        
        # Remove pontos ligados às ruas removidas
        data_points_clean = state.data_points[~state.data_points['from_line_id'].isin(ids_to_remove)].copy()

        safe_crs_s, safe_crs_m, safe_crs_p = data_streets_clean.crs, map_streets_clean.crs, data_points_clean.crs

        # Cria DataFrames
        df_new_street = pd.DataFrame([dict_AB])
        df_new_map = pd.DataFrame([dict_map_AB])
        df_new_points = pd.DataFrame([dict_pt_A, dict_pt_B])

        # Alinha estrutura
        df_new_street = self._align_dataframe_structure(df_new_street, data_streets_clean)
        df_new_map = self._align_dataframe_structure(df_new_map, map_streets_clean)
        df_new_points = self._align_dataframe_structure(df_new_points, data_points_clean)

        # Cria GDFs temporários
        gdf_new_street = gpd.GeoDataFrame(df_new_street, geometry='geometry')
        gdf_new_map = gpd.GeoDataFrame(df_new_map, geometry='geometry')
        gdf_new_points = gpd.GeoDataFrame(df_new_points, geometry='geometry')

        # Lobotomia CRS
        data_streets_clean.crs = None; gdf_new_street.crs = None
        map_streets_clean.crs = None; gdf_new_map.crs = None
        data_points_clean.crs = None; gdf_new_points.crs = None

        # Concat
        final_data_streets_df = pd.concat([data_streets_clean, gdf_new_street], ignore_index=True)
        final_map_streets_df = pd.concat([map_streets_clean, gdf_new_map], ignore_index=True)
        final_data_points_df = pd.concat([data_points_clean, gdf_new_points], ignore_index=True)

        # Restaura
        final_data_streets = self._sanitize_and_build_gdf(final_data_streets_df, safe_crs_s)
        final_map_streets = self._sanitize_and_build_gdf(final_map_streets_df, safe_crs_m)
        final_data_points = self._sanitize_and_build_gdf(final_data_points_df, safe_crs_p)

        # Re-indexa
        final_data_streets, final_data_points, final_map_streets = self._rebuild_and_reindex_gdfs(
            final_data_streets, final_data_points, final_map_streets
        )

        print("Editor: Reconstruindo map_points...")
        old_map_points_state = state.map_points[['node_index', 'eh_requerido', 'depot', 'custo_servico', 'demanda']].set_index('node_index')
        
        final_map_points_gdf = GeoCalculator.create_map_points(final_data_points)
        final_map_points_gdf = FieldsManager.ensure_fields_exist(final_map_points_gdf, FieldConfigType.EXTENDED)
        
        final_map_points_gdf = final_map_points_gdf.set_index('node_index')
        final_map_points_gdf.update(old_map_points_state)
        final_map_points_gdf = final_map_points_gdf.reset_index()
        final_map_points_gdf = final_map_points_gdf[final_map_points_gdf['node_index'] != node_id_C]

        final_map_points_gdf['tooltip_html'] = final_map_points_gdf.apply(lambda row: self._preformat_node_tooltip(row.to_dict()), axis=1)
        
        final_map_points_gdf = self._sanitize_and_build_gdf(final_map_points_gdf, safe_crs_m)

        print("Editor: Remoção e Mesclagem concluída.")

        return GraphState(
            data_streets=final_data_streets,
            data_points=final_data_points,
            map_streets=final_map_streets,
            map_points=final_map_points_gdf,
            neighborhoods=state.neighborhoods 
        )