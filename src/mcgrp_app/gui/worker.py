# src\mcgrp_app\gui\worker.py

import traceback
import pandas as pd
from pathlib import Path
from shapely.geometry import box, Point

from PySide6.QtCore import QObject, Signal, Slot

from ..core.graph import GraphEditor, ShortestPathAnalyzer
from ..core.pipeline import GeoPipeline
from ..core.utils import FieldConfigType, FieldsManager, GraphState, GeoCalculator, GeoFactory
from ..core.instance import MCGRPInstanceGenerator, MCGRPTPInstanceGenerator

class PipelineWorker(QObject):
    """
    Worker QObject que executa o GeoPipeline em uma thread separada
    para evitar o congelamento da GUI.
    """
    
    # --- Sinais de feedback para a MainWindow ---
    progress_update = Signal(int, int, str)
    processing_complete = Signal(pd.DataFrame, pd.DataFrame, str, dict)
    processing_error = Signal(str)

    # --- SINAIS (Worker -> GUI) ---
    
    # (id, novo_status, dados_da_linha_para_o_dock)
    street_toggled = Signal(int, str, dict)
    node_toggled = Signal(int, str, dict)
    
    # (novo_id_deposito, dados_do_novo, antigo_id_deposito)
    # IDs podem ser -1 para indicar "nenhum"
    depot_changed = Signal(int, dict, int)

    # Sinal para atualização completa
    # Usado para Adição de Nó ou Carregamento do DB
    node_added_and_state_updated = Signal(pd.DataFrame, pd.DataFrame)

    # Sinal de sucesso da finalização
    finalization_complete = Signal(int)
    
    # Sinal de arquivos gerados
    files_generated = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        # Instancias
        self.pipeline = GeoPipeline(None)
        self.graph_editor = GraphEditor()
        
        # Conecta os sinais internos do pipeline aos sinais externos do worker
        # Isso repassa os sinais para a MainWindow
        self.pipeline.progress_update.connect(self.progress_update)
        self.pipeline.processing_complete.connect(self.processing_complete)
        self.pipeline.processing_error.connect(self.processing_error)

    def _get_pipeline_state_dfs(self):
        """Helper para verificar e retornar DataFrames de dados."""
        if not self.pipeline.state or self.pipeline.state.data_streets is None:
            self.processing_error.emit("Estado do pipeline não está pronto.")
            return None, None, None, None
        
        if self.pipeline.state.map_streets is None:
            self.processing_error.emit("Estado do pipeline (mapa) não está pronto.")
            return None, None, None, None
        
        return (
            self.pipeline.state.data_streets, 
            self.pipeline.state.data_points, 
            self.pipeline.state.map_streets, 
            self.pipeline.state.map_points
        )

    @Slot(pd.DataFrame, pd.DataFrame, str)
    def run_pipeline_processing(self, streets_df, neighborhoods_df, run_name: str):
        """
        Este é o Slot que a MainWindow (thread principal) irá chamar.
        Ele executa o trabalho pesado de forma síncrona.
        """
        try:
            print("Worker Thread: Recebeu sinal. Iniciando pipeline...")
            
            # Chama o método que faz todo o trabalho pesado
            self.pipeline.start_processing(streets_df, neighborhoods_df, run_name)
            
            print("Worker Thread: Pipeline concluído.")
            
        except Exception as e:
            # Captura qualquer erro inesperado no pipeline
            print(f"Worker Thread: Erro catastrófico no pipeline: {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Erro fatal na thread do pipeline: {e}")
    
    @Slot(GraphState)
    def set_pipeline_state(self, new_state: GraphState):
        """
        Recebe um estado completo (carregado do DB) e o define
        como o estado ativo do pipeline.
        """
        try:
            print("Worker Thread: Recebeu novo estado do DB.")

            self.pipeline.state = new_state

            # Inicializa colunas
            new_state.data_streets = FieldsManager.ensure_fields_exist(new_state.data_streets, FieldConfigType.EXTENDED)
            new_state.data_points = FieldsManager.ensure_fields_exist(new_state.data_points, FieldConfigType.EXTENDED)
            new_state.map_streets = FieldsManager.ensure_fields_exist(new_state.map_streets, FieldConfigType.EXTENDED)
            new_state.map_points = FieldsManager.ensure_fields_exist(new_state.map_points, FieldConfigType.EXTENDED)

            print("Worker Thread: Novo estado carregado e inicializado com sucesso.")

            # Emite o estado inicializado para a GUI (para preencher o dock)
            self.node_added_and_state_updated.emit(
                self.pipeline.state.map_streets,
                self.pipeline.state.map_points
            )

        except Exception as e:
            print(f"Worker Thread: Falha ao definir novo estado. {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Falha ao carregar estado: {e}")

    # --- Slots GUI para Worker ---

    @Slot(int)
    def on_toggle_street(self, street_id: int):
        dfs = self._get_pipeline_state_dfs()
        if dfs[0] is None: return
        data_streets_df, _, map_streets_df, _ = dfs

        try:
            street_row_series = data_streets_df.loc[data_streets_df['id'] == street_id]
            if street_row_series.empty: return
            
            idx = street_row_series.iloc[0].name
            is_req = data_streets_df.at[idx, 'eh_requerido'] == 'yes'
            new_status = 'no' if is_req else 'yes'
            
            data_streets_df.at[idx, 'eh_requerido'] = new_status
            data_streets_df.at[idx, 'demanda'] = 1 if new_status == 'yes' else 0
            
            map_mask = map_streets_df['id'] == street_id
            if map_mask.any():
                map_streets_df.loc[map_mask, 'eh_requerido'] = new_status
                map_streets_df.loc[map_mask, 'demanda'] = 1 if new_status == 'yes' else 0
            
            self.street_toggled.emit(street_id, new_status, data_streets_df.loc[idx].to_dict())
        except Exception as e:
            self.processing_error.emit(f"Erro ao alternar rua {street_id}: {e}")
            traceback.print_exc()

    @Slot(int, int)
    def on_toggle_node(self, node_id: int, service_cost: int):
        dfs = self._get_pipeline_state_dfs()
        if dfs[1] is None: return
        _, data_points_df, _, map_points_df = dfs

        try:
            # Encontra o nó
            node_mask = data_points_df['node_index'] == node_id
            if not node_mask.any(): return
            
            node_row = data_points_df[node_mask].iloc[0]
            is_req = node_row.get('eh_requerido', 'no') == 'yes'
            
            # Verifica se é temporário (vertex_index == -1)
            v_idx = int(node_row.get('vertex_index', -999)) 
            is_temp = (v_idx == -1)

            if is_temp and is_req:
                # --- REMOÇÃO DE NÓ TEMPORÁRIO ---
                print(f"Worker: Removendo nó temporário {node_id} e mesclando ruas...")
                
                new_state = self.graph_editor.remove_node_and_merge_streets(
                    self.pipeline.state, 
                    node_id
                )
                self.pipeline.state = new_state
                
                self.node_added_and_state_updated.emit(
                    self.pipeline.state.map_streets, 
                    self.pipeline.state.map_points
                )
            else:
                # --- TOGGLE NORMAL ---
                new_status = 'no' if is_req else 'yes'
                data_points_df.loc[node_mask, 'eh_requerido'] = new_status

                if new_status == 'yes':
                    data_points_df.loc[node_mask, 'custo_servico'] = service_cost
                    data_points_df.loc[node_mask, 'demanda'] = 1
                else:
                    data_points_df.loc[node_mask, 'custo_servico'] = 0     # Reseta se desmarcar
                    data_points_df.loc[node_mask, 'demanda'] = 0
                
                map_mask = map_points_df['node_index'] == node_id
                if map_mask.any():
                    map_points_df.loc[map_mask, 'eh_requerido'] = new_status
                    if new_status == 'yes':
                        map_points_df.loc[map_mask, 'custo_servico'] = service_cost
                        map_points_df.loc[map_mask, 'demanda'] = 1
                    else:
                        map_points_df.loc[map_mask, 'custo_servico'] = 0
                        map_points_df.loc[map_mask, 'demanda'] = 0
                
                self.node_toggled.emit(node_id, new_status, data_points_df[node_mask].iloc[0].to_dict())

        except Exception as e:
            self.processing_error.emit(f"Erro ao alternar nó {node_id}: {e}")
            traceback.print_exc()

    @Slot(int)
    def on_set_depot(self, new_id: int):
        dfs = self._get_pipeline_state_dfs()
        if dfs[1] is None: return
        _, data_pts, _, map_pts = dfs

        try:
            new_mask = data_pts['node_index'] == new_id
            if not new_mask.any(): return

            curr_id = -1
            old_mask = data_pts['depot'] == 'yes'
            
            # Verifica depósito antigo
            if old_mask.any():
                old_depot_row = data_pts[old_mask].iloc[0]
                curr_id = int(old_depot_row['node_index'])

                # Verifica se é temporário
                v_idx = int(old_depot_row.get('vertex_index', -999))
                is_temp = (v_idx == -1)

                # CASO A: Clicou no mesmo nó
                if curr_id == new_id:
                    if is_temp:
                        # Se era temporário, remove ele
                        print(f"Worker: Clicou no mesmo depósito temporário {curr_id}. Removendo...")
                        self.pipeline.state = self.graph_editor.remove_node_and_merge_streets(self.pipeline.state, curr_id)
                        self.node_added_and_state_updated.emit(self.pipeline.state.map_streets, self.pipeline.state.map_points)
                        return
                    else:
                        # Se era original, apenas desmarca
                        data_pts.loc[old_mask, 'depot'] = 'no'
                        map_pts.loc[map_pts['depot'] == 'yes', 'depot'] = 'no'
                        self.depot_changed.emit(-1, {}, curr_id)
                        return

                # CASO B: Clicou em nó diferente
                if is_temp:
                    print(f"Worker: Removendo depósito temporário ANTIGO {curr_id} antes de setar o novo...")
                    # Remove o nó antigo e atualiza o estado
                    self.pipeline.state = self.graph_editor.remove_node_and_merge_streets(self.pipeline.state, curr_id)
                    
                    # Atualizando GDFs
                    dfs = self._get_pipeline_state_dfs()
                    data_pts = dfs[1]
                    map_pts = dfs[3]
                    
                    # Recalcula máscara do novo nó
                    new_mask = data_pts['node_index'] == new_id
                    if not new_mask.any():
                        print("Erro: Novo nó alvo desapareceu após remoção do antigo!")
                        return
                    
                    # Marca o novo nó como depósito no GDF atualizado
                    data_pts.loc[new_mask, 'depot'] = 'yes'
                    map_mask = map_pts['node_index'] == new_id
                    if map_mask.any():
                        map_pts.loc[map_mask, 'depot'] = 'yes'

                    # Como houve remoção de geometria, renderiza-se o mapa
                    self.node_added_and_state_updated.emit(
                        self.pipeline.state.map_streets, 
                        self.pipeline.state.map_points
                    )
                    return
                else:
                    # Apenas desmarca se for original
                    data_pts.loc[old_mask, 'depot'] = 'no'
                    map_pts.loc[map_pts['depot'] == 'yes', 'depot'] = 'no'

            # Marca novo
            data_pts.loc[new_mask, 'depot'] = 'yes'
            map_pts.loc[map_pts['node_index'] == new_id, 'depot'] = 'yes'
            
            self.depot_changed.emit(new_id, data_pts[new_mask].iloc[0].to_dict(), curr_id)

        except Exception as e:
            self.processing_error.emit(f"Erro ao definir depósito: {e}")

    @Slot(object)
    def on_box_select_streets(self, selection_box: box):
        """Define 'eh_requerido' para ruas dentro da caixa."""
        dfs = self._get_pipeline_state_dfs()
        if dfs[0] is None: return
        data_streets_df, map_streets_df = dfs[0], dfs[2]
        
        try:
            temp_gdf = GeoFactory.to_gdf(map_streets_df, self.pipeline.state.crs)

            selected_mask = temp_gdf.intersects(selection_box)
            selected_ids = set(temp_gdf[selected_mask]['id'])
            if not selected_ids:
                return
            
            # Encontra as linhas no GDF de DADOS
            rows_to_update_mask_data = data_streets_df['id'].isin(selected_ids)
            rows_to_update_mask_data = rows_to_update_mask_data & (data_streets_df['eh_requerido'] != 'yes')

            if rows_to_update_mask_data.any():
                data_streets_df.loc[rows_to_update_mask_data, 'eh_requerido'] = 'yes'
                data_streets_df.loc[rows_to_update_mask_data, 'demanda'] = 1

                # Emite sinal para CADA rua alterada
                for index, street_row in data_streets_df[rows_to_update_mask_data].iterrows():
                    self.street_toggled.emit(street_row['id'], 'yes', street_row.to_dict())

            # Encontra as linhas no GDF de MAPA
            rows_to_update_mask_map = map_streets_df['id'].isin(selected_ids)
            if rows_to_update_mask_map.any():
                map_streets_df.loc[rows_to_update_mask_map, 'eh_requerido'] = 'yes'
                map_streets_df.loc[rows_to_update_mask_map, 'demanda'] = 1
                
        except Exception as e:
            self.processing_error.emit(f"Worker: Erro na seleção em caixa (ruas): {e}")
            traceback.print_exc()

    @Slot(int, dict, bool, int)
    def on_add_node_at_street(self, street_id: int, click_coords: dict, is_depot: bool, service_cost: int):
        """
        Adiciona um nó e divide a rua associada em duas.
        """
        dfs = self._get_pipeline_state_dfs()
        if dfs[0] is None: return
        data_streets, data_pts, _, map_pts = dfs
        
        try:
            print(f"Worker: Recebida requisição para dividir a rua {street_id} em {click_coords}")

            if is_depot:
                # Verifica se já existe um depósito
                existing_depot_mask = data_pts['depot'] == 'yes'
                
                if existing_depot_mask.any():
                    old_depot_row = data_pts[existing_depot_mask].iloc[0]
                    old_depot_id = int(old_depot_row['node_index'])
                    
                    # Verifica se é temporário
                    v_idx = int(old_depot_row.get('vertex_index', -999))
                    is_temp_depot = (v_idx == -1)

                    if is_temp_depot:
                        # CASO 1: Depósito antigo é TEMPORÁRIO -> DELETAR
                        print(f"Worker: Removendo depósito temporário anterior {old_depot_id}...")
                        
                        self.pipeline.state = self.graph_editor.remove_node_and_merge_streets(
                            self.pipeline.state, old_depot_id
                        )
                        
                        # Atualiza referências locais
                        dfs = self._get_pipeline_state_dfs()
                        data_streets = dfs[0]
                        data_pts = dfs[1]
                        map_streets_ref = dfs[2]
                        map_pts = dfs[3]
                        
                        # Verifica se a rua clicada ainda existe após a fusão
                        if street_id not in data_streets['id'].values:
                            print(f"Worker: Rua {street_id} foi mesclada. Redescobrindo...")

                            temp_gdf = GeoFactory.to_gdf(map_streets_ref, self.pipeline.state.crs)

                            click_point = Point(click_coords['lon'], click_coords['lat'])
                            closest_idx = temp_gdf.distance(click_point).idxmin()
                            street_id = int(temp_gdf.loc[closest_idx, 'id'])
                            print(f"Worker: Nova rua alvo encontrada: {street_id}")

                    else:
                        # CASO 2: Depósito antigo é VERDADEIRO -> APENAS DESMARCAR
                        print(f"Worker: Desmarcando depósito verdadeiro anterior {old_depot_id}...")
                        
                        # Desmarca no GDF de DADOS (que será usado no split)
                        data_pts.loc[existing_depot_mask, 'depot'] = 'no'
                        
                        # Desmarca no GDF de MAPA (para consistência visual imediata)
                        map_pts.loc[map_pts['node_index'] == old_depot_id, 'depot'] = 'no'
            
            # Preparação do novo nó
            new_geom = Point(click_coords['lon'], click_coords['lat'])
            
            new_id = int(data_pts['node_index'].max()) + 1
            print(f"Worker: Novo ID do nó: {new_id}")
            
            req_val = 'no' if is_depot else 'yes'
            depot_val = 'yes' if is_depot else 'no'
            
            new_node_data = {
                'geometry': new_geom, 
                'from_line_id': street_id,
                'node_index': new_id, 
                'eh_requerido': req_val, 
                'depot': depot_val,
                'custo_servico': 0 if is_depot else service_cost,
                'demanda': 0 if is_depot else 1
            }
            
            temp_df = pd.DataFrame([new_node_data])
            temp_df = FieldsManager.ensure_fields_exist(temp_df, FieldConfigType.EXTENDED)
            new_node_row = temp_df.iloc[0]

            new_state = self.graph_editor.split_street(
                self.pipeline.state, street_id, new_node_row, is_depot
            )
            self.pipeline.state = new_state

            print(f"Worker: Divisão concluída. Total de nós (mapa): {len(self.pipeline.state.map_points)}")
            
            self.node_added_and_state_updated.emit(
                self.pipeline.state.map_streets, 
                self.pipeline.state.map_points
            )

        except Exception as e:
            self.processing_error.emit(f"Worker: Erro ao adicionar/dividir nó: {e}")
            traceback.print_exc()

    @Slot(str, int)
    def on_finalize_instance(self, run_name: str, current_db_id: int):
        """
        Finaliza a instância: Reindexa e Salva como Requerido.
        """
        try:
            if self.pipeline.state is None: return

            print("Worker: Iniciando finalização...")
            
            # Re-indexação Final
            self.pipeline.state = self.graph_editor.finalize_reindexing(self.pipeline.state)
            
            # Salva no Banco (Atualiza ou Cria)
            db_id_to_update = current_db_id if current_db_id != -1 else None
            
            final_run_id = self.pipeline.save_required_instance(run_name, db_id_to_update)
            
            print(f"Worker: Finalização concluída. ID: {final_run_id}")
            
            # Emite sucesso e atualiza a visualização
            self.node_added_and_state_updated.emit(
                self.pipeline.state.map_streets, 
                self.pipeline.state.map_points
            )
            self.finalization_complete.emit(final_run_id)

        except Exception as e:
            traceback.print_exc()
            self.processing_error.emit(f"Erro na finalização: {e}")

    @Slot(str, int, int)
    def on_generate_files(self, instance_name: str, capacity: int, vehicle_count: int):
        """
        Gera os arquivos .dat para MCGRP e MCGRP-TP.
        """
        try:
            if self.pipeline.state is None:
                self.processing_error.emit("Estado do grafo não disponível.")
                return

            print(f"Worker: Gerando arquivos para '{instance_name}'...")

            # Re-indexa
            self.pipeline.state = self.graph_editor.finalize_reindexing(self.pipeline.state)

            # MCGRP Padrão
            gen_mcgrp = MCGRPInstanceGenerator(self.pipeline.state)
            path_mcgrp = gen_mcgrp.generate_instance(instance_name, capacity, vehicle_count)
            
            # MCGRP com Turn Penalties
            gen_tp = MCGRPTPInstanceGenerator(self.pipeline.state)
            path_tp = gen_tp.generate_instance(instance_name, capacity, vehicle_count)
            
            # Mensagem de sucesso
            msg = (
                f"Arquivos gerados com sucesso na pasta 'instancias':\n\n"
                f"{Path(path_mcgrp).name}\n"
                f"{Path(path_tp).name}"
            )
            self.files_generated.emit(msg)
            print("Worker: Arquivos gerados.")

        except Exception as e:
            print(f"Erro ao gerar arquivos: {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Falha na geração dos arquivos: {e}")

    @Slot()
    def on_reduce_graph(self):
        """
        Executa a análise de conectividade e remove bairros irrelevantes.
        """
        if self.pipeline.state is None:
            self.processing_error.emit("Estado do grafo não disponível.")
            return

        try:
            print("Worker: Iniciando redução de grafo...")
            
            # Executa a Análise
            analyzer = ShortestPathAnalyzer(self.pipeline.state)
            kept_neighborhood_ids = analyzer.analyze_neighborhoods()
            
            if not kept_neighborhood_ids:
                print("Aviso: Análise retornou conjunto vazio. Mantendo tudo.")
                self.node_added_and_state_updated.emit(
                    self.pipeline.state.map_streets, self.pipeline.state.map_points
                )
                return

            print(f"Worker: Bairros a manter: {kept_neighborhood_ids}")

            # Filtra o Estado (GraphState)
            state = self.pipeline.state
            
            # Filtra Ruas (Dados e Mapa)
            # Mantém ruas cujo id_bairro esteja no conjunto de mantidos
            state.data_streets = state.data_streets[
                state.data_streets['id_bairro'].isin(kept_neighborhood_ids)
            ].copy().reset_index(drop=True)
            
            state.map_streets = state.map_streets[
                state.map_streets['id_bairro'].isin(kept_neighborhood_ids)
            ].copy().reset_index(drop=True)
            
            # Filtra Pontos (Dados e Mapa)
            valid_line_ids = set(state.data_streets['id'])
            
            state.data_points = state.data_points[
                state.data_points['from_line_id'].isin(valid_line_ids)
            ].copy().reset_index(drop=True)
            
            state.map_points = state.map_points[
                state.map_points['id_bairro'].isin(kept_neighborhood_ids)
            ].copy().reset_index(drop=True)

            # Filtra Bairros
            if state.neighborhoods is not None and not state.neighborhoods.empty:
                state.neighborhoods = state.neighborhoods[
                    state.neighborhoods['id_bairro'].isin(kept_neighborhood_ids)
                ].copy().reset_index(drop=True)

            # Remove nós folhas
            analyzer.prune_dead_ends()

            print(f"Worker: Grafo reduzido. Ruas restantes: {len(state.data_streets)}")

            # Atualiza o Estado (GraphState)
            self.pipeline.state = state

            # Atualiza a GUI
            self.node_added_and_state_updated.emit(
                self.pipeline.state.map_streets,
                self.pipeline.state.map_points
            )

        except Exception as e:
            print(f"Erro ao reduzir grafo: {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Falha na redução do grafo: {e}")