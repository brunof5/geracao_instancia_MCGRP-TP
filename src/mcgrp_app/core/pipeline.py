# src\mcgrp_app\core\pipeline.py

import pandas as pd
import geopandas as gpd
import traceback
from typing import Optional

from PySide6.QtCore import QObject, Signal, Slot

from .processing import GeoProcessor, PointExploder, LineStringSplitter
from .graph import ReducedGraphProcessor, GraphIndexer
from .utils import FieldsManager, FieldConfigType, GraphState
from ..persistence import FileManager, DataBaseManager

class GeoPipeline(QObject):
    """
    Orquestrador da Camada de Aplicação.
    """
    
    # Sinais para comunicar com a MainWindow

    # (passo_atual, total_passos, descrição)
    progress_update = Signal(int, int, str)
    # (gdf_ruas, gdf_pontos, mensagem, estatísticas)
    processing_complete = Signal(gpd.GeoDataFrame, gpd.GeoDataFrame, str, dict)
    # (mensagem)
    processing_error = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.state: Optional[GraphState] = None

        # Instâncias dos trabalhadores
        self.exploder = PointExploder()
        self.splitter = LineStringSplitter()
        self.processor: Optional[GeoProcessor] = None
        self.reducer: Optional[ReducedGraphProcessor] = None
        self.indexer = GraphIndexer()

        # Instância do banco de dados
        self.db_manager = DataBaseManager()

        # Descrição de cada passo do pré-processamento
        self.step_titles = [
            "Filtrando e normalizando...",
            "Explodindo pontos...",
            "Removendo pontos próximos de fronteiras...",
            "Dividindo ruas pelas interseções...",
            "Removendo pontos intermediários...",
            "Garantindo ruas com dois pontos...",
            "Mesclando ruas...",
            "Indexando grafo..."
        ]
        self.total_steps = len(self.step_titles)
    
    def _validate_inputs(self, streets_gdf: gpd.GeoDataFrame, neighborhoods_gdf: gpd.GeoDataFrame):
        """Valida os GDFs de entrada."""
        if not isinstance(streets_gdf, gpd.GeoDataFrame) or streets_gdf.empty:
            raise ValueError("GeoDataFrame de Ruas é inválido ou está vazio.")
        if not isinstance(neighborhoods_gdf, gpd.GeoDataFrame) or neighborhoods_gdf.empty:
            raise ValueError("GeoDataFrame de Bairros é inválido ou está vazio.")
        print("Validação de entrada do pipeline concluída com sucesso.")
    
    def _preformat_tooltips(self, state: GraphState) -> GraphState:
        """Helper para pré-formatar o HTML do(s) tooltip(s)."""

        # Garante que as colunas de estado existam antes de formatar
        if state.map_streets is not None and 'eh_requerido' not in state.map_streets.columns:
            state.map_streets['eh_requerido'] = 'no'
        
        # --- Ruas ---
        if state.map_streets is not None:
            state.map_streets['total_dist_fmt'] = state.map_streets['total_dist'].apply(
                lambda x: f"{x:.3f} km" if pd.notna(x) else "N/A"
            )
            
            def format_rua_html(row):
                oneway = (row.get('oneway', 'no') or 'no').lower()
                if oneway in ['yes', '1', 'true']:
                    header = f"<b>Arco:</b> {row['arc_index']} (De: {row['from_node']}, Para: {row['to_node']})"
                else:
                    header = f"<b>Aresta:</b> {row['edge_index']}"
                
                rua = row.get('name', 'desconhecida')
                bairro = row.get('bairro', 'N/A')
                dist = row.get('total_dist_fmt', 'N/A')
                custo_val = row.get('custo_travessia')
                custo = f"{custo_val} s" if pd.notna(custo_val) else "N/A"
                
                return (
                    f"{header}"
                    f"<br><b>Rua:</b> {rua}"
                    f"<br><b>Bairro:</b> {bairro}"
                    f"<br><b>Comprimento:</b> {dist}"
                    f"<br><b>Custo Travessia:</b> {custo}"
                )
            
            state.map_streets['tooltip_html'] = state.map_streets.apply(format_rua_html, axis=1)

        # --- Pontos ---
        if state.map_points is not None:
            if 'eh_requerido' not in state.map_points.columns:
                state.map_points['eh_requerido'] = 'no'
            if 'depot' not in state.map_points.columns:
                state.map_points['depot'] = 'no'
            
            def format_no_html(row):
                node_idx = int(row['node_index']) if pd.notna(row.get('node_index')) else '?'
                custo_serv = int(row['custo_servico']) if pd.notna(row.get('custo_servico')) else 0
                
                return (
                    f"<b>Nó:</b> {node_idx}"
                    f"<br><b>Custo de serviço:</b> {custo_serv}s"
                )

            state.map_points['tooltip_html'] = state.map_points.apply(format_no_html, axis=1)
            
        return state
    
    def export_data(self, filename: str, field_config_type: FieldConfigType):
        """
        Exporta os GDFs do estado atual para arquivos GeoPackage.
        """
        print(f"Iniciando exportação para '{filename}'...")
        
        try:
            field_config = FieldsManager.get_field_config(field_config_type)
        except Exception as e:
            print(f"  Aviso: Falha ao obter field_config: {e}. Exportando todos os campos.")
            field_config = None
        
        # Prepara os datasets de LÓGICA (data)
        data_datasets = {
            "streets": self.state.data_streets,
            "points": self.state.data_points
        }
        
        # Prepara os datasets de VISUALIZAÇÃO (map)
        map_datasets = {
            "streets": self.state.map_streets,
            "points": self.state.map_points
        }
        
        # Chama o FileManager para exportar
        try:
            FileManager.export_to_geopackage(
                data_datasets, 
                f"{filename}_data", 
                field_config
            )
            
            FileManager.export_to_geopackage(
                map_datasets, 
                f"{filename}_map", 
                field_config
            )
        except Exception as e:
            print(f"  Erro durante a exportação de depuração: {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Falha ao exportar dados de depuração: {e}")
    
    def _save_to_database(self, state: GraphState, status: str, run_name: str) -> GraphState:
        """
        Usa o FileManager para salvar os GDFs (e bairros) em arquivos .gpkg
        e usa o DBManager para registrar esses arquivos no catálogo.
        """

        # Garante run_name único
        run_name = self._ensure_unique_run_name(run_name)

        # Define os caminhos de arquivo (usando o diretório do DBManager)
        data_gpkg_path = self.db_manager.RUNS_DIR / f"{run_name}_data.gpkg"
        map_gpkg_path = self.db_manager.RUNS_DIR / f"{run_name}_map.gpkg"
        neigh_gpkg_path = self.db_manager.RUNS_DIR / f"{run_name}_neighborhoods.gpkg"
        
        # Prepara os datasets
        field_config = FieldsManager.get_field_config(FieldConfigType.EXTENDED)
        data_datasets = {
            "streets": state.data_streets, 
            "points": state.data_points
        }
        map_datasets = {
            "streets": state.map_streets, 
            "points": state.map_points
        }
        neigh_dataset = {
            "neighborhoods": state.neighborhoods
        }
        
        # Salva os arquivos .gpkg no disco
        print(f"  Salvando GDFs em {self.db_manager.RUNS_DIR}...")
        try:
            FileManager.export_to_geopackage(
                data_datasets, str(data_gpkg_path).replace(".gpkg", ""), field_config
            )
            FileManager.export_to_geopackage(
                map_datasets, str(map_gpkg_path).replace(".gpkg", ""), field_config
            )
            FileManager.export_to_geopackage(
                neigh_dataset, str(neigh_gpkg_path).replace(".gpkg", ""), 
                {"Polygon": ["id_bairro", "bairro", "geometry"]}
            )
        except Exception as e:
            print(f"  Erro crítico ao salvar arquivos GPKG: {e}")
            raise
        
        # Salva o registro no catálogo do banco de dados
        self.db_manager.save_processed_run(
            run_name, 
            str(data_gpkg_path), 
            str(map_gpkg_path), 
            str(neigh_gpkg_path)
        )
        
        return state
    
    def _ensure_unique_run_name(self, base_name: str) -> str:
        """
        Retorna um run_name único dentro de `directory`.
        Se `base_name` já existir como prefixo de algum arquivo .gpkg,
        acrescenta um sufixo numérico incremental.
        """
        directory = self.db_manager.RUNS_DIR
        
        # Arquivos possíveis que o run vai gerar
        targets = [
            f"{base_name}_data.gpkg",
            f"{base_name}_map.gpkg",
            f"{base_name}_neighborhoods.gpkg"
        ]

        # Se nenhum arquivo existe, pode usar o nome original
        if not any((directory / t).exists() for t in targets):
            return base_name

        # Caso exista, gera "base_name1", "base_name2", ...
        counter = 1
        while True:
            candidate = f"{base_name}{counter}"
            candidate_targets = [
                f"{candidate}_data.gpkg",
                f"{candidate}_map.gpkg",
                f"{candidate}_neighborhoods.gpkg"
            ]
            if not any((directory / t).exists() for t in candidate_targets):
                return candidate
            counter += 1
    
    def save_required_instance(self, run_name: str, existing_run_id: Optional[int] = None) -> int:
        """
        Salva o estado atual como uma instância 'requerida'.
        Se existing_run_id for fornecido, atualiza os arquivos existentes.
        Caso contrário, cria um novo registro.
        """
        print(f"Pipeline: Salvando instância requerida '{run_name}'...")

        if existing_run_id:
            try:
                original_name = self.db_manager.get_run_name(existing_run_id)
                print(f"Pipeline: Atualizando instância '{original_name}' (ID: {existing_run_id})...")
                run_name = original_name
            except Exception as e:
                print(f"Pipeline: Falha ao recuperar nome original: {e}. Usando '{run_name}'.")
        
        print(f"Pipeline: Salvando arquivos para '{run_name}'...")
        
        # Define nomes de arquivo com sufixo _req
        base_name = run_name
        if not base_name.endswith("_req") and existing_run_id is None:
            base_name = f"{run_name}_req"
            
        # Define caminhos
        data_gpkg_path = self.db_manager.RUNS_DIR / f"{base_name}_data.gpkg"
        map_gpkg_path = self.db_manager.RUNS_DIR / f"{base_name}_map.gpkg"
        neigh_gpkg_path = self.db_manager.RUNS_DIR / f"{base_name}_neighborhoods.gpkg"
        
        # Prepara datasets (igual ao save normal)
        field_config = FieldsManager.get_field_config(FieldConfigType.EXTENDED)
        data_datasets = {"streets": self.state.data_streets, "points": self.state.data_points}
        map_datasets = {"streets": self.state.map_streets, "points": self.state.map_points}
        neigh_dataset = {"neighborhoods": self.state.neighborhoods}
        
        # Salva arquivos
        try:
            FileManager.export_to_geopackage(data_datasets, str(data_gpkg_path).replace(".gpkg", ""), field_config)
            FileManager.export_to_geopackage(map_datasets, str(map_gpkg_path).replace(".gpkg", ""), field_config)
            FileManager.export_to_geopackage(neigh_dataset, str(neigh_gpkg_path).replace(".gpkg", ""), {"Polygon": ["id_bairro", "bairro", "geometry"]})
        except Exception as e:
            print(f"  Erro crítico ao salvar arquivos GPKG: {e}")
            raise

        # Atualiza ou Cria no DB
        if existing_run_id:
            print(f"Pipeline: Atualizando registro existente ID {existing_run_id}...")
            self.db_manager.update_run_paths(
                existing_run_id, 
                str(data_gpkg_path), 
                str(map_gpkg_path), 
                str(neigh_gpkg_path),
                status='requerido'
            )
            return existing_run_id
        else:
            print("Pipeline: Criando novo registro de instância requerida...")
            new_id = self.db_manager.save_processed_run(
                base_name, 
                str(data_gpkg_path), 
                str(map_gpkg_path), 
                str(neigh_gpkg_path),
                status='requerido'
            )
            return new_id
    
    # --- SLOTS (Métodos que respondem a eventos) ---
    
    @Slot(gpd.GeoDataFrame, gpd.GeoDataFrame, str)
    def start_processing(self, streets_raw_gdf, neighborhoods_raw_gdf, base_run_name: str):
        """
        Ponto de entrada do pipeline.
        Cria o estado e o passa pela cadeia de processamento.
        """
        # Armazena as contagens
        stats = {}

        try:
            print("Pipeline: Processamento iniciado.")
            self._validate_inputs(streets_raw_gdf, neighborhoods_raw_gdf)
            stats['before_s'] = len(streets_raw_gdf)        # Contagem "Antes" (Ruas)

            # Cria o objeto GraphState inicial
            self.state = GraphState(
                data_streets=streets_raw_gdf.copy(),
                map_streets=streets_raw_gdf.copy(),
                neighborhoods=neighborhoods_raw_gdf,
                data_points=None,
                map_points=None
            )

            # Inicializa os trabalhadores que dependem de outros dados (ex.: bairros)
            self.processor = GeoProcessor(self.state.neighborhoods)
            self.reducer = ReducedGraphProcessor(self.state.neighborhoods)

            # --- INÍCIO DO PRÉ-PROCESSAMENTO ---
            
            print("\n--- INICIANDO PASSO 1 (Processamento de Ruas) ---")
            self.progress_update.emit(1, self.total_steps, self.step_titles[0])
            self.state = self.processor.filter_and_normalize(self.state)
            self.state = self.processor.process_neighborhood_boundaries(self.state)
            print("--- PASSO 1 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 2 (Explosão de Pontos) ---")
            self.progress_update.emit(2, self.total_steps, self.step_titles[1])
            self.state = self.exploder.explode_and_label(self.state)
            stats['before_n'] = len(self.state.data_points)     # Contagem "Antes" (Nós)
            print("--- PASSO 2 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 3 (Remover Extremidades) ---")
            self.progress_update.emit(3, self.total_steps, self.step_titles[2])
            self.state = self.processor.remove_invalid_endpoints(self.state)
            print("--- PASSO 3 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 4 (Dividir Ruas pelas Interseções) ---")
            self.progress_update.emit(4, self.total_steps, self.step_titles[3])
            self.state = self.splitter.split_by_special_vertices(self.state, split_on_united=True)
            print("--- PASSO 4 CONCLUÍDO ---")
            
            print("\n--- INICIANDO PASSO 5 (Reduzir Grafo) ---")
            self.progress_update.emit(5, self.total_steps, self.step_titles[4])
            self.state = self.reducer.create_reduced_graph(self.state)
            print("--- PASSO 5 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 6 (Garantir Ruas com 2 Pontos) ---")
            self.progress_update.emit(6, self.total_steps, self.step_titles[5])
            self.state = self.splitter.split_into_two_point_segments(self.state)
            print("--- PASSO 6 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 7 (Mesclar Ruas de Fronteiras) ---")
            self.progress_update.emit(7, self.total_steps, self.step_titles[6])
            self.state = self.reducer.remove_boundary_vertices(self.state)
            print("--- PASSO 7 CONCLUÍDO ---")

            print("\n--- INICIANDO PASSO 8 (Indexar Grafo) ---")
            self.progress_update.emit(8, self.total_steps, self.step_titles[7])
            self.state = self.indexer.assign_indices(self.state)
            print("--- PASSO 8 CONCLUÍDO ---")

            # --- FIM DO PRÉ-PROCESSAMENTO ---

            print("\nPipeline: Garantindo a existência de demais colunas nos GDFs...")
            self.state.data_streets = FieldsManager.ensure_fields_exist(
                self.state.data_streets, FieldConfigType.EXTENDED
            )

            self.state.data_points = FieldsManager.ensure_fields_exist(
                self.state.data_points, FieldConfigType.EXTENDED
            )

            self.state.map_streets = FieldsManager.ensure_fields_exist(
                self.state.map_streets, FieldConfigType.EXTENDED
            )

            self.state.map_points = FieldsManager.ensure_fields_exist(
                self.state.map_points, FieldConfigType.EXTENDED
            )

            # Coleta estatísticas "Depois"
            stats['after_s'] = len(self.state.data_streets)
            stats['after_n'] = len(self.state.data_points)

            print("\nPipeline: Pré-formatando colunas de tooltip...")
            self.state = self._preformat_tooltips(self.state)

            print("\nPipeline: Salvando no Banco de Dados...")
            self.state = self._save_to_database(self.state, "processado", base_run_name)

            # Emite os GDFs de mapa para a GUI exibir
            self.processing_complete.emit(
                self.state.map_streets,
                self.state.map_points,
                "Processamento base concluído e salvo.",
                stats
            )
        
        except Exception as e:
            print(f"Pipeline: Erro no processamento - {e}")
            traceback.print_exc()
            self.processing_error.emit(f"Erro no pipeline: {e}")