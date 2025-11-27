# src\mcgrp_app\gui\main_window.py

import traceback
import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import Optional
from shapely.geometry import box

from PySide6.QtCore import Qt, Slot, Signal, QThread, QObject
from PySide6.QtGui import QAction, QGuiApplication, QCloseEvent
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWidgets import (
    QMainWindow,  QMessageBox, QFileDialog, QListWidgetItem, 
    QDockWidget, QProgressDialog, QDialog, QListWidget, 
    QWidget, QVBoxLayout, QGroupBox, QRadioButton, 
    QCheckBox, QLabel, QButtonGroup, QAbstractButton, 
    QPushButton, QInputDialog
)

from .load_from_db_dialog import LoadFromDBDialog
from .widgets.map_widget import MapWidget
from ..persistence import FileManager
from .worker import PipelineWorker
from ..core.utils import GraphState, GeoFactory

# --- CLASSE DA PONTE (JS <-> Python) ---
class Bridge(QObject):
    """
    Objeto que será exposto ao JavaScript.
    Ele vive na thread principal (GUI) e recebe sinais do JS.
    """
    # Sinal (layerName, id, click_coords_dict)
    map_clicked = Signal(str, int, dict)
    js_ready = Signal()
    box_selected = Signal(dict)

    @Slot(str, int, dict)
    def on_map_clicked(self, layerName: str, feature_id: int, click_coords: dict):
        """Slot que o JavaScript chama."""
        print(f"Clique recebido do JS: Camada='{layerName}', ID={feature_id}, Coords={click_coords}")
        self.map_clicked.emit(layerName, feature_id, click_coords)

    @Slot()
    def on_js_ready(self):
        """Slot que o JS chama quando está pronto."""
        print("Ponte: JavaScript está carregado e pronto.")
        self.js_ready.emit()

    @Slot(str)
    def on_js_log(self, message: str):
        """Recebe logs do JavaScript (console) e imprime no terminal Python."""
        print("JS:", message)

    @Slot(dict)
    def on_box_select(self, bounds_dict):
        """Slot que o JS chama quando um retângulo é desenhado."""
        print(f"Seleção em caixa recebida do JS: {bounds_dict}")
        self.box_selected.emit(bounds_dict)

class MainWindow(QMainWindow):
    """
    Janela principal da aplicação MCGRP.
    """
    # Sinal para iniciar o processamento no worker
    start_processing_signal = Signal(pd.DataFrame, pd.DataFrame, str)
    # Sinal para atualizar o estado interno do grafo
    load_state_signal = Signal(GraphState)

    # --- Sinais GUI para Worker ---
    toggle_street_request = Signal(int)             # (street_id)
    toggle_node_request = Signal(int, int)          # (node_id, service_cost)
    set_depot_request = Signal(int)                 # (node_id)
    box_select_streets_request = Signal(object)     # (shapely.geometry.box)
    add_node_request = Signal(int, dict, bool, int) # (street_id, click_coords, is_depot, service_cost)
    finalize_request = Signal(str, int)             # (run_name, current_id)
    generate_files_request = Signal(str, int, int)  # (name, capacity, vehicle_count)
    reduce_graph_request = Signal()
    
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("MCGRP(TP) - Ferramenta de Geração de Instâncias")
        self.resize(1200, 800)      # largura, altura
        self._center_on_screen()

        # Instanciar as camadas de lógica
        self.file_manager = FileManager(self)
        self.processing_thread = QThread(self)      # Cria a thread
        self.pipeline_worker = PipelineWorker()     # Cria o worker
        # Move o worker para a thread
        self.pipeline_worker.moveToThread(self.processing_thread)

        # Criação da ponte e do canal
        self.bridge = Bridge(self)
        self.web_channel = QWebChannel(self)
        # Expõe o objeto 'self.bridge' ao JS sob o nome 'py_bridge_name'
        self.web_channel.registerObject("py_bridge_name", self.bridge)

        self.progress_dialog: Optional[QProgressDialog] = None

        # Armazenar os dados processados (cópias locais para a GUI)
        self.processed_streets_df: Optional[pd.DataFrame] = None
        self.processed_points_df: Optional[pd.DataFrame] = None

        # Referências da GUI
        self.selection_group_box: Optional[QGroupBox] = None
        self.details_dock: Optional[QDockWidget] = None
        self.details_list_widget: Optional[QListWidget] = None
        self.finalize_btn: Optional[QPushButton] = None
        self.mode_dock: Optional[QDockWidget] = None

        self.radio_select_edges: Optional[QRadioButton] = None
        self.radio_select_nodes: Optional[QRadioButton] = None
        self.radio_select_depot: Optional[QRadioButton] = None

        self.check_show_bairros: Optional[QCheckBox] = None

        self.pointer_mode_group: Optional[QButtonGroup] = None
        self.radio_pointer_mode: Optional[QRadioButton] = None
        self.radio_box_select_mode: Optional[QRadioButton] = None

        self.current_depot_info: Optional[dict] = None

        self.current_loaded_run_id: int = -1

        self._setup_ui()
        self._connect_signals()

        # Inicia a thread
        self.processing_thread.start()

        self._set_interaction_enabled(False)

    def _center_on_screen(self):
        """Centraliza a janela principal no monitor primário."""
        try:
            # Obtém a geometria da tela primária
            screen_geometry = QGuiApplication.primaryScreen().geometry()
            
            # Calcula o ponto (x, y) para centralizar a janela
            x = (screen_geometry.width() - self.width()) / 2
            y = (screen_geometry.height() - self.height()) / 2
            
            # Move a janela para a posição calculada
            self.move(int(x), int(y))
        except Exception as e:
            # Fallback caso ocorra algum erro
            print(f"Aviso: Não foi possível centralizar a janela. {e}")
            self.move(100, 100)

    def _setup_ui(self):
        """Inicializa os componentes da interface do usuário."""
        self._create_actions()
        self._setup_menu_bar()
        self._setup_status_bar()
        self._setup_central_widget()
        self._setup_docks()

    def _create_actions(self):
        """Cria as ações reutilizáveis da aplicação."""

        self.load_shp_action = QAction("Carregar Bairros (.shp/.zip)...", self)
        self.load_shp_action.setStatusTip("Carregar camada de polígonos de bairros")
        
        self.load_gpkg_action = QAction("Carregar Ruas (.gpkg)...", self)
        self.load_gpkg_action.setStatusTip("Carrega uma camada de ruas (GeoPackage)")

        self.load_db_action = QAction("Carregar Mapa Base...", self)
        self.load_db_action.setStatusTip("Carrega um registro 'processado' salvo")

        self.load_instance_action = QAction("Carregar Instância...", self)
        self.load_instance_action.setStatusTip("Carrega uma instância para edição")

        self.exit_action = QAction("Sair", self)
        self.exit_action.setStatusTip("Fecha a aplicação")

        self.reduce_graph_action = QAction("Reduzir Grafo", self)
        self.reduce_graph_action.setStatusTip("Reduz o grafo")
        
        self.create_files_action = QAction("Criar Arquivos a Partir da Instância", self)
        self.create_files_action.setStatusTip("Gera arquivos .dat para os solvers MCGRP e MCGRP-TP")

    def _setup_menu_bar(self):
        """Cria e popula a barra de menus."""
        menu_bar = self.menuBar()
        
        # Menu "Arquivo"
        file_menu = menu_bar.addMenu("Arquivo")
        file_menu.addAction(self.load_shp_action)
        file_menu.addAction(self.load_gpkg_action)
        file_menu.addSeparator()
        file_menu.addAction(self.load_db_action)
        file_menu.addAction(self.load_instance_action)
        file_menu.addSeparator()
        file_menu.addAction(self.exit_action)

        # Menu "Ferramentas"
        tools_menu = menu_bar.addMenu("Ferramentas")
        tools_menu.addAction(self.reduce_graph_action)
        tools_menu.addAction(self.create_files_action)

        # (Pode-se adicionar outros menus como "Editar", "Ver", "Ajuda")

    def _connect_signals(self):
        """Conecta os sinais das ações aos seus respectivos slots (métodos) da aplicação."""
        # Ações de Menu
        self.load_gpkg_action.triggered.connect(self._on_load_gpkg_triggered)
        self.load_shp_action.triggered.connect(self._on_load_shp_triggered)
        self.exit_action.triggered.connect(self.close)

        # Sinais do DataBaseManager
        self.load_db_action.triggered.connect(self._on_load_from_db_triggered)
        self.load_instance_action.triggered.connect(self._on_load_instance_triggered)

        # Ferramentas
        self.reduce_graph_action.triggered.connect(self._on_reduce_graph_triggered)
        self.create_files_action.triggered.connect(self._on_create_files_triggered)

        # Sinais vindos do FileManager
        self.file_manager.streets_loaded.connect(self._on_streets_loaded)
        self.file_manager.neighborhoods_loaded.connect(self._on_neighborhoods_loaded)
        self.file_manager.error_occurred.connect(self._on_load_error)

        # Conecta o sinal da MainWindow ao slot do Worker
        self.start_processing_signal.connect(self.pipeline_worker.run_pipeline_processing)
        self.load_state_signal.connect(self.pipeline_worker.set_pipeline_state)

        # Conecta o sinal do estado do grafo ao slot do Worker
        self.toggle_street_request.connect(self.pipeline_worker.on_toggle_street)
        self.toggle_node_request.connect(self.pipeline_worker.on_toggle_node)
        self.set_depot_request.connect(self.pipeline_worker.on_set_depot)
        self.box_select_streets_request.connect(self.pipeline_worker.on_box_select_streets)
        self.add_node_request.connect(self.pipeline_worker.on_add_node_at_street)
        
        # Conecta os sinais do Worker aos slots da MainWindow
        self.pipeline_worker.progress_update.connect(self._on_progress_update)
        self.pipeline_worker.processing_complete.connect(self._on_processing_complete)
        self.pipeline_worker.processing_error.connect(self._on_load_error)

        # Slots para Toggles
        self.pipeline_worker.street_toggled.connect(self._on_street_toggled)
        self.pipeline_worker.node_toggled.connect(self._on_node_toggled)
        self.pipeline_worker.depot_changed.connect(self._on_depot_changed)

        self.pipeline_worker.node_added_and_state_updated.connect(self._on_node_added_and_state_updated)

        # Geração de Arquivos
        self.generate_files_request.connect(self.pipeline_worker.on_generate_files)

        # Conecta os sinais da ponte
        self.bridge.map_clicked.connect(self._on_map_clicked)
        self.bridge.box_selected.connect(self._on_box_select)
        self.bridge.js_ready.connect(self._on_js_ready)

        # UI Elements
        if self.radio_select_edges and self.radio_select_nodes and self.radio_select_depot:
            self.radio_select_edges.toggled.connect(self._on_selection_mode_changed)
            self.radio_select_nodes.toggled.connect(self._on_selection_mode_changed)
            self.radio_select_depot.toggled.connect(self._on_selection_mode_changed)

        if self.check_show_bairros:
            self.check_show_bairros.toggled.connect(self._on_bairros_visibility_changed)

        if self.details_list_widget:
            self.details_list_widget.itemClicked.connect(self._on_details_item_clicked)

        if self.pointer_mode_group:
            self.pointer_mode_group.buttonToggled.connect(self._on_pointer_mode_changed)

        if self.finalize_btn:
            self.finalize_btn.clicked.connect(self._on_finalize_clicked)

        # Finalização
        self.finalize_request.connect(self.pipeline_worker.on_finalize_instance)
        self.pipeline_worker.finalization_complete.connect(self._on_finalization_complete)

        # Arquivos gerados
        self.pipeline_worker.files_generated.connect(self._on_files_generated)

        # Reduzir grafo
        self.reduce_graph_request.connect(self.pipeline_worker.on_reduce_graph)
        
        # Conecta o fim da thread para limpeza
        self.processing_thread.finished.connect(self.pipeline_worker.deleteLater)
    
    def _setup_status_bar(self):
        """Configura a barra de status inferior."""
        self.statusBar().showMessage("Carregue o arquivo dos bairros (.shp/.zip) para começar.")

    def _setup_central_widget(self):
        """
        Define o widget central (mapa) e anexa o QWebChannel.
        """
        # Cria o MapWidget
        self.map_widget = MapWidget()
        
        # Obtém a Page interna do widget
        page = self.map_widget.get_page()
        
        # Define o canal na página
        page.setWebChannel(self.web_channel)
        
        self.setCentralWidget(self.map_widget)

    def _setup_docks(self):
        """
        Configura painéis laterais (docks).
        - Esquerda: Para detalhes/informações.
        - Direita: Para o modo de seleção.
        """
        # --- DOCK DA ESQUERDA ---

        # Para informar detalhes sobre a seleção de ruas, depósito, etc
        details_dock = QDockWidget("Detalhes da Seleção", self)
        details_dock.setAllowedAreas(Qt.DockWidgetArea.LeftDockWidgetArea)

        # Widget container para o dock da esquerda
        details_container = QWidget()
        details_layout = QVBoxLayout(details_container)
        details_layout.setContentsMargins(0, 0, 0, 0)
        
        self.details_list_widget = QListWidget()
        self.details_list_widget.setWordWrap(True)
        details_layout.addWidget(self.details_list_widget)

        # Botão Finalizar
        self.finalize_btn = QPushButton("Finalizar")
        self.finalize_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 5px;")
        details_layout.addWidget(self.finalize_btn)

        details_dock.setWidget(details_container)
        self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, details_dock)

        # Armazena a referência
        self.details_dock = details_dock

        # --- DOCK DA DIREITA ---

        mode_dock = QDockWidget("Modo de Seleção", self)
        mode_dock.setAllowedAreas(Qt.DockWidgetArea.RightDockWidgetArea)
        
        # Widget de conteúdo e layout
        dock_content_widget = QWidget()
        dock_layout = QVBoxLayout(dock_content_widget)

        # Grupo para os Botões de Rádio
        self.selection_group_box = QGroupBox()
        group_layout = QVBoxLayout()
        
        self.radio_select_edges = QRadioButton("Selecionar Ruas")
        self.radio_select_nodes = QRadioButton("Selecionar/Inserir Nós")
        self.radio_select_depot = QRadioButton("Selecionar/Inserir Depósito")
        
        self.radio_select_edges.setChecked(True)
        
        group_layout.addWidget(self.radio_select_edges)
        group_layout.addWidget(self.radio_select_nodes)
        group_layout.addWidget(self.radio_select_depot)
        self.selection_group_box.setLayout(group_layout)
        dock_layout.addWidget(self.selection_group_box)

        self.check_show_bairros = QCheckBox("Exibir Bairros")
        self.check_show_bairros.setChecked(False)
        dock_layout.addWidget(self.check_show_bairros)

        # Modo de ponteiro
        pointer_group_box = QGroupBox("Ferramenta do Ponteiro")
        pointer_layout = QVBoxLayout()
        
        self.radio_pointer_mode = QRadioButton("Ponteiro (Arrastar/Clicar)")
        self.radio_box_select_mode = QRadioButton("Seleção em Caixa (Arrastar)")
        self.radio_pointer_mode.setChecked(True)
        
        # Usa um QButtonGroup para gerenciá-los
        self.pointer_mode_group = QButtonGroup(self)
        self.pointer_mode_group.addButton(self.radio_pointer_mode, 1)           # ID 1 = 'pan'
        self.pointer_mode_group.addButton(self.radio_box_select_mode, 2)        # ID 2 = 'box'

        pointer_layout.addWidget(self.radio_pointer_mode)
        pointer_layout.addWidget(self.radio_box_select_mode)
        pointer_group_box.setLayout(pointer_layout)
        dock_layout.addWidget(pointer_group_box)
        
        dock_layout.addStretch()        # Empurra para o topo
        mode_dock.setWidget(dock_content_widget)
        
        self.addDockWidget(Qt.DockWidgetArea.RightDockWidgetArea, mode_dock)

        # Armazena a referência
        self.mode_dock = mode_dock

        self.details_dock.setVisible(False)
        self.mode_dock.setVisible(False)

    def closeEvent(self, event: QCloseEvent):
        """
        Limpa a thread e os arquivos temporários ao fechar.
        """
        print("Fechando a aplicação...")

        self.processing_thread.quit()               # Pede para a thread parar
        self.processing_thread.wait(5000)           # Espera até 5 seg
        
        if self.processing_thread.isRunning():
            print("Aviso: A thread de processamento não parou a tempo. Terminando...")
            self.processing_thread.terminate()      # Força o término
        else:
            print("Thread de processamento parada com sucesso.")

        print("Limpando arquivos temporários...")
        if self.map_widget:
            self.map_widget.cleanup()
        
        event.accept()

    def _set_interaction_enabled(self, enabled: bool):
        """Habilita ou desabilita a interação com controles de mapa."""
        if self.mode_dock: self.mode_dock.setEnabled(enabled)
        if self.details_dock: self.details_dock.setEnabled(enabled)

    def _ask_service_cost(self) -> Optional[int]:
        """Abre diálogo para pedir custo de serviço."""
        cost, ok = QInputDialog.getInt(
            self, 
            "Custo de Serviço", 
            "Tempo de atendimento (segundos):", 
            0,           # value (padrão)
            0,           # minValue
            100000,      # maxValue
            1            # step
        )
        if ok:
            return cost
        return None
    
    def _load_state_from_run_id(self, run_id: int):
        """Helper para carregar os GDFs, converter para DF e enviar ao worker."""
        try:
            paths = self.pipeline_worker.pipeline.db_manager.get_run_paths(run_id)
            
            neigh_gdf = self.file_manager.load_gpkg_layer(paths['neighborhoods'], "neighborhoods")
            ms_gdf = self.file_manager.load_gpkg_layer(paths['map'], "streets")
            mp_gdf = self.file_manager.load_gpkg_layer(paths['map'], "points")
            ds_gdf = self.file_manager.load_gpkg_layer(paths['data'], "streets")
            dp_gdf = self.file_manager.load_gpkg_layer(paths['data'], "points")
            
            if any(x is None for x in [neigh_gdf, ms_gdf, mp_gdf, ds_gdf, dp_gdf]):
                raise ValueError("Falha ao ler camadas do arquivo.")
            
            self.file_manager.neighborhoods_gdf = neigh_gdf

            # Conversão para DataFrames
            state_crs = ms_gdf.crs if ms_gdf.crs else GeoFactory.DEFAULT_CRS

            ds_df = GeoFactory.from_gdf(ds_gdf)
            dp_df = GeoFactory.from_gdf(dp_gdf)
            ms_df = GeoFactory.from_gdf(ms_gdf)
            mp_df = GeoFactory.from_gdf(mp_gdf)
            neigh_df = GeoFactory.from_gdf(neigh_gdf)
            
            # Envia para o worker inicializar o estado
            new_state = GraphState(
                data_streets=ds_df, 
                data_points=dp_df, 
                map_streets=ms_df, 
                map_points=mp_df, 
                neighborhoods=neigh_df,
                crs=state_crs
            )
            
            self._set_interaction_enabled(False)
            self.load_state_signal.emit(new_state)
            
        except Exception as e:
            self._on_load_error(f"Falha ao carregar registro: {e}")
            traceback.print_exc()
    
    # --- SLOTS (Métodos que respondem a eventos) ---

    @Slot()
    def _on_js_ready(self):
        """Chamado quando o mapa terminou de carregar o JS."""
        print("GUI: Mapa pronto. Habilitando interação.")
        self._set_interaction_enabled(True)
    
    @Slot()
    def _on_load_gpkg_triggered(self):
        """
        Chamado quando a ação 'Carregar Ruas' é disparada.
        Abre o diálogo para seleção do arquivo de ruas (GPKG), 
        se os bairros já estiverem carregados.
        """
        if self.file_manager.neighborhoods_gdf is None:
            QMessageBox.warning(
                self, 
                "Aviso de Pré-requisito", 
                "Por favor, carregue a camada de Bairros (.shp/.zip) primeiro."
            )
            return
        
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecionar GeoPackage de Ruas",
            "",     # Diretório inicial
            "Arquivos GeoPackage (*.gpkg);;Todos os arquivos (*)"
        )

        if file_path:
            self.statusBar().showMessage(f"Carregando ruas: {file_path} ...")

            # Delega o trabalho pesado para o file_manager
            self.file_manager.load_geopackage_streets(file_path)

    @Slot()
    def _on_load_shp_triggered(self):
        """
        Chamado quando a ação 'Carregar Bairros' é disparada.
        Abre o diálogo para carregar bairros (SHP/ZIP).
        """
        file_path, _ = QFileDialog.getOpenFileName(
            self, 
            "Selecionar Shapefile de Bairros", 
            "", 
            "Arquivos Zip (*.zip);;Arquivos Shapefile (*.shp);;Todos os arquivos (*)"
        )

        if file_path:
            self.statusBar().showMessage(f"Carregando bairros: {file_path} ...")
            
            # Delega o trabalho pesado para o file_manager
            self.file_manager.load_shapefile_neighborhoods(file_path)

    @Slot()
    def _on_load_from_db_triggered(self):
        """
        Carrega do DB, atualiza o estado e mostra os docks.
        """
        # Instancia o diálogo e passa os gerenciadores
        dialog = LoadFromDBDialog(
            self.pipeline_worker.pipeline.db_manager, 
            self.file_manager, 
            self,
            status_filter='processado'
        )
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            run_id = dialog.selected_run_id
            if run_id is None:
                self._on_load_error("Nenhum registro foi selecionado.")
                return
            
            print(f"Carregando registro ID: {run_id} do banco de dados...")
            self.statusBar().showMessage(f"Carregando registro ID: {run_id}...")

            self._load_state_from_run_id(run_id)

            self.current_loaded_run_id = -1
            self.statusBar().showMessage(f"Mapa base {run_id} carregado com sucesso.", 5000)

            if self.details_dock: self.details_dock.setVisible(True)
            if self.mode_dock: self.mode_dock.setVisible(True)
    
    @Slot()
    def _on_load_instance_triggered(self):
        """Carrega Instância e atualiza registro ao finalizar."""
        dialog = LoadFromDBDialog(
            self.pipeline_worker.pipeline.db_manager, 
            self.file_manager, 
            self, 
            status_filter='requerido'
        )
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            run_id = dialog.selected_run_id
            if not run_id: return
            
            self._load_state_from_run_id(run_id)
            
            self.current_loaded_run_id = run_id

            if self.details_dock: self.details_dock.setVisible(True)
            if self.mode_dock: self.mode_dock.setVisible(True)
            
            self.statusBar().showMessage(f"Instância {run_id} carregada para edição.")
    
    @Slot(gpd.GeoDataFrame, str)
    def _on_streets_loaded(self, gdf, file_name):
        """
        Chamado quando as ruas são carregadas.
        Inicia o pipeline, mostra a barra de progresso e esconde os docks.
        """
        self.statusBar().showMessage(f"Ruas carregadas. Iniciando processamento...", 5000)

        # Esconde os docks
        if self.details_dock: self.details_dock.setVisible(False)
        if self.mode_dock: self.mode_dock.setVisible(False)

        # Barra de progresso
        step_titles = self.pipeline_worker.pipeline.step_titles
        total_steps = self.pipeline_worker.pipeline.total_steps
        
        self.progress_dialog = QProgressDialog(self)
        self.progress_dialog.setWindowTitle("Pré-processamento")
        self.progress_dialog.setWindowFlag(Qt.Window, True) 
        self.progress_dialog.setLabelText(f"{step_titles[0]} (Passo 1/{total_steps})")
        self.progress_dialog.setRange(0, total_steps)
        self.progress_dialog.setValue(0)
        self.progress_dialog.setModal(False)
        self.progress_dialog.setAutoClose(False)
        self.progress_dialog.setAutoReset(False)
        self.progress_dialog.show()

        self.current_depot_info = None

        # Desabilita botões para impedir que o usuário carregue outro arquivo
        self.load_gpkg_action.setEnabled(False)
        self.load_shp_action.setEnabled(False)
        self.statusBar().showMessage("Processando pipeline... Por favor, aguarde.")

        # Obtém os bairros
        neighborhoods_gdf = self.file_manager.neighborhoods_gdf
        neighborhoods_df = GeoFactory.from_gdf(neighborhoods_gdf)

        # Obtém as ruas
        streets_df = GeoFactory.from_gdf(gdf)

        run_name = Path(file_name).stem

        self._set_interaction_enabled(False)
        
        print("MainWindow: Emitindo sinal 'start_processing_signal' para a thread do worker.")
        self.start_processing_signal.emit(streets_df, neighborhoods_df, run_name)

    @Slot(gpd.GeoDataFrame, str)
    def _on_neighborhoods_loaded(self, gdf, file_name):
        """Chamado quando o FileManager emite 'neighborhoods_loaded'."""
        self.statusBar().showMessage(f"Bairros carregados com sucesso: {file_name}", 5000)
        QMessageBox.information(self, "Sucesso", f"{len(gdf)} bairros carregados.")
        
        # Atualiza o mapa
        self.map_widget.update_layers(neighborhoods_gdf=gdf)

    @Slot(str)
    def _on_load_error(self, error_message):
        """Chamado por erro do FileManager OU do PipelineWorker."""
        self.statusBar().showMessage("Falha na operação.", 5000)
        QMessageBox.critical(self, "Erro", error_message)

        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None

        self.current_depot_info = None
        
        self.load_gpkg_action.setEnabled(True)
        self.load_shp_action.setEnabled(True)
        
        if self.details_dock: self.details_dock.setVisible(False)
        if self.mode_dock: self.mode_dock.setVisible(False)

        self.statusBar().showMessage("Pronto.")

    @Slot(int, int, str)
    def _on_progress_update(self, step_num: int, total_steps: int, description: str):
        """Atualiza a barra de progresso."""
        if self.progress_dialog:
            self.progress_dialog.setValue(step_num)
            self.progress_dialog.setLabelText(f"{description} ({step_num}/{total_steps})")
    
    @Slot(pd.DataFrame, pd.DataFrame, str, dict)
    def _on_processing_complete(self, lines_df, points_df, message, stats_dict):
        """Chamado ao concluir, fecha progresso e mostra os docks."""
        if self.progress_dialog:
            self.progress_dialog.setValue(self.progress_dialog.maximum())
            self.progress_dialog.close()
            self.progress_dialog = None
        
        self.statusBar().showMessage(f"Pipeline concluído: {message}", 5000)

        final_message = "Pré-processamento concluído!\n\n"
        final_message += f"Ruas: {stats_dict.get('before_s', 0)} para {stats_dict.get('after_s', 0)}\n"
        final_message += f"Nós: {stats_dict.get('before_n', 0)} para {stats_dict.get('after_n', 0)}"

        QMessageBox.information(self, "Processamento Concluído", final_message)
        
        # Armazena as cópias locais para a GUI
        self.processed_streets_df = lines_df
        self.processed_points_df = points_df

        self.current_depot_info = None

        # Atualiza o mapa e reconstroí o dock
        self._on_node_added_and_state_updated(lines_df, points_df)

        # Reabilita a GUI
        self.load_gpkg_action.setEnabled(True)
        self.load_shp_action.setEnabled(True)

        # Mostra os docks
        if self.details_dock: self.details_dock.setVisible(True)
        if self.mode_dock: self.mode_dock.setVisible(True)

        self.statusBar().showMessage("Pronto.")

    @Slot(bool)
    def _on_selection_mode_changed(self, checked: bool):
        """
        Chamado quando um botão de rádio é ligado.
        Controla a visibilidade da camada de Nós no mapa.
        """
        if not checked:
            return
        
        if not self.map_widget:
            return      # Mapa não existe

        # Obtém o botão que emitiu o sinal
        sender = self.sender()

        # Selecionar ruas
        if sender == self.radio_select_edges:
            self.map_widget.set_street_visibility(True)
            self.map_widget.set_node_visibility(False)
            self.radio_box_select_mode.setEnabled(True)
        
        # Selecionar/Inserir nós
        elif sender == self.radio_select_nodes:
            self.map_widget.set_street_visibility(True)
            self.map_widget.set_node_visibility(True)
            self.radio_box_select_mode.setEnabled(False)
        
        # Selecionar/Inserir depósito
        elif sender == self.radio_select_depot:
            self.map_widget.set_street_visibility(True)
            self.map_widget.set_node_visibility(True)
            self.radio_box_select_mode.setEnabled(False)
            self.radio_pointer_mode.setChecked(True)

    @Slot(str, int, dict)
    def _on_map_clicked(self, layer_name: str, feature_id: int, click_coords: dict):
        """
        Slot que recebe o clique do JS (via Bridge).
        APENAS EMITE SINAIS DE REQUISIÇÃO.
        """
        # Verifica se temos um estado carregado (usando as cópias locais)
        if self.processed_streets_df is None or self.processed_points_df is None:
            print("Clique ignorado, estado não está pronto.")
            return

        if not self.radio_pointer_mode.isChecked():
            print("Clique ignorado, modo 'Seleção em Caixa' está ativo.")
            return
        
        # --- Selecionar Arestas ---
        if layer_name == "streets" and self.radio_select_edges.isChecked():
            self.toggle_street_request.emit(feature_id)

        # --- Seleção/Inserção de Nós ---
        elif self.radio_select_nodes.isChecked():
            if layer_name == "nodes":
                node_mask = self.processed_points_df['node_index'] == feature_id
                if not node_mask.any():
                    return
                
                # Validação: Não pode ser Depósito
                node_row = self.processed_points_df[node_mask].iloc[0]
                if node_row.get('depot') == 'yes':
                    QMessageBox.warning(self, "Ação Inválida", "Depósito não pode ser marcado como Requerido.")
                    return
                
                # Se for marcar como requerido (atualmente 'no'), pede custo
                is_required = node_row.get('eh_requerido', 'no') == 'yes'
                service_cost = 0
                
                if not is_required:     # Vai virar 'yes'
                    cost_input = self._ask_service_cost()
                    if cost_input is None: return       # Cancelou
                    service_cost = cost_input
                
                self.toggle_node_request.emit(feature_id, service_cost)

            elif layer_name == "streets":
                # Inserir Nó Requerido
                cost_input = self._ask_service_cost()
                if cost_input is None: return
                self.add_node_request.emit(feature_id, click_coords, False, cost_input) # is_depot=False
            
        # --- Selecionar Depósito ---
        elif self.radio_select_depot.isChecked():
            if layer_name == "nodes":
                node_mask = self.processed_points_df['node_index'] == feature_id
                if not node_mask.any():
                    return

                # Validação: Não pode ser Requerido
                node_row = self.processed_points_df[node_mask].iloc[0]
                if node_row.get('eh_requerido') == 'yes':
                    QMessageBox.warning(self, "Ação Inválida", "Nó Requerido não pode ser definido como Depósito.")
                    return

                self.set_depot_request.emit(feature_id)

            elif layer_name == "streets":
                self.add_node_request.emit(feature_id, click_coords, True, 0)

    @Slot(dict)
    def _on_box_select(self, bounds_dict: dict):
        """
        Recebe os limites (lat/lon) do retângulo.
        APENAS EMITE SINAIS DE REQUISIÇÃO.
        """
        if (self.processed_streets_df is None or self.radio_select_depot.isChecked()):
            print("Seleção em caixa ignorada (pipeline não pronto ou modo depósito ativo).")
            return

        try:
            selection_box = box(
                bounds_dict['min_lon'], bounds_dict['min_lat'],
                bounds_dict['max_lon'], bounds_dict['max_lat']
            )
            
            # Ruas
            if self.radio_select_edges.isChecked():
                self.box_select_streets_request.emit(selection_box)

        except Exception as e:
            print(f"Erro ao criar 'box' de seleção: {e}")
            traceback.print_exc()
    
    @Slot(pd.DataFrame, pd.DataFrame)
    def _on_node_added_and_state_updated(self, map_streets_df: pd.DataFrame, map_points_df: pd.DataFrame):
        """
        Recebe os GDFs atualizados do worker, força a re-renderização
        do mapa e reconstrói o dock de detalhes.
        """
        print("MainWindow: Recebendo estado atualizado do worker...")

        # Desabilita enquanto renderiza
        self._set_interaction_enabled(False)
        
        # Armazena as cópias locais
        self.processed_streets_df = map_streets_df
        self.processed_points_df = map_points_df

        current_crs = self.pipeline_worker.pipeline.state.crs
        
        ms_gdf = GeoFactory.to_gdf(map_streets_df, current_crs)
        mp_gdf = GeoFactory.to_gdf(map_points_df, current_crs)
        
        # Atualiza o mapa
        self.map_widget.update_layers(
            neighborhoods_gdf=self.file_manager.neighborhoods_gdf,
            streets_gdf=ms_gdf,
            points_gdf=mp_gdf
        )
        
        # Reconstrói o dock de detalhes
        self._rebuild_details_dock()

        # Reabilitando UI
        self._set_interaction_enabled(True)
        
        print("MainWindow: Visualização atualizada.")

    @Slot(int, str, dict)
    def _on_street_toggled(self, street_id: int, new_status: str, row_data: dict):
        """Atualiza a UI (mapa e dock) quando o worker confirma a mudança da RUA."""
        try:
            color = '#FFFF00' if new_status == 'yes' else '#db1e2a'
            action = 'add' if new_status == 'yes' else 'remove'

            # Atualiza cor no mapa (via JS)
            self.map_widget.update_street_color(street_id, color)
            
            # Atualiza DataFrame local
            if self.processed_streets_df is not None:
                mask = self.processed_streets_df['id'] == street_id

                if mask.any():
                    idx = self.processed_streets_df[mask].index

                    self.processed_streets_df.loc[idx, 'eh_requerido'] = new_status
                    self.processed_streets_df.loc[idx, 'demanda'] = 1 if new_status == 'yes' else 0
                    # Obtém a geometria do GDF de mapa (para o zoom)
                    row_data['geometry'] = self.processed_streets_df.loc[idx[0], 'geometry']
            
            row_series = pd.Series(row_data)

            # Atualiza o dock
            if action == 'add':
                self._add_item_to_details_dock(row_series, f"rua_{street_id}", False)
            else:
                self._remove_item_from_details_dock(f"rua_{street_id}")
                
        except Exception as e:
            print(f"GUI Error: Falha ao atualizar UI para rua {street_id}: {e}")
            traceback.print_exc()

    @Slot(int, str, dict)
    def _on_node_toggled(self, node_id: int, new_status: str, row_data: dict):
        """Atualiza a UI (mapa e dock) quando o worker confirma a mudança do NÓ."""
        try:
            color = '#FFFF00' if new_status == 'yes' else 'blue'
            action = 'add' if new_status == 'yes' else 'remove'
            
            # Atualiza DataFrame local
            if self.processed_points_df is not None:
                mask = self.processed_points_df['node_index'] == node_id

                if mask.any():
                    idx = self.processed_points_df[mask].index

                    self.processed_points_df.loc[mask, 'eh_requerido'] = new_status
                    self.processed_points_df.loc[mask, 'demanda'] = 1 if new_status == 'yes' else 0
                    # Obtém a geometria (para o zoom)
                    row_data['geometry'] = self.processed_streets_df.loc[idx[0], 'geometry']

                    # Verifica depósito para cor
                    is_depot = self.processed_points_df.loc[idx[0], 'depot'] == 'yes'
                    if not is_depot:
                        self.map_widget.update_node_style(node_id, color, color)

            row_series = pd.Series(row_data)

            # Atualiza o dock
            if action == 'add':
                self._add_item_to_details_dock(row_series, f"no_{node_id}", False)
            else:
                self._remove_item_from_details_dock(f"no_{node_id}")

        except Exception as e:
            print(f"GUI Error: Falha ao atualizar UI para nó {node_id}: {e}")
            traceback.print_exc()

    @Slot(int, dict, int)
    def _on_depot_changed(self, new_id: int, new_row_data: dict, old_id: int):
        """Atualiza a UI para mudança de depósito."""
        try:
            # --- Lida com o Antigo Depósito (se houver) ---
            if old_id != None and old_id != -1 and self.processed_points_df is not None:
                self._remove_item_from_details_dock(f"deposito_{old_id}")
                
                # Reseta DataFrame local
                mask_old = self.processed_points_df['node_index'] == old_id
                if mask_old.any():
                    self.processed_points_df.loc[mask_old, 'depot'] = 'no'
                
                    # Reseta cor (via JS)
                    is_req = self.processed_points_df.loc[mask_old, 'eh_requerido'].iloc[0] == 'yes'
                    color = '#FFFF00' if is_req else 'blue'
                    self.map_widget.update_node_style(old_id, color, color)

            # --- Lida com o Novo Depósito (se houver) ---
            if new_id != None and new_id != -1 and self.processed_points_df is not None:
                # Atualiza DataFrame local
                mask_new = self.processed_points_df['node_index'] == new_id

                if mask_new.any():
                    self.processed_points_df.loc[mask_new, 'depot'] = 'yes'
                    new_row_data['geometry'] = self.processed_points_df.loc[mask_new].iloc[0]['geometry']

                    # Atualiza cor (via JS)
                    self.map_widget.update_node_style(new_id, '#800080', '#800080')     # Roxo
                    # Atualiza dock
                    self._add_item_to_details_dock(pd.Series(new_row_data), f"deposito_{new_id}", True)
                    self.current_depot_info = {'id': new_id}
            else:
                # (Caso de deseleção)
                self.current_depot_info = None

        except Exception as e:
            print(f"GUI Error: Falha ao atualizar UI para depósito: {e}")
            traceback.print_exc()
    
    # --- HELPER para reconstruir o dock ---
    
    def _rebuild_details_dock(self):
        """
        Limpa e re-popula o dock de detalhes com base nos
        GDFs de 'mapa' (processed_*) atuais.
        """
        if not self.details_list_widget:
            return
            
        self.details_list_widget.clear()
        
        if self.processed_points_df is not None:
            # Depósito
            depot_mask = self.processed_points_df['depot'] == 'yes'
            if depot_mask.any():
                depot_row = self.processed_points_df[depot_mask].iloc[0]
                self._add_item_to_details_dock(depot_row, f"deposito_{depot_row['node_index']}", True)

            # Nós Requeridos
            nodes_mask = (self.processed_points_df['eh_requerido'] == 'yes') & \
                         (self.processed_points_df['depot'] != 'yes')
            # Pega apenas uma linha por nó (GDF de mapa é único por node_index)
            for _, node_row in self.processed_points_df[nodes_mask].iterrows():
                self._add_item_to_details_dock(node_row, f"no_{node_row['node_index']}", False)

        # Ruas Requeridas
        if self.processed_streets_df is not None:
            streets_mask = self.processed_streets_df['eh_requerido'] == 'yes'
            for _, street_row in self.processed_streets_df[streets_mask].iterrows():
                self._add_item_to_details_dock(street_row, f"rua_{street_row['id']}", False)
    
    def _add_item_to_details_dock(self, row: pd.Series, item_key: str, is_depot: bool):
        """Cria o 'card' e o adiciona no dock de Detalhes."""
        if not self.details_list_widget: return

        # Evita adicionar duplicatas
        if self._get_item_from_details_dock(item_key) is not None:
            return
        
        text = ""
        
        # Formatação
        if 'edge_index' in row or 'arc_index' in row:
            # É uma RUA
            if pd.notna(row.get('arc_index')):
                id_str = f"Arco: {int(row['arc_index'])}"
            else:
                id_str = f"Aresta: {int(row['edge_index'])}"
            
            name = row.get('name', 'desconhecida')
            bairro = row.get('bairro', 'N/A')
            text = f"<b>{id_str}</b><br>Rua: {name}<br>Bairro: {bairro}"
            
        elif 'node_index' in row:
            # É um NÓ
            node_id_int = int(row['node_index'])
            if is_depot:
                text = f"<b>Depósito: {node_id_int}</b>"
            else:
                text = f"<b>Nó: {node_id_int}</b>"

        if not text:
            return
        
        item = QListWidgetItem()
        item.setData(Qt.ItemDataRole.UserRole, item_key)        # Armazena uma chave única

        # Armazena os dados de zoom no item
        item.setData(Qt.ItemDataRole.UserRole + 1, row.geometry)
        
        # Cria um QLabel (o "conteúdo")
        label = QLabel(text)
        label.setWordWrap(True)     # Garante a quebra de linha

        if is_depot:
            self.details_list_widget.insertItem(0, item)
        else:
            self.details_list_widget.addItem(item)
        
        # Define o QLabel como o widget visual daquele item
        self.details_list_widget.setItemWidget(item, label)

        # Garante que o QListWidget aloque espaço suficiente
        item.setSizeHint(label.sizeHint())
    
    def _remove_item_from_details_dock(self, item_key: str):
        """Encontra e remove um 'card' do dock de Detalhes."""
        item = self._get_item_from_details_dock(item_key)
        if item:
            row = self.details_list_widget.row(item)
            self.details_list_widget.takeItem(row)

    def _get_item_from_details_dock(self, item_key: str) -> Optional[QListWidgetItem]:
        """Procura um item no dock de detalhes pela sua chave."""
        if not self.details_list_widget:
            return None
        for i in range(self.details_list_widget.count()):
            item = self.details_list_widget.item(i)
            if item.data(Qt.ItemDataRole.UserRole) == item_key:
                return item
        return None
    
    @Slot()
    def _on_finalize_clicked(self):
        """Valida o grafo e inicia o processo de salvamento."""
        if self.processed_points_df is None or self.processed_streets_df is None:
            return

        # Verifica Depósito
        has_depot = (self.processed_points_df['depot'] == 'yes').any()
        
        # Verifica Requeridos (Nós OU Ruas)
        has_req_node = (self.processed_points_df['eh_requerido'] == 'yes').any()
        has_req_street = (self.processed_streets_df['eh_requerido'] == 'yes').any()
        
        if not has_depot:
            QMessageBox.warning(self, "Impossível Finalizar", "A instância precisa ter um <b>Depósito</b> definido.")
            return
            
        if not (has_req_node or has_req_street):
            QMessageBox.warning(self, "Impossível Finalizar", "A instância precisa ter pelo menos um <b>elemento requerido</b> (nó ou rua).")
            return
        
        run_name = "instancia_nova"
        
        if self.current_loaded_run_id == -1:
            text, ok = QInputDialog.getText(self, "Finalizar Instância", "Dê um nome para esta instância:")
            if not ok or not text.strip(): 
                return
            run_name = text.strip()
        else:
            reply = QMessageBox.question(
                self, "Atualizar Instância",
                "Você deseja atualizar a instância existente no banco de dados?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                text, ok = QInputDialog.getText(self, "Salvar como Nova", "Dê um novo nome para a instância:")
                if not ok or not text.strip(): return
                run_name = text.strip()
                self.current_loaded_run_id = -1
        
        self.statusBar().showMessage("Finalizando, re-indexando e salvando...")
        self._set_interaction_enabled(False)
        
        # Emite sinal para o worker fazer o trabalho pesado
        self.finalize_request.emit(run_name, self.current_loaded_run_id)
    
    @Slot(int)
    def _on_finalization_complete(self, new_id):
        """Chamado quando o worker termina de salvar."""
        self.statusBar().showMessage("Instância salva com sucesso!", 5000)
        QMessageBox.information(self, "Sucesso", f"Instância salva no banco de dados.\nID do Registro: {new_id}")
        
        # Atualiza o ID atual para futuras edições na mesma sessão
        self.current_loaded_run_id = new_id
        
        # Reabilita a interface
        self._set_interaction_enabled(True)
    
    @Slot(bool)
    def _on_bairros_visibility_changed(self, checked: bool):
        """Chamado quando a checkbox 'Exibir Bairros' é marcada/desmarcada."""
        if self.map_widget:
            self.map_widget.set_neighborhood_visibility(checked)

    @Slot(QListWidgetItem)
    def _on_details_item_clicked(self, item: QListWidgetItem):
        """
        Chamado quando um item no dock 'Detalhes da Seleção' é clicado.
        Dá zoom no mapa para a geometria daquele item.
        """
        try:
            # Obtém a geometria que armazenamos no item
            geom = item.data(Qt.ItemDataRole.UserRole + 1)
            
            if geom and self.map_widget:
                print(f"Dando zoom no item: {item.data(Qt.ItemDataRole.UserRole)}")
                self.map_widget.zoom_to_geometry(geom)
            elif not geom:
                print(f"Aviso: Item '{item.data(Qt.ItemDataRole.UserRole)}' não possui geometria para zoom.")
                
        except Exception as e:
            print(f"Erro ao dar zoom no item: {e}")

    @Slot(QAbstractButton, bool)
    def _on_pointer_mode_changed(self, button: QAbstractButton, checked: bool):
        """Chamado quando o 'pointer_mode_group' (Ponteiro ou Caixa) muda."""
        # Só nos importa o botão que foi LIGADO
        if not checked or not self.map_widget:
            return

        if button == self.radio_pointer_mode:
            print("Mudando para modo: Ponteiro (Pan/Click)")
            self.map_widget.set_selection_mode("pan")
        elif button == self.radio_box_select_mode:
            print("Mudando para modo: Seleção em Caixa")
            self.map_widget.set_selection_mode("box")

    @Slot()
    def _on_reduce_graph_triggered(self):
        """
        Acionado pelo menu Ferramentas > Reduzir Grafo.
        Solicita ao worker que filtre os bairros irrelevantes.
        """
        if self.processed_streets_df is None:
            QMessageBox.warning(self, "Aviso", "Nenhum mapa carregado para reduzir.")
            return

        # Pede confirmação, pois é uma operação destrutiva
        reply = QMessageBox.question(
            self,
            "Confirmar Redução",
            "Esta operação irá remover todos os bairros que não possuem elementos requeridos "
            "e que não são utilizados como rota de passagem para o depósito.\n\n"
            "Deseja continuar?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.statusBar().showMessage("Analisando conectividade e reduzindo grafo...")
            self._set_interaction_enabled(False)
            
            # Emite o sinal para o worker
            self.reduce_graph_request.emit()

    @Slot()
    def _on_create_files_triggered(self):
        """Abre diálogos para criar arquivos .dat."""
        if self.processed_points_df is None:
            QMessageBox.warning(self, "Aviso", "Nenhuma instância carregada.")
            return
            
        # Pergunta Capacidade
        cap, ok = QInputDialog.getInt(self, "Configuração da Instância", "Capacidade/Tempo Limite (segundos):", 3600, 1, 1000000)
        if not ok: return
        
        # Pergunta Veículos
        veh, ok = QInputDialog.getInt(self, "Configuração da Instância", "Quantidade de Veículos:", 1, 1, 100)
        if not ok: return
        
        name, ok = QInputDialog.getText(self, "Configuração da Instância", "Nome base do arquivo:", text="instancia")
        if not ok or not name.strip(): return
        
        self.statusBar().showMessage("Gerando arquivos de instância...")
        self._set_interaction_enabled(False)
        self.generate_files_request.emit(name.strip(), cap, veh)

    @Slot(str)
    def _on_files_generated(self, message):
        self.statusBar().showMessage("Arquivos gerados com sucesso!", 5000)
        QMessageBox.information(self, "Geração Concluída", message)
        self._set_interaction_enabled(True)