# src\mcgrp_app\gui\load_from_db_dialog.py

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QTableWidget, QAbstractItemView,
    QPushButton, QDialogButtonBox, QMessageBox, QHeaderView,
    QTableWidgetItem, QLabel
)

from ..persistence import FileManager, DataBaseManager

class LoadFromDBDialog(QDialog):
    """
    Janela de diálogo para carregar ou deletar execuções salvas no banco de dados.
    """
    
    def __init__(self, db_manager: DataBaseManager, file_manager: FileManager, parent=None, status_filter: str = 'processado'):
        super().__init__(parent)
        
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.status_filter = status_filter
        self.selected_run_id = None
        
        # Ajusta o título com base no filtro
        title_type = "Mapa Base" if status_filter == 'processado' else "Instância"
        self.setWindowTitle(f"Carregar {title_type}")
        self.setMinimumSize(600, 400)
        
        self.layout = QVBoxLayout(self)

        # Título interno
        lbl_info = QLabel(f"Selecione um registro para carregar:")
        self.layout.addWidget(lbl_info)
        
        # Tabela
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(4)     # id (hidden), run_name, datetime, delete
        self.table_widget.setHorizontalHeaderLabels(["ID", "Nome", "Data/Hora", "Ação"])
        self.table_widget.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_widget.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table_widget.setEditTriggers(QAbstractItemView.EditTriggers.NoEditTriggers)
        
        # Ajusta colunas
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        self.layout.addWidget(self.table_widget)
        
        # Botões OK / Cancelar
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        
        # Desabilita OK até que uma linha seja selecionada
        self.button_box.button(QDialogButtonBox.StandardButton.Ok).setEnabled(False)
        self.table_widget.itemSelectionChanged.connect(self._on_selection_changed)
        
        self.layout.addWidget(self.button_box)
        
        # Popula a tabela
        self.populate_table()

    def populate_table(self):
        """Busca os dados filtrados do DBManager e preenche a tabela."""
        self.table_widget.setRowCount(0)        # Limpa a tabela
        
        try:
            runs = self.db_manager.get_runs_by_status(self.status_filter)
        except Exception as e:
            QMessageBox.critical(self, "Erro de Banco de Dados", f"Não foi possível ler o catálogo: {e}")
            return
            
        self.table_widget.setRowCount(len(runs))
        
        for row_idx, run_data in enumerate(runs):
            run_id, run_name, formatted_time = run_data
            
            # Coluna 0: ID (para uso interno)
            id_item = QTableWidgetItem(str(run_id))
            id_item.setData(Qt.ItemDataRole.UserRole, run_id)       # Armazena o ID real
            self.table_widget.setItem(row_idx, 0, id_item)
            
            # Coluna 1: Nome (visível)
            name_item = QTableWidgetItem(run_name)
            self.table_widget.setItem(row_idx, 1, name_item)
            
            # Coluna 2: Data/Hora (visível)
            date_item = QTableWidgetItem(formatted_time)
            self.table_widget.setItem(row_idx, 2, date_item)
            
            # Coluna 3: Botão Deletar
            delete_btn = QPushButton("Deletar")
            delete_btn.clicked.connect(lambda _, r=row_idx, rid=run_id: self._on_delete_clicked(r, rid))
            self.table_widget.setCellWidget(row_idx, 3, delete_btn)

        # Esconde a coluna de ID
        self.table_widget.setColumnHidden(0, True)

    def _on_selection_changed(self):
        """Habilita o botão OK se uma linha for selecionada."""
        selected_items = self.table_widget.selectedItems()
        is_selected = len(selected_items) > 0
        
        self.button_box.button(QDialogButtonBox.StandardButton.Ok).setEnabled(is_selected)
        
        if is_selected:
            # Obtém o ID da coluna oculta
            self.selected_run_id = self.table_widget.item(selected_items[0].row(), 0).data(Qt.ItemDataRole.UserRole)
        else:
            self.selected_run_id = None

    def _on_delete_clicked(self, table_row_index: int, run_id: int):
        """Chamado quando o botão 'Deletar' de uma linha é clicado."""
        run_name = self.table_widget.item(table_row_index, 1).text()
        
        reply = QMessageBox.warning(
            self,
            "Confirmar Exclusão",
            f"Você tem certeza que deseja deletar permanentemente o registro '{run_name}' (ID: {run_id})?\n\n"
            "Isso também deletará os arquivos .gpkg associados do disco.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            print(f"Deletando execução ID: {run_id}")
            try:
                # Obtém os caminhos dos arquivos
                paths_dict = self.db_manager.get_run_paths(run_id)
                # Deleta os arquivos do disco
                self.file_manager.delete_gpkg_files(paths_dict)
                # Deleta o registro do DB
                self.db_manager.delete_run(run_id)
                # Atualiza a tabela
                self.populate_table()
            except Exception as e:
                QMessageBox.critical(self, "Erro ao Deletar", f"Não foi possível deletar a execução: {e}")