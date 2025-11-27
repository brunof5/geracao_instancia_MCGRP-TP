# src\mcgrp_app\persistence\db_manager.py

import sqlite3
import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from PySide6.QtCore import QObject

class DataBaseManager(QObject):
    """
    Gerencia o banco de dados de catálogo (SQLite) para rastrear
    execuções processadas e requeridas.
    """
    
    # Caminho para o banco de dados
    DB_PATH = Path(__file__).resolve().parent.parent.parent.parent / "data" / "database.db"
    
    # Caminho para onde os arquivos .gpkg reais serão salvos
    RUNS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "runs"

    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Garante que os diretórios existam
        self.RUNS_DIR.mkdir(parents=True, exist_ok=True)
        self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Cria a tabela de catálogo se ela não existir
        self._create_catalog_table()

    def _get_connection(self):
        """Retorna uma nova conexão SQLite."""
        return sqlite3.connect(self.DB_PATH)

    def _create_catalog_table(self):
        """
        Cria a tabela 'ProcessedRuns' se ela não existir.
        Esta tabela apenas rastreia os arquivos GPKG.
        """
        query = """
        CREATE TABLE IF NOT EXISTS ProcessedRuns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('processado', 'requerido')),
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP,
            data_gpkg_path TEXT NOT NULL,
            map_gpkg_path TEXT NOT NULL,
            neighborhoods_gpkg_path TEXT NOT NULL
        );
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
            print(f"Banco de dados de catálogo inicializado em: {self.DB_PATH}")
        except Exception as e:
            print(f"Erro ao criar tabela de catálogo: {e}")

    def save_processed_run(self, run_name: str, data_path: str, map_path: str, neigh_path: str, status: str = 'processado') -> int:
        """
        Salva um novo registro no catálogo.
        """
        query = """
        INSERT INTO ProcessedRuns (run_name, status, created_at, data_gpkg_path, map_gpkg_path, neighborhoods_gpkg_path)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        timestamp = datetime.datetime.now()
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    run_name,
                    status,
                    timestamp,
                    data_path,
                    map_path,
                    neigh_path
                ))
                conn.commit()
                run_id = cursor.lastrowid
                print(f"Registro '{status}' (ID: {run_id}) salvo no banco de dados.")
                return run_id
        except Exception as e:
            print(f"Erro ao salvar registro no DB: {e}")
            raise

    def update_run_paths(self, run_id: int, data_path: str, map_path: str, neigh_path: str, status: str = 'requerido'):
        """
        Atualiza os caminhos e o status de um registro existente.
        Usado quando o usuário finaliza uma instância já carregada.
        """
        query = """
        UPDATE ProcessedRuns
        SET data_gpkg_path = ?, map_gpkg_path = ?, neighborhoods_gpkg_path = ?, status = ?, updated_at = ?
        WHERE id = ?
        """
        timestamp = datetime.datetime.now()
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    data_path,
                    map_path,
                    neigh_path,
                    status,
                    timestamp,
                    run_id
                ))
                conn.commit()
                print(f"Registro (ID: {run_id}) atualizado para status '{status}'.")
        except Exception as e:
            print(f"Erro ao atualizar registro {run_id}: {e}")
            raise
    
    def get_runs_by_status(self, status: str) -> List[Tuple]:
        """
        Busca registros filtrados pelo status ('processado' ou 'requerido').
        Retorna (id, run_name, formatted_datetime).
        """
        query = """
        SELECT id, run_name, STRFTIME('%d/%m/%Y - %H:%M:%S', created_at) as formatted_time
        FROM ProcessedRuns
        WHERE status = ?
        ORDER BY created_at DESC
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (status,))
                return cursor.fetchall()
        except Exception as e:
            print(f"Erro ao buscar execuções com status {status}: {e}")
            return []
    
    def get_processed_runs(self) -> List[Tuple]:
        """Wrapper para status 'processado'."""
        return self.get_runs_by_status('processado')

    def get_required_runs(self) -> List[Tuple]:
        """Wrapper para instâncias 'requerido'."""
        return self.get_runs_by_status('requerido')
    
    def get_run_paths(self, run_id: int) -> Dict[str, str]:
        """
        Busca os caminhos de todos os arquivos .gpkg para um ID de execução.
        """
        query = """
        SELECT data_gpkg_path, map_gpkg_path, neighborhoods_gpkg_path
        FROM ProcessedRuns
        WHERE id = ?
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (run_id,))
                paths = cursor.fetchone()
                if paths:
                    return {
                        'data': paths[0],
                        'map': paths[1],
                        'neighborhoods': paths[2]
                    }
                raise ValueError(f"Nenhum registro encontrado com o ID {run_id}")
        except Exception as e:
            print(f"Erro ao buscar caminhos para run_id {run_id}: {e}")
            raise

    def get_run_name(self, run_id: int) -> str:
        """
        Retorna o nome da execução (run_name) para um dado ID.
        """
        query = "SELECT run_name FROM ProcessedRuns WHERE id = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (run_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                raise ValueError(f"ID {run_id} não encontrado.")
        except Exception as e:
            print(f"Erro ao buscar nome da execução {run_id}: {e}")
            raise
    
    def delete_run(self, run_id: int):
        """
        Deleta um registro do catálogo pelo ID.
        """
        query = "DELETE FROM ProcessedRuns WHERE id = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (run_id,))
                conn.commit()
                print(f"Registro (ID: {run_id}) deletado do banco de dados.")
        except Exception as e:
            print(f"Erro ao deletar run_id {run_id}: {e}")
            raise