import sys

import faulthandler
import sys

# Ativa o handler de falhas. Imprime o traceback
faulthandler.enable()

# Windows, exceções C++
try:
    import ctypes
    ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002 | 0x8000)
except:
    pass

from PySide6.QtWidgets import QApplication

from src.mcgrp_app.gui.main_window import MainWindow

if __name__ == "__main__":
    # Cria a instância da aplicação
    app = QApplication(sys.argv)

    # Cria a instância da janela principal
    window = MainWindow()

    # Exibe a janela
    window.show()

    # Inicia o loop de eventos da aplicação
    sys.exit(app.exec())