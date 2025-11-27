# Ferramenta de Geração de Instâncias MCGRP(TP) para Coleta Seletiva

Este projeto é um protótipo de uma aplicação desktop (local) desenvolvida em Python e PySide6. O objetivo principal é fornecer uma interface gráfica para usuários visualizarem, definirem e exportarem instâncias do **Problema de Roteamento Geral Capacitado Misto (MCGRP)** e sua variante com penalidade de conversão (MCGRP-TP), focado em um estudo de caso de coleta seletiva.

A aplicação permite carregar dados geoespaciais (camada dos bairros em `.shp` e camada de ruas em `.gpkg`), selecionar visualmente os elementos da rede (vértices e arcos requeridos, depósito) e gerar arquivos de instância `.dat` compatíveis com *solvers* (modelos exatos ou meta-heurísticas).

## 🚀 Funcionalidades

* **Visualização:** Carregar arquivos de entrada e exibir as camadas em um mapa interativo (via Folium).
* **Definição de Instância:** Permitir que o usuário selecione graficamente:
    * O local do depósito.
    * Vértices requeridos (grandes geradores).
    * Arcos requeridos (coleta porta-a-porta).
* **Geração de Instância:** Processar os dados geoespaciais e gerar um arquivo `.dat` que formaliza o problema MCGRP e MCGRP-TP.
* **Persistência:** Salvar e carregar instâncias em um banco de dados local (SQLite).

## 🛠️ Stack

* **Linguagem:** Python 3.x
* **GUI:** PySide6 (Qt)
* **Visualização:** Folium (Leaflet.js)
* **Geoprocessamento:** GeoPandas, Shapely
* **Banco de Dados:** SQLite

## ⚙️ Instalação e Execução

Siga os passos abaixo para configurar o ambiente e executar a aplicação.

### Pré-requisitos

* **Python 3.9** ou superior.
* **Git**.

### Passos (usando `pip` e `venv`)

1.  **Clone o repositório:**
    ```bash
    # Clone este repositório
    cd app
    ```

2.  **Crie e ative um ambiente virtual:**

    *No Windows:*
    ```bash
    python -m venv venv
    .\venv\Scripts\activate
    ```

    *No macOS/Linux:*
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    (Dentro do ambiente virtual ativado)
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute a aplicação:**
    ```bash
    python main.py
    ```

---