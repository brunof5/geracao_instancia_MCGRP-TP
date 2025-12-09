# Ferramenta de Geração de Instâncias MCGRP(TP) para Coleta Seletiva

Este repositório corresponde ao protótipo geoespacial utilizado no Trabalho de Conclusão de Curso "O Problema de Roteamento em Nós, Arestas e Arcos com Penalidades de Conversão (NEARP / NEARP-TP)".

O protótipo é uma aplicação desktop (local) desenvolvida em Python e PySide6. O objetivo principal é fornecer uma interface gráfica para usuários visualizarem, definirem e exportarem instâncias do **Problema de Roteamento Geral Capacitado Misto (MCGRP)** e sua variante com penalidade de conversão (MCGRP-TP), focado em um estudo de caso de coleta seletiva.

A aplicação permite carregar dados geoespaciais (camada dos bairros em `.shp` e camada de ruas em `.gpkg`), selecionar visualmente os elementos da rede (vértices e arcos requeridos, depósito) e gerar arquivos de instância `.dat` compatíveis com *solvers* (modelos exatos ou meta-heurísticas).

## 🔗 Repositórios Relacionados

Este é o 1º componente de um conjunto de três repositórios que compõem toda a solução do TCC:

1. [Protótipo Geoespacial](https://github.com/brunof5/geracao_instancia_MCGRP-TP): Gera instâncias MCGRP/MCGRP-TP a partir de dados reais.

2. [Modelagem Matemática](https://github.com/brunof5/modelagem_MCGRP-TP) (NEARP / NEARP-TP): Implementa os modelos exatos utilizados para análise comparativa.

3. [Meta-Heurística HGS-CARP](https://github.com/brunof5/HGS-CARP): Implementação do algoritmo HGS-CARP adaptado para lidar com penalidades de conversão no contexto do TCC.

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

## 📚 Artigo / TCC (Base Teórica)

FERREIRA, B. C. **O Problema de Roteamento em Nós, Arestas e Arcos com Penalidades de Conversão: Um Estudo no Contexto da Coleta Seletiva de Lixo**. TCC (Bacharelado) — Faculdade de Ciência da Computação, Universidade Federal de Lavras. Lavras, p. 81. 2025.
