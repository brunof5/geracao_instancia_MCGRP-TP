# src\mcgrp_app\gui\widgets\map_widget.py

import io
import os
import tempfile
import traceback
import folium
from pathlib import Path
from folium.utilities import JsCode
from shapely.geometry.base import BaseGeometry

from PySide6.QtWidgets import QWidget, QVBoxLayout
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineSettings, QWebEnginePage
from PySide6.QtCore import QUrl, QFileInfo, Slot

class MapWidget(QWidget):
    """
    Widget de mapa com a ponte QWebChannel integrada.
    Ele expõe uma 'page' para a MainWindow e pode
    receber comandos de JS.
    """
    LAVRAS_COORDINATES = [-21.2465, -45.0000]
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)      # Remove margens
        
        self.web_view = QWebEngineView()

        # A 'page' é o que permite a comunicação
        self.page = QWebEnginePage(self)
        self.web_view.setPage(self.page)

        settings = self.web_view.settings()
        # Permite que conteúdo local (file://) acesse URLs remotas (https://)
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True
        )
        # Permite que conteúdo local acesse outros arquivos locais
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.LocalContentCanAccessFileUrls, True
        )

        self.layout.addWidget(self.web_view)

        # Armazena os dados
        self.streets_gdf = None
        self.neighborhoods_gdf = None
        self.points_gdf = None

        # Estado de visibilidade
        self.show_nodes = False
        self.show_neighborhoods = False

        # Armazena o caminho do arquivo temporário atual
        self.current_temp_file = None

        self._map_name = None

        # Constrói o script de utilidade (via arquivo)
        self.js_utility_script = self._build_js_utility_script()
        
        # Renderiza o mapa inicial
        self._render_map()

    def get_page(self) -> QWebEnginePage:
        """
        Retorna a QWebEnginePage interna para que a MainWindow 
        possa anexar o QWebChannel a ela.
        """
        return self.page
    
    def _create_base_map(self) -> folium.Map:
        """Cria e retorna um objeto de mapa base Folium."""
        m = folium.Map(
            location=self.LAVRAS_COORDINATES,
            zoom_start=13,
            tiles="OpenStreetMap"
        )
        self._map_name = m.get_name()
        return m
    
    def _fit_bounds(self, folium_map: folium.Map):
        """Ajusta o zoom do mapa para a camada de ruas ou bairros."""
        
        # Prioriza as ruas para o zoom
        gdf_to_fit = self.streets_gdf
        
        if gdf_to_fit is None:
            # Se não houver ruas, usa os bairros
            gdf_to_fit = self.neighborhoods_gdf
        
        if gdf_to_fit is not None and not gdf_to_fit.empty:
            try:
                # Pega os limites totais do GeoDataFrame
                # gdf.total_bounds -> [minx, miny, maxx, maxy]
                bounds_array = gdf_to_fit.total_bounds
                
                # Folium espera: [[miny, minx], [maxy, maxx]]
                sw = [bounds_array[1], bounds_array[0]]
                ne = [bounds_array[3], bounds_array[2]]
                
                # Ajusta o mapa a esses limites
                folium_map.fit_bounds([sw, ne], padding=(10, 10))
            except Exception as e:
                print(f"Aviso: Não foi possível ajustar o zoom (fit_bounds). {e}")
                traceback.print_exc()
    
    def _render_map(self):
        """
        Renderiza o mapa, injeta o script JS, salva em um arquivo HTML 
        temporário e o carrega via URL.
        """
        # Novo mapa base limpo
        m = self._create_base_map()

        # Armazena as camadas por nome para o LayerControl e JS
        m.add_child(folium.Element("<script>window.leafletLayers = {};</script>"))

        # Adiciona Camada de Bairros (se existir)
        if self.neighborhoods_gdf is not None:
            # Cria um "Grupo de Features" para esta camada
            neigh_layer = folium.FeatureGroup(name="Bairros")

            # Define quais campos (colunas) do GeoDataFrame serão usados no tooltip
            tooltip_bairros = folium.GeoJsonTooltip(
                fields=['id_bairro', 'bairro'],
                aliases=['ID:', 'Bairro:']
            )
            
            folium.GeoJson(
                self.neighborhoods_gdf,
                style_function=lambda x: {
                    'fillColor': 'transparent',
                    'color': '#8d614c',     # Cor da borda
                    'weight': 2,              # Espessura da borda
                    'fillOpacity': 0.3        # Transparência
                },
                tooltip=tooltip_bairros
            ).add_to(neigh_layer)
            
            # Adiciona a camada ao mapa
            neigh_layer.add_to(m)
        
        # Adiciona Camada de Ruas (se existir)
        if self.streets_gdf is not None:
            street_layer = folium.FeatureGroup(name="Ruas")

            on_each_street = JsCode("""
                function(feature, layer) {
                    if (feature && feature.properties && feature.properties.tooltip_html) {
                        layer.bindTooltip(feature.properties.tooltip_html, { sticky: false, parseHtml: true });
                    }
                    layer.on('click', function(e) {
                        try {
                            if (typeof window.py_bridge !== 'undefined' && window.py_bridge) {
                                var click_coords = {lat: e.latlng.lat, lon: e.latlng.lng};
                                window.py_bridge.on_map_clicked('streets', feature.properties.id, click_coords);
                            }
                        } catch(err){}
                    });
                }
            """)
            
            folium.GeoJson(
                self.streets_gdf,
                style_function=lambda feature: {
                    'color': '#FFFF00' if feature['properties'].get('eh_requerido') == 'yes' 
                        else '#db1e2a'
                    },
                on_each_feature=on_each_street
            ).add_to(street_layer)

            street_layer.add_to(m)

        # Adiciona Camada de Nós (se existir)
        if self.points_gdf is not None:
            points_layer = folium.FeatureGroup(name="Nós")

            on_each_node = JsCode("""
                function(feature, layer) {
                    if (feature && feature.properties && feature.properties.tooltip_html) {
                        layer.bindTooltip(feature.properties.tooltip_html, { sticky: false, parseHtml: true });
                    } else {
                        // Fallback
                        layer.bindTooltip('<b>Nó:</b> ' + feature.properties.node_index, { sticky: false, parseHtml: true });
                    }

                    layer.on('click', function(e) {
                        try {
                            if (typeof window.py_bridge !== 'undefined' && window.py_bridge) {
                                var click_coords = {lat: e.latlng.lat, lon: e.latlng.lng};
                                window.py_bridge.on_map_clicked('nodes', feature.properties.node_index, click_coords);
                            }
                        } catch(err){}
                    });
                }
            """)

            point_marker = JsCode("""
                function(feature, latlng) {
                    var props = feature.properties || {};
                    var style = {
                        radius: 2,
                        fillOpacity: 0.7,
                        node_index: props.node_index
                    };
                    if (props.depot == 'yes') {
                        style.color = '#800080';
                        style.fillColor = '#800080';
                    } else if (props.eh_requerido == 'yes') {
                        style.color = '#FFFF00';
                        style.fillColor = '#FFFF00';
                    } else {
                        style.color = 'blue';
                        style.fillColor = 'blue';
                    }
                    
                    return L.circleMarker(latlng, style);
                }
            """)
            
            folium.GeoJson(
                self.points_gdf,
                point_to_layer=point_marker,
                on_each_feature=on_each_node
            ).add_to(points_layer)

            points_layer.add_to(m)

        # Ajusta o zoom automaticamente para conter os dados
        self._fit_bounds(m)

        # Salva o mapa em um buffer, injeta o JS e carrega
        buffer = io.BytesIO()
        m.save(buffer, close_file=False)
        html_string = buffer.getvalue().decode('utf-8')
        
        # Define o <script> para carregar a biblioteca interna do Qt
        script_tag = '<script src="qrc:///qtwebchannel/qwebchannel.js"></script>'
        
        # Injeta ambos os scripts (a biblioteca e o acima)
        html_string = html_string.replace(
            "</body>", f"{script_tag}\n{self.js_utility_script}\n</body>"
        )

        # Oculta/mostra os nós via JS após render
        if not self.show_nodes:
            html_string = html_string.replace("</body>", "<script>setTimeout(function(){ window.toggleLayer('nodes', false); }, 350);</script></body>")

        # Oculta/mostra os bairros via JS após render
        if self.streets_gdf is None and self.points_gdf is None:
            html_string = html_string.replace("</body>", "<script>setTimeout(function(){ window.toggleLayer('neighborhoods', true); }, 350);</script></body>")
        elif not self.show_neighborhoods:
            html_string = html_string.replace("</body>", "<script>setTimeout(function(){ window.toggleLayer('neighborhoods', false); }, 350);</script></body>")
        else:
            html_string = html_string.replace("</body>", "<script>setTimeout(function(){ window.toggleLayer('neighborhoods', true); }, 350);</script></body>")

        # Salva o mapa em HTML e carrega via URL
        try:
            # Limpa o arquivo temporário anterior, se existir
            if self.current_temp_file and os.path.exists(self.current_temp_file):
                os.remove(self.current_temp_file)

            # Salva o HTML modificado (com script)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode='w', encoding='utf-8') as temp_file:
                temp_file.write(html_string)
                self.current_temp_file = temp_file.name
            
            # Carrega o mapa a partir do CAMINHO do arquivo
            url = QUrl.fromLocalFile(QFileInfo(self.current_temp_file).absoluteFilePath())
            print(f"Carregando mapa de: {url.toString()}")
            self.web_view.setUrl(url)
            
        except Exception as e:
            print(f"Erro ao salvar/carregar mapa temporário: {e}")
            traceback.print_exc()
    
    def update_layers(self, streets_gdf=None, neighborhoods_gdf=None, points_gdf=None):
        """
        Recebe novos GeoDataFrames e redesenha o mapa.
        Se um GDF não for fornecido, usa o que já está armazenado.
        """
        if neighborhoods_gdf is not None:
            print("MapWidget: Recebendo GDF de bairros...")
            self.neighborhoods_gdf = neighborhoods_gdf
        
        if streets_gdf is not None:
            print("MapWidget: Recebendo GDF de ruas...")
            self.streets_gdf = streets_gdf

        if points_gdf is not None:
            print("MapWidget: Recebendo GDF de nós...")
            self.points_gdf = points_gdf
        
        # Renderiza o mapa com as novas camadas
        self._render_map()

    def cleanup(self):
        """Limpa o arquivo de mapa temporário ao fechar."""
        try:
            if self.current_temp_file and os.path.exists(self.current_temp_file):
                os.remove(self.current_temp_file)
                print(f"Arquivo temporário removido: {self.current_temp_file}")
                self.current_temp_file = None
        except Exception as e:
            print(f"Erro ao limpar arquivo temporário: {e}")
            traceback.print_exc()

    @Slot(bool)
    def set_node_visibility(self, visible: bool):
        """
        Slot público para a MainWindow controlar a visibilidade da camada de Nós.
        """
        self.show_nodes = visible
        # Envia um comando JS para ligar/desligar a camada
        js = f"window.toggleLayer('nodes', {str(visible).lower()});"
        self.web_view.page().runJavaScript(js)

    @Slot(bool)
    def set_street_visibility(self, visible: bool):
        """
        Slot público para a MainWindow controlar a visibilidade da camada de Ruas.
        """
        js = f"window.showLayer('streets', {str(visible).lower()});"
        self.web_view.page().runJavaScript(js)

    @Slot(bool)
    def set_neighborhood_visibility(self, visible: bool):
        """
        Slot público para a MainWindow controlar a visibilidade da camada de Bairros.
        """
        self.show_neighborhoods = visible
        # Envia um comando JS para ligar/desligar a camada
        js = f"window.toggleLayer('neighborhoods', {str(visible).lower()});"
        self.web_view.page().runJavaScript(js)
    
    @Slot(int, str)
    def update_street_color(self, street_id: int, color: str):
        """
        Envia um comando JS para mudar a cor de uma rua.
        """
        js_command = f"window.setLayerStyle('streets', {street_id}, {{'color': '{color}'}});"
        self.web_view.page().runJavaScript(js_command)
    
    @Slot(int, str, str)
    def update_node_style(self, node_id: int, fill_color: str, border_color: str):
        """
        Envia um comando JS para mudar o estilo (cor) de um nó.
        """
        js_command = f"window.setLayerStyle('nodes', {node_id}, {{'fillColor': '{fill_color}', 'color': '{border_color}'}});"
        self.web_view.page().runJavaScript(js_command)
    
    @Slot(BaseGeometry)
    def zoom_to_geometry(self, geom: BaseGeometry):
        """
        Recebe uma geometria (Shapely) e envia um comando JS
        para o Leaflet dar zoom nela.
        """
        if not geom or geom.is_empty:
            return
            
        try:
            # Converte a geometria para um GeoJSON 'bounds'
            # (minx, miny, maxx, maxy)
            bounds = geom.bounds
            
            # Leaflet (JS) espera: [[miny, minx], [maxy, maxx]]
            bounds_js = f"[[{bounds[1]}, {bounds[0]}], [{bounds[3]}, {bounds[2]}]]"
            
            # Cria o comando JS
            js_command = f"""
            if (window._folium_map) {{
                window._folium_map.fitBounds({bounds_js}, {{ maxZoom: 18 }});
            }}
            """
            self.web_view.page().runJavaScript(js_command)
            
        except Exception as e:
            print(f"Erro no MapWidget.zoom_to_geometry: {e}")
            traceback.print_exc()
    
    @Slot(str)
    def set_selection_mode(self, mode: str):
        """
        Ativa o modo 'pan' (pan/click) ou 'box' (drag-select) no JS.
        """
        if mode not in ['pan', 'box']:
            mode = 'pan'
            
        js_command = f"window.setPointerMode('{mode}');"
        self.web_view.page().runJavaScript(js_command)
    
    def _build_js_utility_script(self) -> str:
        """
        Lê o script de utilidade JS de um arquivo externo.
        """
        try:
            # Obtém o caminho para o arquivo JS no mesmo diretório do map_widget.py
            js_path = Path(__file__).parent / "map_utility.js"
            with open(js_path, "r", encoding="utf-8") as f:
                js_code = f.read()
            
            # Retorna o script envolvido nas tags <script>
            return f"<script>{js_code}</script>"
            
        except Exception as e:
            print(f"Erro CRÍTICO: Não foi possível carregar map_utility.js: {e}")
            traceback.print_exc()
            return "<script>console.error('Falha ao carregar JS utility');</script>"