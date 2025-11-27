// src\mcgrp_app\gui\widgets\map_utility.js

(function(){
    // Ponte global
    window.py_bridge = null;
    window._folium_map = null;
    window._layerIndex = { streets: new Map(), nodes: new Map(), neighborhoods: new Map() };

    // Variáveis para o modo box
    var currentPointerMode = 'pan';      // 'pan' ou 'box'
    var boxSelectRectangle = null;
    var boxStartPoint = null;

    let _qweb_ready = false;
    let _map_ready = false;
    let _layers_ready = false;

    function try_fire_js_ready() {
        if (_qweb_ready && _map_ready && _layers_ready &&
            window.py_bridge && typeof window.py_bridge.on_js_ready === 'function') 
        {
            console.log("JS fully ready, calling py_bridge.on_js_ready()");
            window.py_bridge.on_js_ready();
        }
    }

    // --- Console ---
    (function(){
        var origLog = console.log.bind(console);
        var origErr = console.error.bind(console);
        console.log = function(){
            try{ origLog.apply(console, arguments); }catch(e){}
            try{
                if (window.py_bridge && typeof window.py_bridge.on_js_log === 'function'){
                    var args = Array.prototype.slice.call(arguments).map(function(a){ try{ return String(a); }catch(_){ return '[obj]'; }});
                    window.py_bridge.on_js_log("LOG: " + args.join(" "));
                }
            }catch(e){}
        };
        console.error = function(){
            try{ origErr.apply(console, arguments); }catch(e){}
            try{
                if (window.py_bridge && typeof window.py_bridge.on_js_log === 'function'){
                    var args = Array.prototype.slice.call(arguments).map(function(a){ try{ return String(a); }catch(_){ return '[obj]'; }});
                    window.py_bridge.on_js_log("ERR: " + args.join(" "));
                }
            }catch(e){}
        };
    })();

    // --- Conexão QWebChannel ---
    (function attachPoll(){
        try{
            if (typeof qt !== 'undefined' && qt && typeof qt.webChannelTransport !== 'undefined'){
                new QWebChannel(qt.webChannelTransport, function(channel){
                    window.py_bridge = channel.objects.py_bridge_name;
                    console.log("QWEBCHANNEL_ATTACHED");
                    _qweb_ready = true;
                    try_fire_js_ready();
                });
                return;
            }
        }catch(e){}
        setTimeout(attachPoll, 200);
    })();


    // --- Captura do Mapa e Anexação de Eventos ---
    (function waitForMap(){
        try{
            for (var k in window){
                try{
                    if (window[k] && window[k] instanceof L.Map){
                        window._folium_map = window[k];
                        console.log("MAP_CAPTURED:", k);

                        var map = window._folium_map;
                    
                        // Tenta habilitar o middleClickDrag
                        try {
                            map.middleClickDrag.enable(); 
                            console.log("MiddleClickDrag habilitado.");
                        } catch(e) {
                            console.warn("MiddleClickDrag não está disponível.");
                        }
                        
                        // Eventos para desenhar o retângulo
                        map.on('mousedown', onMapMouseDown);
                        map.on('mousemove', onMapMouseMove);
                        map.on('mouseup', onMapMouseUp);

                        _map_ready = true;

                        // Aqui ainda não está tudo pronto → esperar layers!
                        try_fire_js_ready();

                        break;
                    }
                }catch(e){}
            }
        }catch(e){}
        if (!window._folium_map) setTimeout(waitForMap, 200);
    })();

    // --- Funções de Evento do Mouse (Lógica do Retângulo) ---
    function onMapMouseDown(e) {
        // Só ativa se o modo for 'box' e for o botão esquerdo (0)
        if (currentPointerMode !== 'box' || e.originalEvent.button !== 0) {
            return;
        }
        
        // Ponto inicial
        boxStartPoint = e.latlng;
        
        // Cria o retângulo visual
        if (boxSelectRectangle) {
            window._folium_map.removeLayer(boxSelectRectangle);
        }
        boxSelectRectangle = L.rectangle([boxStartPoint, boxStartPoint], {
            color: "#0078A8", weight: 1, interactive: false
        }).addTo(window._folium_map);
    }
    
    function onMapMouseMove(e) {
        // Só atualiza se estivermos no meio de um desenho
        if (!boxStartPoint || !boxSelectRectangle) {
            return;
        }
        // Atualiza o retângulo
        boxSelectRectangle.setBounds(L.latLngBounds(boxStartPoint, e.latlng));
    }
    
    function onMapMouseUp(e) {
        if (!boxStartPoint || !boxSelectRectangle) {
            return;
        }
        
        var map = window._folium_map;
        var bounds = boxSelectRectangle.getBounds();
        
        // Limpa o retângulo
        map.removeLayer(boxSelectRectangle);
        boxStartPoint = null;
        boxSelectRectangle = null;
        
        // Envia os limites para o Python (se for válido)
        if (bounds.isValid() && py_bridge) {
            var bounds_dict = {
                'min_lat': bounds.getSouth(),
                'min_lon': bounds.getWest(),
                'max_lat': bounds.getNorth(),
                'max_lon': bounds.getEast()
            };
            py_bridge.on_box_select(bounds_dict);
        }
    }

    // --- Inspeciona map layers e indexa streets/nodes/neighborhoods ---
    window.rebuildFeatureIndex = function(){
        try{
            console.log("rebuildFeatureIndex: starting");
            window._layerIndex.streets.clear();
            window._layerIndex.nodes.clear();
            window._layerIndex.neighborhoods.clear();
            var map = window._folium_map;
            if (!map) { console.log("rebuildFeatureIndex: no map"); return; }

            map.eachLayer(function(layer){
                try{
                    // direct feature (GeoJSON)
                    if (layer.feature && layer.feature.properties){
                        var p = layer.feature.properties;
                        // Identifica ruas
                        if (p.id !== undefined && p.node_index === undefined) {
                            window._layerIndex.streets.set(p.id, layer);
                        }
                        // Identifica nós
                        if (p.node_index !== undefined) {
                            window._layerIndex.nodes.set(p.node_index, layer);
                        }
                        // Identifica bairros
                        if (p.bairro !== undefined && p.id_bairro !== undefined && p.id === undefined && p.node_index === undefined) {
                            window._layerIndex.neighborhoods.set(p.id_bairro, layer);
                        }
                        return;
                    }

                    // LayerGroup -> desce nos sublayers
                    if (layer instanceof L.LayerGroup){
                        layer.eachLayer(function(subl){
                            try{
                                if (subl.feature && subl.feature.properties){
                                    var sp = subl.feature.properties;
                                    // Identifica nós
                                    if (sp.node_index !== undefined){
                                        window._layerIndex.nodes.set(sp.node_index, subl);
                                    }
                                    // Identifica bairros (polígonos)
                                    if (sp.bairro !== undefined && sp.id_bairro !== undefined && sp.id === undefined && sp.node_index === undefined){
                                        window._layerIndex.neighborhoods.set(sp.id_bairro, subl);
                                    }

                                    return;
                                }
                                if (subl instanceof L.CircleMarker && subl.options && subl.options.node_index !== undefined){
                                    window._layerIndex.nodes.set(subl.options.node_index, subl);
                                    return;
                                }
                            }catch(e){}
                        });
                    }
                }catch(e){}
            });

            console.log("rebuildFeatureIndex: streets keys=", window._layerIndex.streets.size, " nodes keys=", window._layerIndex.nodes.size, " neighborhoods keys=", window._layerIndex.neighborhoods.size);
        }catch(e){
            console.error("rebuildFeatureIndex error:", e);
        }
    };

    // --- Funções de utilidade (Python -> JS) ---

    window.setLayerStyle = function(layerName, layerId, newStyle){
        try{
            var map = window._folium_map;
            if (!map) { console.error("setLayerStyle: no map"); return; }
            console.log("setLayerStyle called:", layerName, layerId, newStyle);

            var applied = 0;
            if (window._layerIndex && window._layerIndex[layerName] && window._layerIndex[layerName].size > 0){
                var entry = window._layerIndex[layerName].get(String(layerId)) || window._layerIndex[layerName].get(layerId);
                if (entry){
                    try{ if (typeof entry.setStyle === 'function') { entry.setStyle(newStyle); applied++; } }catch(e){}
                }
            }

            if (applied === 0){
                // fallback full-scan
                map.eachLayer(function(l){
                    try{
                        if (l.feature && l.feature.properties){
                            var p = l.feature.properties;
                            if ((p.id !== undefined && p.id === layerId) || (p.node_index !== undefined && p.node_index === layerId)){
                                if (typeof l.setStyle === 'function') l.setStyle(newStyle);
                            }
                        }
                    }catch(e){}
                });
            }
            if (applied > 0) console.log("setLayerStyle applied:", applied);
        }catch(e){
            console.error("setLayerStyle error:", e);
        }
    };

    window.showLayer = function(layerName, show){
        try{
            console.log("showLayer called (indexed):", layerName, show);
            var map = window._folium_map;
            if (!map) return;

            var processed = 0;

            if (!window._layerIndex || !window._layerIndex[layerName] || window._layerIndex[layerName].size === 0){
                console.log("showLayer: índice vazio -> rebuildFeatureIndex()");
                window.rebuildFeatureIndex();
            }

            if (window._layerIndex && window._layerIndex[layerName]){
                window._layerIndex[layerName].forEach(function(layer){
                    try{
                        if (show) map.addLayer(layer); else map.removeLayer(layer);
                        processed++;
                    }catch(e){}
                });
            }

            console.log("showLayer (indexed): processed items count=", processed);

            if (processed === 0){
                console.log("showLayer: indexed result 0 -> fallback full-scan");
                var fallback = 0;
                map.eachLayer(function(layer){
                    try{
                        if (layer.feature && layer.feature.properties){
                            var p = layer.feature.properties;
                            // Ruas possuem 'id' mas não 'node_index'
                            if (layerName === "streets" && p.id !== undefined && p.node_index === undefined){
                                if (show) map.addLayer(layer); else map.removeLayer(layer);
                                fallback++;
                            }
                            // Nós possuem 'node_index'
                            if (layerName === "nodes" && p.node_index !== undefined){
                                if (show) map.addLayer(layer); else map.removeLayer(layer);
                                fallback++;
                            }
                            // Bairros possuem 'bairro' e 'id_bairro', mas não 'id' nem 'node_index'
                            if (layerName === "neighborhoods" && p.bairro !== undefined && p.id_bairro !== undefined && p.id === undefined && p.node_index === undefined){
                                if (show) map.addLayer(layer); else map.removeLayer(layer);
                                fallback++;
                            }
                        }
                        // CircleMarker especial para nós
                        if (layer instanceof L.CircleMarker && layer.options && layer.options.node_index !== undefined && layerName === "nodes"){
                            if (show) map.addLayer(layer); else map.removeLayer(layer);
                            fallback++;
                        }
                    }catch(e){}
                });
                console.log("showLayer fallback: processed items count=", fallback);
            }

            _layers_ready = true;
            try_fire_js_ready();
        }catch(e){
            console.error("showLayer error:", e);
        }
    };

    // --- setPointerMode ---
    window.setPointerMode = function(mode) {
        console.log("JS: Mudando modo do ponteiro para", mode);
        currentPointerMode = mode;
        var map = window._folium_map;
        if (!map) return;
        
        if (mode === 'box') {
            // DESABILITA o pan do mapa (para permitir o desenho)
            map.dragging.disable();
            // (O pan com botão do meio continua)
        } else {
            // HABILITA o pan do mapa (modo padrão)
            map.dragging.enable();
        }
    };

    window.toggleLayer = window.showLayer;

})();