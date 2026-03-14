/* Phase 7 — Map, search, point click, satellite layer, toggles, dropdowns */

(function () {
  const NC_CENTER = [35.5, -79.5];
  const NC_ZOOM = 7;
  const NC_BOUNDS = [[33.75, -84.5], [36.65, -75.2]];

  const map = L.map("map", { center: NC_CENTER, zoom: NC_ZOOM });

  const darkLayer = L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "&copy; OSM, CARTO",
  });
  const satelliteLayer = L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", {
    attribution: "&copy; Esri",
  });
  darkLayer.addTo(map);
  L.control.layers({ "Dark": darkLayer, "Satellite": satelliteLayer }).addTo(map);

  let countyLayer = null;
  let countyGeoJsonData = null;
  const layerHigh = L.markerClusterGroup({ chunkedLoading: true });
  const layerModerate = L.markerClusterGroup({ chunkedLoading: true });
  const layerLow = L.markerClusterGroup({ chunkedLoading: true });

  // Single "focus" pin for analyzed/chatted location — zoom here and show so user knows where to look
  let focusPinLayer = null;
  function setFocusPin(lat, lon) {
    if (typeof lat !== "number" || typeof lon !== "number") return;
    if (focusPinLayer) map.removeLayer(focusPinLayer);
    focusPinLayer = L.layerGroup();
    const pin = L.circleMarker([lat, lon], {
      radius: 10,
      fillColor: "#2388ff",
      fillOpacity: 0.9,
      color: "#fff",
      weight: 2,
    });
    pin.bindTooltip("Analyzed location", { permanent: false, direction: "top" });
    focusPinLayer.addLayer(pin);
    focusPinLayer.addTo(map);
  }
  function clearFocusPin() {
    if (focusPinLayer) { map.removeLayer(focusPinLayer); focusPinLayer = null; }
  }

  function tierColor(tier) {
    if (tier === "HIGH") return "#f85149";
    if (tier === "MODERATE") return "#d29922";
    return "#3fb950";
  }

  function highPctToColor(pct) {
    if (pct >= 30) return "#f85149";
    if (pct >= 15) return "#d29922";
    return "#3fb950";
  }

  function addPointMarker(p, layer) {
    const m = L.circleMarker([p.lat, p.lon], {
      radius: 5,
      fillColor: tierColor(p.tier),
      fillOpacity: 0.8,
      color: "#1a2332",
      weight: 1,
    });
    const coordStr = p.lat.toFixed(4) + ", " + p.lon.toFixed(4);
    m.bindTooltip(coordStr + " · " + p.tier, { permanent: false, direction: "top" });
    m._pointData = p;
    m.on("click", function () {
      openLocationDetail(p.lat, p.lon);
    });
    layer.addLayer(m);
  }

  let lastDetailLat = null;
  let lastDetailLon = null;

  function updateExternalMapLinks(lat, lon) {
    lastDetailLat = lat;
    lastDetailLon = lon;
    const mapsUrl = "https://www.google.com/maps?q=" + lat + "," + lon + "&z=18";
    const earthUrl = "https://earth.google.com/web/@"
      + lat + "," + lon + ",50a,0d,60y,0h,0t,0r";
    const btnMaps = document.getElementById("btn-open-google-maps");
    const btnEarth = document.getElementById("btn-open-google-earth");
    if (btnMaps) { btnMaps.href = mapsUrl; btnMaps.classList.remove("disabled"); }
    if (btnEarth) { btnEarth.href = earthUrl; btnEarth.classList.remove("disabled"); }
  }

  function openLocationDetail(lat, lon) {
    const panel = document.getElementById("location-detail-panel");
    const title = document.getElementById("location-detail-title");
    const coordsEl = document.getElementById("location-detail-coords");
    const statusEl = document.getElementById("location-detail-status");
    const resultEl = document.getElementById("location-detail-result");
    panel.classList.remove("hidden");
    title.textContent = "Location detail";
    coordsEl.textContent = "Lat: " + lat + "  Lon: " + lon;
    updateExternalMapLinks(lat, lon);
    statusEl.textContent = "Analyzing this location…";
    resultEl.innerHTML = "";
    document.getElementById("btn-download-report").disabled = true;
    document.getElementById("btn-download-report").dataset.currentMarkdown = "";

    fetch("/api/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat: lat, lon: lon }),
    })
      .then((r) => r.json())
      .then((data) => {
        if (data.error) {
          statusEl.textContent = data.error;
          return;
        }
        if (data.out_of_coverage) {
          statusEl.textContent = "Limited data — coverage is North Carolina only. Analysis below may be partial.";
        } else {
          statusEl.textContent = "";
        }
        resultEl.innerHTML = typeof marked !== "undefined" ? marked.parse(data.markdown || "") : data.markdown;
        document.getElementById("btn-download-report").dataset.currentMarkdown = data.markdown || "";
        document.getElementById("btn-download-report").disabled = !data.markdown;
      })
      .catch((e) => {
        statusEl.textContent = "Request failed: " + e.message;
      });
  }

  document.getElementById("btn-download-report").addEventListener("click", function () {
    const md = this.dataset.currentMarkdown;
    if (!md) return;
    const blob = new Blob([md], { type: "text/markdown" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "location-risk-report.md";
    a.click();
    URL.revokeObjectURL(a.href);
  });

  // Stats
  fetch("/api/stats")
    .then((r) => r.json())
    .then((data) => {
      if (data.error) {
        document.getElementById("stats-loading").textContent = data.error;
        return;
      }
      document.getElementById("stats-loading").classList.add("hidden");
      document.getElementById("stats-content").classList.remove("hidden");
      const d = data.tier_distribution;
      document.getElementById("stat-high").textContent = d.HIGH ? (d.HIGH.count / 1e6).toFixed(2) + "M (" + d.HIGH.pct + "%)" : "—";
      document.getElementById("stat-moderate").textContent = d.MODERATE ? (d.MODERATE.count / 1e6).toFixed(2) + "M (" + d.MODERATE.pct + "%)" : "—";
      document.getElementById("stat-low").textContent = d.LOW ? (d.LOW.count / 1e6).toFixed(2) + "M (" + d.LOW.pct + "%)" : "—";
      document.getElementById("stat-total").textContent = data.total ? (data.total / 1e6).toFixed(2) + "M" : "—";
    })
    .catch(() => {
      document.getElementById("stats-loading").textContent = "Failed to load stats";
    });

  // Countries dropdown (from dataset; fallback to US if API fails)
  const countrySelect = document.getElementById("select-country");
  function setCountryOptions(countries) {
    if (!countries || !countries.length) {
      countrySelect.innerHTML = '<option value="US">United States</option>';
      return;
    }
    countrySelect.innerHTML = "";
    countries.forEach((c) => {
      const opt = document.createElement("option");
      opt.value = c.code;
      opt.textContent = c.name;
      countrySelect.appendChild(opt);
    });
  }
  fetch("/api/countries")
    .then((r) => r.json())
    .then((data) => {
      if (data.countries && data.countries.length) {
        setCountryOptions(data.countries);
      } else {
        setCountryOptions([{ code: "US", name: "United States" }]);
      }
    })
    .catch(() => {
      setCountryOptions([{ code: "US", name: "United States" }]);
    });

  // Counties dropdown: "All counties" first, then full list so user can choose
  const countySelect = document.getElementById("select-county");
  function setCountyOptions(counties) {
    countySelect.innerHTML = "";
    const allOpt = document.createElement("option");
    allOpt.value = "";
    allOpt.textContent = "All counties";
    countySelect.appendChild(allOpt);
    if (counties && counties.length) {
      counties.forEach((name) => {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = name;
        countySelect.appendChild(opt);
      });
    }
  }
  fetch("/api/counties")
    .then((r) => r.json())
    .then((data) => {
      if (data.error) return;
      setCountyOptions(data.counties || []);
    })
    .catch(() => {
      setCountyOptions([]);
    });

  document.getElementById("select-county").addEventListener("change", function () {
    const name = this.value;
    if (!name || !countyGeoJsonData || !countyLayer) return;
    const features = countyGeoJsonData.features || [];
    const feature = features.find((f) => {
      const p = f.properties;
      return p && (p.county === name || p.NAMELSAD === name || (p.NAME && (p.NAME + " County") === name));
    });
    if (feature && feature.geometry) {
      const layer = L.geoJSON(feature);
      map.fitBounds(layer.getBounds(), { maxZoom: 11, padding: [30, 30] });
    }
  });

  // Risk overlay master toggle (county + points) — off = clear view for technicians
  function applyCountyLayerStyle(overlayOn) {
    if (!countyLayer) return;
    countyLayer.eachLayer(function (layer) {
      const props = (layer.feature && layer.feature.properties) || {};
      const pct = props.high_pct != null ? props.high_pct : 0;
      if (overlayOn) {
        layer.setStyle({
          fillColor: highPctToColor(pct),
          fillOpacity: 0.55,
          color: "#2d3a4d",
          weight: 1,
        });
      } else {
        layer.setStyle({
          fillOpacity: 0,
          color: "#5a6a7d",
          weight: 1,
        });
      }
    });
  }

  function updateOverlayVisibility() {
    const overlayOn = document.getElementById("toggle-risk-overlay").checked;
    if (countyLayer) {
      if (!map.hasLayer(countyLayer)) map.addLayer(countyLayer);
      applyCountyLayerStyle(overlayOn);
    }
    updatePointLayers();
  }

  // Risk layer toggles (point clusters always follow these; overlay toggle only affects county fill)
  function updatePointLayers() {
    const showHigh = document.getElementById("toggle-high").checked;
    const showMod = document.getElementById("toggle-moderate").checked;
    const showLow = document.getElementById("toggle-low").checked;
    if (showHigh && !map.hasLayer(layerHigh)) map.addLayer(layerHigh);
    if (!showHigh && map.hasLayer(layerHigh)) map.removeLayer(layerHigh);
    if (showMod && !map.hasLayer(layerModerate)) map.addLayer(layerModerate);
    if (!showMod && map.hasLayer(layerModerate)) map.removeLayer(layerModerate);
    if (showLow && !map.hasLayer(layerLow)) map.addLayer(layerLow);
    if (!showLow && map.hasLayer(layerLow)) map.removeLayer(layerLow);
  }
  document.getElementById("toggle-risk-overlay").addEventListener("change", updateOverlayVisibility);
  document.getElementById("toggle-high").addEventListener("change", updatePointLayers);
  document.getElementById("toggle-moderate").addEventListener("change", updatePointLayers);
  document.getElementById("toggle-low").addEventListener("change", updatePointLayers);

  // County choropleth
  fetch("/api/map-data")
    .then((r) => r.json())
    .then((geojson) => {
      if (geojson.error) return;
      countyGeoJsonData = geojson;
      countyLayer = L.geoJSON(geojson, {
        style: (f) => {
          const props = f.properties || {};
          const pct = props.high_pct != null ? props.high_pct : 0;
          return {
            fillColor: highPctToColor(pct),
            fillOpacity: 0.55,
            color: "#2d3a4d",
            weight: 1,
          };
        },
        onEachFeature: (feature, layer) => {
          const name = (feature.properties && (feature.properties.county || feature.properties.NAMELSAD)) || "";
          if (name) {
            layer.bindTooltip(name, { permanent: false });
            layer.on("click", () => openCountyBriefing(name));
          }
        },
      });
      map.addLayer(countyLayer);
      applyCountyLayerStyle(document.getElementById("toggle-risk-overlay").checked);
    })
    .catch(() => {});

  // Points (by tier for toggles)
  fetch("/api/map-points")
    .then((r) => r.json())
    .then((points) => {
      if (points.error || !Array.isArray(points)) return;
      points.forEach((p) => {
        if (p.tier === "HIGH") addPointMarker(p, layerHigh);
        else if (p.tier === "MODERATE") addPointMarker(p, layerModerate);
        else addPointMarker(p, layerLow);
      });
      updatePointLayers();
    })
    .catch(() => {});

  function openCountyBriefing(name) {
    const panel = document.getElementById("county-panel");
    const title = document.getElementById("county-title");
    const status = document.getElementById("county-status");
    const result = document.getElementById("county-result");
    panel.classList.remove("hidden");
    title.textContent = name.endsWith("County") ? name : name + " County";
    status.textContent = "Loading…";
    result.innerHTML = "";
    const slug = encodeURIComponent(name.replace(/\s+County$/i, "").trim());
    fetch("/api/county/" + slug)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) {
          status.textContent = data.error;
          return;
        }
        status.textContent = "";
        result.innerHTML = typeof marked !== "undefined" ? marked.parse(data.markdown || "") : data.markdown;
      })
      .catch((e) => {
        status.textContent = "Failed: " + e.message;
      });
  }

  // Search
  document.getElementById("btn-search").addEventListener("click", () => {
    const input = document.getElementById("search-input").value.trim();
    if (!input) return;
    runAnalyze(input, null, null);
  });

  document.getElementById("btn-gps").addEventListener("click", () => {
    const status = document.getElementById("search-status");
    status.textContent = "Getting location…";
    if (!navigator.geolocation) {
      status.textContent = "GPS not supported";
      return;
    }
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const lat = pos.coords.latitude;
        const lon = pos.coords.longitude;
        document.getElementById("search-input").value = lat + ", " + lon;
        runAnalyze(null, lat, lon);
      },
      () => { status.textContent = "GPS denied or unavailable"; }
    );
  });

  function runAnalyze(address, lat, lon) {
    const status = document.getElementById("search-status");
    const resultEl = document.getElementById("analyze-result");
    status.textContent = "Analyzing…";
    resultEl.classList.add("hidden");
    const body = address != null ? { address } : { lat, lon };
    fetch("/api/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then((r) => r.json())
      .then((data) => {
        if (data.error) {
          status.textContent = data.error;
          return;
        }
        if (data.out_of_coverage) {
          status.textContent = "Limited data — coverage is NC only. Analysis below may be partial.";
        } else {
          status.textContent = "";
        }
        resultEl.classList.remove("hidden");
        resultEl.innerHTML = typeof marked !== "undefined" ? marked.parse(data.markdown || "") : data.markdown;
        if (data.lat != null && data.lon != null) zoomAndPin(data.lat, data.lon);
      })
      .catch((e) => {
        status.textContent = "Request failed: " + e.message;
      });
  }

  // Floating chat: bubble opens window; messages one after another; zoom map when coords in response
  const chatWindow = document.getElementById("chat-window");
  const chatBubble = document.getElementById("chat-bubble");
  const chatClose = document.getElementById("chat-close");
  const chatMessages = document.getElementById("chat-messages");
  const chatInput = document.getElementById("chat-input");
  const chatSend = document.getElementById("chat-send");

  if (chatBubble && chatWindow) {
    chatBubble.addEventListener("click", function () {
      chatWindow.classList.add("is-open");
      chatWindow.setAttribute("aria-hidden", "false");
      chatInput.focus();
    });
  }
  if (chatClose && chatWindow) {
    chatClose.addEventListener("click", function () {
      chatWindow.classList.remove("is-open");
      chatWindow.setAttribute("aria-hidden", "true");
    });
  }

  function appendChat(role, content, isHtml) {
    if (!chatMessages) return;
    const div = document.createElement("div");
    div.className = "chat-msg " + role;
    const inner = document.createElement("div");
    inner.className = "content";
    if (isHtml && typeof marked !== "undefined") {
      inner.innerHTML = marked.parse(content || "");
    } else {
      inner.textContent = content || "";
    }
    div.appendChild(inner);
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return div;
  }

  function zoomMapTo(lat, lon) {
    if (typeof lat !== "number" || typeof lon !== "number") return;
    if (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
      map.setView([lat, lon], 14);
    }
  }
  function zoomAndPin(lat, lon) {
    zoomMapTo(lat, lon);
    setFocusPin(lat, lon);
  }

  if (chatSend && chatInput) {
    chatSend.addEventListener("click", sendChatMessage);
    chatInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter") sendChatMessage();
    });
  }

  function sendChatMessage() {
    const message = chatInput.value.trim();
    if (!message) return;
    appendChat("user", message, false);
    chatInput.value = "";
    chatSend.disabled = true;
    const thinkingEl = appendChat("assistant", "Thinking…", false);
    if (thinkingEl) thinkingEl.classList.add("thinking");

    fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: message }),
    })
      .then((r) => r.json())
      .then((data) => {
        if (thinkingEl && thinkingEl.parentNode) thinkingEl.remove();
        chatSend.disabled = false;
        if (data.error) {
          appendChat("assistant", "Error: " + data.error, false);
          return;
        }
        appendChat("assistant", data.markdown || "", true);
        if (data.lat != null && data.lon != null) zoomAndPin(data.lat, data.lon);
      })
      .catch((e) => {
        if (thinkingEl && thinkingEl.parentNode) thinkingEl.remove();
        chatSend.disabled = false;
        appendChat("assistant", "Request failed: " + e.message, false);
      });
  }

  // Resize sidebar
  (function () {
    const sidebar = document.getElementById("sidebar");
    const handle = document.getElementById("resize-handle");
    const layout = document.querySelector(".layout");
    if (!sidebar || !handle) return;
    let startX = 0, startW = 0;
    handle.addEventListener("mousedown", (e) => {
      e.preventDefault();
      startX = e.clientX;
      startW = sidebar.offsetWidth;
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      function move(e) {
        const dx = e.clientX - startX;
        const w = Math.max(280, Math.min(600, startW + dx));
        layout.style.gridTemplateColumns = w + "px 6px 1fr";
      }
      function up() {
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      }
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  })();
})();
