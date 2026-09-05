// Ordered source module: 20-controls.js
    let dependencyRenderer = null;
    const details = document.getElementById("details");
    const search = document.getElementById("search");
    const searchStatus = document.getElementById("search-status");
    const pathQuery = document.getElementById("path-query");
    const pathLock = document.getElementById("path-lock");
    const graphTab = document.getElementById("graph-tab");
    const openApiTab = document.getElementById("openapi-tab");
    const kafkaTab = document.getElementById("kafka-tab");
    const persistenceTab = document.getElementById("persistence-tab");
    const requestReplyTab = document.getElementById("request-reply-tab");
    const buildTab = document.getElementById("build-tab");
    const issuesTab = document.getElementById("issues-tab");
    const pathsTab = document.getElementById("paths-tab");
    const graphLegend = document.getElementById("graph-legend");
    const graphPanel = document.getElementById("graph-panel");
    const dependenciesPanel = document.getElementById("dependencies-panel");
    const issuesPanel = document.getElementById("issues-panel");
    const pathsPanel = document.getElementById("paths-panel");
    const openApiPanel = document.getElementById("openapi-panel");
    const kafkaPanel = document.getElementById("kafka-panel");
    const persistencePanel = document.getElementById("persistence-panel");
    const requestReplyPanel = document.getElementById("request-reply-panel");
    const advancedControls = document.getElementById("advanced-controls");
    const graphCanvas = document.getElementById("graph");
    const dependencyCanvas = document.getElementById("dependency-graph");
    function ensureDependencyRenderer() {
      if (dependencyRenderer !== null) return dependencyRenderer;
      const dependencyData = dependencyGraphData();
      const dependencyPositions = buildHierarchyPositions(dependencyData.nodes, dependencyData.links);
      const dependencyNetwork = new graphology.MultiDirectedGraph();
      dependencyData.nodes.forEach(node => {
        const position = dependencyPositions.get(node.id) || { x: 0, y: 0 };
        dependencyNetwork.addNode(node.id, {
          label: node.name,
          x: position.x,
          y: position.y,
          size: node.size,
          color: node.color,
          type: "build_module",
        });
      });
      dependencyData.links.forEach((link, index) => dependencyNetwork.addEdgeWithKey(
        `dependency-edge-${index}`, link.source, link.target, {
          label: link.label,
          size: 1.5,
          color: relationColor(link),
          kind: link.kind,
          type: "arrow",
        }
      ));
      dependencyRenderer = new Sigma(dependencyNetwork, dependencyCanvas, {
        labelColor: { color: document.documentElement.dataset.theme === "dark" ? "#dce8f7" : "#172033" },
        nodeProgramClasses: { build_module: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER) },
        renderEdgeLabels: false,
        labelDensity: .12,
        labelGridCellSize: 110,
        labelRenderedSizeThreshold: 8,
        // Keep camera behavior consistent with the architecture views. The
        // shared controls below provide zoom, pan and no-inertia semantics.
        doubleClickZoomingRatio: 1,
        enableCameraZooming: false,
        enableCameraPanning: false,
        inertiaDuration: 0,
        inertiaRatio: 0,
      });
      dependencyRenderer.on("clickNode", ({ node }) => selectDependencyModule(node));
      dependencyRenderer.on("clickStage", reset);
      dependencyRenderer.on("doubleClickStage", event => event.preventSigmaDefault?.());
      dependencyRenderer.on("doubleClickNode", event => event.preventSigmaDefault?.());
      const handleDependencyWheel = event => {
        event.preventDefault();
        const camera = dependencyRenderer.getCamera();
        const state = camera.getState();
        const viewport = dependencyCanvas.getBoundingClientRect();
        const cursor = { x: event.clientX - viewport.left, y: event.clientY - viewport.top };
        const graphPoint = dependencyRenderer.viewportToGraph(cursor);
        const delta = event.deltaMode === 1 ? event.deltaY * 16 : event.deltaY;
        const factor = Math.exp(Math.max(-120, Math.min(120, delta)) * .0012);
        const ratio = Math.max(.01, Math.min(100, state.ratio * factor));
        camera.setState({ ...state, ratio });
        const projected = dependencyRenderer.graphToViewport(graphPoint);
        const nextState = camera.getState();
        const width = Math.max(viewport.width, 1);
        const height = Math.max(viewport.height, 1);
        camera.setState({
          ...nextState,
          x: nextState.x - (cursor.x - projected.x) / width,
          y: nextState.y + (cursor.y - projected.y) / height,
        });
      };
      dependencyCanvas.addEventListener("wheel", handleDependencyWheel, { passive: false });
      let dependencyPan = null;
      const finishDependencyPan = event => {
        if (!dependencyPan || event.pointerId !== dependencyPan.pointerId) return;
        dependencyCanvas.releasePointerCapture?.(event.pointerId);
        dependencyPan = null;
        window.removeEventListener("pointermove", moveDependencyPan, true);
        window.removeEventListener("pointerup", finishDependencyPan, true);
        window.removeEventListener("pointercancel", finishDependencyPan, true);
      };
      const moveDependencyPan = event => {
        if (!dependencyPan || event.pointerId !== dependencyPan.pointerId) return;
        const state = dependencyPan.cameraState;
        const viewport = dependencyCanvas.getBoundingClientRect();
        const width = Math.max(viewport.width, 1);
        const height = Math.max(viewport.height, 1);
        dependencyRenderer.getCamera().setState({
          ...state,
          x: state.x - (event.clientX - dependencyPan.startX) / width,
          y: state.y + (event.clientY - dependencyPan.startY) / height,
        });
      };
      const startDependencyPan = event => {
        if (event.button !== 0) return;
        dependencyPan = {
          pointerId: event.pointerId,
          startX: event.clientX,
          startY: event.clientY,
          cameraState: dependencyRenderer.getCamera().getState(),
        };
        event.preventDefault();
        event.stopImmediatePropagation();
        dependencyCanvas.setPointerCapture?.(event.pointerId);
        window.addEventListener("pointermove", moveDependencyPan, true);
        window.addEventListener("pointerup", finishDependencyPan, true);
        window.addEventListener("pointercancel", finishDependencyPan, true);
      };
      dependencyCanvas.addEventListener("pointerdown", startDependencyPan, true);
      return dependencyRenderer;
    }
    const indexingIssuesList = document.getElementById("indexing-issues");
    const indexingIssuesEmpty = document.getElementById("indexing-issues-empty");
    const indexingIssuesTitle = document.getElementById("indexing-issues-title");
    const indexingIssues = graphData.indexing_issues || [];
    const inventoryStatus = document.getElementById("inventory-status");
    const openApiReferencesList = document.getElementById("openapi-references");
    const openApiReferencesEmpty = document.getElementById("openapi-references-empty");
    const openApiReferencesFilter = document.getElementById("openapi-reference-filter");
    const dtoReferencesList = document.getElementById("dto-references");
    const dtoReferencesEmpty = document.getElementById("dto-references-empty");
    const dtoReferencesFilter = document.getElementById("dto-reference-filter");
    const openapiReferencesTitle = document.getElementById("openapi-references-title");
    const dtoReferencesTitle = document.getElementById("dto-references-title");
    const mongoClassReferencesList = document.getElementById("mongo-class-references");
    const mongoClassReferencesEmpty = document.getElementById("mongo-class-references-empty");
    const mongoClassReferencesFilter = document.getElementById("mongo-class-reference-filter");
    const mongoClassReferencesTitle = document.getElementById("mongo-class-references-title");
    const requestReplyPatternsList = document.getElementById("request-reply-patterns");
    const requestReplyEmpty = document.getElementById("request-reply-empty");
    const requestReplyTitle = document.getElementById("request-reply-title");
    const analyzedPathsList = document.getElementById("analyzed-paths");
    const analyzedPathsEmpty = document.getElementById("analyzed-paths-empty");
    const pathHistoryTitle = document.getElementById("path-history-title");
    const layoutStatus = document.getElementById("layout-status");
    const layerViewToggle = document.getElementById("layer-view-toggle");
    const layoutButtons = new Map([
      ["forceatlas2", document.getElementById("layout-forceatlas2")],
      ["noverlap", document.getElementById("layout-noverlap")],
      ["forceatlas2-noverlap", document.getElementById("layout-forceatlas2-noverlap")],
      ["elk", document.getElementById("layout-elk")],
      ["cluster", document.getElementById("layout-cluster")],
    ]);
    const layoutLabels = new Map([
      ["forceatlas2", "placement liens groupés"],
      ["noverlap", "placement sans chevauchement"],
      ["forceatlas2-noverlap", "vue graphe"],
      ["elk", "vue couches"],
      ["cluster", "vue namespaces"],
    ]);
    const pathStops = [];
    const analyzedPaths = [];
    const MAX_SIMPLE_PATH_DEPTH = 8;
    const MAX_SIMPLE_PATHS = 8;
    const MAX_SIMPLE_PATH_EXPLORATIONS = 2000;
    function restoreInitialNodePositions() {
      network.forEachNode(node => {
        const position = initialNodePositions.get(node);
        network.setNodeAttribute(node, "x", position.x);
        network.setNodeAttribute(node, "y", position.y);
      });
    }
    function namespaceForNode(node) {
      const data = nodeDataById.get(node);
      if (data?.kind !== "microservice") {
        const producer = preferredOwnerForNode(node);
        const producerNamespace = producer?.cluster_path || producer?.project_namespace_path
          || producer?.project_namespace || producer?.architecture_namespace;
        if (producerNamespace) return producerNamespace;
      }
      if (data?.cluster_path) return data.cluster_path;
      if (data?.project_namespace_path) return data.project_namespace_path;
      if (data?.project_namespace) return data.project_namespace;
      if (data?.architecture_namespace) return data.architecture_namespace;
      const namespaces = [...(data?.runtime_namespaces || []), ...(data?.fact_namespaces || [])];
      if (namespaces.length) return namespaces[0];
      if (data?.kind !== "microservice") return "root";
      const neighbourNamespaces = network.neighbors(node).flatMap(neighbour => {
        const neighbourData = nodeDataById.get(neighbour);
        return [...(neighbourData?.runtime_namespaces || []), ...(neighbourData?.fact_namespaces || [])];
      });
      return neighbourNamespaces[0] || "root";
    }
    function ownerCandidatesForNode(node) {
      const data = nodeDataById.get(node);
      if (data?.kind === "microservice") return [node];
      if (data?.owner_service) {
        const ownerId = `microservice:${data.owner_service}`;
        if (nodeDataById.get(ownerId)) return [ownerId];
      }
      return [
        node,
        ...graphData.links
          .filter(link => link.target === node)
          .map(link => link.source)
          .filter(candidate => nodeDataById.get(candidate)?.kind === "microservice"),
      ];
    }
    function preferredOwnerForNode(node) {
      const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
      const layerRank = new Map(layerOrder.map((layer, index) => [layer, index]));
      return ownerCandidatesForNode(node)
        .map(candidate => nodeDataById.get(candidate))
        .filter(candidate => candidate?.kind === "microservice")
        .sort((left, right) => (
          (layerRank.get(right.layer) ?? -1) - (layerRank.get(left.layer) ?? -1)
          || left.name.localeCompare(right.name)
        ))[0];
    }
    function architectureLayerForNode(node) {
      const data = nodeDataById.get(node);
      if (data?.layer_label || data?.layer) return data.layer_label || data.layer;
      const owner = preferredOwnerForNode(node);
      return owner?.layer_label || owner?.layer || "Unknown";
    }
    function layeredLayerForNode(node) {
      const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
      const data = nodeDataById.get(node);
      const declaredLayer = data?.architecture_layer || data?.layer_label;
      if (declaredLayer && layerOrder.includes(declaredLayer)) return declaredLayer;
      if (data?.kind === "microservice" && layerOrder.includes(data.layer)) return data.layer;
      const neighbourLayer = network.neighbors(node)
        .map(neighbour => nodeDataById.get(neighbour)?.layer)
        .find(layer => layerOrder.includes(layer));
      return neighbourLayer || "application";
    }
    function clusterPathForNode(node) {
      const data = nodeDataById.get(node);
      const owner = ownerCandidatesForNode(node)
        .map(candidate => nodeDataById.get(candidate))
        .find(candidate => candidate?.project_namespace_path || candidate?.project_namespace);
      const path = data?.project_namespace_path
        || data?.architecture_namespace_path
        || owner?.project_namespace_path
        || owner?.architecture_namespace_path
        || namespaceForNode(node);
