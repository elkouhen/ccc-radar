// Ordered source module: 10-rebuild.js
    function layoutGraphNodes(nodes, links) {
      const layoutNodes = nodes.map((node, index) => {
        const angle = (Math.PI * 2 * index) / Math.max(1, nodes.length);
        return { ...node, x: Math.cos(angle), y: Math.sin(angle), vx: 0, vy: 0 };
      });
      const layoutById = new Map(layoutNodes.map(node => [node.id, node]));
      // The layout is deliberately recomputed from the visible dependencies.
      // This prevents hidden relation types from influencing node positions.
      for (let iteration = 0; iteration < 720; iteration += 1) {
        const cooling = .14 * (1 - iteration / 720) + .015;
        for (let i = 0; i < layoutNodes.length; i += 1) {
          for (let j = i + 1; j < layoutNodes.length; j += 1) {
            const a = layoutNodes[i], b = layoutNodes[j];
            const dx = b.x - a.x || (i < j ? .001 : -.001);
            const dy = b.y - a.y || .001;
            const distance2 = dx * dx + dy * dy + .012;
            const strength = 1.25 / distance2;
            a.vx -= dx * strength; a.vy -= dy * strength;
            b.vx += dx * strength; b.vy += dy * strength;
          }
        }
        links.forEach(link => {
          const source = layoutById.get(link.source), target = layoutById.get(link.target);
          if (!source || !target) return;
          const dx = target.x - source.x, dy = target.y - source.y;
          const distance = Math.hypot(dx, dy) || .001;
          // Leave room for the labels on either side of a relation. The
          // browser camera fits the resulting graph, so this only improves
          // readability instead of making a large graph harder to navigate.
          const desired = ["kafka", "request_reply"].includes(link.kind) ? 1.28 : link.kind === "mongodb" ? .86 : 1.02;
          const pull = (distance - desired) * .035;
          const ux = dx / distance, uy = dy / distance;
          source.vx += ux * pull; source.vy += uy * pull;
          target.vx -= ux * pull; target.vy -= uy * pull;
        });
        layoutNodes.forEach(node => {
          node.vx += -node.x * .008; node.vy += -node.y * .008;
          node.x += node.vx * cooling; node.y += node.vy * cooling;
          node.vx *= .72; node.vy *= .72;
        });
      }
      // Preserve the architectural reading after the force calculation:
      // each software layer occupies a horizontal row, with domain at the
      // bottom. Resources without a layer follow the services they connect.
      const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
      const cardSpacingX = 4.0;
      const cardSpacingY = 2.6;
      const nodesByLayer = new Map(layerOrder.map(layer => [layer, []]));
      const resourcesByLayer = new Map(layerOrder.map(layer => [layer, []]));
      const layerForNode = node => {
        if (node?.architecture_layer && nodesByLayer.has(node.architecture_layer)) return node.architecture_layer;
        if (node?.kind === "microservice" && nodesByLayer.has(node.layer)) return node.layer;
        const relatedLayers = links
          .filter(link => link.source === node?.id || link.target === node?.id)
          .map(link => layoutById.get(link.source === node.id ? link.target : link.source)?.layer)
          .filter(layer => nodesByLayer.has(layer));
        return relatedLayers[0] || "application";
      };
      layoutNodes.forEach(node => {
        if (node.kind === "microservice" && nodesByLayer.has(node.layer)) nodesByLayer.get(node.layer).push(node);
        else resourcesByLayer.get(layerForNode(node)).push(node);
      });
      let layerCursor = 0;
      nodesByLayer.forEach((items, layer) => {
        const serviceColumns = Math.min(6, Math.max(1, items.length));
        const resourceItems = resourcesByLayer.get(layer) || [];
        const resourceColumns = Math.min(4, Math.max(1, resourceItems.length));
        const serviceRows = Math.max(1, Math.ceil(items.length / serviceColumns));
        const resourceRows = resourceItems.length ? Math.ceil(resourceItems.length / resourceColumns) : 0;
        const rows = Math.max(serviceRows, resourceRows);
        const layerHeight = Math.max(3.8, (rows - 1) * cardSpacingY + 3.8);
        const centerY = -(layerCursor + layerHeight / 2);
        items.sort((left, right) => left.name.localeCompare(right.name));
        items.forEach((node, index) => {
          const column = index % serviceColumns;
          const row = Math.floor(index / serviceColumns);
          node.x = (column - (serviceColumns - 1) / 2) * cardSpacingX;
          node.y = centerY + (row - (serviceRows - 1) / 2) * cardSpacingY;
        });
        resourceItems.sort((left, right) => left.name.localeCompare(right.name));
        resourceItems.forEach((node, index) => {
          const column = index % resourceColumns;
          const row = Math.floor(index / resourceColumns);
          const serviceRight = ((serviceColumns - 1) / 2) * cardSpacingX;
          node.x = serviceRight + 4.8 + column * cardSpacingX;
          node.y = centerY + (row - (resourceRows - 1) / 2) * cardSpacingY;
        });
        layerCursor += layerHeight + 1.1;
      });
      const verticalOffset = layerCursor / 2;
      layoutNodes.forEach(node => { node.y += verticalOffset; });
      return layoutNodes;
    }
    function layoutIsolatedNodes(nodes, connectedNodes) {
      if (!nodes.length) return [];
      const startX = connectedNodes.length
        ? Math.max(...connectedNodes.map(node => node.x)) + 1.8
        : 0;
      // The HTML cards are fixed-size rectangles. Six columns keep the
      // default overview readable for medium/large graphs while preserving a
      // deterministic envelope that the camera can fit reliably.
      const columns = Math.min(6, Math.max(1, Math.ceil(Math.sqrt(nodes.length / 1.7))));
      return nodes.map((node, index) => ({
        ...node,
        // Keep the initial graph layout in the same coordinate envelope as
        // the fixed-size HTML cards. A sub-unit grid makes Sigma fit several
        // cards into the same CSS rectangle before any user interaction.
        x: startX + (index % columns) * 3.5,
        y: (Math.floor(index / columns) - (Math.ceil(nodes.length / columns) - 1) / 2) * 3.5,
        isolated: true,
      }));
    }
    function rebuildGraph() {
      const visibleLinks = graphData.links.filter(link => (
        isVisibleRelation(link.kind)
        && isVisibleNode(nodeDataById.get(link.source))
        && isVisibleNode(nodeDataById.get(link.target))
      ));
      const visibleNodeIds = new Set(visibleLinks.flatMap(link => [link.source, link.target]));
      const filteredNodes = graphData.nodes.filter(node => isVisibleNode(node));
      const connectedNodes = filteredNodes.filter(node => visibleNodeIds.has(node.id));
      // Keep visible services in the layout even when their only relations
      // point to a service hidden by the selected layer/namespace filter.
      const isolatedNodes = filteredNodes.filter(node => !visibleNodeIds.has(node.id));
      const positionedConnectedNodes = layoutGraphNodes(connectedNodes, visibleLinks);
      const layoutNodes = [
        ...positionedConnectedNodes,
        ...layoutIsolatedNodes(isolatedNodes, positionedConnectedNodes),
      ];
      graphState.graphPanCleanup?.();
      graphState.graphPanCleanup = null;
      graphState.graphWheelCleanup?.();
      graphState.graphWheelCleanup = null;
      // A filtered/layout graph has a new coordinate system. Never compare
      // its camera with a safe state captured from the previous graph.
      graphState.cameraFitAdjusting = false;
      graphState.lastSafeCameraState = null;
      graphState.clusterLayoutPositions = new Map();
      graphLayersOverlay.replaceChildren();
      graphGroupsOverlay.replaceChildren();
      nodeLabelOverlay.replaceChildren();
      renderer?.kill();
      network = new graphology.MultiDirectedGraph();
      const visualNodeKind = node => {
        if (node.kind === "data_schema") return "mongodb_collection";
        if (node.kind === "message_channel") return "kafka_topic";
        return node.kind;
      };
      layoutNodes.forEach(node => network.addNode(node.id, {
        // The readable geometry is rendered by the HTML card overlay; Sigma
        // keeps its compact node marker underneath it.
        // Sigma's layout algorithms use this radius for collision solving.
        // It is deliberately larger than the tiny canvas marker because the
        // visible object is the fixed-size HTML card above it.
        label: "", x: node.x, y: node.y, size: 18, color: node.color,
        type: node.external ? "external_microservice"
          : ["microservice", "kafka_topic", "mongodb_collection"].includes(node.kind)
            ? node.kind : visualNodeKind(node) === "kafka_topic" || visualNodeKind(node) === "mongodb_collection"
              ? visualNodeKind(node) : "generic",
      }));
      visibleLinks.forEach((link, index) => network.addEdgeWithKey(`edge-${index}`, link.source, link.target, {
        label: link.label, size: .85, color: relationColor(link), kind: link.kind, type: "arrow",
      }));
      initialNodePositions = new Map();
      network.forEachNode((node, attributes) => initialNodePositions.set(node, { x: attributes.x, y: attributes.y }));
      renderer = new Sigma(network, document.getElementById("graph"), {
        labelColor: { color: document.documentElement.dataset.theme === "dark" ? "#dce8f7" : "#172033" },
        nodeProgramClasses: {
          microservice: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER),
          external_microservice: createNodeProgram(EXTERNAL_MICROSERVICE_FRAGMENT_SHADER),
          kafka_topic: createNodeProgram(KAFKA_TOPIC_FRAGMENT_SHADER),
          mongodb_collection: createNodeProgram(MONGODB_COLLECTION_FRAGMENT_SHADER),
          generic: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER),
        },
        // Sigma uses a separate hover/picking layer. Its default hover
        // renderer is circular, so use the same rectangular programs here.
        nodeHoverProgramClasses: {
          microservice: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER),
          external_microservice: createNodeProgram(EXTERNAL_MICROSERVICE_FRAGMENT_SHADER),
          kafka_topic: createNodeProgram(KAFKA_TOPIC_FRAGMENT_SHADER),
          mongodb_collection: createNodeProgram(MONGODB_COLLECTION_FRAGMENT_SHADER),
          generic: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER),
        },
        // Keep clicks enabled while disabling Sigma's circular hover overlay;
        // selection feedback is rendered by the HTML card glow instead.
        enableNodeHoverEvents: true,
        hoverRenderer: () => {},
        renderEdgeLabels: false, labelDensity: .06, labelGridCellSize: 160, labelRenderedSizeThreshold: 10,
        // A drag can end close enough to a second click to trigger Sigma's
        // double-click zoom. Zoom remains available through the wheel and the
        // explicit controls, so disable this ambiguous gesture.
        doubleClickZoomingRatio: 1,
        // The shared wheel handler below keeps canvas and HTML-card zoom
        // behavior identical. Native Sigma wheel zoom would only receive
        // events whose target is the canvas.
        enableCameraZooming: false,
        // Panning is handled by the shared camera controller below. Disable
        // Sigma's native inertia so releasing the pointer cannot continue the
        // gesture with an unexpected drift.
        enableCameraPanning: false,
        inertiaDuration: 0,
        inertiaRatio: 0,
        labelAlignment: "center",
        nodeReducer: (node, data) => {
          if (!isVisibleNodeId(node)) return { ...data, hidden: true, label: "" };
          if (!graphState.selectedId || graphState.relatedNodes.has(node)) {
            const order = graphState.pathMicroserviceOrder.get(node);
            return order ? { ...data, label: `${order}. ${data.label}` } : data;
          }
          return { ...data, color: "#d8e0ea", label: "" };
        },
        edgeReducer: (edge, data) => {
          if (!isVisibleNodeId(network.source(edge)) || !isVisibleNodeId(network.target(edge))) return { ...data, hidden: true };
          if (graphState.selectedId && graphState.relatedEdges.has(edge)) return { ...data, size: 1.5 };
          if (graphState.clusteredView || graphState.layeredClusterView) return { ...data, size: .5 };
          if (graphState.selectedId) return { ...data, color: "#d8dee9", size: .35 };
          return data;
        },
      });
      renderOverlays = () => {
        const nodePoints = new Map();
        const graphPointToViewport = graphPoint => {
          return renderer.graphToViewport(graphPoint);
        };
        network.forEachNode((id, attributes) => {
          if (!isVisibleNodeId(id) || attributes.hidden) return;
          // graphToViewport is Sigma's public conversion and includes its
          // current camera, normalization and aspect-ratio handling. The
          // overlays use the same workspace rectangle as the renderer, so
          // these local coordinates are directly usable by the cards/groups.
          const point = graphPointToViewport({ x: attributes.x, y: attributes.y });
          const node = nodeDataById.get(id);
          if (!node || !point) return;
          nodePoints.set(id, point);
        });
        // Card dimensions stay constant. Overlap is an accepted overview
        // state; zoom and pan must never change card size or fight the camera.
        graphGroupsOverlay.replaceChildren();
        nodeLabelOverlay.replaceChildren();
        const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
        if (!graphState.layeredView && !graphState.clusteredView) {
          graphLayersOverlay.replaceChildren();
          graphGroupsOverlay.replaceChildren();
        } else {
        graphLayersOverlay.replaceChildren();
        const layerColors = { external: "#64748b", api: "#0891b2", application: "#2563eb", orchestration: "#9333ea", infrastructure: "#d97706", shared: "#64748b", module: "#475569", domain: "#7c3aed", persistence: "#0f766e" };
        const visibleServices = [...nodePoints.entries()]
          .map(([id, point]) => ({ id, point, node: nodeDataById.get(id) }))
          .filter(item => item.node?.kind === "microservice" && layeredLayerForNode(item.id));
        const layers = layerOrder
          .map(id => ({ id, items: visibleServices.filter(item => layeredLayerForNode(item.id) === id) }))
          .filter(layer => layer.items.length);
        const layerCenters = layers.map(layer => ({
          ...layer,
          center: layer.items.reduce((sum, item) => sum + item.point.y, 0) / layer.items.length,
        }));
        const pointsByLayer = new Map(layerCenters.map(layer => [layer.id, layer.items.map(item => item.point)]));
        if (!nodePoints.size) {
          graphLayersOverlay.replaceChildren();
          graphGroupsOverlay.replaceChildren();
          return;
        }
        [...nodePoints.entries()]
          .filter(([id]) => nodeDataById.get(id)?.kind !== "microservice")
          .forEach(([id, point]) => {
            if (!layerCenters.length) return;
            const nearest = layerCenters.reduce((best, layer) => (
              Math.abs(layer.center - point.y) < Math.abs(best.center - point.y) ? layer : best
            ), layerCenters[0]);
            if (nearest) {
              pointsByLayer.get(nearest.id).push(point);
            }
          });
        const allLayerPoints = graphState.layeredClusterView
          ? [...nodePoints.values()]
          : [...pointsByLayer.values()].flat();
        // Reserve a left gutter inside every layer band for its title.  The
        // cluster rectangles keep their graph-space positions, while the
        // widened band starts earlier so the overlay title never sits on top
        // of the first cluster.
        const layerTitleGutter = 182;
        const contentMinX = allLayerPoints.length
          ? Math.max(0, Math.min(...allLayerPoints.map(point => point.x)) - 92)
          : 0;
        const contentMaxX = allLayerPoints.length
          ? Math.min(window.innerWidth, Math.max(...allLayerPoints.map(point => point.x)) + 92)
          : window.innerWidth;
        // ELK can retain an empty layer shell when filters remove all of its
        // nodes. Never feed empty point sets to Math.min/Math.max: Infinity
        // collapses the band geometry to the top edge and makes every band
        // overlap after projection.
        const renderedLayerCenters = layerCenters.filter(layer => (
          (pointsByLayer.get(layer.id) || []).length > 0
        ));
        const layerBounds = renderedLayerCenters.map(layer => {
          const points = pointsByLayer.get(layer.id) || [];
          return {
            ...layer,
            contentTop: Math.min(...points.map(point => point.y)) - 44,
            contentBottom: Math.max(...points.map(point => point.y)) + 44,
          };
        });
        if (!layerCenters.length) {
          graphLayersOverlay.replaceChildren();
        }
        const layerBands = SystemLensLayerGeometry.computeLayerBands(layerBounds, {
          contentMinX,
          contentMaxX,
          viewportWidth: window.innerWidth,
          titleGutter: layerTitleGutter,
        });
        if (!graphState.clusteredView || graphState.layeredClusterView) renderedLayerCenters.forEach((layer, index) => {
          const bandBounds = layerBands[index];
          const bandHeight = Math.min(window.innerHeight, bandBounds.top + bandBounds.height) - bandBounds.top;
          if (bandHeight < 4) return;
          const band = document.createElement("div");
          band.className = "graph-layer-band";
          band.style.left = `${bandBounds.left}px`; band.style.top = `${bandBounds.top}px`;
          band.style.width = `${bandBounds.width}px`;
          band.style.height = `${bandHeight}px`;
          band.style.setProperty("--layer-accent", layerColors[layer.id] || "#64748b");
          const title = document.createElement("span"); title.className = "graph-layer-title";
          title.textContent = layer.id.replaceAll("_", " "); band.append(title); graphLayersOverlay.append(band);
        });
        const namespaces = new Map();
        [...nodePoints.entries()].forEach(([id, point]) => {
          const node = nodeDataById.get(id);
          const layer = graphState.clusteredView && !graphState.layeredClusterView
            ? "namespaces"
            : graphState.layeredClusterView
              ? layeredLayerForNode(id)
              : node?.architecture_layer || (node?.kind === "microservice" ? node.layer : null);
          const values = graphState.clusteredView || graphState.layeredClusterView
            ? [namespaceForNode(id)]
            : node?.architecture_namespace
              ? [node.architecture_namespace]
              : [...new Set([...(node?.runtime_namespaces || []), ...(node?.fact_namespaces || [])])];
          values.forEach(namespace => {
            if (!layer || !namespace) return;
            const key = `${layer}:${namespace}`;
            const group = namespaces.get(key) || { layer, namespace, points: [], ids: [] };
            group.points.push(point); group.ids.push(id); namespaces.set(key, group);
          });
        });
        const namespaceBounds = new Map();
        namespaces.forEach(group => {
          let minX = Math.min(...group.points.map(point => point.x)) - 68;
          let maxX = Math.max(...group.points.map(point => point.x)) + 68;
          let minY = Math.min(...group.points.map(point => point.y)) - 44;
          let maxY = Math.max(...group.points.map(point => point.y)) + 44;
          if (graphState.clusteredView || graphState.layeredClusterView) {
            // The cards are HTML rectangles in viewport coordinates. Build
            // the namespace envelope from those projected centers so graph
            // zoom cannot make a cluster smaller than its visible children.
            // Keep this in sync with .graph-node-card-label's CSS scale.
            // The envelope must contain the rendered HTML card, not Sigma's
            // logical node dimensions.
            const cardScale = GRAPH_CARD_SCALE;
            const cardHalfWidth = 110 * cardScale / 2;
            const cardHalfHeight = 70 * cardScale / 2;
            minX = Math.min(...group.points.map(point => point.x)) - cardHalfWidth - 24;
            maxX = Math.max(...group.points.map(point => point.x)) + cardHalfWidth + 24;
            minY = Math.min(...group.points.map(point => point.y)) - cardHalfHeight - 24;
            maxY = Math.max(...group.points.map(point => point.y)) + cardHalfHeight + 10;
          }
          const renderedWidth = Math.max(150, maxX - minX);
          const renderedHeight = Math.max(92, maxY - minY);
          namespaceBounds.set(`${group.layer}:${group.namespace}`, {
            minX, maxX: minX + renderedWidth, minY, maxY: minY + renderedHeight,
          });
          const box = document.createElement("div"); box.className = "graph-namespace-group";
          box.dataset.namespace = group.namespace;
          box.style.left = `${minX}px`; box.style.top = `${minY}px`;
          box.style.width = `${renderedWidth}px`;
          box.style.height = `${renderedHeight}px`;
          box.style.setProperty("--namespace-accent", layerColors[group.layer] || "#64748b");
          const title = document.createElement("span"); title.className = "graph-namespace-title";
          title.textContent = group.namespace === "root" ? "ROOT" : group.namespace;
          box.append(title);
          if (graphState.clusteredView || graphState.layeredClusterView) {
            const graphPoints = group.ids.map(id => ({
              point: nodePoints.get(id),
              kind: nodeDataById.get(id)?.kind,
            })).filter(item => item.point);
            const servicePoints = graphPoints.filter(item => item.kind === "microservice");
            const resourcePoints = graphPoints.filter(item => item.kind !== "microservice");
            const addSubLayerTitle = (label, points) => {
              if (!points.length) return;
              const subTitle = document.createElement("span");
              subTitle.className = "graph-cluster-sublayer-title";
              subTitle.textContent = label;
              subTitle.style.top = `${Math.max(28, Math.min(...points.map(item => item.point.y)) - minY - 38)}px`;
              box.append(subTitle);
            };
            addSubLayerTitle("Microservices", servicePoints);
            addSubLayerTitle("Ressources", resourcePoints);
          }
          graphLayersOverlay.append(box);
        });
        if (!showProjectGroups.checked) return;
        const namespaceGroupEntries = [...namespaces.entries()];
        (graphData.groups || []).forEach(group => {
          const children = group.children || [];
          const childNamespaceBounds = namespaceGroupEntries
            .filter(([, namespaceGroup]) => (
              (!group.namespace
                || namespaceGroup.namespace === group.namespace
                || namespaceGroup.namespace.startsWith(`${group.namespace}/`))
              && namespaceGroup.ids.some(id => children.includes(id))
            ))
            .map(([key]) => namespaceBounds.get(key))
            .filter(Boolean);
          let minX;
          let maxX;
          let minY;
          let maxY;
          if (childNamespaceBounds.length) {
            // A parent group is defined by its rendered children. This keeps
            // nested boxes aligned after zoom/pan and guarantees containment.
            minX = Math.min(...childNamespaceBounds.map(bounds => bounds.minX)) - 22;
            maxX = Math.max(...childNamespaceBounds.map(bounds => bounds.maxX)) + 22;
            minY = Math.min(...childNamespaceBounds.map(bounds => bounds.minY)) - 28;
            maxY = Math.max(...childNamespaceBounds.map(bounds => bounds.maxY)) + 22;
          } else {
            const points = children.map(id => nodePoints.get(id)).filter(Boolean);
            if (points.length < 2) return;
            minX = Math.min(...points.map(point => point.x)) - 68;
            maxX = Math.max(...points.map(point => point.x)) + 68;
            minY = Math.min(...points.map(point => point.y)) - 52;
            maxY = Math.max(...points.map(point => point.y)) + 52;
          }
          const container = document.createElement("div");
          container.className = "graph-project-group";
          container.dataset.namespaceGroup = group.namespace || group.name || "root";
          container.style.left = `${minX}px`;
          container.style.top = `${minY}px`;
          container.style.width = `${Math.max(150, maxX - minX)}px`;
          container.style.height = `${Math.max(120, maxY - minY)}px`;
          container.style.setProperty("--group-accent", "#64748b");
          const title = document.createElement("span");
          title.className = "graph-project-group-title";
          title.textContent = group.name;
          container.append(title);
          graphGroupsOverlay.append(container);
        });
        }
        network.forEachNode((id, attributes) => {
          const point = nodePoints.get(id);
          const node = nodeDataById.get(id);
          if (!node || !point) return;
          const label = document.createElement("span");
          const isTopic = node.kind === "message_channel" || node.kind === "kafka_topic";
          const isDatabase = node.kind === "data_schema" || node.kind === "mongodb_collection";
          const isResource = isTopic || isDatabase;
          label.className = `graph-node-card-label${isResource ? " is-resource" : ""}${isTopic ? " is-topic" : ""}${isDatabase ? " is-collection" : ""}${graphState.selectedId === id ? " is-selected" : ""}${graphState.hoveredId === id ? " is-hovered" : ""}${graphState.selectedId && graphState.selectedId !== id && graphState.relatedNodes && !graphState.relatedNodes.has(id) ? " is-dimmed" : ""}`;
          label.dataset.nodeKind = node.kind;
          label.dataset.nodeId = id;
          const cardScale = GRAPH_CARD_SCALE;
          label.style.setProperty("--graph-card-scale", String(cardScale));
          label.style.left = `${point.x}px`;
          label.style.top = `${point.y}px`;
          // Cards keep one fixed screen-space size in every view.
          label.style.transform = `translate(-50%, -50%) scale(${cardScale})`;
          label.style.setProperty("--card-accent", node.color || "#64748b");
          const icon = document.createElement("span");
          icon.className = `graph-node-card-icon ${isTopic ? "is-topic" : isDatabase ? "is-database" : "is-service"}`;
          const name = document.createElement("span");
          name.className = "graph-node-card-name";
          name.textContent = node.name;
          const kind = document.createElement("span");
          kind.className = "graph-node-card-kind";
          kind.textContent = node.technology || nodeKindLabel(node);
          label.append(icon);
          label.append(name, kind);
          // Cards sit above Sigma's canvas and therefore normally consume the
          // pointer stream. Pan the camera directly when a drag starts on a
          // card, while preserving a plain click for node selection. Keeping
          // the initial camera state fixed avoids jumps when overlays are
          // rebuilt during the drag.
          let forwardedPointer = null;
          let suppressNextCardClick = false;
          const finishForwardedPointer = event => {
            if (!forwardedPointer) return;
            const wasDrag = forwardedPointer.moved;
            forwardedPointer = null;
            suppressNextCardClick = wasDrag;
            nodeLabelOverlay.style.pointerEvents = "none";
            try { label.releasePointerCapture?.(event.pointerId); } catch (_error) { /* node was rebuilt during the gesture */ }
            window.removeEventListener("pointermove", moveForwardedPointer, true);
            window.removeEventListener("pointerup", finishForwardedPointer, true);
            window.removeEventListener("pointercancel", finishForwardedPointer, true);
            if (!wasDrag) selectNode(id);
          };
          const moveForwardedPointer = event => {
            if (!forwardedPointer || event.pointerId !== forwardedPointer.pointerId) return;
            if (Math.hypot(
              event.clientX - forwardedPointer.startX,
              event.clientY - forwardedPointer.startY,
            ) > 3) forwardedPointer.moved = true;
            if (!forwardedPointer.moved) return;
            const camera = renderer.getCamera();
            const state = forwardedPointer.cameraState;
            // Sigma's internal drawing buffer can use a device-pixel ratio;
            // camera panning, however, is driven by CSS viewport pixels.
            const viewport = document.getElementById("graph").getBoundingClientRect();
            const width = Math.max(viewport.width, 1);
            const height = Math.max(viewport.height, 1);
            camera.setState({
              ...state,
              x: state.x - (event.clientX - forwardedPointer.startX) / width,
              y: state.y + (event.clientY - forwardedPointer.startY) / height,
            });
          };
          label.addEventListener("pointerdown", event => {
            if (event.button !== 0 || forwardedPointer) return;
            forwardedPointer = {
              pointerId: event.pointerId,
              startX: event.clientX,
              startY: event.clientY,
              moved: false,
              cameraState: renderer.getCamera().getState(),
            };
            event.preventDefault();
            event.stopPropagation();
            label.setPointerCapture?.(event.pointerId);
            nodeLabelOverlay.style.pointerEvents = "none";
            window.addEventListener("pointermove", moveForwardedPointer, true);
            window.addEventListener("pointerup", finishForwardedPointer, true);
            window.addEventListener("pointercancel", finishForwardedPointer, true);
          });
          // Sigma also listens to the legacy mouse stream. Without stopping
          // it, a card drag would be handled once by our direct pan and once
          // by Sigma's native mouse pan, producing an amplified movement.
          label.addEventListener("mousedown", event => {
            if (event.button !== 0) return;
            event.preventDefault();
            event.stopImmediatePropagation();
          });
          label.addEventListener("click", event => {
            event.stopPropagation();
            if (suppressNextCardClick) {
              suppressNextCardClick = false;
              return;
            }
            selectNode(id);
          });
          nodeLabelOverlay.append(label);
        });
      };
      // Camera updates can fire several times during one drag. Coalesce them
      // into the next animation frame so the canvas and its HTML overlays are
      // repainted from the same camera state. Rebuilding synchronously for
      // every intermediate pan state can briefly expose partial cluster boxes.
      let labelRefreshScheduled = false;
      const scheduleNodeLabelRefresh = () => {
        if (labelRefreshScheduled) return;
        labelRefreshScheduled = true;
        requestAnimationFrame(() => {
          labelRefreshScheduled = false;
          requestGraphRender();
        });
      };
      renderer.on("afterRender", scheduleNodeLabelRefresh);
      renderer.getCamera().on("updated", scheduleNodeLabelRefresh);
      requestGraphRender();
      renderer.on("enterNode", ({ node }) => { graphState.hoveredId = node; requestGraphRender(); });
      renderer.on("leaveNode", () => { graphState.hoveredId = null; requestGraphRender(); });
      renderer.on("clickNode", ({ node }) => selectNode(node));
      renderer.on("clickStage", reset);
      renderer.on("doubleClickStage", event => event.preventSigmaDefault?.());
      renderer.on("doubleClickNode", event => event.preventSigmaDefault?.());
      const graphCanvas = document.getElementById("graph");
      const handleGraphWheel = event => {
        if (event.target.closest?.(".toolbar, #details")) return;
        event.preventDefault();
        const camera = renderer.getCamera();
        const state = camera.getState();
        const viewport = graphCanvas.getBoundingClientRect();
        const cursor = {
          x: event.clientX - viewport.left,
          y: event.clientY - viewport.top,
        };
        const graphPoint = renderer.viewportToGraph(cursor);
        const delta = event.deltaMode === 1 ? event.deltaY * 16 : event.deltaY;
        const factor = Math.exp(Math.max(-120, Math.min(120, delta)) * .0012);
        const ratio = Math.max(.01, Math.min(100, state.ratio * factor));
        camera.setState({
          ...state,
          // A larger Sigma ratio is a zoom-out. Bound manual wheel changes
          // so one trackpad burst cannot jump across the safe camera state.
          ratio,
        });
        const projected = renderer.graphToViewport(graphPoint);
        const dx = cursor.x - projected.x;
        const dy = cursor.y - projected.y;
        const nextState = camera.getState();
        const width = Math.max(viewport.width, 1);
        const height = Math.max(viewport.height, 1);
        camera.setState({
          ...nextState,
          // Keep the graph point under the cursor fixed while changing scale.
          x: nextState.x - dx / width,
          y: nextState.y + dy / height,
        });
      };
      graphCanvas.addEventListener("wheel", handleGraphWheel, { passive: false });
      nodeLabelOverlay.addEventListener("wheel", handleGraphWheel, { passive: false });
      graphState.graphWheelCleanup = () => {
        graphCanvas.removeEventListener("wheel", handleGraphWheel);
        nodeLabelOverlay.removeEventListener("wheel", handleGraphWheel);
      };
      let graphPan = null;
      const finishGraphPan = event => {
        if (!graphPan || event.pointerId !== graphPan.pointerId) return;
        graphPan = null;
        window.removeEventListener("pointermove", moveGraphPan, true);
        window.removeEventListener("pointerup", finishGraphPan, true);
        window.removeEventListener("pointercancel", finishGraphPan, true);
        try { graphCanvas.releasePointerCapture?.(event.pointerId); } catch (_error) { /* canvas was rebuilt during the gesture */ }
      };
      const moveGraphPan = event => {
        if (!graphPan || event.pointerId !== graphPan.pointerId) return;
        const state = graphPan.cameraState;
        const viewport = graphCanvas.getBoundingClientRect();
        const width = Math.max(viewport.width, 1);
        const height = Math.max(viewport.height, 1);
        renderer.getCamera().setState({
          ...state,
          x: state.x - (event.clientX - graphPan.startX) / width,
          y: state.y + (event.clientY - graphPan.startY) / height,
        });
      };
      const startGraphPan = event => {
        if (event.button !== 0 || event.target.closest(".graph-node-card-label")) return;
        graphPan = {
          pointerId: event.pointerId,
          startX: event.clientX,
          startY: event.clientY,
          cameraState: renderer.getCamera().getState(),
        };
        event.preventDefault();
        event.stopImmediatePropagation();
        graphCanvas.setPointerCapture?.(event.pointerId);
        window.addEventListener("pointermove", moveGraphPan, true);
        window.addEventListener("pointerup", finishGraphPan, true);
        window.addEventListener("pointercancel", finishGraphPan, true);
      };
      graphCanvas.addEventListener("pointerdown", startGraphPan, true);
      graphState.graphPanCleanup = () => {
        graphCanvas.removeEventListener("pointerdown", startGraphPan, true);
        try { graphCanvas.releasePointerCapture?.(graphPan?.pointerId); } catch (_error) { /* canvas was rebuilt during the gesture */ }
        window.removeEventListener("pointermove", moveGraphPan, true);
        window.removeEventListener("pointerup", finishGraphPan, true);
        window.removeEventListener("pointercancel", finishGraphPan, true);
      };
      graphCanvas.dataset.relationCount = String(visibleLinks.length);
      graphCanvas.dataset.visibleNodeCount = String(layoutNodes.length);
      graphCanvas.dataset.visibleNodeKinds = [...new Set(layoutNodes.map(node => node.kind))].sort().join(",");
      graphCanvas.dataset.invalidCoordinates = String(
        layoutNodes.some(node => !Number.isFinite(node.x) || !Number.isFinite(node.y))
      );
      graphCanvas.setAttribute("aria-label", `Graphe des interactions : ${visibleLinks.length} relations`);
      // Keep Sigma's native camera coordinate system; layout coordinates are
      // centered above so the complete vertical stack stays in view.
      renderer.getCamera().animatedReset({ duration: 0 });
    }
    rebuildGraph();
