// Ordered source module: 30-layouts.js
      return path === "root" ? "ROOT" : path;
    }
    function packClusterGraphPositions() {
      document.getElementById("graph").dataset.clusterSubLayers = "microservices-first,resources-second";
      const namespaceGroups = new Map();
      network.forEachNode(node => {
        const namespace = namespaceForNode(node);
        if (!namespaceGroups.has(namespace)) namespaceGroups.set(namespace, []);
        namespaceGroups.get(namespace).push(node);
      });
      const groups = [...namespaceGroups.entries()].sort(([left], [right]) => left.localeCompare(right));
      // Keep namespace positions deterministic. The camera fits node centers
      // and cards remain fixed-size overlays; dense groups may overlap in the
      // overview instead of triggering a second layout or camera correction.
      const nodeGapX = 1100;
      const nodeGapY = 900;
      const clusterPaddingX = 90;
      const clusterPaddingY = 80;
      const clusterGapX = 3600;
      const clusterGapY = 2600;
      const packedGroups = groups.map(([namespace, nodes]) => {
        const orderedNodes = [...nodes].sort((left, right) => {
          const leftPosition = graphState.clusterLayoutPositions.get(left);
          const rightPosition = graphState.clusterLayoutPositions.get(right);
          return (rightPosition?.y ?? 0) - (leftPosition?.y ?? 0)
            || (leftPosition?.x ?? 0) - (rightPosition?.x ?? 0)
            || left.localeCompare(right);
        });
        const microservices = orderedNodes.filter(node => nodeDataById.get(node)?.kind === "microservice");
        const resources = orderedNodes.filter(node => nodeDataById.get(node)?.kind !== "microservice");
        const subLayers = SystemLensLayerGeometry.computeClusterSubLayers(
          microservices, resources, { nodeGapX, nodeGapY, subLayerGapY: 1200, maxColumns: 5 }
        );
        // The overlay envelope is deliberately larger than the node grid:
        // refreshNodeLabels adds 120px horizontally and 100px vertically on
        // each side when it draws a namespace box.  Pack using that envelope,
        // while keeping the node coordinates centered on the grid itself.
        return {
          namespace,
          nodes: orderedNodes,
          resources,
          subLayers,
          width: subLayers.width,
          height: Math.abs(subLayers.height),
          envelopeWidth: subLayers.width + 240,
          envelopeHeight: Math.abs(subLayers.height) + 200,
        };
      });
      const outerColumns = Math.min(3, Math.max(1, Math.ceil(Math.sqrt(packedGroups.length))));
      const outerRows = Math.ceil(packedGroups.length / outerColumns);
      const outerWidth = Math.max(...packedGroups.map(group => group.envelopeWidth + clusterPaddingX * 2));
      const rowHeights = Array.from({ length: outerRows }, (_, row) => Math.max(
        ...packedGroups.slice(row * outerColumns, (row + 1) * outerColumns)
          .map(group => group.envelopeHeight + clusterPaddingY * 2),
      ));
      let cursorY = 0;
      packedGroups.forEach((group, index) => {
        const row = Math.floor(index / outerColumns);
        const column = index % outerColumns;
        const clusterX = (column - (outerColumns - 1) / 2) * (outerWidth + clusterGapX);
        const clusterY = cursorY + clusterPaddingY;
        group.nodes.forEach(node => {
          const position = group.subLayers.positions[node];
          network.setNodeAttribute(node, "x", clusterX - group.width / 2 + position.x);
          network.setNodeAttribute(node, "y", clusterY + position.y);
          network.setNodeAttribute(
            node,
            "clusterSubLayer",
            group.resources.includes(node) ? "resources" : "microservices",
          );
        });
        if (column === outerColumns - 1 || index === packedGroups.length - 1) {
          cursorY += rowHeights[row] + clusterGapY;
        }
      });
      const graphElement = document.getElementById("graph");
      graphElement.dataset.clusterSubLayers = "microservices-first,resources-second";
      graphElement.dataset.clusterSubLayerPositions = JSON.stringify(
        [...new Set([...network.nodes()].map(node => network.getNodeAttribute(node, "clusterSubLayer")))]
      );
      graphElement.dataset.clusterLayout = JSON.stringify(
        Object.fromEntries([...network.nodes()].map(node => [node, {
          cluster: namespaceForNode(node),
          subLayer: network.getNodeAttribute(node, "clusterSubLayer"),
          x: network.getNodeAttribute(node, "x"),
          y: network.getNodeAttribute(node, "y"),
        }]))
      );
    }
    function packLayeredClusterGraphPositions() {
      const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
      const groupsByLayer = new Map(layerOrder.map(layer => [layer, new Map()]));
      const layerForNode = node => layeredLayerForNode(node);
      network.forEachNode(node => {
        const layer = layerForNode(node);
        const namespace = namespaceForNode(node);
        const namespaces = groupsByLayer.get(layer);
        if (!namespaces.has(namespace)) namespaces.set(namespace, []);
        namespaces.get(namespace).push(node);
      });
      const nodeGapX = 1000;
      const nodeGapY = 700;
      const clusterPaddingX = 100;
      const clusterPaddingY = 120;
      const clusterGapX = 220;
      const namespaceGapY = 300;
      const layerGapY = 970;
      let cursorY = 0;
      layerOrder.forEach(layer => {
        const groups = [...groupsByLayer.get(layer).entries()]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([namespace, nodes]) => {
            const orderedNodes = [...nodes].sort((left, right) => {
              const leftPosition = graphState.clusterLayoutPositions.get(left);
              const rightPosition = graphState.clusterLayoutPositions.get(right);
              return (rightPosition?.y ?? 0) - (leftPosition?.y ?? 0)
                || (leftPosition?.x ?? 0) - (rightPosition?.x ?? 0)
                || left.localeCompare(right);
            });
            const columns = Math.min(12, Math.max(1, Math.ceil(Math.sqrt(orderedNodes.length))));
            const microservices = orderedNodes.filter(node => nodeDataById.get(node)?.kind === "microservice");
            const resources = orderedNodes.filter(node => nodeDataById.get(node)?.kind !== "microservice");
            return {
              namespace,
              nodes: orderedNodes,
              columns,
              microservices,
              resources,
            };
          });
        if (!groups.length) return;
        const maxClusterRows = 3;
        const maxColumns = group => Math.min(12, group.nodes.length);
        const layoutLayerGroups = () => {
          const columns = Math.min(10, Math.max(1, groups.length));
          const rows = Math.ceil(groups.length / columns);
          groups.forEach(group => {
            group.subLayers = SystemLensLayerGeometry.computeClusterSubLayers(
              group.microservices,
              group.resources,
              { nodeGapX, nodeGapY, subLayerGapY: 700, maxColumns: group.columns },
            );
            group.width = group.subLayers.width;
            group.height = Math.abs(group.subLayers.height);
          });
          const outerWidth = Math.max(...groups.map(group => group.width + 240 + clusterPaddingX * 2));
          const rowHeights = Array.from({ length: rows }, (_, row) => Math.max(
            ...groups.slice(row * columns, (row + 1) * columns)
              .map(group => group.height + 200 + clusterPaddingY * 2),
          ));
          let layerCursorY = cursorY;
          groups.forEach((group, index) => {
            const row = Math.floor(index / columns);
            const column = index % columns;
            const clusterX = (column - (columns - 1) / 2) * (outerWidth + clusterGapX);
            const clusterY = -(layerCursorY + clusterPaddingY);
            // Nodes are laid out from the first row towards negative graph Y.
            // The envelope must therefore extend by the full row span from
            // the first node, not by half of that span on either side.
            group.minY = clusterY - group.height - clusterPaddingY;
            group.maxY = clusterY + clusterPaddingY;
            group.nodes.forEach(node => {
              const position = group.subLayers.positions[node];
              network.setNodeAttribute(node, "x", clusterX - group.width / 2 + position.x);
              network.setNodeAttribute(node, "y", clusterY + position.y);
              network.setNodeAttribute(
                node,
                "clusterSubLayer",
                group.resources.includes(node) ? "resources" : "microservices",
              );
            });
            if (column === columns - 1 || index === groups.length - 1) {
              layerCursorY += rowHeights[row] + namespaceGapY;
            }
          });
          return layerCursorY;
        };
        // Keep exceptionally tall clusters from crossing a neighbouring layer.
        // Adding columns trades vertical pressure for horizontal space; the
        // outer width is recomputed on every pass so the diagram grows with it.
        for (let pass = 0; pass < 12; pass += 1) {
          layoutLayerGroups();
          const overflowing = groups.filter(group => (
            Math.ceil(group.nodes.length / group.columns) > maxClusterRows
            || group.maxY - group.minY > (maxClusterRows - 1) * nodeGapY + 2 * clusterPaddingY
          ));
          if (!overflowing.length) break;
          const changed = overflowing.some(group => {
            if (group.columns >= maxColumns(group)) return false;
            group.columns += 1;
            return true;
          });
          if (!changed) break;
        }
        cursorY = layoutLayerGroups() + layerGapY;
      });
      document.getElementById("graph").dataset.layeredSubLayers = "microservices-first,resources-second";
    }
    async function applyFcoseClusterLayout() {
      graphState.clusterLayoutPositions = new Map();
      if (typeof window.cytoscape !== "function") {
        packClusterGraphPositions();
        return false;
      }
      const namespaceGroups = new Map();
      network.forEachNode(node => {
        const namespace = namespaceForNode(node);
        if (!namespaceGroups.has(namespace)) namespaceGroups.set(namespace, []);
        namespaceGroups.get(namespace).push(node);
      });
      const groups = [...namespaceGroups.entries()].sort(([left], [right]) => left.localeCompare(right));
      const parentByNamespace = new Map(groups.map(([namespace], index) => [namespace, `cluster-${index}`]));
      const elements = [];
      groups.forEach(([namespace, nodes]) => {
        const parent = parentByNamespace.get(namespace);
        elements.push({ data: { id: parent } });
        nodes.forEach(node => {
          const attributes = network.getNodeAttributes(node);
          elements.push({
            data: { id: node, parent },
            position: { x: attributes.x, y: attributes.y },
          });
        });
      });
      network.forEachEdge((edge, _attributes, source, target) => {
        elements.push({ data: { id: `edge-${edge}`, source, target } });
      });
      let cy;
      try {
        cy = window.cytoscape({ headless: true, elements });
        const layout = cy.layout({
          name: "fcose",
          quality: "proof",
          randomize: true,
          animate: false,
          fit: false,
          nodeDimensionsIncludeLabels: false,
          tilingPaddingVertical: 40,
          tilingPaddingHorizontal: 40,
          idealEdgeLength: 180,
          nodeRepulsion: 9000,
          gravity: 0.25,
          gravityCompound: 1.0,
        });
        let layoutStopped = false;
        await Promise.race([
          new Promise((resolve, reject) => {
          layout.one("layoutstop", resolve);
          try { layout.run(); } catch (error) { reject(error); }
          }),
          new Promise(resolve => setTimeout(() => resolve(false), 1500)),
        ]).then(result => { layoutStopped = result !== false; });
        if (!layoutStopped) throw new Error("fCoSE n'a pas terminé dans le délai prévu");
        cy.nodes().filter(node => !node.isParent()).forEach(node => {
          const position = node.position();
          graphState.clusterLayoutPositions.set(node.id(), { x: position.x, y: position.y });
        });
        const fcoseApplied = graphState.clusterLayoutPositions.size === network.order;
        if (!fcoseApplied) {
          network.forEachNode(node => {
            const attributes = network.getNodeAttributes(node);
            graphState.clusterLayoutPositions.set(node, { x: attributes.x, y: attributes.y });
          });
        }
        packClusterGraphPositions();
        return fcoseApplied;
      } catch (error) {
        console.warn("fCoSE est indisponible pour le rendu par clusters ; la grille déterministe est utilisée.", error);
        network.forEachNode(node => {
          const attributes = network.getNodeAttributes(node);
          graphState.clusterLayoutPositions.set(node, { x: attributes.x, y: attributes.y });
        });
        packClusterGraphPositions();
        return false;
      } finally {
        cy?.destroy();
      }
    }
    async function applyElkLayout(libraries) {
      const layerOrder = ["api", "application", "orchestration", "infrastructure", "domain", "persistence", "external"];
      const groups = new Map(layerOrder.map(layer => [layer, new Map()]));
      const layerForNode = node => {
        const data = nodeDataById.get(node);
        if (data?.architecture_layer && groups.has(data.architecture_layer)) return data.architecture_layer;
        if (data?.kind === "microservice" && groups.has(data.layer)) return data.layer;
        const neighbourLayers = network.neighbors(node)
          .map(neighbour => nodeDataById.get(neighbour)?.layer)
          .filter(layer => groups.has(layer));
        return neighbourLayers[0] || "application";
      };
      network.forEachNode(node => {
        const layer = layerForNode(node);
        const namespace = namespaceForNode(node);
        const layerGroups = groups.get(layer);
        if (!layerGroups.has(namespace)) layerGroups.set(namespace, []);
        layerGroups.get(namespace).push(node);
      });
      const children = [...groups.entries()]
        .filter(([, namespaces]) => namespaces.size)
        .map(([layer, namespaces]) => ({
          id: `elk-layer-${layer}`,
          layoutOptions: {
            "elk.algorithm": "box",
            "elk.direction": "DOWN",
            "elk.hierarchyHandling": "INCLUDE_CHILDREN",
            "elk.padding": "[top=34,left=28,bottom=28,right=28]",
          },
          children: [...namespaces.entries()].map(([namespace, nodes]) => ({
            id: `elk-namespace-${layer}-${namespace.replace(/[^a-zA-Z0-9_-]/g, "-")}`,
            layoutOptions: {
              "elk.algorithm": "box",
              "elk.direction": "DOWN",
              "elk.hierarchyHandling": "INCLUDE_CHILDREN",
              "elk.padding": "[top=28,left=22,bottom=22,right=22]",
              "elk.spacing.nodeNode": "55",
            },
            children: nodes.map(node => ({ id: node, width: 150, height: 86 })),
          })),
        }));
      const layerOrderEdges = children.slice(1).map((layer, index) => ({
        id: `elk-layer-order-${index}`,
        sources: [children[index].id],
        targets: [layer.id],
      }));
      const result = await libraries.elk.layout({
        id: "systemlens-architecture",
        layoutOptions: {
          "elk.algorithm": "layered",
          "elk.direction": "DOWN",
          "elk.hierarchyHandling": "INCLUDE_CHILDREN",
          "elk.edgeRouting": "ORTHOGONAL",
          "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
          "elk.layered.spacing.nodeNodeBetweenLayers": "110",
          "elk.spacing.nodeNode": "70",
          "elk.spacing.edgeNode": "40",
          "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
        },
        children,
        // Architecture relations are rendered by Sigma. The synthetic order
        // edges only stack software layers vertically for the layer view.
        edges: layerOrderEdges,
      });
      const offsetX = (result.width || 0) / -2;
      const offsetY = (result.height || 0) / -2;
      const placeChildren = (items, parentX = 0, parentY = 0, parentId = "") => items.forEach((child, index) => {
        const x = parentX + Number(child.x || 0);
        const y = parentY + Number(child.y || 0);
        if (child.children) placeChildren(child.children, x, y, child.id);
        else {
          const namespaceChild = parentId.startsWith("elk-namespace-");
          const namespaceColumns = 5;
          // ELK coordinates are later normalized by Sigma. Keep a generous
          // margin so the 110x70 HTML cards never touch after projection.
          const spreadX = namespaceChild ? (index % namespaceColumns) * 1500 : 0;
          const spreadY = namespaceChild ? Math.floor(index / namespaceColumns) * 900 : 0;
          network.setNodeAttribute(child.id, "x", x + spreadX + offsetX);
          // ELK's Y axis grows downwards, while Sigma's graph Y axis grows
          // upwards. Invert it so the canonical order keeps domain at bottom.
          network.setNodeAttribute(child.id, "y", -((namespaceChild ? parentY + spreadY : y) + offsetY));
        }
      });
      placeChildren(result.children || []);
    }
    function setActiveLayout(layout) {
      layoutButtons.forEach((button, key) => {
        const active = key === layout;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", String(active));
      });
    }
    function resolveGraphCardOverlaps() {
      // Cards are intentionally fixed-size HTML overlays. The overview
      // contract allows overlap, so moving Sigma nodes after the camera fit
      // would make the fitted bounds stale and could push nodes off-screen.
      // Keep this hook for compatibility with older layout callers.
      return;
      const cardScale = GRAPH_CARD_SCALE;
      const cardWidth = 110 * cardScale;
      const cardHeight = 70 * cardScale;
      const gap = 4;
      const ids = [...network.nodes()];
      const points = new Map(ids.map(id => {
          const attributes = network.getNodeAttributes(id);
          const point = renderer.graphToViewport({ x: attributes.x, y: attributes.y });
          return [id, { x: point.x, y: point.y }];
        }));
      const cellWidth = cardWidth + gap;
      const cellHeight = cardHeight + gap;
      const separatePair = (left, right) => {
        const overlapX = cellWidth - Math.abs(left.x - right.x);
        const overlapY = cellHeight - Math.abs(left.y - right.y);
        if (overlapX <= 0 || overlapY <= 0) return false;
        const moveAlongX = overlapX <= overlapY;
        const direction = moveAlongX
          ? (left.x >= right.x ? 1 : -1)
          : (left.y >= right.y ? 1 : -1);
        if (moveAlongX) {
          left.x += direction * overlapX / 2;
          right.x -= direction * overlapX / 2;
        } else {
          left.y += direction * overlapY / 2;
          right.y -= direction * overlapY / 2;
        }
        return true;
      };
      // Small and medium architectures benefit from an exact pair pass. It
      // avoids the stale-cell problem of an in-place spatial hash when a card
      // crosses a bucket during the same pass. Keep the hash path for large
      // graphs so collision repair remains bounded instead of quadratic.
      for (let pass = 0; pass < (ids.length <= 400 ? 32 : 48); pass += 1) {
        if (ids.length <= 400) {
          let changed = false;
          for (let leftIndex = 0; leftIndex < ids.length; leftIndex += 1) {
            for (let rightIndex = leftIndex + 1; rightIndex < ids.length; rightIndex += 1) {
              changed = separatePair(points.get(ids[leftIndex]), points.get(ids[rightIndex])) || changed;
            }
          }
          if (!changed) break;
          continue;
        }
        const buckets = new Map();
        let changed = false;
        ids.forEach(id => {
          const point = points.get(id);
          if (!point) return;
          const cellX = Math.floor(point.x / cellWidth);
          const cellY = Math.floor(point.y / cellHeight);
          for (let offsetX = -1; offsetX <= 1; offsetX += 1) {
            for (let offsetY = -1; offsetY <= 1; offsetY += 1) {
              for (const otherId of buckets.get(`${cellX + offsetX}:${cellY + offsetY}`) || []) {
                const other = points.get(otherId);
                if (!other) continue;
                changed = separatePair(point, other) || changed;
              }
            }
          }
          const key = `${cellX}:${cellY}`;
          const bucket = buckets.get(key) || [];
          bucket.push(id);
          buckets.set(key, bucket);
        });
        if (!changed) break;
      }
      points.forEach((point, id) => {
        const graphPoint = renderer.viewportToGraph(point);
        network.mergeNodeAttributes(id, { x: graphPoint.x, y: graphPoint.y });
      });
    }

    async function applyLayout(layout) {
      const request = ++graphState.layoutRequest;
      const previousLayout = graphState.activeLayout;
      updateGraphState({
        activeLayout: layout,
        lastSafeCameraState: null,
        cameraFitAdjusting: false,
      });
      const label = layoutLabels.get(layout);
      const nextLayeredView = ["elk", "cluster"].includes(layout);
      const switchingView = graphState.layeredView !== nextLayeredView || graphState.clusteredView !== (layout === "cluster");
      if (switchingView) {
        updateGraphState({
          selectedId: null,
          hoveredId: null,
          relatedNodes: null,
          relatedEdges: null,
          pathMicroserviceOrder: new Map(),
        });
        clearPathControls();
        setDetailsEmpty("Selectionnez un noeud pour isoler ses relations et afficher ses APIs.");
      }
      const zoomOutButton = document.getElementById("zoom-out");
      zoomOutButton.disabled = false;
      zoomOutButton.title = "Dézoomer";
      const nextView = !nextLayeredView ? "couches" : layout === "elk" ? "clusters" : "graphe";
      const nextViewLabel = {
        couches: "Afficher le rendu en couches",
        clusters: "Afficher le rendu par clusters",
        graphe: "Afficher le rendu graphe",
      }[nextView];
      layerViewToggle.textContent = nextViewLabel.replace("Afficher ", "");
      layerViewToggle.setAttribute("aria-label", nextViewLabel);
      layerViewToggle.title = nextViewLabel;
      setActiveLayout(layout);
      layoutStatus.textContent = `Calcul de la disposition ${label}…`;
      // Cluster placement is deterministic and local; it does not need any
      // external layout library. Keep the library loading requirement for
      // ForceAtlas2/Noverlap and the ELK layer view only.
      // The layer view only needs ELK. Do not make it wait for the dynamic
      // ForceAtlas2/Noverlap imports used by the other views: while those
      // imports are pending, ELK layout completion (and therefore stable pan
      // interaction) could be delayed indefinitely.
      const libraries = layout === "cluster"
        ? {}
        : layout === "elk"
          ? { elk: typeof window.ELK === "function" ? new window.ELK() : null }
          : await layoutLibraries;
      if (request !== graphState.layoutRequest) return;
      if (libraries === null && layout !== "cluster") {
        updateGraphState({ activeLayout: previousLayout });
        setActiveLayout(previousLayout);
        layoutStatus.textContent = "Les dispositions du graphe sont indisponibles ; la disposition initiale est conservee.";
        return;
      }
      if (layout === "elk" && libraries.elk === null) {
        updateGraphState({ activeLayout: previousLayout });
        setActiveLayout(previousLayout);
        layoutStatus.textContent = "ELK.js est indisponible ; la disposition initiale est conservee.";
        return;
      }
      try {
        restoreInitialNodePositions();
        if (layout === "elk") {
          await applyElkLayout(libraries);
          packLayeredClusterGraphPositions();
        }
        if (layout === "cluster") {
          // The deterministic packer is the source of truth for this view:
          // it sizes namespaces from their two sub-layers and reserves sibling
          // gaps before projection. fCoSE can block the browser main thread
          // on large compound graphs, which prevents the fallback and leaves
          // partially packed cluster envelopes on screen.
          packClusterGraphPositions();
        }
        if (layout === "forceatlas2" || layout === "forceatlas2-noverlap") {
          libraries.forceAtlas2.assign(network, {
            iterations: Math.min(320, Math.max(120, network.order * 4)),
            settings: {
              adjustSizes: true,
              barnesHutOptimize: network.order >= 30,
              barnesHutTheta: .7,
              gravity: .7,
              scalingRatio: 35,
              slowDown: 3,
            },
          });
        }
        if (["noverlap", "forceatlas2", "forceatlas2-noverlap"].includes(layout)) {
          libraries.noverlap.assign(network, {
            maxIterations: 240,
            settings: { expansion: 2.2, gridSize: 80, margin: 55, ratio: 1.5, speed: 2 },
          });
        }
      } catch (error) {
        console.error(`Impossible de calculer la disposition ${label}.`, error);
        updateGraphState({ activeLayout: previousLayout });
        setActiveLayout(previousLayout);
        restoreInitialNodePositions();
        renderer.refresh();
        layoutStatus.textContent = `La disposition ${label} a echoue ; la disposition initiale est restauree.`;
        return;
      }
      if (request !== graphState.layoutRequest) return;
      renderer.refresh();
      fitCameraToVisibleGraph(renderer);
      await new Promise(resolve => setTimeout(resolve, 300));
      if (request !== graphState.layoutRequest) return;
      // Commit the view mode only after its layout and camera are ready. This
      // keeps the old overlays coherent while an async layout is pending and
      // prevents a half-switched view from mixing old positions with new
      // cluster/layer geometry.
      updateGraphState({
        layeredView: nextLayeredView,
        clusteredView: layout === "cluster",
        layeredClusterView: layout === "elk",
      });
      if (["forceatlas2", "noverlap", "cluster", "elk"].includes(layout)) {
        // Force layouts optimize Sigma's node radii, not the larger HTML cards.
        // Resolve the final projected envelope once after fitting the graph
        // view; compound views use deterministic packing instead.
        resolveGraphCardOverlaps();
        renderer.refresh();
      }
      // Sigma's noverlap solver works with Sigma node radii, while the
      // readable labels are larger HTML rectangles. Resolve any residual
      // collision using the actual projected card envelope after the camera
      // has fitted the layout.
      requestGraphRender();
      layoutStatus.textContent = `${label} actif.`;
    }
    const nodesByNormalizedName = new Map();
