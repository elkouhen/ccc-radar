// Ordered source module: 60-bootstrap.js
      if (position) renderer.getCamera().animate({ x: position.x, y: position.y, ratio: .55 }, { duration: 260 });
      persistState();
    }
    function reset() {
      updateGraphState({
        selectedId: null,
        relatedNodes: null,
        relatedEdges: null,
        pathMicroserviceOrder: new Map(),
      });
      if (document.querySelector('.filter-preset[data-preset="selection"]')?.classList.contains("is-active")) {
        setActiveRelationPreset("all");
      }
      renderer.refresh();
      setDetailsEmpty("Selectionnez un noeud pour isoler ses relations et afficher ses APIs.");
      search.value = "";
      clearPathControls();
      persistState();
    }
    function restoreState() {
      const params = new URLSearchParams(location.hash.slice(1));
      const sourceId = params.get("from");
      const targetId = params.get("to");
      pathLock.checked = params.get("lock") === "1";
      const restoredStops = [sourceId, ...params.getAll("via"), targetId];
      if (
        sourceId
        && targetId
        && isValidPathStops(restoredStops)
      ) {
        pathStops.push(...restoredStops);
        renderPathQuery();
        showShortestPath();
        return;
      }
      const selectedIdFromUrl = params.get("selected");
      if (selectedIdFromUrl && nodeDataById.has(selectedIdFromUrl)) selectNode(selectedIdFromUrl);
    }
    function activeRenderer() {
      return dependencyCanvas.hidden ? renderer : ensureDependencyRenderer();
    }
    function fitCameraToVisibleGraph(targetRenderer = renderer) {
      if (!targetRenderer) return;
      targetRenderer.refresh();
      // Sigma owns the normalized camera coordinate system. Mixing projected
      // CSS pixels into CameraState.x/y sends overlays millions of pixels off
      // screen after a view switch. Its native reset computes the fit from
      // the current graph bounds and keeps canvas and HTML overlays aligned.
      targetRenderer.getCamera().animatedReset({ duration: 0 });
      if (targetRenderer === renderer) {
        const layoutRequest = graphState.layoutRequest;
        requestAnimationFrame(() => {
          if (layoutRequest !== graphState.layoutRequest) return;
          requestGraphRender();
        });
      }
    }
    document.getElementById("zoom-in").addEventListener("click", () => {
      const renderer = activeRenderer();
      const camera = renderer.getCamera();
      const state = camera.getState();
      camera.setState({ ...state, ratio: Math.max(.01, state.ratio * .8) });
      requestGraphRender();
    });
    document.getElementById("zoom-out").addEventListener("click", () => {
      const renderer = activeRenderer();
      const camera = renderer.getCamera();
      const state = camera.getState();
      camera.setState({ ...state, ratio: Math.min(100, state.ratio * 1.25) });
      requestGraphRender();
    });
    document.getElementById("fit-view").addEventListener("click", () => fitCameraToVisibleGraph(activeRenderer()));
    document.getElementById("reset").addEventListener("click", reset);
    document.getElementById("inspector-close").addEventListener("click", closeInspector);
    inspectorModal.addEventListener("click", event => { if (event.target === inspectorModal) closeInspector(); });
    window.addEventListener("keydown", event => { if (event.key === "Escape" && !inspectorModal.hidden) closeInspector(); });
    document.getElementById("show-path").addEventListener("click", showShortestPath);
    document.getElementById("show-simple-paths").addEventListener("click", showSimplePaths);
    document.getElementById("question-topic").addEventListener("click", () => {
      setToolbarTab("graph");
      applyRelationPreset("kafka");
      search.placeholder = "orders.created ou orders -> orders.created";
      search.focus();
    });
    document.getElementById("question-service").addEventListener("click", () => {
      setToolbarTab("graph");
      applyRelationPreset("all");
      search.placeholder = "orders ou orders -> payments";
      search.focus();
    });
    document.getElementById("question-path").addEventListener("click", () => {
      setToolbarTab("graph");
      search.focus();
    });
    document.getElementById("question-messages").addEventListener("click", () => {
      setToolbarTab("kafka");
      dtoReferencesFilter.focus();
    });
    layoutButtons.forEach((button, layout) => button.addEventListener("click", () => applyLayout(layout)));
    layerViewToggle.addEventListener("click", () => {
      const nextLayout = !graphState.layeredView
        ? "elk"
        : !graphState.clusteredView
          ? "cluster"
          : "forceatlas2-noverlap";
      applyLayout(nextLayout);
    });
    graphTab.addEventListener("click", () => setToolbarTab("graph"));
    openApiTab.addEventListener("click", () => setToolbarTab("openapi"));
    kafkaTab.addEventListener("click", () => setToolbarTab("kafka"));
    persistenceTab.addEventListener("click", () => setToolbarTab("persistence"));
    requestReplyTab.addEventListener("click", () => setToolbarTab("request-reply"));
    buildTab.addEventListener("click", () => setToolbarTab("dependencies"));
    issuesTab.addEventListener("click", () => setToolbarTab("issues"));
    pathsTab.addEventListener("click", () => setToolbarTab("paths"));
    inventoryStatus.addEventListener("click", () => setToolbarTab("issues"));
    filterPresetButtons.forEach(button => button.addEventListener("click", () => applyRelationPreset(button.dataset.preset)));
    [
      relationHttp,
      relationKafka,
      relationMongodb,
      nodeMicroservice,
      nodeExternalMicroservice,
      nodeKafkaTopic,
      nodeMongodbCollection,
      showProjectGroups,
    ].forEach(control => control.addEventListener("click", () => {
      setActiveRelationPreset(null);
      // Reflect the checkbox state synchronously. Rebuilding Sigma and
      // applying the active layout are intentionally asynchronous, while the
      // data contract is also consumed by compact-viewport integrations.
      const visibleRelationCount = graphData.links.filter(link => (
        isVisibleRelation(link.kind)
        && isVisibleNode(nodeDataById.get(link.source))
        && isVisibleNode(nodeDataById.get(link.target))
      )).length;
      document.getElementById("graph").dataset.relationCount = String(visibleRelationCount);
      if ([relationHttp, relationKafka, relationMongodb].includes(control)) {
        reset();
        rebuildGraph();
        applyLayout(graphState.activeLayout);
        return;
      }
      reset();
      rebuildGraph();
      applyLayout(graphState.activeLayout);
    }));
    openApiReferencesFilter.addEventListener("input", renderReferences);
    dtoReferencesFilter.addEventListener("input", renderReferences);
    mongoClassReferencesFilter.addEventListener("input", renderReferences);
    pathLock.addEventListener("change", persistState);
    pathQuery.addEventListener("keydown", event => {
      if (event.key === "Enter") showShortestPath();
    });
    renderIndexingIssues();
    renderAnalyzedPaths();
    renderReferences();
    renderRequestReplyPatterns();
    restoreState();
    function updateWorkspaceViewport(refit = false) {
      const toolbar = document.querySelector(".toolbar");
      const detailsPanel = document.getElementById("details");
      const desktop = window.innerWidth > 700;
      const toolbarRight = toolbar?.getBoundingClientRect().right || 0;
      const left = desktop ? Math.min(window.innerWidth - 220, toolbarRight + 24) : 0;
      const detailsHeight = detailsPanel
        ? Math.min(window.innerHeight * .42, detailsPanel.getBoundingClientRect().height + 24)
        : 0;
      const root = document.documentElement;
      root.style.setProperty("--workspace-left", `${Math.max(0, left)}px`);
      root.style.setProperty("--workspace-right", "0px");
      root.style.setProperty("--workspace-top", "0px");
      root.style.setProperty("--workspace-bottom", `${Math.max(0, detailsHeight)}px`);
      renderer?.refresh();
      dependencyRenderer?.refresh();
      if (refit) fitCameraToVisibleGraph(activeRenderer());
    }
    const workspaceObserver = new MutationObserver(() => updateWorkspaceViewport(false));
    workspaceObserver.observe(details, { attributes: true, attributeFilter: ["class"] });
    updateWorkspaceViewport(false);
    applyLayout("forceatlas2-noverlap");
    function runExploreSearch() {
      const query = search.value.trim();
      searchStatus.textContent = "";
      if (!query) { reset(); return; }
      if (query.includes("->")) {
        showShortestPath(query, true);
        return;
      }
      const resolved = resolveExactNodeName(query);
      if (resolved.error) { searchStatus.textContent = resolved.error; return; }
      selectNode(resolved.id);
    }
    search.addEventListener("input", () => {
      const query = search.value.trim();
      searchStatus.textContent = "";
      if (!query) { reset(); return; }
      if (query.includes("->")) return;
      const resolved = resolveExactNodeName(query);
      if (resolved.id) selectNode(resolved.id);
    });
    search.addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); runExploreSearch(); }
    });
    window.addEventListener("resize", () => updateWorkspaceViewport(true));
