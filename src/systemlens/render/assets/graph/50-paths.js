// Ordered source module: 50-paths.js
    function selectDependencyModule(id) {
      const node = (graphData.build_dependencies?.nodes || []).find(item => item.id === id);
      if (!node) return;
      const links = graphData.build_dependencies?.links || [];
      const dependencies = links
        .filter(link => link.source === id)
        .map(link => (graphData.build_dependencies.nodes.find(item => item.id === link.target) || {}).name)
        .filter(Boolean);
      const dependents = links
        .filter(link => link.target === id)
        .map(link => (graphData.build_dependencies.nodes.find(item => item.id === link.source) || {}).name)
        .filter(Boolean);
      details.classList.remove("is-empty");
      details.replaceChildren();
      const header = document.createElement("header");
      header.className = "details-header";
      const kicker = document.createElement("p");
      kicker.className = "details-kicker";
      kicker.textContent = `Module ${node.build_system === "unknown" ? "Maven / Gradle" : node.build_system}`;
      const title = document.createElement("h1");
      title.className = "details-title";
      title.textContent = node.name;
      header.append(kicker, title);
      details.append(header);
      appendList("Depend de", [...new Set(dependencies)].sort());
      appendList("Utilise par", [...new Set(dependents)].sort());
    }
    function setDetailsEmpty(message) {
      details.classList.add("is-empty");
      details.replaceChildren();
      const empty = document.createElement("div");
      empty.className = "details-empty";
      empty.textContent = message;
      details.append(empty);
    }
    function persistState() {
      const params = new URLSearchParams();
      if (pathStops.length) params.set("from", pathStops[0]);
      if (pathStops.length > 1) params.set("to", pathStops[pathStops.length - 1]);
      pathStops.slice(1, -1).forEach(id => params.append("via", id));
      if (pathLock.checked) params.set("lock", "1");
      if (!pathStops.length && graphState.selectedId) {
        params.set("selected", graphState.selectedId);
      }
      const fragment = params.toString();
      try {
        history.replaceState(null, "", fragment ? `#${fragment}` : location.pathname);
      } catch (_error) {
        location.hash = fragment;
      }
    }
    function clearPathControls() {
      pathQuery.value = "";
      pathStops.splice(0, pathStops.length);
    }
    function restResourceLabel(link, target) {
      const servicePrefix = `${target.name}: `;
      if (link.label === `${target.name}: API`) return "";
      return link.label.startsWith(servicePrefix) ? link.label.slice(servicePrefix.length) : link.label;
    }
    function contractsForPublishedRestResource(node, resource) {
      const contracts = node.openapi_contracts || [];
      const matchingContracts = contracts.filter(contract => (
        (contract.resources || []).includes(resource)
      ));
      return matchingContracts.length || contracts.length === 1
        ? (matchingContracts.length ? matchingContracts : contracts)
        : [];
    }
    function relationText(link) {
      const source = nodeDataById.get(link.source);
      const target = nodeDataById.get(link.target);
      if (link.kind === "rest") {
        const resource = restResourceLabel(link, target);
        return resource
          ? `HTTP · ${source.name} appelle ${target.name} (${resource})`
          : `HTTP · ${source.name} appelle ${target.name} (contrat non indexe)`;
      }
      if (link.kind === "mongodb") return `MongoDB · ${source.name} stocke dans ${target.name}`;
      if (link.kind === "request_reply") return `Kafka request/reply · ${source.name} → ${target.name}`;
      if (source.kind === "microservice") {
        const types = link.published_message_types || [];
        return `Kafka · ${source.name} publie${types.length ? ` <${types.join(", ")}>` : ""} sur ${target.name}`;
      }
      return `Kafka · ${target.name} consomme ${source.name}`;
    }
    function shortestPath(sourceId, targetId, matchesLink = () => true) {
      const outgoing = new Map();
      graphData.links.forEach((link, index) => {
        if (!matchesLink(link)) return;
        if (!outgoing.has(link.source)) outgoing.set(link.source, []);
        outgoing.get(link.source).push({ target: link.target, edge: `edge-${index}`, link });
      });
      const queue = [sourceId];
      const previous = new Map([[sourceId, null]]);
      for (let cursor = 0; cursor < queue.length; cursor += 1) {
        const current = queue[cursor];
        if (current === targetId) break;
        for (const step of outgoing.get(current) || []) {
          if (previous.has(step.target)) continue;
          previous.set(step.target, { node: current, edge: step.edge, link: step.link });
          queue.push(step.target);
        }
      }
      if (!previous.has(targetId)) return null;
      const nodes = [];
      const edges = [];
      for (let current = targetId; current !== null;) {
        nodes.unshift(current);
        const step = previous.get(current);
        if (step === null) break;
        edges.unshift(step);
        current = step.node;
      }
      return { nodes, edges };
    }
    function shortestPathThrough(stops) {
      const path = { nodes: [], edges: [] };
      for (let index = 0; index < stops.length - 1; index += 1) {
        const segment = shortestPath(stops[index], stops[index + 1], link => link.kind === "kafka");
        if (segment === null) return null;
        path.nodes.push(...(index === 0 ? segment.nodes : segment.nodes.slice(1)));
        path.edges.push(...segment.edges);
      }
      return path;
    }
    function allSimplePaths(sourceId, targetId, maxDepth = MAX_SIMPLE_PATH_DEPTH, maxPaths = MAX_SIMPLE_PATHS, maxExplorations = MAX_SIMPLE_PATH_EXPLORATIONS) {
      const outgoing = new Map();
      graphData.links.forEach((link, index) => {
        if (link.kind !== "kafka") return;
        if (!outgoing.has(link.source)) outgoing.set(link.source, []);
        outgoing.get(link.source).push({ target: link.target, edge: `edge-${index}`, link });
      });
      outgoing.forEach((steps, source) => {
        const firstStepByTarget = new Map();
        steps.sort((left, right) => (
          nodeDataById.get(left.target).name.localeCompare(nodeDataById.get(right.target).name)
        )).forEach(step => {
          if (!firstStepByTarget.has(step.target)) firstStepByTarget.set(step.target, step);
        });
        outgoing.set(source, [...firstStepByTarget.values()]);
      });
      const paths = [];
      const queue = [{ nodes: [sourceId], edges: [] }];
      let explorations = 0;
      for (let cursor = 0; cursor < queue.length && paths.length < maxPaths; cursor += 1) {
        const candidate = queue[cursor];
        if (candidate.edges.length >= maxDepth) continue;
        const current = candidate.nodes[candidate.nodes.length - 1];
        for (const step of outgoing.get(current) || []) {
          if (candidate.nodes.includes(step.target)) continue;
          explorations += 1;
          if (explorations > maxExplorations) return { paths, limited: true };
          const nextNodes = [...candidate.nodes, step.target];
          const nextEdges = [...candidate.edges, step];
          if (step.target === targetId) {
            paths.push({ nodes: nextNodes, edges: nextEdges });
            continue;
          }
          queue.push({ nodes: nextNodes, edges: nextEdges });
        }
      }
      return {
        paths: paths.sort((left, right) => left.nodes.length - right.nodes.length || (
          left.nodes.map(id => nodeDataById.get(id).name).join("\\u0000").localeCompare(
            right.nodes.map(id => nodeDataById.get(id).name).join("\\u0000")
          )
        )),
        limited: paths.length >= maxPaths,
      };
    }
    function resolveExactNodeName(name, allowedKinds = null) {
      const candidates = nodesByNormalizedName.get(normalizeNodeName(name)) || [];
      if (!candidates.length) return { error: `Noeud introuvable : ${name}. Saisissez son nom exact.` };
      const eligible = allowedKinds
        ? candidates.filter(candidate => allowedKinds.includes(nodeDataById.get(candidate.id).kind))
        : candidates;
      if (!eligible.length) return { error: `Type de noeud invalide : ${name}.` };
      if (eligible.length > 1) return { error: `Nom ambigu : ${name}. Precisez un nom de noeud unique.` };
      return { id: eligible[0].id };
    }
    function parsePathQuery(query = pathQuery.value) {
      const names = query.split("->").map(name => name.trim());
      if (names.length < 2) return { error: "Saisissez au moins deux noeuds separes par ->." };
      if (names.some(name => !name)) return { error: "Chaque etape de l'itineraire doit avoir un nom : retirez le -> en trop ou renseignez le noeud manquant." };
      const stops = [];
      for (const [index, name] of names.entries()) {
        const endpoint = index === 0 || index === names.length - 1;
        const resolved = resolveExactNodeName(
          name,
          endpoint ? ["microservice"] : ["microservice", "kafka_topic"],
        );
        if (resolved.error) return resolved;
        stops.push(resolved.id);
      }
      if (stops.some(id => !["microservice", "kafka_topic"].includes(nodeDataById.get(id).kind))) {
        return { error: "Un itineraire Kafka ne peut contenir que des microservices et des topics Kafka." };
      }
      if (nodeDataById.get(stops[0]).kind !== "microservice" || nodeDataById.get(stops.at(-1)).kind !== "microservice") {
        return { error: "Un itineraire Kafka doit commencer et se terminer par un microservice." };
      }
      if (new Set(stops).size !== stops.length) {
        return { error: "Un itineraire ne peut pas repeter le meme noeud." };
      }
      return { stops };
    }
    function renderPathQuery() {
      const query = pathStops.map(id => nodeDataById.get(id).name).join(" -> ");
      pathQuery.value = query;
      search.value = query;
      searchStatus.textContent = "";
    }
    function setPathMicroserviceOrder(path) {
      graphState.pathMicroserviceOrder = new Map();
      let order = 1;
      path.nodes.forEach(id => {
        if (nodeDataById.get(id).kind !== "microservice") return;
        graphState.pathMicroserviceOrder.set(id, order);
        order += 1;
      });
    }
    function renderPathDetails(path) {
      details.classList.remove("is-empty");
      details.replaceChildren();
      const pathNodeLabel = (id, index) => {
        const node = nodeDataById.get(id);
        const topicDtos = node.kind === "kafka_topic"
          ? (graphData.kafka_dtos || [])
            .filter(dto => (dto.topics || []).includes(node.name))
            .sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)))
          : [];
        const dtoSuffix = topicDtos.length ? ` (${topicDtos.map(dto => dtoLabel(dto)).join(", ")})` : "";
        return `${index + 1}. ${node.name} : ${nodeKindLabel(node)}${dtoSuffix}`;
      };
      const header = document.createElement("header");
      header.className = "path-details-header";
      const kicker = document.createElement("p");
      kicker.className = "path-details-kicker";
      kicker.textContent = "Analyse de flux";
      const title = document.createElement("h1");
      title.className = "path-details-title";
      title.textContent = pathStops.length > 2 ? "Chemin avec noeuds intermediaires" : "Chemin le plus court";
      const summary = document.createElement("p");
      summary.className = "path-details-summary";
      const serviceCount = path.nodes.filter(id => nodeDataById.get(id).kind === "microservice").length;
      const topicCount = path.nodes.filter(id => nodeDataById.get(id).kind === "kafka_topic").length;
      summary.textContent = `${serviceCount} microservice${serviceCount > 1 ? "s" : ""} · ${topicCount} topic${topicCount > 1 ? "s" : ""} Kafka`;
      header.append(kicker, title, summary);
      details.append(header);
      const overview = document.createElement("section");
      overview.className = "details-section";
      const overviewTitle = document.createElement("h2");
      overviewTitle.textContent = "Parcours";
      const overviewList = document.createElement("ol");
      overviewList.className = "path-overview";
      path.nodes.forEach((id, index) => {
        const item = document.createElement("li");
        item.className = "path-overview-item";
        const node = nodeDataById.get(id);
        item.classList.add(node.kind === "kafka_topic" ? "is-topic" : node.kind === "mongodb_collection" ? "is-collection" : node.external ? "is-external" : "is-service");
        const stop = document.createElement("button");
        stop.type = "button";
        stop.className = "path-overview-stop";
        stop.textContent = pathNodeLabel(id, index);
        stop.title = `Afficher les details et les preuves de ${node.name}`;
        stop.addEventListener("click", () => selectNode(id, true));
        item.append(stop);
        overviewList.append(item);
      });
      overview.append(overviewTitle, overviewList);
      details.append(overview);
    }
    function showPath(path, stops = path.nodes) {
      pathStops.splice(0, pathStops.length, ...stops);
      renderPathQuery();
      graphState.selectedId = path.nodes[0];
      graphState.relatedNodes = new Set(path.nodes);
      graphState.relatedEdges = new Set(path.edges.map(step => step.edge));
      setPathMicroserviceOrder(path);
      rememberAnalyzedPath(pathStops);
      renderer.refresh();
      renderPathDetails(path);
      renderer.getCamera().animatedReset({ duration: 220 });
      persistState();
    }
    function renderSimplePathChoices(paths, limited) {
      details.classList.remove("is-empty");
      details.replaceChildren();
      const section = document.createElement("section");
      section.className = "details-section simple-paths";
      const title = document.createElement("h2");
      title.textContent = "Chemins simples disponibles";
      const summary = document.createElement("p");
      summary.className = "simple-paths-summary";
      summary.textContent = `${paths.length} chemin${paths.length > 1 ? "s" : ""} propose${paths.length > 1 ? "s" : ""}, sans repeter de noeud, sur au plus ${MAX_SIMPLE_PATH_DEPTH} relations.${limited ? ` Recherche limitee a ${MAX_SIMPLE_PATHS} chemins et ${MAX_SIMPLE_PATH_EXPLORATIONS} explorations.` : ""}`;
      const list = document.createElement("ol");
      list.className = "simple-paths-list";
      paths.forEach((path, index) => {
        const item = document.createElement("li");
        const choice = document.createElement("button");
        choice.type = "button";
        choice.className = "simple-path-choice";
        choice.textContent = `${index + 1}. ${path.nodes.map(id => nodeDataById.get(id).name).join(" → ")}`;
        choice.addEventListener("click", () => showPath(path));
        item.append(choice);
        list.append(item);
      });
      section.append(title, summary, list);
      details.append(section);
    }
    function showShortestPath(query = pathQuery.value, preserveGraphOnError = false) {
      const parsed = parsePathQuery(query);
      if (parsed.error) {
        if (preserveGraphOnError) { searchStatus.textContent = parsed.error; return false; }
        graphState.selectedId = null; graphState.relatedNodes = null; graphState.relatedEdges = null; graphState.pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(parsed.error);
        pathStops.splice(0, pathStops.length);
        persistState();
        return;
      }
      const stops = parsed.stops;
      const path = shortestPathThrough(stops);
      if (path === null) {
        const message = "Aucun itineraire Kafka oriente ne passe par les noeuds demandes dans cet ordre.";
        if (preserveGraphOnError) { searchStatus.textContent = message; return false; }
        graphState.selectedId = null; graphState.relatedNodes = null; graphState.relatedEdges = null; graphState.pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(message);
        persistState();
        return false;
      }
      showPath(path, stops);
      return true;
    }
    function showSimplePaths() {
      const parsed = parsePathQuery();
      if (parsed.error) {
        graphState.selectedId = null; graphState.relatedNodes = null; graphState.relatedEdges = null; graphState.pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(parsed.error);
        pathStops.splice(0, pathStops.length);
        persistState();
        return;
      }
      if (parsed.stops.length !== 2) {
        setDetailsEmpty("Les chemins simples se recherchent entre un microservice source et un microservice cible, sans noeud intermediaire impose.");
        return;
      }
      const simplePaths = allSimplePaths(parsed.stops[0], parsed.stops[1]);
      graphState.selectedId = null; graphState.relatedNodes = null; graphState.relatedEdges = null; graphState.pathMicroserviceOrder = new Map();
      pathStops.splice(0, pathStops.length);
      renderer.refresh();
      if (!simplePaths.paths.length) {
        setDetailsEmpty(`Aucun chemin simple oriente, de ${nodeDataById.get(parsed.stops[0]).name} vers ${nodeDataById.get(parsed.stops[1]).name}, dans les limites de recherche.`);
        persistState();
        return;
      }
      renderSimplePathChoices(simplePaths.paths, simplePaths.limited);
      persistState();
    }
    function appendServiceKafkaActivities(node, role, title, links, container) {
      if (!links.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      list.className = "service-kafka-list";
      const topicIds = [...new Set(links.map(link => role === "produce" ? link.target : link.source))];
      topicIds.sort((left, right) => nodeDataById.get(left).name.localeCompare(nodeDataById.get(right).name));
      topicIds.forEach(topicId => {
        const topic = nodeDataById.get(topicId);
        const item = document.createElement("li");
        item.className = "service-kafka-item";
        const topicButton = document.createElement("button");
        topicButton.type = "button";
        topicButton.className = "service-kafka-topic";
        topicButton.textContent = topic.name;
        topicButton.title = `Afficher le detail du topic ${topic.name}`;
        topicButton.addEventListener("click", () => selectNode(topicId));
        item.append(topicButton);
        const meta = document.createElement("div");
        meta.className = "service-kafka-meta";
        const dtos = (graphData.kafka_dtos || []).filter(dto => {
          const matchesRole = (
            (role === "produce" && (dto.producers || []).includes(node.name))
            || (role === "consume" && (dto.consumers || []).includes(node.name))
          );
          return (dto.topics || []).includes(topic.name) && matchesRole;
        }).sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)));
        if (dtos.length) {
          dtos.forEach(dto => {
            const dtoButton = document.createElement("button");
            dtoButton.type = "button";
            dtoButton.textContent = `DTO · ${dtoLabel(dto)}`;
            dtoButton.title = `Afficher la structure de ${dtoLabel(dto)}`;
            dtoButton.addEventListener("click", () => openDtoInspector(dto.id));
            meta.append(dtoButton);
          });
        } else {
          const unknown = document.createElement("span");
          unknown.textContent = "DTO non indexe";
          meta.append(unknown);
        }
        item.append(meta);
        list.append(item);
      });
      section.append(heading, list);
      container.append(section);
    }
    function renderDetails(id) {
      const node = nodeDataById.get(id);
      const indexedEdges = graphData.links.filter(link => link.source === id || link.target === id);
      const edges = indexedEdges.filter(
        link => isVisibleRelation(link.kind) && (link.source === id || link.target === id)
      );
      const isMicroservice = node.kind === "microservice";
      const publishedApiCount = isMicroservice ? (node.resources || []).length : 0;
      const publishedTopicCount = isMicroservice ? new Set(
        indexedEdges.filter(link => link.kind === "kafka" && link.source === id).map(link => link.target)
      ).size : 0;
      const collectionCount = isMicroservice ? new Set(
        indexedEdges.filter(link => link.kind === "mongodb" && link.source === id).map(link => link.target)
      ).size : 0;
      details.classList.remove("is-empty");
      details.replaceChildren();
      const kindLabel = nodeKindLabel(node);
      const complexity = node.complexity;
      const header = document.createElement("header");
      header.className = "details-header";
      if (complexity) header.classList.add(`is-${complexity.level}`);
      const kicker = document.createElement("p");
      kicker.className = "details-kicker";
      kicker.textContent = kindLabel;
      const title = document.createElement("h1");
      title.className = "details-title";
      title.textContent = node.name;
      const meta = document.createElement("div");
      meta.className = "details-meta";
      const relationBadge = document.createElement("span");
      relationBadge.className = "detail-badge";
      relationBadge.textContent = `Relations indexees : ${indexedEdges.length}`;
      const visibleBadge = document.createElement("span");
      visibleBadge.className = "detail-badge";
      visibleBadge.textContent = `Affichees : ${edges.length}`;
      meta.append(relationBadge, visibleBadge);
      if (isMicroservice) {
        const layerBadge = document.createElement("span");
        layerBadge.className = "detail-badge";
        layerBadge.textContent = `Layer : ${node.layer_label || "Unknown"}`;
        meta.append(layerBadge);
        const clusterPathBadge = document.createElement("span");
        clusterPathBadge.className = "detail-badge";
        clusterPathBadge.textContent = `Chemin des clusters : ${clusterPathForNode(id)}`;
        meta.append(clusterPathBadge);
        (node.runtime_namespaces || []).forEach(namespace => {
          const namespaceBadge = document.createElement("span");
          namespaceBadge.className = "detail-badge";
          namespaceBadge.textContent = `Namespace : ${namespace}`;
          meta.append(namespaceBadge);
        });
        (node.fact_namespaces || []).forEach(namespace => {
          const factNamespaceBadge = document.createElement("span");
          factNamespaceBadge.className = "detail-badge";
          factNamespaceBadge.textContent = `Facts : ${namespace}`;
          meta.append(factNamespaceBadge);
        });
        [
          `${publishedApiCount} API${publishedApiCount > 1 ? "s" : ""} exposee${publishedApiCount > 1 ? "s" : ""}`,
          `${publishedTopicCount} topic${publishedTopicCount > 1 ? "s" : ""} publie${publishedTopicCount > 1 ? "s" : ""}`,
          `${collectionCount} collection${collectionCount > 1 ? "s" : ""} utilisee${collectionCount > 1 ? "s" : ""}`,
        ].forEach(label => { const badge = document.createElement("span"); badge.className = "detail-badge"; badge.textContent = label; meta.append(badge); });
      } else {
        const architectureLayer = architectureLayerForNode(id);
        const clusterPath = clusterPathForNode(id);
        const layerBadge = document.createElement("span");
        layerBadge.className = "detail-badge";
        layerBadge.textContent = `Layer : ${architectureLayer}`;
        meta.append(layerBadge);
        const clusterPathBadge = document.createElement("span");
        clusterPathBadge.className = "detail-badge";
        clusterPathBadge.textContent = `Chemin des clusters : ${clusterPath}`;
        meta.append(clusterPathBadge);
      }
      const confidenceLabels = { proved: "prouvee", inferred: "inferee", conventional: "conventionnelle" };
      ["proved", "inferred", "conventional"].forEach(confidence => {
        const count = edges.filter(link => link.confidence === confidence).length;
        if (!count) return;
        const badge = document.createElement("span");
        badge.className = "detail-badge";
        badge.textContent = `${count} ${confidenceLabels[confidence]}`;
        badge.title = `Relation ${confidenceLabels[confidence]} : ${[...new Set(edges.filter(link => link.confidence === confidence).map(link => link.provenance))].join(", ")}`;
        meta.append(badge);
      });
      if (complexity) {
        const scoreBadge = document.createElement("span");
        scoreBadge.className = `detail-badge complexity ${complexity.level}`;
        const connectivityLabels = { low: "basse", medium: "médiane", high: "élevée" };
        scoreBadge.textContent = `Connectivité relative : ${connectivityLabels[complexity.level]} (${complexity.score})`;
        const breakdown = complexity.breakdown || {};
        scoreBadge.title = `HTTP : ${breakdown.http || 0} · Kafka : ${breakdown.kafka || 0} · MongoDB : ${breakdown.mongodb || 0} · Rang relatif ${complexity.rank}/${complexity.population} · Tiers : ${complexity.tier_start}-${complexity.tier_end}`;
        meta.append(scoreBadge);
      }
      header.append(kicker, title, meta);
      details.append(header);
      if (!isMicroservice) {
        const clusterPath = clusterPathForNode(id);
        if (clusterPath) {
          const architectureGroup = createDetailsGroup("Architecture");
          appendList("Layer", [architectureLayerForNode(id)], architectureGroup);
          appendList("Chemin des clusters", [clusterPath], architectureGroup);
          discardEmptyDetailsGroup(architectureGroup);
        }
      }
      if (node.kind === "microservice") {
        if (node.vscode_uri) {
          const moduleAction = document.createElement("a");
          moduleAction.className = "module-open-action";
          moduleAction.href = node.vscode_uri;
          const buildSystem = node.build_system === "gradle" ? "Gradle" : "Maven";
          moduleAction.textContent = `Ouvrir le module ${buildSystem} dans VS Code`;
          moduleAction.title = `Ouvrir le repertoire racine du module ${node.name}`;
          details.append(moduleAction);
        }
        const httpCalls = edges.filter(link => link.kind === "rest" && link.source === id);
        const kafkaPublications = edges.filter(link => link.kind === "kafka" && link.source === id);
        const kafkaConsumptions = edges.filter(link => link.kind === "kafka" && link.target === id);
        const mongoCollections = edges.filter(link => link.kind === "mongodb" && link.source === id);
        const openApiContracts = node.openapi_contracts || [];
        const kubernetesWorkloads = node.kubernetes_workloads || [];
        appendFindings(node.findings || []);
        if (node.project_namespace || node.runtime_namespaces?.length || node.fact_namespaces?.length) {
          const architectureGroup = createDetailsGroup("Architecture");
          appendList("Layer", [node.layer_label || "Unknown"], architectureGroup);
          appendList("Chemin des clusters", [clusterPathForNode(id)], architectureGroup);
          appendList("Namespaces Kubernetes", node.runtime_namespaces || [], architectureGroup);
          appendList("Namespaces de faits", node.fact_namespaces || [], architectureGroup);
          discardEmptyDetailsGroup(architectureGroup);
        }
        if (kubernetesWorkloads.length) {
          const kubernetesGroup = createDetailsGroup("Kubernetes");
          appendList("Workloads", kubernetesWorkloads.map(workload => {
            const request = `requests CPU ${workload.cpu_request_millicores ?? "-"}m · RAM ${workload.memory_request_bytes ?? "-"}B`;
            const limit = `limits CPU ${workload.cpu_limit_millicores ?? "-"}m · RAM ${workload.memory_limit_bytes ?? "-"}B`;
            return `${workload.kind} ${workload.namespace}/${workload.name} · replicas ${workload.replicas ?? "-"} · ${request} · ${limit}`;
          }), kubernetesGroup);
          discardEmptyDetailsGroup(kubernetesGroup);
        }
        const publishedApis = [
          ...openApiContracts.map(contract => ({
            label: `${contract.spec ? "Contrat OpenAPI" : "Contrat OpenAPI indisponible"} · ${contract.path}`,
            title: `Ouvrir le contrat OpenAPI ${contract.path}`,
            action: () => openOpenApiContract(contract),
          })),
          ...(node.resources || [])
            .filter(resource => !contractsForPublishedRestResource(node, resource).length)
            .map(resource => ({
              label: `REST · ${resource}`,
              title: "Mettre en evidence les consommateurs de cette API REST",
              action: () => focusPublishedRestResource(id, resource),
            })),
        ];
        const relationsGroup = createDetailsGroup("Relations");
        appendRelationList("APIs consommees", httpCalls, id, link => (
          `API de ${nodeDataById.get(link.target).name}`
        ), relationsGroup);
        appendActionList("APIs publiees", publishedApis, relationsGroup);
        appendServiceKafkaActivities(node, "consume", "Topics consommes", kafkaConsumptions, relationsGroup);
        appendServiceKafkaActivities(node, "produce", "Topics publies", kafkaPublications, relationsGroup);
        appendRelationList("Collections MongoDB", mongoCollections, id, link => (
          nodeDataById.get(link.target).name
        ), relationsGroup);
        discardEmptyDetailsGroup(relationsGroup);
        const sourceEntries = [
          ...openApiContracts.map(contract => ({
            label: `OpenAPI · ${contract.path}`,
            title: `Ouvrir ${contract.path} dans VS Code`,
            action: () => { if (contract.vscode_uri) window.location.href = contract.vscode_uri; },
          })),
          ...(node.kafka_endpoints || []).map(endpoint => ({
            label: `Kafka · ${endpoint.location}`,
            title: `Ouvrir ${endpoint.location} dans VS Code`,
            action: () => { if (endpoint.vscode_uri) window.location.href = endpoint.vscode_uri; },
          })),
        ];
        const sourcesGroup = createDetailsGroup("Sources", false);
        appendActionList("Fichiers de preuve", sourceEntries, sourcesGroup);
        discardEmptyDetailsGroup(sourcesGroup);
      }
      if (node.kind === "kafka_topic") {
        const relationsGroup = createDetailsGroup("Relations");
        appendRelationList("Services producteurs", edges.filter(link => link.kind === "kafka" && link.target === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        appendRelationList("Services consommateurs", edges.filter(link => link.kind === "kafka" && link.source === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        appendRelationList("Pattern request/reply", edges.filter(link => link.kind === "request_reply" && (link.source === id || link.target === id)), id,
          link => nodeDataById.get(link.source === id ? link.target : link.source).name, relationsGroup);
        const dtos = (graphData.kafka_dtos || [])
          .filter(dto => (dto.topics || []).includes(node.name))
          .sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)));
        appendActionList("DTO Kafka", dtos.map(dto => ({
          label: dtoLabel(dto),
          title: "Afficher les champs et les relations Kafka de ce DTO",
          action: () => openDtoInspector(dto.id),
        })), relationsGroup);
        const indexedDtoTypes = new Set(dtos.flatMap(dto => [dto.id, dto.name, dto.qualified_name].filter(Boolean)));
        const unresolvedTypes = [...new Set([
          ...(node.published_message_types || []),
          ...(node.consumed_message_types || []),
        ])].filter(type => !indexedDtoTypes.has(type) && !indexedDtoTypes.has(type.split(".").at(-1)));
        appendList("Types de message non resolus", unresolvedTypes, relationsGroup);
        const endpointSources = graphData.nodes
          .filter(candidate => candidate.kind === "microservice")
          .flatMap(candidate => (candidate.kafka_endpoints || []).map(endpoint => ({ service: candidate.name, ...endpoint })))
          .filter(endpoint => endpoint.topic === node.name);
        appendActionList("Sources producteurs et consommateurs", endpointSources.map(endpoint => ({
          label: `${endpoint.service} · ${endpoint.role === "produce" ? "publication" : "consommation"} · ${endpoint.location}`,
          title: `Ouvrir ${endpoint.location} dans VS Code`,
          action: () => { if (endpoint.vscode_uri) window.location.href = endpoint.vscode_uri; },
        })), relationsGroup);
        discardEmptyDetailsGroup(relationsGroup);
      }
      if (node.kind === "mongodb_collection") {
        const relationsGroup = createDetailsGroup("Relations");
        const persistenceClasses = node.persistence_classes || [];
        appendRelationList("Services utilisant cette collection", edges.filter(link => link.kind === "mongodb" && link.target === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        appendActionList("Classes Java de persistance", persistenceClasses.map(item => ({
          label: item.qualified_name,
          title: "Afficher les champs et la source de cette classe",
          action: () => openMongoPersistenceInspector(item.id),
        })), relationsGroup);
        if (!persistenceClasses.length) {
          appendList("Classes Java de persistance", [
            "Aucune classe Java associée dans l’index. Relancez systemlens index après la mise à jour.",
          ], relationsGroup);
        }
        discardEmptyDetailsGroup(relationsGroup);
      }
      if (["data_schema", "message_channel"].includes(node.kind)) {
        const factsGroup = createDetailsGroup("Ressource enrichie");
        if (node.technology) appendList("Technologie", [node.technology], factsGroup);
        const metadata = Object.entries(node.metadata || {}).map(([key, value]) => (
          `${key} : ${Array.isArray(value) ? value.join(", ") : String(value)}`
        ));
        appendList("Métadonnées", metadata.length ? metadata : ["Aucune métadonnée"], factsGroup);
        appendRelationList("Relations", edges.filter(link => link.source === id || link.target === id), id,
          link => `${link.label} · ${nodeDataById.get(link.source === id ? link.target : link.source)?.name || "ressource"}`,
          factsGroup);
        discardEmptyDetailsGroup(factsGroup);
      }
    }
    function focusNodeRelations(id, matches) {
      if (!pathLock.checked) clearPathControls();
      graphState.pathMicroserviceOrder = new Map();
      graphState.selectedId = id;
      graphState.relatedNodes = new Set([id]);
      graphState.relatedEdges = new Set();
      network.forEachEdge((edge, attributes, source, target) => {
        if (!isVisibleRelation(attributes.kind) || !matches(attributes, source, target)) return;
        graphState.relatedEdges.add(edge); graphState.relatedNodes.add(source); graphState.relatedNodes.add(target);
      });
      renderer.refresh();
      renderDetails(id);
      const position = renderer.getNodeDisplayData(id);
      if (position) renderer.getCamera().animate({ x: position.x, y: position.y, ratio: .55 }, { duration: 260 });
      persistState();
    }
    function focusPublishedRestResource(id, resource) {
      const target = nodeDataById.get(id);
      focusNodeRelations(id, (link, _source, targetId) => (
        link.kind === "rest" && targetId === id && restResourceLabel(link, target) === resource
      ));
    }
    function selectNode(id, preservePath = false) {
      if (!preservePath && !pathLock.checked) clearPathControls();
      graphState.pathMicroserviceOrder = new Map();
      graphState.selectedId = id;
      graphState.relatedNodes = new Set([id]);
      graphState.relatedEdges = new Set();
      network.forEachEdge((edge, attributes, source, target) => {
        if (!isVisibleRelation(attributes.kind)) return;
        if (source === id || target === id) {
          graphState.relatedEdges.add(edge); graphState.relatedNodes.add(source); graphState.relatedNodes.add(target);
        }
      });
      renderer.refresh();
      renderDetails(id);
      const position = renderer.getNodeDisplayData(id);

