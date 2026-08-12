"""Graphe d'interactions entre services dérivé à la requête à partir
d'endpoints déjà indexés — aucune table de graphe en base (ADR-27)."""

from dataclasses import dataclass, replace
import os
import re
import sys
import time

from codeatlas.models import MessageEndpoint


@dataclass(frozen=True)
class GraphEdge:
    kind: str  # "rest" | "kafka"
    from_service: str
    to_service: str
    from_endpoint: MessageEndpoint  # site d'appel (call) ou de production (produce)
    to_endpoint: MessageEndpoint | None  # site exposé (serve) ou de consommation (consume)


@dataclass(frozen=True)
class OutboundCallInConsumer:
    consumer: MessageEndpoint
    call: MessageEndpoint


def qualified_rest_resource(service_name: str, resource: str) -> str:
    """Return the stable identity of a REST resource published by a service.

    ``MessageEndpoint.topic`` deliberately stays as ``METHOD /path`` because
    route matching operates on that syntax.  Graphs and dependency views use
    this qualified form instead, so identical routes exposed by two services
    remain distinct facts.
    """
    return f"{service_name}: {resource}"


def graph_edge_rest_resource(edge: GraphEdge) -> str:
    """Retourne la ressource REST d'une arête, ou l'API logique du service.

    Une dépendance issue de ``Rest*Config*`` peut désigner un microservice sans
    qu'une ressource ``serve`` ait été détectée sur celui-ci. Dans ce cas,
    ``to_endpoint`` est absent mais l'arête A → B reste un fait utile.
    """
    resource = edge.to_endpoint.topic if edge.to_endpoint is not None else "API"
    return qualified_rest_resource(edge.to_service, resource)


def _trace(stage: str, **fields: object) -> None:
    if os.environ.get("CODEATLAS_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"CODEATLAS_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )


def group_endpoints_by_module(
    endpoints: list[MessageEndpoint],
) -> dict[str, list[MessageEndpoint]]:
    """Regroupe des endpoints par module Maven (`endpoint.module`).

    La forme du dictionnaire est la même que celle produite par
    `workspace.load_federation`, et peut donc alimenter directement
    `build_graph`. Un endpoint sans module (`None` — repo non-Maven, ou fichier
    hors arborescence Maven) est ignoré : sans nom stable, il ne peut jamais
    former une arête inter-service fiable.
    """

    grouped: dict[str, list[MessageEndpoint]] = {}
    for endpoint in endpoints:
        if endpoint.module is None:
            continue
        grouped.setdefault(endpoint.module, []).append(endpoint)
    return grouped


def _split_path(path: str) -> list[str]:
    return [segment for segment in path.partition("?")[0].split("/") if segment]


def _matches_wildcard_path(pattern_segments: list[str], concrete_segments: list[str]) -> bool:
    if not pattern_segments or pattern_segments[-1] != "**":
        return False
    prefix = pattern_segments[:-1]
    if len(concrete_segments) <= len(prefix):
        return False
    return all(
        _segment_matches(pattern_seg, concrete_seg)
        for pattern_seg, concrete_seg in zip(prefix, concrete_segments)
    )


def _is_template_segment(segment: str) -> bool:
    return segment.startswith("{") and segment.endswith("}")


def _segment_matches(call_segment: str, serve_segment: str) -> bool:
    if call_segment == serve_segment:
        return True
    return _is_template_segment(serve_segment) or _is_template_segment(call_segment)


_SERVICE_URL_GETTER_RE = re.compile(r"\.get([A-Z][A-Za-z0-9]*)ServiceUrl\(")
_SERVICE_URL_HOST_RE = re.compile(r"https?://([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)\b", re.IGNORECASE)
_LOAD_BALANCED_URI_RE = re.compile(r"lb://([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)\b", re.IGNORECASE)
_CONFIGURED_API_DOMAIN_RE = re.compile(r"\bcodeatlas-api-domain:([a-z0-9][a-z0-9-]*)\b", re.IGNORECASE)
_EXTERNAL_MICROSERVICE_RE = re.compile(
    r"\bcodeatlas-external-microservice:([a-z0-9][a-z0-9-]*)\b", re.IGNORECASE
)


def _camel_to_kebab(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()


def configured_api_client_domain(endpoint: MessageEndpoint) -> str | None:
    """Domaine ``codeatlas-api-domain:`` tamponné sur un endpoint, normalisé en minuscules.

    Preuve injectée par le scanner pour les clients créés via
    ``createInternalClientApi`` : la route HTTP n'est pas au site d'appel, mais
    le domaine logique y figure et correspond, par convention, au nom du
    microservice hôte. Consommé par la fédération et par le graphe de
    dépendances pour relier l'appelant au microservice hôte.
    """
    match = _CONFIGURED_API_DOMAIN_RE.search(endpoint.snippet)
    return match.group(1).lower() if match is not None else None


def external_microservice_name(endpoint: MessageEndpoint) -> str | None:
    """Return the external microservice explicitly named by a Strategy1 call."""
    match = _EXTERNAL_MICROSERVICE_RE.search(endpoint.snippet)
    return match.group(1).lower() if match is not None else None


def external_microservice_names(edges: list[GraphEdge]) -> set[str]:
    """Return graph targets that Strategy1 explicitly marks as external."""
    return {
        edge.to_service
        for edge in edges
        if edge.kind == "rest" and external_microservice_name(edge.from_endpoint) is not None
    }


def _rest_target_service_hint(
    call: MessageEndpoint, *, strategy1: bool = False
) -> str | None:
    if strategy1:
        getter_match = _SERVICE_URL_GETTER_RE.search(call.snippet)
        if getter_match is not None:
            return f"{_camel_to_kebab(getter_match.group(1))}-service"
    host_match = _SERVICE_URL_HOST_RE.search(call.snippet)
    if host_match is not None:
        return host_match.group(1).lower()
    load_balanced_match = _LOAD_BALANCED_URI_RE.search(call.snippet)
    if load_balanced_match is not None:
        return load_balanced_match.group(1).lower()
    return configured_api_client_domain(call)


def _is_configured_api_client_call(call: MessageEndpoint) -> bool:
    """Le site est un client créé par `createInternalClientApi`.

    Son code d'appel ne porte pas nécessairement le chemin HTTP. Le domaine
    injecté par le scanner permet alors de le résoudre contre les ressources
    exposées par le microservice hôte pendant la fédération.
    """
    return configured_api_client_domain(call) is not None


def _http_methods_match(call_topic: str, serve_topic: str) -> bool:
    call_method, _, _ = call_topic.partition(" ")
    serve_method, _, _ = serve_topic.partition(" ")
    return call_method == serve_method or call_method == "ANY" or serve_method == "ANY"


def _resolved_configured_api_call(
    call: MessageEndpoint, serve: MessageEndpoint
) -> MessageEndpoint | None:
    """Projette un client d'API typé sur une ressource de son hôte.

    Cette résolution est délibérément réservée aux appels portant l'évidence
    `codeatlas-api-domain:` : un appel HTTP dynamique ordinaire ne doit jamais être
    relié aveuglément à toutes les routes d'un service.
    """
    if (
        not call.topic_dynamic
        or not _is_configured_api_client_call(call)
        or not _http_methods_match(call.topic, serve.topic)
    ):
        return None
    return replace(call, topic=serve.topic, topic_dynamic=False)


def _service_matches_hint(service_name: str, hint: str | None) -> bool:
    if hint is None:
        return True
    normalized = service_name.lower()
    return normalized == hint or normalized.endswith(f"-{hint}") or hint in normalized


def paths_match(call_topic: str, serve_topic: str) -> bool:
    """Best-effort : même méthode HTTP, et les segments de chemin du call
    (littéral ↔ template `{param}`) préfixent ou couvrent ceux de la route
    exposée. Un call sans aucun littéral exploitable (`<dynamic>`) ne matche
    jamais — mieux vaut une arête absente qu'une fausse arête."""

    call_method, _, call_path = call_topic.partition(" ")
    serve_method, _, serve_path = serve_topic.partition(" ")
    if (
        call_path == "<dynamic>"
        or (call_method != serve_method and call_method != "ANY" and serve_method != "ANY")
    ):
        return False

    call_segments = _split_path(call_path)
    serve_segments = _split_path(serve_path)
    if not call_segments:
        return False
    if _matches_wildcard_path(call_segments, serve_segments):
        return True
    if _matches_wildcard_path(serve_segments, call_segments):
        return True
    if len(call_segments) == len(serve_segments):
        return all(
            _segment_matches(call_seg, serve_seg)
            for call_seg, serve_seg in zip(call_segments, serve_segments)
        )
    return False


def build_graph(
    endpoints_by_service: dict[str, list[MessageEndpoint]], *, strategy1: bool = False
) -> list[GraphEdge]:
    """Construit les arêtes REST et Kafka entre services distincts.

    Une arête REST associe un appel à une route exposée compatible. Lorsqu'une
    cible de service est présente dans le site d'appel (URL de service ou
    ``lb://``), elle restreint cet appariement. Le getter de configuration
    ``getXxxServiceUrl()`` est une convention Strategy1 et n'est considéré que
    lorsque ``strategy1=True``.
    Pas d'auto-arête : un service qui s'appelle lui-même n'entre pas dans le
    graphe inter-services.

    Les entrées Kafka provenant d'un manifeste sont autoritatives
    pour le service décrit : elles remplacent les détections de code de ce
    service, qui peuvent être incomplètes ou produire des faux positifs. Les
    services absents du manifeste conservent leurs endpoints détectés."""

    all_endpoints = [
        (service, endpoint)
        for service, endpoints in endpoints_by_service.items()
        for endpoint in endpoints
    ]
    calls = [(s, e) for s, e in all_endpoints if e.system == "rest" and e.role == "call"]
    serves = [(s, e) for s, e in all_endpoints if e.system == "rest" and e.role == "serve"]
    manifest_services = {
        service
        for service, endpoint in all_endpoints
        if endpoint.system == "kafka" and endpoint.source == "manifest"
    }
    kafka_endpoints = [
        (service, endpoint)
        for service, endpoint in all_endpoints
        if endpoint.system == "kafka"
        and (service not in manifest_services or endpoint.source == "manifest")
    ]
    produces = [(s, e) for s, e in kafka_endpoints if e.role == "produce"]
    consumes = [(s, e) for s, e in kafka_endpoints if e.role == "consume"]

    edges: list[GraphEdge] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    gateway_proxy_targets: set[tuple[str, str]] = set()
    for call_service, call in calls:
        target_hint = _rest_target_service_hint(call, strategy1=strategy1)
        for serve_service, serve in serves:
            if call_service == serve_service:
                continue
            if not _service_matches_hint(serve_service, target_hint):
                continue
            effective_call = call
            if not paths_match(call.topic, serve.topic):
                resolved_call = _resolved_configured_api_call(call, serve)
                if resolved_call is None:
                    continue
                effective_call = resolved_call
                _trace(
                    "rest_client.graph.resource_resolved",
                    caller=call_service,
                    host=serve_service,
                    source_topic=call.topic,
                    resource=serve.topic,
                )
            if effective_call is not None:
                proxy_target = (call.id, serve_service)
                if call.framework == "spring-cloud-gateway" and call.topic.startswith("ANY "):
                    if proxy_target in gateway_proxy_targets:
                        continue
                    gateway_proxy_targets.add(proxy_target)
                key = ("rest", call_service, serve_service, call.id, serve.id)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(GraphEdge("rest", call_service, serve_service, effective_call, serve))

    # Une configuration Rest*Config* établit une dépendance A → B même si B
    # n'expose aucune ressource REST détectée. Une arête spécifique à une
    # ressource, déjà ajoutée ci-dessus, reste plus précise et suffit alors.
    resolved_configured_calls = {
        (edge.from_endpoint.id, edge.to_service)
        for edge in edges
        if edge.kind == "rest" and configured_api_client_domain(edge.from_endpoint) is not None
    }
    for call_service, call in calls:
        domain = configured_api_client_domain(call)
        if domain is None or domain not in endpoints_by_service or call_service == domain:
            continue
        if (call.id, domain) in resolved_configured_calls:
            continue
        key = ("rest", call_service, domain, "configured-api", "")
        if key in seen:
            continue
        seen.add(key)
        edges.append(GraphEdge("rest", call_service, domain, call, None))

    # `getRest().get("partner")` names a microservice
    # outside the indexed workspace.  Keep it as a microservice relation (not
    # an untyped external API) so the topology can label the target external.
    for call_service, call in calls:
        service = external_microservice_name(call)
        if service is None or call_service == service:
            continue
        key = ("rest", call_service, service, "configured-external", "")
        if key in seen:
            continue
        seen.add(key)
        edges.append(GraphEdge("rest", call_service, service, call, None))

    # A dynamic topic is evidence of a Kafka integration, not an identity.
    # Matching two `<dynamic>` placeholders would fabricate an inter-service
    # dependency, so only statically resolved topic names may form a Kafka edge.
    for produce_service, produce in produces:
        if produce.topic_dynamic:
            continue
        for consume_service, consume in consumes:
            if produce_service == consume_service:
                continue
            if consume.topic_dynamic:
                continue
            if produce.topic == consume.topic:
                key = ("kafka", produce_service, consume_service, produce.id, consume.id)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(GraphEdge("kafka", produce_service, consume_service, produce, consume))

    return edges


def find_outbound_calls_in_consumers(
    endpoints: list[MessageEndpoint],
) -> list[OutboundCallInConsumer]:
    """Un appel REST (`call`) dont le site tombe dans la plage de lignes
    d'un handler de consommation Kafka (`consume`) du même fichier. `endpoints`
    doit venir d'un seul service : fichier et lignes ne sont comparables qu'au
    sein d'un même repo."""

    consumers = [e for e in endpoints if e.system == "kafka" and e.role == "consume"]
    calls = [e for e in endpoints if e.system == "rest" and e.role == "call"]

    results: list[OutboundCallInConsumer] = []
    for consumer in consumers:
        for call in calls:
            if call.path != consumer.path:
                continue
            if consumer.start_line <= call.start_line <= consumer.end_line:
                results.append(OutboundCallInConsumer(consumer, call))
    return results
