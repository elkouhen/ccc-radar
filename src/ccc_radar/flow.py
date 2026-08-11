"""Traçage d'un flux de messages Kafka ou d'une route REST à travers les
endpoints indexés (BACKLOG-10 K5). Résout une requête (nom de topic/route
exact, sinon correspondance approximative sur le texte du topic) vers tous
ses sites — producteurs/consommateurs Kafka, ou serveurs/appelants REST —

Résolution textuelle d'abord (égalité exacte, puis sous-chaîne insensible
à la casse si le résultat est non ambigu) ; `resolve_topic_by_similarity`
(BACKLOG-10 K3) prend le relais en dernier recours, quand aucun candidat
textuel n'existe, via la similarité vectorielle sur les endpoints
(`Store.knn_search_endpoints`) — utile pour une requête en langage naturel
qui ne correspond à aucun nom de topic/route littéral (ex. « qui traite le
paiement d'une commande » plutôt que le nom exact du topic). Seulement
disponible pour le projet courant (pas de fédération multi-services : le
support serait une extension future, pas un manque documenté ici en
détail) — voir `docs/SPEC-TECH.md`.
"""

from dataclasses import dataclass

from ccc_radar.embedder import EmbedderLike
from ccc_radar.models import MessageEndpoint
from ccc_radar.store import Store


class FlowError(Exception):
    """Aucun topic/route ne correspond à la requête parmi les endpoints
    indexés (ou plusieurs correspondent de façon ambiguë)."""


@dataclass(frozen=True)
class FlowSite:
    service: str | None  # None hors fédération (projet courant seul)
    endpoint: MessageEndpoint


@dataclass(frozen=True)
class FlowResult:
    query: str
    resolved_topic: str
    sites: list[FlowSite]
    warnings: list[str]


_DEFAULT_SIMILARITY_THRESHOLD = 0.35


def group_endpoints_by_module_for_flow(
    endpoints: list[MessageEndpoint],
) -> dict[str | None, list[MessageEndpoint]]:
    """Regroupe par module Maven (`endpoint.module`, BACKLOG-13 M1) pour
    `trace_message_flow` (M3). Contrairement à `graph.group_endpoints_by_module`
    (qui ignore les endpoints sans module — sans nom stable, ils ne
    peuvent jamais former une arête inter-service fiable), `flow` ne
    supprime jamais un site : lister tous les producteurs/consommateurs
    d'un topic est le contrat, même ceux qu'aucune information de module
    ne permet d'attribuer (regroupés sous la clé `None`, comme avant
    l'attribution de module — BACKLOG-13)."""
    grouped: dict[str | None, list[MessageEndpoint]] = {}
    for endpoint in endpoints:
        grouped.setdefault(endpoint.module, []).append(endpoint)
    return grouped


def resolve_topic_by_similarity(
    store: Store,
    embedder: EmbedderLike,
    query: str,
    endpoints: list[MessageEndpoint],
    min_score: float = _DEFAULT_SIMILARITY_THRESHOLD,
) -> str | None:
    """Dernier recours (BACKLOG-10 K3) quand `resolve_topic` ne trouve aucun
    candidat textuel unique : plus proche voisin parmi les endpoints déjà
    embeddés (`cccr index`) dans `store`, mais seulement si son score dépasse
    `min_score` — sous ce seuil, aucun résultat n'est un meilleur signal
    qu'une requête qui ne ressemble à rien d'indexé, mieux vaut échouer
    explicitement que renvoyer un candidat non pertinent (même philosophie
    que `topic_dynamic` : jamais résolu au hasard). `endpoints` sert
    uniquement à retrouver le topic associé à l'endpoint gagnant (le KNN
    lui-même interroge `store` directement) — aucun résultat si le store n'a
    aucun endpoint embeddé (repo sans pack d'inventaire, ou pas encore
    réindexé)."""
    topic_by_id = {e.id: e.topic for e in endpoints}
    query_vec = embedder.embed_query(query)
    for endpoint_id, score in store.knn_search_endpoints(query_vec, top_k=1):
        if score >= min_score and endpoint_id in topic_by_id:
            return topic_by_id[endpoint_id]
    return None


def resolve_topic(query: str, all_topics: set[str]) -> str | None:
    """Nom exact d'abord ; sinon sous-chaîne insensible à la casse, mais
    seulement si elle désigne un unique topic/route — une correspondance
    ambiguë ne doit jamais choisir arbitrairement."""
    if query in all_topics:
        return query
    query_lower = query.lower()
    matches = sorted({topic for topic in all_topics if query_lower in topic.lower()})
    if len(matches) == 1:
        return matches[0]
    return None


def trace_flow(
    query: str,
    endpoints_by_service: dict[str | None, list[MessageEndpoint]],
    warnings: list[str] | None = None,
) -> FlowResult:
    """`warnings` : avertissements de fédération (service non indexé/
    incompatible, K7 CA2) déjà émis par `load_federation` — reportés tels
    quels, jamais absorbés silencieusement : un site manquant à cause d'un
    service non fédéré doit rester visible, pas confondu avec une absence
    réelle de producteur/consommateur."""
    all_topics = {e.topic for endpoints in endpoints_by_service.values() for e in endpoints}
    resolved = resolve_topic(query, all_topics)
    if resolved is None:
        raise FlowError(
            f"Aucun topic/route ne correspond à {query!r} parmi les endpoints indexés."
        )

    sites: list[FlowSite] = []
    seen_sites: set[tuple[str | None, str]] = set()
    for service, endpoints in endpoints_by_service.items():
        for endpoint in endpoints:
            if endpoint.topic != resolved:
                continue
            key = (service, endpoint.id)
            if key in seen_sites:
                continue
            seen_sites.add(key)
            sites.append(FlowSite(service=service, endpoint=endpoint))

    return FlowResult(
        query=query, resolved_topic=resolved, sites=sites, warnings=list(warnings or [])
    )
