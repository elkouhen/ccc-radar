"""Inférence des endpoints REST/Kafka depuis le code Java (BACKLOG-16).

Ce paquet remplace l'ancien module `scanner.py` monolithique. Il est
organisé en couches sans cycle d'import :

- `_core`: primitives Java/texte génériques (lecture de source, résolution
  de nom qualifié, inférence de type de message, module Maven/Gradle
  depuis un chemin, construction de `MessageEndpoint`).
- `_spring_properties`: résolution de propriétés Spring (fichiers
  `application*.yml/properties`, profils, config-server local) et de
  champs annotés `@Value`.
- `rest_client_config`: découverte des configurations « configured API
  client » (convention Strategy1 des classes `Rest*Config*`).
- `rest_mvc`: inférence REST (MVC, Feign, Spring Data REST, OpenAPI,
  Gateway, RestTemplate/RestClient, WebClient, WebFlux) — dépend de
  `_core`, `_spring_properties` et `rest_client_config`.
- `kafka_ast`: inférence Kafka au niveau AST — dépend de `_core` et
  `_spring_properties`.
- `kafka_conventions`: inférence Kafka par convention Strategy1 et par
  manifestes Markdown/JSON — dépend de `_core`.

Ce module `__init__` réexporte l'API publique historique de `scanner.py`
(y compris `_local_spring_application_names`, importé directement par
`relations.py`) et fournit `clear_analysis_caches`, qui doit purger les
caches `lru_cache` répartis dans plusieurs sous-modules."""

from __future__ import annotations

from systemlens import gradle as gradle_module
from systemlens import java_parser
from systemlens import maven as maven_module
from systemlens.scanner._core import _java_qualified_name, _java_source
from systemlens.scanner._spring_properties import (
    _load_flat_spring_properties,
    _load_value_annotated_fields,
    _local_spring_application_names,
)
from systemlens.scanner.kafka_ast import infer_kafka_endpoints
from systemlens.scanner.kafka_conventions import (
    apply_kafka_topic_strategy1,
    infer_json_kafka_flow_graph_endpoints,
    infer_kafka_topic_strategy1_endpoints,
    infer_markdown_topic_manifest_endpoints,
)
from systemlens.scanner.rest_client_config import (
    _rest_configuration_client_domains_in_module,
)
from systemlens.scanner.rest_mvc import (
    _class_base_path,
    _file_uses_restclient,
    _file_uses_resttemplate,
    _openapi_generator_contract_owners,
    infer_framework_endpoints,
)

__all__ = [
    "_local_spring_application_names",
    "apply_kafka_topic_strategy1",
    "clear_analysis_caches",
    "infer_framework_endpoints",
    "infer_json_kafka_flow_graph_endpoints",
    "infer_kafka_endpoints",
    "infer_kafka_topic_strategy1_endpoints",
    "infer_markdown_topic_manifest_endpoints",
]


def clear_analysis_caches() -> None:
    """BACKLOG-16 P2 : vide tous les `lru_cache` d'analyse best-effort
    (package/qualified-name Java, propriétés Spring, champs `@Value`,
    module Maven, service Gradle) — à appeler en tête de chaque
    indexation. Ces caches accélèrent une indexation en cours (un même
    fichier de config lu plusieurs fois), mais un serveur MCP est un
    process long-vivant : sans purge, `reindex_findings` reservirait des
    valeurs résolues avant la modification des fichiers qui a motivé la
    réindexation."""
    _java_qualified_name.cache_clear()
    _openapi_generator_contract_owners.cache_clear()
    _java_source.cache_clear()
    _load_flat_spring_properties.cache_clear()
    _load_value_annotated_fields.cache_clear()
    _class_base_path.cache_clear()
    _rest_configuration_client_domains_in_module.cache_clear()
    _file_uses_resttemplate.cache_clear()
    _file_uses_restclient.cache_clear()
    java_parser.clear_caches()
    maven_module.clear_caches()
    gradle_module.clear_caches()
