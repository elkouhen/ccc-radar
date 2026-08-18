# microservices-kafka-mq — rapport d'audit `systemlens`

Boucle d'amélioration du 18 août 2026 (itération suivant celle du 17 juillet
2026, conservée dans l'historique Git de ce fichier). Périmètre : Java/Spring,
HTTP REST, Kafka. Les protocoles hors périmètre (gRPC, RabbitMQ/AMQP,
messagerie propriétaire) sont signalés séparément ; leur absence du graphe
HTTP/Kafka n'est pas un faux négatif.

## Étape 0 — Préflight et traçabilité

- **Dépôt** : `~/examples/microservices-kafka-mq` (multi-module Maven,
  Spring Boot **2.1.1.RELEASE**, Java 8).
- **Commit/branche** : `5a597e2382013e6faeb85ec4f417bf4eed838088` (`master`),
  **identique** à l'itération précédente — aucune modification du code source
  entre les deux boucles. L'analyse directe de l'itération précédente reste
  donc valide ; elle a été revérifiée point par point sur le code (voir Étape
  2) plutôt que refaite de zéro.
- **État Git avant nettoyage** : artefacts non suivis résiduels de l'ancien
  outillage : `.cccf/`, `.cccr/`, `.gitignore` (référence `.cocoindex_code/`,
  un outil tiers), `graph.drawio`. Tous supprimés (aucun commit, aucun fichier
  source/build touché) avant régénération.
- **Outillage** : `systemlens` (dépôt `ccc-radar`, anciennement `cccr` — CLI
  renommée depuis la boucle précédente), Semgrep 1.172.0 disponible sur
  l'hôte, Python 3.13 (`.venv` du dépôt `ccc-radar`).
- **`systemlens doctor`** (après `init`) : `systemlens` ✓, `configuration` ✓,
  `analyse AST` ✓ (extracteurs Tree-sitter Java disponibles). **Changement
  d'architecture notable** : `doctor` ne vérifie plus Semgrep séparément et la
  commande `index` n'expose **plus d'option `--semgrep`** — l'extraction REST
  (y compris les mappings Spring MVC method-level) repose désormais
  entièrement sur les extracteurs AST Tree-sitter internes. Cela **résout**
  la limitation P2 documentée précédemment (« index sans Semgrep » omettant
  `POST/GET /api/order`) : ces mappings sont désormais détectés par
  `systemlens index --full`, sans dépendance à Semgrep pour ce périmètre.
- **Régénération** : `rm -rf .systemlens && systemlens init && systemlens index --full`
  (régénération complète autorisée dans le dépôt exemple). 57 fichiers
  scannés, **26 endpoints** (24 REST + 2 Kafka) après la correction appliquée
  cette boucle (21 avant correction — voir Étape 4).

Sorties brutes : `reports/raw/kafka-mq-{microservices,modules,modules-list,apis,topics,mongodb,graph,coverage,audit,indexing-issues,integrations-order,integrations-invoicing}-{2,3}.json`
(`-2` = avant correction du fix de cette boucle, `-3` = après).

## Étape 2 — Analyse directe (référence, hors `systemlens`)

Le code étant inchangé (même commit), l'analyse directe de la boucle
précédente a été **revérifiée** (et non refaite en aveugle) par relecture
ciblée des fichiers cités : `AppRestController.java`, `OrderRepository.java`,
`UserRepository.java`, `InvoiceController.java`, `OrderKafkaListener.java`,
`OrderService.java`, et recherche de motifs (`RestTemplate`/`WebClient`/
`FeignClient`, `grpc`/`amqp`/`jms`/`MongoRepository`/`@Document`,
`ProducerRecord`/`StreamsBuilder`/`@StreamListener`). Tous les constats
antérieurs sont confirmés à l'identique.

Dépôt trompeur : le nom « kafka-mq » évoque une messagerie propriétaire, mais
le code est **Kafka pur** ; la mention RabbitMQ/JMS dans le `README.md` n'est
qu'une comparaison en prose.

### Services (2)
- **microservice-order** — `spring.application.name=order`, port 8080,
  `OrderApp` `@SpringBootApplication`. Dépendances notables : `spring-boot-starter-data-jpa`,
  **`spring-boot-starter-data-rest`**, `spring-kafka`, `spring-boot-starter-security`,
  springfox-swagger2.
- **microservice-invoicing** — `spring.application.name=invoicing`, port 8081,
  `InvoiceApp` `@SpringBootApplication`. Dépendances : `spring-boot-starter-data-jpa`,
  `spring-boot-starter-web`, `spring-kafka` (**pas** de data-rest).

### HTTP servi — Spring MVC explicite
| # | Service | Méthode | Chemin | Preuve (fichier:ligne) |
|---|---------|---------|--------|------------------------|
| H1 | order | POST | `/api/order` | `controller/AppRestController.java:53` |
| H2 | order | GET | `/api/order` | `controller/AppRestController.java:69` |
| H3 | invoicing | ANY | `/` | `web/InvoiceController.java:27` (`@RequestMapping("/")` sans `method` ⇒ toutes méthodes) |
| H4 | invoicing | GET | `/{id}` | `web/InvoiceController.java:22` |

### HTTP servi — Spring Data REST (auto-généré, base `/`, order seul)
`spring-boot-starter-data-rest` est présent côté order ; `SpringRestDataConfig`
ne fixe ni `baseUri` ni d'exposition globale ⇒ base `/`. Spring Data REST expose
donc tout repository sans `exported=false` :
- **OrderRepository** `@RepositoryRestResource(path="order")` → `/order`, `/order/{id}`
  (CRUD), **plus la ressource de recherche `/order/search/lastUpdate`**
  (méthode `lastUpdate()` annotée `@Query`, `logic/OrderRepository.java:12-13`,
  exposée par défaut par Spring Data REST sous `/search/<nom-de-méthode>`).
- **UserRepository** (`JpaRepository<User,Integer>`, **sans** annotation) → `/users`,
  `/users/{id}` (CRUD), **plus 4 ressources de recherche** :
  `/users/search/findByUsernameCaseInsensitive`, `/users/search/findByEmail`,
  `/users/search/findByEmailAndActivationKey`,
  `/users/search/findByEmailAndResetPasswordKey`
  (`repository/UserRepository.java:12-13,15-16,18-19,21-22`).
  Exposition par défaut, probablement involontaire — fuit le contenu et les
  capacités de recherche de la table utilisateurs.
- `CustomerRepository`, `ItemRepository` : `exported=false` → non exposés.

### HTTP appelé (clients)
**Aucun.** Pas de `RestTemplate`/`WebClient`/OpenFeign en production. La dépendance
`httpclient` (invoicing) n'est pas utilisée dans le code de production. ⇒ **Aucune
arête HTTP inter-services.**

### Kafka
| # | Rôle | Topic | Framework | Service | Preuve (fichier:ligne) |
|---|------|-------|-----------|---------|------------------------|
| K1 | producteur | `order` | `KafkaTemplate.send` (JsonSerializer) | order | `logic/OrderService.java:40` |
| K2 | consommateur | `order` | `@KafkaListener` (InvoiceDeserializer) | invoicing | `events/OrderKafkaListener.java:23` |

Aucun Kafka Streams, Spring Cloud Stream, `poll`/`subscribe` natif, `ProducerRecord`.
**Arête résolue** : `order --produit--> topic 'order' --consommé par--> invoicing`.

### Mongo
**Aucun.** Repositories JPA relationnels (`PagingAndSortingRepository`/`JpaRepository`,
entités `@Entity`/`@Table`), base MySQL configurée. Aucun `@Document`,
`MongoRepository`, `MongoTemplate`.

### Hors périmètre
**Aucun protocole dans le code.** Pas de gRPC, AMQP/RabbitMQ, JMS, WebSocket.

### Exclusion des tests (revérifiée)
Les listeners/producteurs de test (`kafka/KafkaListenerBean.java`, `OrderKafkaTest`,
`InvoiceKafkaTest`) restent correctement **absents** du code de production
analysé, et absents de l'inventaire `systemlens`.

## Étape 1 — Inventaire `systemlens` (après correction de cette boucle)

`systemlens microservices` détecte **2 microservices** (`microservice-order`,
`microservice-invoicing`, `starts_application=true`, technologies Java/Spring Boot/Kafka).
`systemlens modules` liste en plus l'agrégateur `microservices-kafka` (`kind=aggregator`).

Endpoints REST servis (24) + Kafka (2) = **26**. Détail avec preuves
(`systemlens modules integrations <module> --json`) :

| Service | Endpoint | Framework | Preuve (fichier:ligne) |
|---------|----------|-----------|------------------------|
| invoicing | `ANY /` | spring | `web/InvoiceController.java:27` |
| invoicing | `GET /{id}` | spring | `web/InvoiceController.java:22` |
| invoicing | `GET /actuator/**` | spring-actuator | `application.properties:1` |
| order | `POST /api/order` | spring | `controller/AppRestController.java:53` |
| order | `GET /api/order` | spring | `controller/AppRestController.java:69` |
| order | `GET/POST /order`, `GET/PUT/PATCH/DELETE /order/{id}` | spring-data-rest | `logic/OrderRepository.java:9` |
| order | **`GET /order/search/lastUpdate`** *(nouveau cette boucle)* | spring-data-rest | `logic/OrderRepository.java:12-13` |
| order | `GET/POST /users`, `GET/PUT/PATCH/DELETE /users/{id}` | spring-data-rest | `repository/UserRepository.java:9` |
| order | **4× `GET /users/search/<méthode>`** *(nouveau cette boucle)* | spring-data-rest | `repository/UserRepository.java:12-22` |
| order | `GET /swagger-ui.html` | swagger-ui | `config/SwaggerConfig.java:27` |
| order | `GET /actuator/**` | spring-actuator | `application.properties:1` |

Kafka : topic `order`, producteur `microservice-order` (type `Order`), consommateur
`microservice-invoicing` (type `Invoice`). `systemlens analyze coverage` : **26
intégrations, 53 relations toutes haute confiance, rien de non résolu**
(`unresolved.*` tous vides). `systemlens export microservices` produit
l'arête `order → topic 'order' → invoicing` avec sites précis
(`OrderService.java:39-40`, `OrderKafkaListener.java:23-28`).

**Audit** : `systemlens analyze audit` signale — à juste titre — un **contrat de
message Kafka potentiellement incompatible** : *« `order` publie Order mais
consomme Invoice »* (confiance medium), inchangé depuis la boucle précédente.

## Étape 3 — Comparaison structurée et note

### 1. Services/modules
| Présents dans les deux | Seulement `systemlens` | Seulement analyse directe |
|---|---|---|
| microservice-order, microservice-invoicing | `microservices-kafka` (agrégateur, à juste titre) | — |

Pas d'écart fonctionnel (le comportement `kind=library` en `modules` vs
`kind=microservice` en `microservices` reste cohérent, seuls les modules
démarrant une app remontant comme services).

### 2. HTTP
| Endpoint (direct) | `systemlens` | Cause éventuelle d'écart |
|---|---|---|
| `POST /api/order`, `GET /api/order` | ✅ | — |
| `ANY /`, `GET /{id}` (invoicing) | ✅ | `systemlens` restitue `ANY /` (exact : `@RequestMapping("/")` sans méthode) |
| SDR `/order`, `/users` (CRUD) | ✅ | — |
| **SDR `/order/search/lastUpdate`** | ✅ **(corrigé cette boucle)** | Faux négatif corrigé : `logic/OrderRepository.py::_infer_spring_data_rest_search_endpoints` |
| **SDR `/users/search/*` (4 méthodes)** | ✅ **(corrigé cette boucle)** | idem |
| `GET /actuator/**`, `GET /swagger-ui.html` | ✅ (tagués `spring-actuator`/`swagger-ui`) | Endpoints framework réels, mais toujours mélangés aux APIs métier dans `http_apis_exposed` (P2 ergonomie, **ticket ouvert SL-016**) |

Aucun endpoint inventé. Aucun faux positif métier. `@RestResource(exported =
false)` correctement exclu (vérifié par test dédié, voir Étape 4).

### 3. Kafka (endpoints + usage méthode)
| Élément (direct) | `systemlens` | Écart |
|---|---|---|
| Producteur `order` — `KafkaTemplate.send` (`OrderService.java:40`) | ✅ `OrderService.java:39-40` | — |
| Consommateur `order` — `@KafkaListener` (`OrderKafkaListener.java:23`) | ✅ `OrderKafkaListener.java:23-28` | — |
| Types message `Order` (pub) / `Invoice` (cons) | ✅ | — |
| Arête `order → 'order' → invoicing` | ✅ résolue, haute confiance | — |

Aucun écart Kafka. Listener de test (`KafkaListenerBean`) toujours absent.

### 4. Mongo
| Élément (direct) | `systemlens` | Écart |
|---|---|---|
| Aucune collection, aucune opération | ✅ `systemlens mongodb` vide | — (aucun faux positif) |

### 5. Arêtes
| Arête (direct) | `systemlens` | Écart |
|---|---|---|
| Kafka `order → topic 'order' → invoicing` | ✅ | — |
| HTTP inter-services | aucune (aucun client HTTP) | ✅ aucune |

### 6. Hors périmètre
Aucun protocole constaté dans le code. Rien à signaler.

### Note `systemlens` : **5 / 5**

Justification : couverture **complète** du périmètre annoncé sur ce dépôt,
après correction des ressources de recherche Spring Data REST cette boucle.
- Services 2/2, Kafka producteur/consommateur/topic/arête **parfaits**, types
  de message corrects, audit pertinent sur l'incompatibilité Order/Invoice.
- HTTP : les 4 endpoints Spring MVC explicites, le SDR CRUD annoté (`/order`)
  et par défaut (`/users`), **et désormais les 5 ressources de recherche SDR**
  (`/order/search/lastUpdate`, `/users/search/*`) — plus aucun faux négatif
  HTTP connu sur ce dépôt.
- Mongo : aucun faux positif (correctement vide).
- Arêtes : Kafka résolue avec sites précis ; aucune arête HTTP inventée ;
  `analyze coverage` ne signale aucune relation non résolue.
- Le seul point encore ouvert — le mélange endpoints framework
  (actuator/swagger) et endpoints métier dans la vue de synthèse
  `http_apis_exposed` — est une question d'**ergonomie de présentation**, pas
  un faux négatif/positif : les deux catégories sont présentes et
  correctement taguées (`framework`) au niveau `modules integrations`. Il ne
  pénalise donc pas la note, mais reste tracé (SL-016) pour une meilleure
  lisibilité.

## Diagrammes

- Inventaire direct (référence) : `reports/assets/microservices-kafka-mq-direct.drawio`
  (export `…-direct.png`).
- Inventaire `systemlens` : `reports/assets/microservices-kafka-mq-cccr.drawio`
  (export `…-cccr.png` — noms hérités de l'ancien identifiant d'outil `cccr`).

**Aucune régénération nécessaire cette boucle** : la topologie (2 services, 1
topic Kafka, 1 arête `order → 'order' → invoicing`, aucune arête HTTP) est
strictement identique à la boucle précédente. Seul le détail interne du nœud
`microservice-order` gagne 5 chemins d'API supplémentaires (ressources de
recherche SDR), qui ne créent ni nouveau nœud ni nouvelle arête au niveau du
diagramme service/topic demandé par la consigne.

## Étape 4 — Amélioration appliquée cette boucle

### P1 — Ressources de recherche Spring Data REST non détectées (corrigé)

- **Dépôt révélateur** : `~/examples/microservices-kafka-mq`.
- **Preuve** : `OrderRepository.lastUpdate()` (`@Query`, ligne 12-13) et 4
  méthodes de `UserRepository` (`findByUsernameCaseInsensitive`, `findByEmail`,
  `findByEmailAndActivationKey`, `findByEmailAndResetPasswordKey`) sont
  exposées par Spring Data REST sous `/<base>/search/<méthode>` par
  convention, mais n'apparaissaient dans aucun inventaire `systemlens` avant
  cette boucle (0 endpoint `search/*` sur 21).
- **Fichiers modifiés** :
  `src/systemlens/scanner/rest_mvc.py` — nouvelle fonction
  `_infer_spring_data_rest_search_endpoints`, appelée depuis
  `_infer_spring_data_rest_endpoints` pour chaque méthode déclarée
  directement dans le corps de l'interface repository. Respecte
  `@RestResource(exported = false)` (méthode exclue) et
  `@RestResource(path = "...")` (nom de ressource explicite), sinon utilise
  le nom de la méthode Java.
- **Tests de non-régression** : `tests/fixtures/spring_data_rest_repo/` (pom
  avec dépendance `spring-boot-starter-data-rest`, `OrderRepository` annotée
  avec une méthode `@Query` exposée et une méthode `@RestResource(exported =
  false)` non exposée, `UserRepository` sans annotation avec une méthode de
  recherche dérivée) + deux tests dans `tests/test_ast_only.py` :
  `test_spring_data_rest_exposes_annotated_repository_crud_and_search_resources`
  et `test_spring_data_rest_exposes_default_pluralized_path_and_search_resource`.
- **Critère d'acceptation** : les deux tests vérifient que les chemins CRUD et
  `search/<méthode>` attendus sont présents, et que la méthode
  `@RestResource(exported = false)` **n'** apparaît **pas**.
- **Validation** : `uv run ruff check` (clean), `uv run mypy` (0 erreur sur 43
  fichiers), `uv run pytest` (168 passed, 2 deselected — aucune régression).
  Réindexation du dépôt révélateur : 21 → **26** endpoints, `analyze coverage`
  toujours 0 relation non résolue.

### P2 — Résolu comme effet de bord du refactor de l'outillage

L'ancienne limitation « `systemlens index` sans `--semgrep` omet les mappings
Spring MVC method-level » (ex. `POST/GET /api/order`) n'existe plus : l'option
`--semgrep` a été retirée de `index`, l'extraction REST reposant désormais
intégralement sur les extracteurs AST Tree-sitter. Vérifié : `systemlens index
--full` (sans aucune dépendance Semgrep pour ce chemin) détecte bien les 2
endpoints `AppRestController`. **Aucune action requise**, ce point est clos.

### P2 — Reporté (ticket ouvert)

- **SL-016** (issue GitHub
  [#10](https://github.com/elkouhen/systemlens/issues/10)) : séparer les
  endpoints framework (`spring-actuator`, `swagger-ui`) des endpoints métier
  dans `http_apis_exposed` (`systemlens microservices --json`). Ergonomie de
  présentation uniquement — aucun faux négatif/positif sous-jacent (le
  `framework` correct est déjà porté par `modules integrations`).

## Limites et arrêt de la boucle

Après cette itération, il ne reste sur ce dépôt qu'un écart P2 documenté et
non pénalisant (SL-016, ergonomie de présentation). Conformément à la
consigne, la boucle s'arrête ici pour `microservices-kafka-mq` : aucun faux
négatif/positif HTTP ou Kafka résiduel dans le périmètre annoncé.
