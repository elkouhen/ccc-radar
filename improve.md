# Prompt — boucle d'amélioration de l'inventaire microservices

## Objectif

Tu travailles dans le dépôt `systemlens`. Pour chacun des dépôts d'exemple
ci-dessous, mesure la qualité de l'inventaire et du graphe produits par `systemlens`
en les confrontant à une analyse directe du code. Transforme ensuite les écarts
confirmés en backlog priorisé et, lorsque cela est sûr, en améliorations du
dépôt `systemlens`.

Dépôts cibles :


- `~/examples/microservices-kafka-mq`


Le périmètre principal est Java/Spring, HTTP REST et Kafka. Signaler séparément
les protocoles hors périmètre (par exemple gRPC, RabbitMQ/AMQP, messagerie
propriétaire) : leur absence du graphe HTTP/Kafka n'est pas un faux négatif tant
qu'ils ne sont pas annoncés comme pris en charge.

## Règles de travail

1. Ne modifie jamais le code source ni les fichiers de build d'un dépôt sous
   `~/examples`. La suppression et la recréation de l'index `.systemlens` sont
   autorisées directement dans ces dépôts ; ne crée aucun commit.
2. Les commandes de consultation doivent être non mutatives. Une migration ou
   une régénération de l'index `.systemlens` est autorisée lorsqu'elle est nécessaire
   à l'analyse ; note explicitement toute incompatibilité rencontrée.
3. Avant une indexation, exécute `systemlens doctor` et vérifie les règles actives.
   En cas d'échec de Semgrep, conserve stderr et classe le problème comme
   prérequis/outillage : ne conclus pas que le dépôt ne contient aucun endpoint.
   Sur chaque module, supprimer d'abord le répertoire `.systemlens`, puis le recréer
   avec `systemlens init` avant d'exécuter `systemlens index` et toute analyse ou comparaison
   avec l'inventaire `systemlens`. Pour les dépôts sous `~/examples`, effectuer ces
   opérations directement dans le dépôt, sans modifier le code ni les fichiers
   de build.
4. N'utilise pas les résultats de `systemlens` pendant l'analyse directe. Termine et
   enregistre cette dernière avant de comparer les deux inventaires.
5. Analyse le code de production par défaut. Les éléments sous `src/test`,
   `test` ou les fixtures doivent être exclus ou marqués `test` ; ne les compte
   pas comme relation de production.
6. N'invente pas de relation : une arête HTTP exige un appelant et une cible
   résolus ; une arête Kafka exige un producteur ou consommateur observé et un
   topic identifié. Un producteur ou consommateur sans homologue reste un
   endpoint valide, mais une relation non résolue.

## Étape 0 — Préflight et traçabilité

Pour chaque dépôt :

1. Relever le commit/branche et l'état Git sans modifier le dépôt.
2. Vérifier `.systemlens/`, la version de schéma, la fraîcheur de l'inventaire et le
   résultat de `systemlens doctor`.
3. Dans chaque module, supprimer le répertoire `.systemlens`, exécuter `systemlens init`
   pour le recréer, puis lancer `systemlens index`. Ne réaliser cette régénération que
   si les règles et Semgrep sont opérationnels.
4. Consigner les versions de `systemlens`, Semgrep et les règles actives afin que le
   rapport soit reproductible.

## Étape 1 — Analyse outillée avec `systemlens`

Utilise les commandes MCP ou CLI disponibles (`microservices`, `modules`,
`endpoints`, `graph`, `flow`, `audit`) et conserve leurs sorties brutes JSON.
Extraire au minimum :

- les services/modules détectés, avec la raison de leur classification ;
- les endpoints HTTP servis et appelés (`méthode`, `chemin`, service/module,
  fichier et ligne) ;
- les endpoints Kafka produits et consommés (`topic`, rôle, framework,
  service/module, fichier et ligne) ;
- les informations d'usage Kafka (listener, poll/consumer natif,
  `KafkaTemplate`, `ProducerRecord`, Spring Cloud Stream, Kafka Streams) ;
- les informations Mongo disponibles (collections, opérations telles que
  `findById`, `findAll`, `save`, `aggregate`, avec leur preuve) ;
- les arêtes et leurs preuves : HTTP `appelant -> serveur`, Kafka
  `producteur -> topic -> consommateur`.

Produire un premier diagramme Draw.io : un nœud par service et par topic Kafka,
des flèches visuellement distinctes pour HTTP et Kafka, et des libellés
`MÉTHODE chemin` ou `topic / rôle`. Les routes génériques/dynamiques doivent
être explicitement marquées comme telles, sans être confondues avec une cible
résolue.

## Étape 2 — Analyse directe, indépendante de `systemlens`

Explorer le dépôt avec une lecture de code ciblée. Chercher notamment :

- serveurs HTTP : `@RequestMapping`, `@GetMapping`, `@PostMapping`,
  `@PutMapping`, `@PatchMapping`, `@DeleteMapping`, routes WebFlux/Gateway ;
- clients HTTP : `RestTemplate`, `WebClient`, OpenFeign, clients déclaratifs,
  URLs/configuration Spring ;
- Kafka : `@KafkaListener`, consommateurs natifs (`poll`, `subscribe`),
  `KafkaTemplate.send`, `ProducerRecord`, `MessageBuilder`, Kafka Streams
  (`builder.stream`, `.to`), Spring Cloud Stream ;
- Mongo : `@Document`, `MongoTemplate`, `MongoRepository`/
  `ReactiveMongoRepository` et appels de repository ;
- protocoles complémentaires : gRPC, RabbitMQ/AMQP, JMS, etc.

Reconstruire le même inventaire normalisé que lors de l'étape 1. Fusionner les
préfixes de route de classe et de méthode. Résoudre les propriétés/constantes
quand la preuve est locale ; sinon garder une valeur dynamique avec la source
de l'incertitude. Générer un second diagramme Draw.io selon la même convention.

## Étape 3 — Comparaison structurée

> **Note de qualité :** attribuer à `systemlens` une note globale sur **5**, par
> rapport au scan direct, qui sert de référence. Justifier la note à partir de
> la couverture des services, endpoints HTTP et Kafka, opérations Mongo et
> arêtes résolues ; les éléments hors périmètre ou dynamiques non résolubles ne
> doivent pas la pénaliser.

Comparer les inventaires par clés normalisées et produire les tableaux suivants :

1. **Services/modules** : présents dans les deux, seulement `systemlens`, seulement
   analyse directe.
2. **HTTP** : rôle, méthode, chemin, service/module, preuve ; pour chaque écart,
   donner une cause probable (préfixe non fusionné, appel dynamique, framework
   non couvert, test inclus/exclu, cible non résolue).
3. **Kafka** : rôle, topic, framework, service/module, preuve ; expliquer les
   écarts (DSL Streams, propriété non résolue, topic dynamique, API non
   couverte, test inclus/exclu). Inclure aussi l'usage Kafka au niveau méthode
   (listener, `poll`/`subscribe`, `KafkaTemplate.send`, `ProducerRecord`,
   Spring Cloud Stream, `builder.stream`, `.to`) avec le service/module,
   fichier, ligne, topic résolu ou dynamique et preuve. Différencier les
   méthodes présentes dans les deux inventaires, seulement `systemlens` et seulement
   analyse directe.
4. **Mongo** : collection et opération, avec les mêmes catégories d'écart.
   Comparer séparément les collections (`@Document`, collection explicite ou
   dynamique) et les méthodes/opérations observées (`findById`, `findAll`,
   `save`, `aggregate`, `MongoTemplate` ou repository), par service/module,
   fichier, ligne et preuve ; distinguer les éléments présents dans les deux
   inventaires, seulement `systemlens` et seulement analyse directe.
5. **Arêtes** : absentes, en trop ou ambiguës, avec leurs endpoints sources.
6. **Hors périmètre** : protocoles constatés mais non encore pris en charge.

Ne confonds pas un endpoint isolé avec une anomalie. La couverture attendue est
l'inventaire de chaque producer/consumer et de chaque client/serveur détectable;
le rapprochement en arête n'est attendu que lorsque topic ou cible sont
compatibles et résolus.

## Étape 4 — Amélioration et itération

1. Convertir uniquement les écarts confirmés en tâches `P0`, `P1`, `P2` :
   - **P0** : empêche l'indexation, corrompt/mute un dépôt externe, ou rend un
     résultat de lecture inutilisable ;
   - **P1** : faux négatif/faux positif reproductible du périmètre HTTP/Kafka ;
   - **P2** : extension de protocole, ergonomie, rendu ou dette de test.
2. Pour chaque tâche retenue, préciser le dépôt révélateur, la preuve, les
   fichiers pressentis et un critère d'acceptation testable.
3. Implémenter uniquement les améliorations dans `systemlens`, jamais dans les
   exemples. Ajouter le test de non-régression avant ou avec la correction.
4. Exécuter les tests ciblés puis la suite pertinente. Si un prérequis externe
   bloque les tests, distinguer clairement cet échec des régressions du code.
5. Réindexer directement le dépôt révélateur et refaire les étapes 1 à 3.
   Arrêter lorsqu'il ne reste que des écarts documentés, hors périmètre ou non
   reproductibles.

## Sorties attendues

Après une boucle complète, produire :

- un rapport Markdown par dépôt dans `reports/<nom-du-depot>.md` ;
- les deux sources Draw.io et leurs exports image dans
  `reports/assets/<nom>-systemlens.*` et `reports/assets/<nom>-direct.*` ;
- les sorties JSON brutes ou leurs chemins temporaires reproductibles ;
- dans chaque rapport : préflight, inventaires, tableaux de diff, diagrammes,
  limites et axes d'amélioration ;
- un backlog consolidé dans `BACKLOG.md`, fusionné avec les tâches existantes
  sans les écraser, dédoublonné et ordonné par priorité.

Si la consigne d'exécution demande de supprimer `reports/`, supprimer d'abord
le contenu historique, puis régénérer uniquement les rapports issus de la
boucle terminée. Si la suppression devait être définitive, le signaler avant de
produire les sorties attendues, car ces deux attentes sont incompatibles.
