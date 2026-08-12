# REST and OpenAPI detection

`archlens` derives REST facts from Java ASTs and module metadata. This page records
the supported static patterns and their limits; the public endpoint contract is
in [SPEC-FONC.md](./SPEC-FONC.md).

## Endpoint inventory

The extractor recognises:

- Spring MVC and WebFlux mapping annotations, including class-level path
  prefixes and multi-line annotations;
- Spring Data REST repository exposure;
- OpenFeign client interfaces;
- RestTemplate and WebClient calls, including common fluent call chains;
- Spring Cloud Gateway route declarations.

Each result records `serve` or `call`, the HTTP method and path, framework,
source location, module and qualified Java name. Literal values and resolvable
Spring properties are materialised. A path that cannot be resolved statically
is marked dynamic rather than converted into a guessed route.

## Module metadata

Module discovery records `@RestController` classes and OpenAPI contracts found
under standard source/resource locations. Maven modules can additionally expose
generated OpenAPI-client paths when the `openapi-generator-maven-plugin` and
standard generated-source layouts are present.

Use these commands to inspect the result:

```bash
archlens index
archlens apis
archlens apis consumers "POST /orders"
archlens modules show order-service
archlens modules --json
```

## Limits

This is static source analysis. Custom framework annotations, reflection,
runtime-generated routes, arbitrary string assembly and non-standard generated
source layouts can remain unresolved. Gradle OpenAPI-generator metadata is not
currently inferred from plugin configuration.

The fixtures in `tests/` cover representative source shapes, path resolution,
dynamic values and generated-client metadata.
