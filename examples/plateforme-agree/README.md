# Approved platform demo dataset

This synthetic `systemlens-ai-graph-v1` manifest models an approved-services
platform with 50 services, 10 messaging/data resources, 6 service namespaces
(plus `platform-data` for the data resources), and 6 software layers.

The service layers are represented by metadata:

- `api`
- `application`
- `domain`
- `infrastructure`
- `shared`
- `module`

The dataset is deliberately self-contained and uses synthetic relative
evidence paths. It contains no credentials or production identifiers.

## Import and render

From an indexed SystemLens repository:

```bash
systemlens import-facts \
  examples/plateforme-agree/architecture.ai-boundaries.pass-001.json \
  --namespace ai-boundaries
systemlens export microservices --html plateforme-agreee.html
```

The microservice export exposes filters for software layer, Kubernetes runtime
namespace, and fact namespace. The imported facts use `ai-boundaries` as their
fact namespace and the `platform-*` values as runtime namespaces.
