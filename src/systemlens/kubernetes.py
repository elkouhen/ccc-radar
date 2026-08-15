"""Optional Kubernetes workload discovery through the local ``kubectl`` CLI."""

import json
import subprocess
from dataclasses import dataclass
from typing import Callable


class KubernetesDiscoveryError(RuntimeError):
    """``kubectl`` could not provide a trustworthy workload inventory."""


@dataclass(frozen=True)
class KubernetesWorkload:
    """Resource dimensions aggregated across regular workload containers."""

    kind: str
    namespace: str
    name: str
    replicas: int | None
    cpu_request_millicores: int | None
    memory_request_bytes: int | None
    cpu_limit_millicores: int | None
    memory_limit_bytes: int | None


def _cpu_millicores(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(float(value[:-1])) if value.endswith("m") else int(float(value) * 1000)
    except ValueError:
        return None


def _memory_bytes(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
             "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            try:
                return int(float(value.removesuffix(suffix)) * multiplier)
            except ValueError:
                return None
    try:
        return int(value)
    except ValueError:
        return None


def _sum(values: list[int | None]) -> int | None:
    known = [value for value in values if value is not None]
    return sum(known) if known else None


def discover_workloads(
    *, namespace: str | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> list[KubernetesWorkload]:
    """List Deployments and StatefulSets from the active kubectl context.

    The command is intentionally opt-in: invoking it can contact a Kubernetes
    API server. Only regular containers are summed; init containers have
    different scheduling semantics and are not a steady-state service size.
    """
    command = ["kubectl", "get", "deployment,statefulset"]
    command.extend(["--namespace", namespace] if namespace else ["--all-namespaces"])
    command.extend(["--output", "json"])
    try:
        result = run(command, check=True, capture_output=True, text=True)
        payload = json.loads(result.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        raise KubernetesDiscoveryError(f"kubectl discovery failed: {exc}") from exc

    workloads: list[KubernetesWorkload] = []
    for item in payload.get("items", []):
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        template_spec = spec.get("template", {}).get("spec", {})
        containers = template_spec.get("containers", [])
        requests = [container.get("resources", {}).get("requests", {}) for container in containers]
        limits = [container.get("resources", {}).get("limits", {}) for container in containers]
        workloads.append(KubernetesWorkload(
            kind=str(item.get("kind", "")), namespace=str(metadata.get("namespace", "default")),
            name=str(metadata.get("name", "")), replicas=spec.get("replicas"),
            cpu_request_millicores=_sum([_cpu_millicores(resources.get("cpu")) for resources in requests]),
            memory_request_bytes=_sum([_memory_bytes(resources.get("memory")) for resources in requests]),
            cpu_limit_millicores=_sum([_cpu_millicores(resources.get("cpu")) for resources in limits]),
            memory_limit_bytes=_sum([_memory_bytes(resources.get("memory")) for resources in limits]),
        ))
    return sorted(workloads, key=lambda item: (item.namespace, item.kind, item.name))
