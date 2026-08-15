import subprocess

import pytest

from systemlens.kubernetes import KubernetesDiscoveryError, discover_workloads


def test_discover_workloads_aggregates_regular_container_resources() -> None:
    payload = '''{"items":[{"kind":"Deployment","metadata":{"name":"orders","namespace":"shop"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"resources":{"requests":{"cpu":"250m","memory":"128Mi"},"limits":{"cpu":"1","memory":"512Mi"}}},{"resources":{"requests":{"cpu":"0.5","memory":"64Mi"},"limits":{"cpu":"500m","memory":"256Mi"}}}],"initContainers":[{"resources":{"requests":{"cpu":"2","memory":"2Gi"}}}]}}}}]}'''

    workloads = discover_workloads(
        namespace="shop",
        run=lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, payload, ""),
    )

    assert workloads[0].name == "orders"
    assert workloads[0].cpu_request_millicores == 750
    assert workloads[0].memory_request_bytes == 192 * 1024**2
    assert workloads[0].cpu_limit_millicores == 1500
    assert workloads[0].memory_limit_bytes == 768 * 1024**2


def test_discover_workloads_reports_kubectl_failures() -> None:
    with pytest.raises(KubernetesDiscoveryError, match="kubectl discovery failed"):
        discover_workloads(
            run=lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError("kubectl"))
        )
