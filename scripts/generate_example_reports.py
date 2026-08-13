#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = Path.home() / "examples"
REPORTS = ROOT / "reports"
ASSETS = REPORTS / "assets"
SYSTEMLENS = ROOT / ".venv" / "bin" / "systemlens"


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)


def shell_text(cmd: list[str], cwd: Path, default: str = "") -> str:
    proc = run(cmd, cwd)
    if proc.returncode != 0:
        return default
    return proc.stdout.strip()


def markdown_table(rows: list[tuple[str, str]]) -> str:
    lines = ["| Field | Value |", "|---|---|"]
    for key, value in rows:
        safe = value.replace("\n", "<br>") if value else "-"
        lines.append(f"| {key} | {safe} |")
    return "\n".join(lines)


def rel(path: Path) -> str:
    return path.relative_to(REPORTS).as_posix()


def site_ref(site: dict[str, object] | None) -> str:
    if not site:
        return "-"
    path = str(site.get("path", "-"))
    start = site.get("start_line")
    end = site.get("end_line")
    if isinstance(start, int) and isinstance(end, int):
        return f"`{path}:{start}-{end}`"
    return f"`{path}`"


def render_flow_table(headers: list[str], rows: list[tuple[str, ...]], empty: str) -> str:
    if not rows:
        return empty
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def http_flow_rows(graph: dict[str, object]) -> list[tuple[str, ...]]:
    rows: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for edge in graph.get("edges", []):
        if edge.get("kind") != "rest":
            continue
        row = (
            str(edge.get("from_node", "-")),
            str(edge.get("label", "-")),
            str(edge.get("to_node", "-")),
            site_ref(edge.get("from_site")),
            site_ref(edge.get("to_site")),
        )
        if row not in seen:
            seen.add(row)
            rows.append(row)
    return rows


def kafka_flow_rows(graph: dict[str, object]) -> list[tuple[str, ...]]:
    produces: dict[str, list[dict[str, object]]] = {}
    consumes: dict[str, list[dict[str, object]]] = {}
    for edge in graph.get("edges", []):
        kind = edge.get("kind")
        label = str(edge.get("label", "-"))
        if kind == "kafka_produce":
            produces.setdefault(label, []).append(edge)
        elif kind == "kafka_consume":
            consumes.setdefault(label, []).append(edge)

    rows: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for topic in sorted(set(produces) | set(consumes)):
        topic_producers = produces.get(topic, [])
        topic_consumers = consumes.get(topic, [])
        if topic_producers and topic_consumers:
            for producer in topic_producers:
                for consumer in topic_consumers:
                    row = (
                        str(producer.get("from_node", "-")),
                        topic,
                        str(consumer.get("to_node", "-")),
                        site_ref(producer.get("from_site")),
                        site_ref(consumer.get("to_site")),
                    )
                    if row not in seen:
                        seen.add(row)
                        rows.append(row)
            continue
        if topic_producers:
            for producer in topic_producers:
                row = (
                    str(producer.get("from_node", "-")),
                    topic,
                    "-",
                    site_ref(producer.get("from_site")),
                    "-",
                )
                if row not in seen:
                    seen.add(row)
                    rows.append(row)
            continue
        for consumer in topic_consumers:
            row = (
                "-",
                topic,
                str(consumer.get("to_node", "-")),
                "-",
                site_ref(consumer.get("to_site")),
            )
            if row not in seen:
                seen.add(row)
                rows.append(row)
    return rows


def main() -> int:
    REPORTS.mkdir(exist_ok=True)
    ASSETS.mkdir(exist_ok=True)

    if not EXAMPLES.is_dir():
        raise SystemExit(f"Examples directory not found: {EXAMPLES}")
    if not SYSTEMLENS.exists():
        raise SystemExit(f"systemlens executable not found: {SYSTEMLENS}")

    results: list[dict[str, object]] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    for repo in sorted(path for path in EXAMPLES.iterdir() if path.is_dir()):
        slug = repo.name
        page = REPORTS / f"{slug}.md"
        html_path = ASSETS / f"{slug}-systemlens.html"

        git_ok = shell_text(["git", "rev-parse", "--is-inside-work-tree"], repo) == "true"
        branch = shell_text(["git", "branch", "--show-current"], repo, "-") if git_ok else "-"
        commit = shell_text(["git", "rev-parse", "HEAD"], repo, "-") if git_ok else "-"
        short_commit = commit[:8] if commit not in {"", "-"} else "-"
        commit_date = shell_text(["git", "show", "-s", "--format=%cI", "HEAD"], repo, "-") if git_ok else "-"
        subject = shell_text(["git", "show", "-s", "--format=%s", "HEAD"], repo, "-") if git_ok else "-"
        remote = shell_text(["git", "remote", "get-url", "origin"], repo, "-") if git_ok else "-"
        status = shell_text(["git", "status", "--short"], repo, "") if git_ok else ""
        clean = "yes" if git_ok and not status else "no"
        tracked_files = shell_text(["git", "ls-files"], repo, "") if git_ok else ""
        tracked_count = str(len([line for line in tracked_files.splitlines() if line])) if git_ok else "-"
        pom_count = str(len(list(repo.rglob("pom.xml"))))

        init_state = "already initialized"
        if not (repo / ".systemlens" / "config.yml").exists():
            proc = run([str(SYSTEMLENS), "init"], repo)
            if proc.returncode != 0:
                raise SystemExit(
                    f"systemlens init failed for {repo.name}:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
                )
            init_state = "initialized during report generation"

        index_proc = run([str(SYSTEMLENS), "index"], repo)
        if index_proc.returncode != 0:
            raise SystemExit(
                f"systemlens index failed for {repo.name}:\nSTDOUT:\n{index_proc.stdout}\nSTDERR:\n{index_proc.stderr}"
            )

        graph_proc = run([str(SYSTEMLENS), "export", "microservices", "--json"], repo)
        if graph_proc.returncode != 0:
            raise SystemExit(
                "systemlens export microservices --json failed for "
                f"{repo.name}:\nSTDOUT:\n{graph_proc.stdout}\nSTDERR:\n{graph_proc.stderr}"
            )
        graph = json.loads(graph_proc.stdout)

        micro_proc = run([str(SYSTEMLENS), "microservices", "--json"], repo)
        if micro_proc.returncode != 0:
            raise SystemExit(
                f"systemlens microservices --json failed for {repo.name}:\nSTDOUT:\n{micro_proc.stdout}\nSTDERR:\n{micro_proc.stderr}"
            )
        micro = json.loads(micro_proc.stdout)

        html_proc = run(
            [str(SYSTEMLENS), "export", "microservices", "--html", str(html_path)], repo
        )
        if html_proc.returncode != 0:
            raise SystemExit(
                "systemlens export microservices --html failed for "
                f"{repo.name}:\nSTDOUT:\n{html_proc.stdout}\nSTDERR:\n{html_proc.stderr}"
            )

        warnings = []
        note = graph.get("note") or ""
        if note:
            warnings.append(str(note))
        warnings.extend(str(item) for item in micro.get("warnings", []))

        http_rows = http_flow_rows(graph)
        kafka_rows = kafka_flow_rows(graph)

        services = micro.get("services", [])
        if services:
            service_lines = [
                "| Service | Kind | Indexed | Endpoints | Findings | Path |",
                "|---|---|---:|---:|---:|---|",
            ]
            for svc in services:
                service_lines.append(
                    f"| {svc['name']} | {svc['kind']} | {'yes' if svc['indexed'] else 'no'} | {svc['endpoint_count']} | {svc['finding_count']} | `{svc['path']}` |"
                )
            services_md = "\n".join(service_lines)
        else:
            services_md = "No Maven microservices were discovered from this directory."

        warning_md = "\n".join(f"- {warning}" for warning in warnings) if warnings else "None."
        git_table = markdown_table(
            [
                ("Path", f"`{repo}`"),
                ("Origin", remote),
                ("Branch", branch),
                ("HEAD", f"`{short_commit}`"),
                ("Commit date", commit_date),
                ("Last commit subject", subject),
                ("Working tree clean", clean),
                ("Tracked files", tracked_count),
                ("pom.xml files", pom_count),
                ("systemlens init state", init_state),
                ("Report generated", now),
            ]
        )
        graph_table = markdown_table(
            [
                ("Services", str(len(graph.get("services", [])))),
                ("Nodes", str(len(graph.get("nodes", [])))),
                ("Edges", str(len(graph.get("edges", [])))),
                ("HTTP flows", str(len(http_rows))),
                ("Kafka flows", str(len(kafka_rows))),
                ("Outbound calls in consumers", str(len(graph.get("outbound_calls_in_consumers", [])))),
                ("Warnings", str(len(warnings))),
            ]
        )

        http_md = render_flow_table(
            ["Caller", "HTTP endpoint", "Callee", "Caller site", "Server site"],
            http_rows,
            "None.",
        )
        kafka_md = render_flow_table(
            ["Producer", "Topic", "Consumer", "Producer site", "Consumer site"],
            kafka_rows,
            "None.",
        )

        page.write_text(
            "\n".join(
                [
                    f"# {slug}",
                    "",
                    "## Repository",
                    "",
                    git_table,
                    "",
                    "## systemlens microservice export",
                    "",
                    graph_table,
                    "",
                    f"Interactive artifact: [`{rel(html_path)}`]({rel(html_path)})",
                    "",
                    "## Graph notes and warnings",
                    "",
                    warning_md,
                    "",
                    "## Flows",
                    "",
                    "### Kafka",
                    "",
                    kafka_md,
                    "",
                    "### HTTP",
                    "",
                    http_md,
                    "",
                    "## Discovered services",
                    "",
                    services_md,
                    "",
                ]
            ),
            encoding="utf-8",
        )

        results.append(
            {
                "name": slug,
                "page": page.name,
                "branch": branch,
                "commit": short_commit,
                "services": len(graph.get("services", [])),
                "edges": len(graph.get("edges", [])),
                "http_flows": len(http_rows),
                "kafka_flows": len(kafka_rows),
                "warnings": len(warnings),
            }
        )

    index_lines = [
        "# Example reports",
        "",
        "Generated pages for each directory in `~/examples`, with an interactive SystemLens HTML export, flow summaries, and basic Git repository metadata.",
        "",
        "| Repository | Branch | Commit | Services | Edges | HTTP flows | Kafka flows | Warnings | Page |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for item in results:
        index_lines.append(
            f"| {item['name']} | {item['branch']} | `{item['commit']}` | {item['services']} | {item['edges']} | {item['http_flows']} | {item['kafka_flows']} | {item['warnings']} | [{item['page']}]({item['page']}) |"
        )

    (REPORTS / "README.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    print(f"Generated {len(results)} report pages in {REPORTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
