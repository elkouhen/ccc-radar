#!/usr/bin/env python3
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

from archlens.config import Config
from archlens.embedder import Embedder
from archlens.indexer import index_repo
from archlens.search import search_findings
from archlens.store import Store

FIXTURE_REPO = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "vuln_repo"
QUERIES_PATH = Path(__file__).resolve().parent / "queries.yml"
TOP_K = 3
MIN_HIT_RATE = 0.75


def load_queries() -> list[dict]:
    data = yaml.safe_load(QUERIES_PATH.read_text())
    return data["queries"]


def main() -> int:
    config = Config(rules=["rules/rules.yml"])
    embedder = Embedder(config.embedding_model)
    queries = load_queries()

    rows = []
    # copie temporaire : ne jamais laisser de .archlens/findings.db dans le fixture committée
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_root = Path(tmp_dir) / "vuln_repo"
        shutil.copytree(FIXTURE_REPO, repo_root)

        with Store(repo_root) as store:
            index_repo(repo_root, config, store, embedder, full=True)

            for q in queries:
                hits = search_findings(store, embedder, q["query"], limit=TOP_K)
                rank = next(
                    (
                        i
                        for i, hit in enumerate(hits, start=1)
                        if hit.finding.rule_id == q["rule_id"]
                        and hit.finding.path == q["path"]
                    ),
                    None,
                )
                rows.append((q["query"], q["rule_id"], q["path"], rank))

    hit_count = sum(1 for _, _, _, rank in rows if rank is not None)
    hit_rate = hit_count / len(rows)

    print(f"{'requête':55} {'attendu':45} {'obtenu':6}")
    print("-" * 108)
    for query, rule_id, path, rank in rows:
        expected = f"{rule_id} @ {path}"
        obtained = f"#{rank}" if rank is not None else "MISS"
        print(f"{query[:53]:55} {expected[:43]:45} {obtained:6}")

    print("-" * 108)
    print(f"Top-{TOP_K} hit rate: {hit_rate:.2f} ({hit_count}/{len(rows)})")

    return 0 if hit_rate >= MIN_HIT_RATE else 1


if __name__ == "__main__":
    sys.exit(main())
