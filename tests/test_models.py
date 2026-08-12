from codeatlas.models import compute_finding_id


def test_compute_finding_id_ignores_indentation_differences() -> None:
    id_a = compute_finding_id("custom.sql-fstring", "app/db.py", "    cursor.execute(query)")
    id_b = compute_finding_id("custom.sql-fstring", "app/db.py", "cursor.execute(query)   ")

    assert id_a == id_b


def test_compute_finding_id_changes_with_rule_id() -> None:
    base = compute_finding_id("custom.sql-fstring", "app/db.py", "cursor.execute(query)")
    other = compute_finding_id("custom.other-rule", "app/db.py", "cursor.execute(query)")

    assert base != other


def test_compute_finding_id_changes_with_path() -> None:
    base = compute_finding_id("custom.sql-fstring", "app/db.py", "cursor.execute(query)")
    other = compute_finding_id("custom.sql-fstring", "app/other.py", "cursor.execute(query)")

    assert base != other


def test_compute_finding_id_changes_with_snippet_content() -> None:
    base = compute_finding_id("custom.sql-fstring", "app/db.py", "cursor.execute(query)")
    other = compute_finding_id("custom.sql-fstring", "app/db.py", "cursor.execute(other_query)")

    assert base != other


def test_compute_finding_id_changes_with_location_when_provided() -> None:
    base = compute_finding_id(
        "custom.sql-fstring", "app/db.py", "cursor.execute(query)", 10, 10
    )
    duplicate_later = compute_finding_id(
        "custom.sql-fstring", "app/db.py", "cursor.execute(query)", 20, 20
    )

    assert base != duplicate_later
