from pathlib import Path

from systemlens.maven import module_name_for_path


def test_module_name_for_path_finds_nearest_pom(tmp_path: Path) -> None:
    module_dir = tmp_path / "order-service"
    (module_dir / "app").mkdir(parents=True)
    (module_dir / "pom.xml").write_text(
        "<project><artifactId>order-service</artifactId></project>"
    )
    (module_dir / "app" / "OrderController.java").write_text("class OrderController {}")

    assert (
        module_name_for_path(tmp_path, "order-service/app/OrderController.java")
        == "order-service"
    )


def test_module_name_for_path_falls_back_to_directory_name_without_artifact_id(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "payment-service"
    module_dir.mkdir()
    (module_dir / "pom.xml").write_text("<project></project>")
    (module_dir / "App.java").write_text("class App {}")

    assert module_name_for_path(tmp_path, "payment-service/App.java") == "payment-service"


def test_module_name_for_path_returns_none_without_any_pom(tmp_path: Path) -> None:
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "Foo.java").write_text("class Foo {}")

    assert module_name_for_path(tmp_path, "app/Foo.java") is None


def test_module_name_for_path_uses_the_nearest_pom_in_a_multi_module_tree(
    tmp_path: Path,
) -> None:
    (tmp_path / "pom.xml").write_text("<project><artifactId>parent</artifactId></project>")
    module_dir = tmp_path / "order-service"
    (module_dir / "app").mkdir(parents=True)
    (module_dir / "pom.xml").write_text(
        "<project><artifactId>order-service</artifactId></project>"
    )
    (module_dir / "app" / "OrderController.java").write_text("class OrderController {}")

    # le pom du module (le plus proche) l'emporte sur le pom parent
    assert (
        module_name_for_path(tmp_path, "order-service/app/OrderController.java")
        == "order-service"
    )


def test_module_name_for_path_never_escapes_repo_root(tmp_path: Path) -> None:
    # un pom.xml existe au-dessus de repo_root (dans tmp_path) mais ne doit
    # jamais être considéré : repo_root est la limite haute de la remontée.
    (tmp_path / "pom.xml").write_text("<project><artifactId>outside</artifactId></project>")
    repo_root = tmp_path / "repo"
    (repo_root / "app").mkdir(parents=True)
    (repo_root / "app" / "Foo.java").write_text("class Foo {}")

    assert module_name_for_path(repo_root, "app/Foo.java") is None
