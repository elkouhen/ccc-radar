from pathlib import Path

from systemlens.gradle import discover_gradle_service_roots, gradle_service_for_path


def test_gradle_service_for_path_groups_all_submodules_under_the_top_level_dir(
    tmp_path: Path,
) -> None:
    service = tmp_path / "customer-service"
    (service / "build.gradle").parent.mkdir(parents=True)
    (service / "build.gradle").write_text("plugins { id 'org.springframework.boot' }\n")
    main_dir = service / "customer-service-main" / "src" / "main" / "java"
    main_dir.mkdir(parents=True)
    (main_dir / "CustomerServiceMain.java").write_text(
        "public class CustomerServiceMain {\n"
        "    public static void main(String[] args) {\n"
        "        SpringApplication.run(CustomerServiceMain.class, args);\n"
        "    }\n"
        "}\n"
    )
    domain_dir = service / "customer-service-domain" / "src" / "main" / "java"
    domain_dir.mkdir(parents=True)
    (domain_dir / "Customer.java").write_text("public class Customer {}")

    assert (
        gradle_service_for_path(
            tmp_path, "customer-service/customer-service-main/src/main/java/CustomerServiceMain.java"
        )
        == "customer-service"
    )
    # un sous-module sans main() est quand même rattaché au même service
    assert (
        gradle_service_for_path(tmp_path, "customer-service/customer-service-domain/src/main/java/Customer.java")
        == "customer-service"
    )


def test_gradle_service_for_path_ignores_a_main_method_that_does_not_start_spring(
    tmp_path: Path,
) -> None:
    tool = tmp_path / "some-cli-tool" / "src" / "main" / "java"
    tool.mkdir(parents=True)
    (tool / "Main.java").write_text(
        "public class Main {\n    public static void main(String[] args) {\n        System.out.println(\"hi\");\n    }\n}\n"
    )

    assert gradle_service_for_path(tmp_path, "some-cli-tool/src/main/java/Main.java") is None


def test_gradle_service_for_path_returns_none_for_a_directory_with_no_spring_boot_main(
    tmp_path: Path,
) -> None:
    other = tmp_path / "end-to-end-tests" / "src" / "endToEndTest" / "java"
    other.mkdir(parents=True)
    (other / "Scenario.java").write_text("public class Scenario {}")

    assert gradle_service_for_path(tmp_path, "end-to-end-tests/src/endToEndTest/java/Scenario.java") is None


def test_gradle_service_for_path_returns_none_for_a_file_at_repo_root(tmp_path: Path) -> None:
    (tmp_path / "Foo.java").write_text("class Foo {}")

    assert gradle_service_for_path(tmp_path, "Foo.java") is None


def test_gradle_service_for_path_uses_the_repo_name_for_a_standard_single_project(
    tmp_path: Path,
) -> None:
    main_dir = tmp_path / "src" / "main" / "java"
    main_dir.mkdir(parents=True)
    (tmp_path / "build.gradle").write_text("plugins { id 'org.springframework.boot' }\n")
    (main_dir / "Application.java").write_text(
        "public class Application {\n"
        "    public static void main(String[] args) {\n"
        "        SpringApplication.run(Application.class, args);\n"
        "    }\n"
        "}\n"
    )

    assert discover_gradle_service_roots(tmp_path) == [tmp_path.name]
    assert gradle_service_for_path(tmp_path, "src/main/java/Application.java") == tmp_path.name


def test_gradle_service_for_path_uses_the_declared_artifact_name(tmp_path: Path) -> None:
    service = tmp_path / "customer-service"
    main_dir = service / "src" / "main" / "java"
    main_dir.mkdir(parents=True)
    (service / "build.gradle").write_text('archivesBaseName = "customer-api"\n')
    (main_dir / "Application.java").write_text(
        "public class Application {\n"
        "    public static void main(String[] args) {\n"
        "        SpringApplication.run(Application.class, args);\n"
        "    }\n"
        "}\n"
    )

    assert discover_gradle_service_roots(tmp_path) == ["customer-api"]
    assert gradle_service_for_path(tmp_path, "customer-service/src/main/java/Application.java") == "customer-api"


def test_gradle_root_service_uses_root_project_name_when_no_archive_name_is_declared(
    tmp_path: Path,
) -> None:
    main_dir = tmp_path / "src" / "main" / "java"
    main_dir.mkdir(parents=True)
    (tmp_path / "settings.gradle.kts").write_text('rootProject.name = "orders-api"\n')
    (main_dir / "Application.java").write_text(
        "public class Application {\n"
        "    public static void main(String[] args) {\n"
        "        SpringApplication.run(Application.class, args);\n"
        "    }\n"
        "}\n"
    )

    assert discover_gradle_service_roots(tmp_path) == ["orders-api"]
    assert gradle_service_for_path(tmp_path, "src/main/java/Application.java") == "orders-api"
