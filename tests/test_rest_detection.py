"""Tests pour la détection des contrôleurs REST et clients OpenAPI générés."""

from pathlib import Path

from archlens.modules import discover_rest_controllers, discover_modules
from archlens.maven import (
    _has_openapi_generator_plugin,
    detect_openapi_generated_clients,
    detect_openapi_generator_input_specs,
)


def _write_rest_controller(path: Path, class_name: str) -> None:
    """Crée un fichier Java avec une classe @RestController."""
    path.write_text(
        f"""
package com.example.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class {class_name} {{

    @GetMapping("/users")
    public String getUsers() {{
        return "users";
    }}

    @PostMapping("/users")
    public String createUser() {{
        return "created";
    }}
}}
"""
    )


def _write_pom_with_openapi_plugin(path: Path, artifact: str = "test-service") -> None:
    """Crée un pom.xml avec le plugin openapi-generator-maven-plugin."""
    path.write_text(
        f"""
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>{artifact}</artifactId>
    <version>1.0.0</version>

    <build>
        <plugins>
            <plugin>
                <groupId>org.openapitools</groupId>
                <artifactId>openapi-generator-maven-plugin</artifactId>
                <version>7.0.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>generate</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
"""
    )


def _write_pom_with_openapi_spec(
    path: Path,
    *,
    artifact: str = "test-service",
    input_spec: str = "${project.basedir}/src/main/openapi/orders.yaml",
) -> None:
    path.write_text(
        f"""
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>{artifact}</artifactId>
    <version>1.0.0</version>
    <properties>
        <publishedSpec>{input_spec}</publishedSpec>
    </properties>

    <build>
        <plugins>
            <plugin>
                <groupId>org.openapitools</groupId>
                <artifactId>openapi-generator-maven-plugin</artifactId>
                <version>7.0.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>generate</goal>
                        </goals>
                        <configuration>
                            <inputSpec>${{publishedSpec}}</inputSpec>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
"""
    )


def test_discovers_rest_controllers_with_restcontroller_annotation(tmp_path: Path) -> None:
    """Teste la détection des classes @RestController."""
    # Créer la structure de répertoires
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example" / "controller"
    java_dir.mkdir(parents=True)

    # Créer un fichier avec @RestController
    controller_file = java_dir / "UserController.java"
    _write_rest_controller(controller_file, "UserController")

    # Tester la détection
    controllers = discover_rest_controllers(tmp_path, set())

    assert len(controllers) == 1
    assert "UserController" in controllers[0]
    assert "UserController.java" in controllers[0]


def test_discovers_multiple_rest_controllers(tmp_path: Path) -> None:
    """Teste la détection de plusieurs contrôleurs REST."""
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example" / "controller"
    java_dir.mkdir(parents=True)

    # Créer plusieurs contrôleurs
    _write_rest_controller(java_dir / "UserController.java", "UserController")
    _write_rest_controller(java_dir / "OrderController.java", "OrderController")
    _write_rest_controller(java_dir / "ProductController.java", "ProductController")

    controllers = discover_rest_controllers(tmp_path, set())

    assert len(controllers) == 3
    assert any("UserController" in ctrl for ctrl in controllers)
    assert any("OrderController" in ctrl for ctrl in controllers)
    assert any("ProductController" in ctrl for ctrl in controllers)


def test_does_not_discover_non_controller_sources(tmp_path: Path) -> None:
    """Teste qu'aucun contrôleur n'est détecté sans @RestController."""
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example"
    java_dir.mkdir(parents=True)

    # Créer un fichier Java sans @RestController
    (java_dir / "RegularClass.java").write_text(
        """
package com.example;

public class RegularClass {
    public void method() {
        System.out.println("Not a controller");
    }
}
"""
    )

    controllers = discover_rest_controllers(tmp_path, set())

    assert len(controllers) == 0


def test_has_openapi_generator_plugin_with_plugin(tmp_path: Path) -> None:
    """Teste la détection du plugin openapi-generator dans le pom.xml."""
    pom_file = tmp_path / "pom.xml"
    _write_pom_with_openapi_plugin(pom_file)

    assert _has_openapi_generator_plugin(pom_file) is True


def test_has_openapi_generator_plugin_without_plugin(tmp_path: Path) -> None:
    """Teste qu'aucun plugin n'est détecté sans openapi-generator."""
    pom_file = tmp_path / "pom.xml"
    pom_file.write_text(
        """
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>test-service</artifactId>
    <version>1.0.0</version>
</project>
"""
    )

    assert _has_openapi_generator_plugin(pom_file) is False


def test_detect_openapi_generated_clients_with_plugin(tmp_path: Path) -> None:
    """Teste la détection des clients OpenAPI générés."""
    # Créer le pom.xml avec le plugin
    pom_file = tmp_path / "pom.xml"
    _write_pom_with_openapi_plugin(pom_file)

    # Créer des fichiers Java générés
    generated_dir = tmp_path / "target" / "generated-sources" / "openapi"
    generated_dir.mkdir(parents=True)

    (generated_dir / "UserApi.java").write_text("// Generated User API")
    (generated_dir / "OrderApi.java").write_text("// Generated Order API")

    # Créer un sous-répertoire
    sub_dir = generated_dir / "com" / "example"
    sub_dir.mkdir(parents=True)
    (sub_dir / "ProductApi.java").write_text("// Generated Product API")

    clients = detect_openapi_generated_clients(pom_file)

    assert len(clients) == 3
    assert any("UserApi.java" in client for client in clients)
    assert any("OrderApi.java" in client for client in clients)
    assert any("ProductApi.java" in client for client in clients)


def test_detect_openapi_generator_input_specs_resolves_maven_properties(tmp_path: Path) -> None:
    pom_file = tmp_path / "pom.xml"
    spec = tmp_path / "src" / "main" / "openapi" / "orders.yaml"
    spec.parent.mkdir(parents=True)
    spec.write_text("openapi: 3.0.0\npaths: {}\n")
    _write_pom_with_openapi_spec(pom_file)

    assert detect_openapi_generator_input_specs(pom_file) == ("src/main/openapi/orders.yaml",)


def test_detect_openapi_generated_clients_without_plugin(tmp_path: Path) -> None:
    """Teste qu'aucun client n'est détecté sans le plugin."""
    pom_file = tmp_path / "pom.xml"
    pom_file.write_text(
        """
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>test-service</artifactId>
    <version>1.0.0</version>
</project>
"""
    )

    clients = detect_openapi_generated_clients(pom_file)

    assert len(clients) == 0


def test_detect_openapi_generated_clients_no_generated_files(tmp_path: Path) -> None:
    """Teste qu'aucun client n'est détecté si le répertoire généré n'existe pas."""
    pom_file = tmp_path / "pom.xml"
    _write_pom_with_openapi_plugin(pom_file)

    clients = detect_openapi_generated_clients(pom_file)

    assert len(clients) == 0


def test_module_enrichment_includes_rest_controllers_and_generated_clients(
    tmp_path: Path,
) -> None:
    """Teste que l'enrichissement de module inclut les contrôleurs REST et clients générés."""
    # Créer la structure Maven
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example" / "controller"
    java_dir.mkdir(parents=True)
    _write_rest_controller(java_dir / "UserController.java", "UserController")

    # Créer le pom.xml avec le plugin
    pom_file = tmp_path / "pom.xml"
    _write_pom_with_openapi_plugin(pom_file)

    # Créer des fichiers générés
    generated_dir = tmp_path / "target" / "generated-sources" / "openapi"
    generated_dir.mkdir(parents=True)
    (generated_dir / "ExternalApi.java").write_text("// Generated")

    # Découvrir les modules
    modules = discover_modules(tmp_path)

    assert len(modules) == 1
    module = modules[0]

    # Vérifier que les contrôleurs REST sont détectés
    assert len(module.rest_controllers) == 1
    assert any("UserController" in ctrl for ctrl in module.rest_controllers)

    # Vérifier que les clients générés sont détectés
    assert len(module.openapi_generated_clients) == 1
    assert any("ExternalApi.java" in client for client in module.openapi_generated_clients)


def test_module_enrichment_includes_plugin_referenced_openapi_spec_for_rest_controller(
    tmp_path: Path,
) -> None:
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example" / "controller"
    java_dir.mkdir(parents=True)
    _write_rest_controller(java_dir / "UserController.java", "UserController")
    spec = tmp_path / "src" / "main" / "openapi" / "orders.yaml"
    spec.parent.mkdir(parents=True)
    spec.write_text("openapi: 3.0.0\npaths: {}\n")
    _write_pom_with_openapi_spec(tmp_path / "pom.xml")

    module = discover_modules(tmp_path)[0]

    assert module.openapi_files == ("src/main/openapi/orders.yaml",)


def test_rest_controller_case_insensitive(tmp_path: Path) -> None:
    """Teste que la détection fonctionne avec différentes casses."""
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example"
    java_dir.mkdir(parents=True)

    # Test avec @RestController (standard)
    (java_dir / "StandardController.java").write_text(
        """
package com.example;

@org.springframework.web.bind.annotation.RestController
public class StandardController {}
"""
    )

    controllers = discover_rest_controllers(tmp_path, set())
    assert len(controllers) == 1
    assert any("StandardController" in ctrl for ctrl in controllers)
