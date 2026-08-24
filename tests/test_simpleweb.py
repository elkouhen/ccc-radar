from pathlib import Path
from threading import Thread
from urllib.error import HTTPError
from urllib.request import urlopen

from typer.testing import CliRunner

from systemlens.simpleweb import app, create_simpleweb_server


def test_simpleweb_rejects_a_missing_directory(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, [str(tmp_path / "missing")])

    assert result.exit_code == 2
    assert "Répertoire introuvable" in result.output


def test_simpleweb_serves_an_explicit_directory(tmp_path: Path) -> None:
    (tmp_path / "report.html").write_text("<h1>Report</h1>", encoding="utf-8")
    http_server = create_simpleweb_server(tmp_path, "127.0.0.1", 0)
    worker = Thread(target=http_server.serve_forever)
    worker.start()

    try:
        port = http_server.server_address[1]
        assert urlopen(f"http://127.0.0.1:{port}/report.html").read() == b"<h1>Report</h1>"
    finally:
        http_server.shutdown()
        worker.join()
        http_server.server_close()


def test_simpleweb_does_not_follow_a_symlink_outside_the_directory(tmp_path: Path) -> None:
    report_directory = tmp_path / "report"
    report_directory.mkdir()
    private_file = tmp_path / "private.json"
    private_file.write_text('{"secret": false}', encoding="utf-8")
    (report_directory / "escape.json").symlink_to(private_file)
    http_server = create_simpleweb_server(report_directory, "127.0.0.1", 0)
    worker = Thread(target=http_server.serve_forever)
    worker.start()

    try:
        port = http_server.server_address[1]
        try:
            urlopen(f"http://127.0.0.1:{port}/escape.json")
        except HTTPError as exc:
            assert exc.code == 404
        else:
            raise AssertionError("The server exposed a file outside its report directory")
    finally:
        http_server.shutdown()
        worker.join()
        http_server.server_close()
