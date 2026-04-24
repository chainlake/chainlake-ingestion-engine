import os

from rpcstream.config.loader import _clean_env_value, _find_env_path, _load_env_file


def test_load_env_file_ignores_shell_commands(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "FOO=bar",
                "export BAR=baz",
                "kubectl create secret generic kafka-credentials -n ingestion\\",
                "  --from-literal=BAR=baz",
            ]
        )
    )
    config_dir = tmp_path / "rpcstream"
    config_dir.mkdir()
    config_file = config_dir / "pipeline.yaml"
    config_file.write_text("pipeline: {}\n")

    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAR", raising=False)

    _load_env_file(str(config_file))

    assert os.environ["FOO"] == "bar"
    assert os.environ["BAR"] == "baz"
    assert "kubectl create secret generic kafka-credentials -n ingestion\\" not in os.environ


def test_find_env_path_prefers_config_ancestors(tmp_path):
    repo_root = tmp_path / "repo"
    nested = repo_root / "rpcstream" / "config"
    nested.mkdir(parents=True)
    env_file = repo_root / ".env"
    env_file.write_text("FOO=bar\n")
    config_file = nested / "pipeline.yaml"
    config_file.write_text("pipeline: {}\n")

    assert _find_env_path(str(config_file)) == env_file


def test_clean_env_value_unwraps_quotes():
    assert _clean_env_value('"hello"') == "hello"
    assert _clean_env_value("'world'") == "world"
    assert _clean_env_value("plain") == "plain"
