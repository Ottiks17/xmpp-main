from app.config import deep_merge, default_config, load_config


def test_deep_merge_preserves_defaults():
    base = default_config()
    merged = deep_merge(base, {"rest_api": {"port": 9000}})
    assert merged["rest_api"]["port"] == 9000
    assert merged["rest_api"]["host"] == "127.0.0.1"
    assert merged["xmpp"]["port"] == 5222


def test_load_config_returns_dict():
    config = load_config("config/config.example.json")
    assert "xmpp" in config
    assert "rest_api" in config

