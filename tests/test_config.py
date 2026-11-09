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


def test_contract_v1_defaults_and_env_overrides(monkeypatch):
    cfg = default_config()
    assert cfg["rest_api"]["allow_api_key_query"] is False
    assert cfg["rest_api"]["legacy_payload_as_message"] is False
    assert cfg["rest_api"]["broadcast_max_recipients"] == 200

    import os
    for name, value in (("REST_ALLOW_API_KEY_QUERY", "true"), ("REST_TRUSTED_SENDERS", "a@d, b@d"),
                        ("REST_BROADCAST_MAX_RECIPIENTS", "50")):
        monkeypatch.setattr(os, "environ", {**os.environ, name: value})
    from app.config import apply_env_overrides
    out = apply_env_overrides(default_config())
    assert out["rest_api"]["allow_api_key_query"] is True
    assert out["rest_api"]["trusted_senders"] == ["a@d", "b@d"]
    assert out["rest_api"]["broadcast_max_recipients"] == 50
