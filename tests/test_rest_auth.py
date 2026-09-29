from rest_server import RESTServer


def test_api_key_required():
    config = {
        "rest_api": {
            "host": "127.0.0.1",
            "port": 8080,
            "endpoint": "/send_message",
            "api_key": "secret",
            "allow_get": False,
        }
    }
    server = RESTServer(config, message_handler=lambda *_: True)
    client = server.app.test_client()

    response = client.post(
        "/send_message",
        json={"to": "user@test", "message": "hi"},
    )
    assert response.status_code == 401

    response = client.post(
        "/send_message",
        json={"to": "user@test", "message": "hi"},
        headers={"X-API-Key": "secret"},
    )
    assert response.status_code == 200


def test_get_disabled_by_default():
    config = {
        "rest_api": {
            "host": "127.0.0.1",
            "port": 8080,
            "endpoint": "/send_message",
            "api_key": "",
            "allow_get": False,
        }
    }
    server = RESTServer(config, message_handler=lambda *_: True)
    client = server.app.test_client()
    response = client.get("/send_message?to=u&m=hi")
    assert response.status_code == 405

