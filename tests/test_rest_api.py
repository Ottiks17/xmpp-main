"""REST API server for sending messages via XMPP with rate limiting."""

import threading
import time
from collections import defaultdict

from flask import Flask, request, jsonify


# ---------------------------------------------------------------------- #
#  Rate limiter
# ---------------------------------------------------------------------- #

class RateLimiter:
    """Simple in-memory rate limiter keyed by client IP."""

    def __init__(self, max_requests=100, window_seconds=60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits = defaultdict(list)
        self._lock = threading.Lock()

    def is_allowed(self, ip):
        """Return True if the request from `ip` is within the rate limit."""
        now = time.time()
        with self._lock:
            timestamps = self._hits[ip]
            cutoff = now - self.window_seconds
            # Drop timestamps outside the current window
            timestamps[:] = [t for t in timestamps if t > cutoff]

            if len(timestamps) >= self.max_requests:
                return False

            timestamps.append(now)
            return True


# ---------------------------------------------------------------------- #
#  REST server
# ---------------------------------------------------------------------- #

class RESTServer:
    """REST API server wrapping a Flask application."""

    def __init__(self, config, message_handler=None, log_callback=None):
        self.config = config or {}
        self.message_handler = message_handler
        self.log_callback = log_callback or (lambda msg, level: None)

        rest_cfg = self.config.get('rest_api', {}) or {}
        rate_cfg = rest_cfg.get('rate_limit', {}) or {}

        self.host = rest_cfg.get('host', '127.0.0.1')
        self.port = rest_cfg.get('port', 8080)
        self.endpoint = rest_cfg.get('endpoint', '/send_message')
        self.api_key = rest_cfg.get('api_key', '')
        self.allow_get = rest_cfg.get('allow_get', False)

        # Flask app + rate limiter as an app attribute
        # (tests access `client.application.rate_limiter`)
        self.app = Flask(__name__)
        self.app.rate_limiter = RateLimiter(
            max_requests=rate_cfg.get('max_requests', 100),
            window_seconds=rate_cfg.get('window_seconds', 60),
        )

        # Register routes and hooks
        self._register_security_headers()
        self._register_routes()

    # ------------------------------------------------------------------ #
    #  Internals
    # ------------------------------------------------------------------ #

    def _log(self, msg, level='INFO'):
        """Safely invoke the log callback."""
        try:
            self.log_callback(msg, level)
        except Exception:
            pass

    def _register_security_headers(self):
        @self.app.after_request
        def _add_security_headers(response):
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            response.headers['Content-Security-Policy'] = "default-src 'self'"
            return response

    def _register_routes(self):
        # -------------------------------------------------------------- #
        #  /health
        # -------------------------------------------------------------- #
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "running"}), 200

        # -------------------------------------------------------------- #
        #  /send_message
        # -------------------------------------------------------------- #
        @self.app.route(self.endpoint, methods=['POST'])
        def send_message():
            # 1. API key check ----------------------------------------- #
            provided_key = request.headers.get('X-API-Key')
            if not self.api_key or provided_key != self.api_key:
                return jsonify({"error": "Unauthorized"}), 401

            # 2. Rate limiting ----------------------------------------- #
            client_ip = request.remote_addr or 'unknown'
            if not self.app.rate_limiter.is_allowed(client_ip):
                return jsonify({"error": "Too many requests"}), 429

            # 3. Payload validation (BEFORE handler check) ------------- #
            data = request.get_json(silent=True) or {}
            if 'to' not in data or 'message' not in data:
                return jsonify(
                    {"error": "Missing required fields: 'to' and 'message'"}
                ), 400

            to_user = data['to']
            message = data['message']

            if not isinstance(message, str) or not message.strip():
                return jsonify({"error": "Message cannot be empty"}), 400

            if len(message) > 256:
                return jsonify(
                    {"error": "Message exceeds 256 characters"}
                ), 400

            # 4. Handler check ----------------------------------------- #
            if self.message_handler is None:
                self._log('[REST API] Message handler not configured', 'ERROR')
                return jsonify(
                    {"error": "Message handler not configured"}
                ), 500

            # 5. Send -------------------------------------------------- #
            try:
                ok = self.message_handler(to_user, message)
            except Exception as exc:
                self._log(f'[REST API] Handler error: {exc}', 'ERROR')
                return jsonify({"error": f"Handler error: {exc}"}), 500

            if not ok:
                return jsonify({"error": "Failed to send message"}), 502

            return jsonify({"status": "success"}), 200

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def set_message_handler(self, handler):
        """Assign (or replace) the message handler."""
        self.message_handler = handler

    def start(self):
        """Start the Flask server (blocking)."""
        self._log(
            f'[REST API] Listening on {self.host}:{self.port}{self.endpoint}'
        )
        self.app.run(host=self.host, port=self.port, threaded=True)

    def start_in_thread(self):
        """Start the Flask server in a daemon thread."""
        t = threading.Thread(target=self.start, daemon=True)
        t.start()
        return t


# ---------------------------------------------------------------------- #
#  Standalone entry point (optional)
# ---------------------------------------------------------------------- #

if __name__ == '__main__':
    import yaml  # optional dependency for standalone use

    with open('config.yaml', 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    def _demo_handler(to_user, message):
        print(f'>>> {to_user}: {message}')
        return True

    server = RESTServer(
        cfg,
        message_handler=_demo_handler,
        log_callback=lambda msg, level: print(f'[{level}] {msg}'),
    )
    server.start()
