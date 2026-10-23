"""HTTP fixtures for the demonstration; these are not the providers' live APIs."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(os.environ.get("SAMPLES_ROOT", "/samples/services"))
ROUTES = {
    "/bling/inventory": ROOT / "bling/inventory.json",
    "/melhor-envio/shipments": ROOT / "melhor_envio/shipments.json",
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        route = urlsplit(self.path).path
        if route == "/health":
            self.reply(200, {"status": "healthy", "simulated": True})
        elif route in ROUTES:
            # Preserve the complete export, including provider references and extraction time.
            doc = json.loads(ROUTES[route].read_text(encoding="utf-8-sig"))
            self.reply(200, {"data": [doc]})
        else:
            self.reply(404, {"error": "Endpoint de simulacao inexistente"})

    def reply(self, status, value):
        raw = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8002), Handler).serve_forever()
