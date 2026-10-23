"""Register CDC and Bronze sink after dependencies are ready."""
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request(base, path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = Request(base + path, data=data, method=method,
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as response:
        payload = response.read()
        return json.loads(payload) if payload else {}


def main():
    base = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
    for attempt in range(90):
        try:
            plugins = request(base, "/connector-plugins")
            break
        except (URLError, TimeoutError):
            if attempt == 89:
                raise RuntimeError("Kafka Connect nao ficou disponivel no prazo esperado") from None
            time.sleep(2)

    definitions = []
    for filename in ("connector-shopvibe-cdc.json", "connector-minio-bronze.json"):
        definition = json.loads((Path("/conectores") / filename).read_text())
        config = definition["config"]
        if config["connector.class"] not in {plugin["class"] for plugin in plugins}:
            raise RuntimeError(f"Plugin ausente: {config['connector.class']}")
        if filename == "connector-shopvibe-cdc.json":
            config["database.user"] = os.environ["POSTGRES_USER"]
            config["database.password"] = os.environ["POSTGRES_PASSWORD"]
        else:
            config["aws.access.key.id"] = os.environ["MINIO_ROOT_USER"]
            config["aws.secret.access.key"] = os.environ["MINIO_ROOT_PASSWORD"]
        definitions.append(definition)
    for definition in definitions:
        register(base, definition)


def register(base, definition):
    config = definition["config"]
    name = definition["name"]
    for attempt in range(60):
        try:
            request(base, f"/connectors/{name}/config", method="PUT", body=config)
            break
        except HTTPError as error:
            # Config validation can contain credentials: omit response bodies.
            if error.code not in (409, 502, 503, 504) or attempt == 59:
                raise RuntimeError(f"Registro de {name} recusado: HTTP {error.code}") from None
            time.sleep(2)
        except (URLError, TimeoutError):
            if attempt == 59:
                raise RuntimeError(f"Falha de conexao ao registrar {name}") from None
            time.sleep(2)

    for attempt in range(60):
        try:
            status = request(base, f"/connectors/{name}/status")
        except HTTPError as error:
            if error.code not in (404, 409, 502, 503, 504):
                raise RuntimeError(f"Consulta de {name} recusada: HTTP {error.code}") from None
            time.sleep(2)
            continue
        except (URLError, TimeoutError):
            time.sleep(2)
            continue
        states = [status["connector"]["state"]] + [task["state"] for task in status["tasks"]]
        if "FAILED" in states:
            raise RuntimeError(f"{name} falhou; consulte /connectors/{name}/status")
        if status["tasks"] and all(state == "RUNNING" for state in states):
            print(f"{name}: connector e tasks RUNNING", flush=True)
            return
        time.sleep(2)
    raise RuntimeError(f"{name} nao ficou RUNNING no prazo esperado")


if __name__ == "__main__":
    main()
