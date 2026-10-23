"""Restart failed tasks after dependency recovery, without resetting offsets."""
import os
import time
from urllib.error import URLError
from urllib.request import urlopen
from register_connector import request

base = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
minio = os.environ.get("MINIO_URL", "http://minio:9000").rstrip("/")
names = ("connector-shopvibe-cdc", "connector-minio-bronze")
while True:
    for name in names:
        try:
            status = request(base, f"/connectors/{name}/status")
            states = [status["connector"]["state"]] + [task["state"] for task in status["tasks"]]
            if "FAILED" not in states:
                continue
            if name == "connector-minio-bronze":
                with urlopen(minio + "/minio/health/ready", timeout=5):
                    pass
            request(base, f"/connectors/{name}/restart?includeTasks=true&onlyFailed=true", method="POST")
            print(f"Retomada solicitada: {name}; offsets preservados", flush=True)
        except (URLError, TimeoutError, OSError, ValueError, KeyError):
            print(f"Aguardando recuperacao de {name}", flush=True)
    time.sleep(10)
