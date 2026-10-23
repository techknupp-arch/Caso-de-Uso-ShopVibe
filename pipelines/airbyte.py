"""Airbyte public API orchestration. All source extraction runs inside Airbyte."""
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .storage import ROOT


class AirbyteClient:
    def __init__(self, config_path=None):
        self.path = Path(config_path or os.getenv("AIRBYTE_CONFIG_FILE", str(ROOT / ".runtime/airbyte.json")))
        if not self.path.exists():
            raise RuntimeError("Airbyte ainda nao configurado. Execute .\\start-shopvibe.ps1 primeiro.")
        self.config = json.loads(self.path.read_text(encoding="utf-8-sig"))
        self.base = os.getenv("AIRBYTE_URL", "http://host.docker.internal:8001").rstrip("/") + "/api/public/v1"
        self.token = None
        self.token_until = 0

    def request(self, path, body=None, method=None, authenticated=True, configuration=False):
        if authenticated and time.monotonic() >= self.token_until:
            auth = self.request("/applications/token", {
                "client_id": self.config["client_id"], "client_secret": self.config["client_secret"],
                "grant-type": "client_credentials"}, authenticated=False)
            self.token = auth["access_token"]
            self.token_until = time.monotonic() + int(auth.get("expires_in", 900)) - 30
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if authenticated:
            headers["Authorization"] = "Bearer " + self.token
        raw = None if body is None else json.dumps(body).encode("utf-8")
        base = self.base.replace("/api/public/v1", "/api/v1") if configuration else self.base
        req = Request(base + path, data=raw, headers=headers, method=method or ("GET" if raw is None else "POST"))
        try:
            with urlopen(req, timeout=300) as response:
                result = response.read()
                return json.loads(result) if result else {}
        except HTTPError as error:
            # The upstream body can contain configurations or credentials.
            raise RuntimeError("Airbyte HTTP {} em {}".format(error.code, path)) from None

    def items(self, path):
        result = self.request(path + ("&" if "?" in path else "?") + "limit=1000")
        return result.get("data", [])

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.config, indent=2) + "\n", encoding="utf-8")
        # The bootstrap runs as root; Airflow runs as uid 50000 in group 0.
        # Allow that shared container group to read without exposing secrets to others.
        os.chmod(temporary, 0o640)
        temporary.replace(self.path)

    def sync(self, provider, job_id=None, timeout=1800):
        connection_id = self.config["connections"][provider]
        if job_id is None:
            # Resume a still-active job after an Airflow retry instead of creating overlapping syncs.
            jobs = self.items("/jobs?connectionId=" + connection_id)
            active = [job for job in jobs if job.get("status") in ("pending", "running", "incomplete")]
            if active:
                job_id = max(active, key=lambda job: int(job["jobId"]))["jobId"]
            else:
                job_id = self.request("/jobs", {"connectionId": connection_id, "jobType": "sync"})["jobId"]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.request("/jobs/" + str(job_id))
            status = job["status"]
            if status == "succeeded":
                return {"provider": provider, "connection_id": connection_id, "job_id": job_id,
                        "status": status, "rows_synced": job.get("rowsSynced"), "bytes_synced": job.get("bytesSynced")}
            if status in ("failed", "cancelled", "canceled"):
                raise RuntimeError("Airbyte {} job {}: {}".format(provider, job_id, status))
            time.sleep(10)
        raise TimeoutError("Airbyte {} job {} ainda nao terminou; consulte a interface".format(provider, job_id))


def sync_provider(provider, **context):
    return AirbyteClient().sync(provider)


def verify_bronze(s3=None):
    from .silver import airbyte_documents
    from .storage import client, objects, digest, json_bytes
    s3 = s3 or client()
    expected = {}
    for provider, dataset in (("bling", "inventory"), ("melhor_envio", "shipments")):
        doc = json.loads((ROOT / "samples/services" / provider / (dataset + ".json")).read_text(encoding="utf-8-sig"))
        expected[provider] = digest(json_bytes(doc))
    found = set()
    keys = [key for key in objects(s3, "bronze", "airbyte/") if key.endswith((".jsonl", ".jsonl.gz"))]
    for key in keys:
        raw = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
        for doc in airbyte_documents(raw, key):
            provider = doc["source_system"]
            if digest(json_bytes(doc)) == expected[provider]:
                found.add(provider)
    missing = sorted(set(expected) - found)
    if missing:
        raise ValueError("Bronze Airbyte sem a versao atual dos exemplos: " + ", ".join(missing))
    return {"passed": True, "providers": sorted(found), "objects": len(keys), "prefix": "bronze/airbyte/"}
