"""Idempotent initial setup through the Airbyte public API; UI edits are preserved."""
import argparse
import json
import os
from pathlib import Path

from pipelines.airbyte import AirbyteClient


PROVIDERS = {"bling": "bling/inventory", "melhor_envio": "melhor-envio/shipments"}


def identity(item):
    for field in ("id", "definitionId", "sourceDefinitionId"):
        if item.get(field):
            return item[field]
    raise ValueError("Resposta Airbyte sem identificador de definicao")


def named(items, name):
    matches = [item for item in items if item.get("name") == name]
    if len(matches) > 1:
        raise RuntimeError("Mais de um recurso Airbyte com nome " + name)
    return matches[0] if matches else None


def configure(api):
    organization = api.request("/organizations/get", {"organizationId": "00000000-0000-0000-0000-000000000000"}, configuration=True)
    if not organization.get("email"):
        organization.update(email="admin@shopvibe.com", organizationName="ShopVibe")
        api.request("/organizations/update", organization, configuration=True)
    api.config["login_email"] = organization["email"]
    workspaces = api.items("/workspaces")
    workspace = next((item for item in workspaces if item["workspaceId"] == api.config.get("workspace_id")), None)
    if workspace is None:
        if len(workspaces) == 1:
            workspace = workspaces[0]
        else:
            workspace = named(workspaces, "ShopVibe") or api.request("/workspaces", {"name": "ShopVibe"})
    wid = workspace["workspaceId"]
    api.config["workspace_id"] = wid
    api.save()
    definition_path = "/workspaces/" + wid + "/definitions/declarative_sources"
    definitions = api.items(definition_path)
    definition = named(definitions, "ShopVibe - APIs simuladas")
    if definition is None:
        manifest = json.loads((Path(__file__).parent / "source-services.json").read_text())
        definition = api.request(definition_path, {"name": "ShopVibe - APIs simuladas", "manifest": manifest})
    definition_id = identity(definition)

    destinations = api.items("/destinations?workspaceIds=" + wid)
    destination = named(destinations, "ShopVibe Bronze MinIO")
    if destination is None:
        destination = api.request("/destinations", {
            "name": "ShopVibe Bronze MinIO", "workspaceId": wid,
            "configuration": {
                "destinationType": "s3", "s3_bucket_name": "bronze", "s3_bucket_path": "airbyte",
                "s3_bucket_region": "us-east-1",
                "s3_endpoint": os.getenv("AIRBYTE_MINIO_ENDPOINT", "http://host.docker.internal:9000"),
                "access_key_id": os.environ["MINIO_ROOT_USER"],
                "secret_access_key": os.environ["MINIO_ROOT_PASSWORD"],
                "format": {"format_type": "JSONL", "compression": {"compression_type": "No Compression"}},
            }})
    # S3 1.9.0/1.9.1 use virtual-host addressing incompatible with this MinIO endpoint.
    # Upgrade only affected versions; preserve newer versions selected in the UI.
    version = api.request("/actor_definition_versions/get_for_destination",
                          {"destinationId": destination["destinationId"]}, configuration=True)
    if version.get("dockerImageTag") in {"1.9.0", "1.9.1"}:
        api.request("/destination_definitions/update", {
            "destinationDefinitionId": destination["definitionId"],
            "dockerImageTag": "1.9.2", "workspaceId": wid,
        }, configuration=True)
    sources = api.items("/sources?workspaceIds=" + wid)
    connections = api.items("/connections?workspaceIds=" + wid)
    api.config.setdefault("connections", {})
    for provider, endpoint in PROVIDERS.items():
        source_name = "ShopVibe " + provider
        source = named(sources, source_name)
        if source is None:
            source = api.request("/sources", {"name": source_name, "workspaceId": wid,
                "definitionId": definition_id,
                "configuration": {"api_url": os.getenv("AIRBYTE_MOCK_URL", "http://host.docker.internal:8002/"),
                                  "resource": endpoint}})
        connection_name = provider + " -> Bronze (append)"
        connection = named(connections, connection_name)
        if connection is None:
            connection = api.request("/connections", {
                "name": connection_name, "sourceId": source["sourceId"],
                "destinationId": destination["destinationId"],
                "schedule": {"scheduleType": "manual"},
                "namespaceDefinition": "destination", "prefix": provider + "_",
                "configurations": {"streams": [{"name": "exports", "syncMode": "full_refresh_append"}]},
                "status": "active"})
        # Never use Airbyte's default overwrite mode for a historical Bronze layer.
        configured = api.request("/connections/" + connection["connectionId"])
        streams = configured.get("configurations", {}).get("streams", [])
        if not streams or any(stream.get("syncMode") != "full_refresh_append" for stream in streams):
            raise RuntimeError("Conexao {} deve usar Full refresh | Append para preservar a Bronze".format(provider))
        api.config["connections"][provider] = connection["connectionId"]
        api.save()
    print(json.dumps({"workspace_id": wid, "connections": api.config["connections"]}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["configure", "sync", "verify"], default="configure", nargs="?")
    args = parser.parse_args()
    if args.command == "configure":
        configure(AirbyteClient())
    elif args.command == "sync":
        api = AirbyteClient()
        for provider in PROVIDERS:
            print(json.dumps(api.sync(provider)), flush=True)
    else:
        from pipelines.airbyte import verify_bronze
        print(json.dumps(verify_bronze(), indent=2))
