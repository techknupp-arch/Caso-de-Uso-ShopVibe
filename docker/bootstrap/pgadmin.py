"""Generate pgAdmin's server definition and password file from Compose values."""
import json
import os
import sys
from pathlib import Path


def pgpass_escape(value):
    if "\n" in value or "\r" in value:
        raise ValueError("Credenciais com quebra de linha nao sao suportadas pelo pgpass")
    return value.replace("\\", "\\\\").replace(":", "\\:")


def main():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    server = {
        "Name": "ShopVibe PostgreSQL", "Group": "ShopVibe",
        "Host": "postgres", "Port": 5432, "Username": user,
        "MaintenanceDB": "shopvibe_db", "SSLMode": "prefer",
        "ConnectionParameters": {"sslmode": "prefer", "passfile": ".pgpass"},
    }
    Path(os.environ["PGADMIN_SERVER_JSON_FILE"]).write_text(
        json.dumps({"Servers": {"1": server}}), encoding="utf-8")
    line = f"postgres:5432:*:{pgpass_escape(user)}:{pgpass_escape(password)}\n"
    source = Path(os.environ["PGPASS_FILE"])
    source.write_text(line, encoding="utf-8")
    source.chmod(0o600)
    uid, gid = int(os.environ.get("PUID", "5050")), int(os.environ.get("PGID", "5050"))
    if os.geteuid() == 0:
        os.chown(source, uid, gid)

    # Refresh credentials even when the pgAdmin config DB already exists.
    email = os.environ["PGADMIN_DEFAULT_EMAIL"].replace("@", "_")
    storage = Path("/var/lib/pgadmin/storage") / email
    storage.mkdir(parents=True, exist_ok=True)
    target = storage / ".pgpass"
    target.write_text(line, encoding="utf-8")
    target.chmod(0o600)
    if os.geteuid() == 0:
        os.chown(storage, uid, gid)
        os.chown(target, uid, gid)
    os.execv("/entrypoint.sh", ["/entrypoint.sh", *sys.argv[1:]])


if __name__ == "__main__":
    main()
