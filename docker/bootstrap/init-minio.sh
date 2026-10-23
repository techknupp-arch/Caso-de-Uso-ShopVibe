#!/bin/sh
set -eu
attempt=0
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo 'MinIO indisponivel ou credenciais invalidas' >&2
        exit 1
    fi
    sleep 2
done
mc mb --ignore-existing local/bronze
mc version enable local/bronze
mc mb --ignore-existing local/silver
mc version enable local/silver
mc mb --ignore-existing local/gold
mc version enable local/gold
echo 'Buckets bronze, silver e gold prontos, com versionamento habilitado'
