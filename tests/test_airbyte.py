import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipelines.airbyte import AirbyteClient, verify_bronze
from pipelines.samples import documents
from pipelines.silver import airbyte_documents, build_silver, validate_snapshot
from pipelines.storage import json_bytes, ingest_sources
from test_silver import MemoryS3


def native_record(doc):
    return json_bytes({"_airbyte_ab_id": "demo", "_airbyte_emitted_at": 1792497600000, "_airbyte_data": doc}) + b"\n"


class AirbyteTests(unittest.TestCase):
    def test_fixture_ingestion_cannot_bypass_airbyte(self):
        s3 = MemoryS3()
        with self.assertRaisesRegex(ValueError, "Airbyte"):
            ingest_sources(["bling"], s3)
        self.assertEqual(ingest_sources(s3=s3)["uploaded"], 5)
        self.assertFalse(any("/bling/" in key or "/melhor_envio/" in key for key in s3.data["bronze"]))

    def test_native_jsonl_and_gzip_are_read_into_silver_without_duplicate_events(self):
        s3 = MemoryS3()
        for index, doc in enumerate(documents()):
            s3.data["bronze"]["external/original-{}.json".format(index)] = json_bytes(doc)
            if doc["source_system"] in ("bling", "melhor_envio"):
                key = "airbyte/{}/exports.jsonl.gz".format(doc["source_system"])
                s3.data["bronze"][key] = gzip.compress(native_record(doc) * 2)
        report = verify_bronze(s3)
        self.assertTrue(report["passed"])
        result = build_silver(s3)
        self.assertEqual(result["tables"]["inventory"], 6)
        self.assertEqual(result["tables"]["shipments"], 2)
        self.assertEqual(result["tables"]["event_history"], 22)
        self.assertEqual(validate_snapshot(result["snapshot_id"], s3), {k: v for k, v in result.items() if k != "reused"})

    def test_invalid_or_empty_airbyte_objects_preserve_last_good_snapshot(self):
        s3 = MemoryS3()
        for index, doc in enumerate(documents()):
            s3.data["bronze"]["external/{}.json".format(index)] = json_bytes(doc)
        build_silver(s3)
        previous = s3.data["silver"]["LATEST.json"]
        for invalid in (b"\n", b'{"_airbyte_data":{}}', b'{"_airbyte_data":"invalid"}'):
            s3.data["bronze"]["airbyte/bling/bad.jsonl"] = invalid
            with self.assertRaises(ValueError):
                build_silver(s3)
            self.assertEqual(s3.data["silver"]["LATEST.json"], previous)

    def test_bronze_verification_requires_current_export_from_both_services(self):
        s3 = MemoryS3()
        doc = next(doc for doc in documents() if doc["source_system"] == "bling")
        s3.data["bronze"]["airbyte/bling/file.jsonl"] = native_record(doc)
        with self.assertRaisesRegex(ValueError, "melhor_envio"):
            verify_bronze(s3)
        doc["source_system"] = "unknown"
        with self.assertRaisesRegex(ValueError, "contrato"):
            list(airbyte_documents(native_record(doc), "airbyte/file.jsonl"))

    def test_airflow_retry_resumes_running_sync_and_failed_job_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "airbyte.json"
            path.write_text(json.dumps({"connections": {"bling": "connection-1"}}))
            api = AirbyteClient(path)
            with patch.object(api, "items", return_value=[{"jobId": 7, "status": "running"}]), \
                    patch.object(api, "request", return_value={"status": "succeeded", "rowsSynced": 1}) as request:
                self.assertEqual(api.sync("bling")["job_id"], 7)
                request.assert_called_once_with("/jobs/7")
            with patch.object(api, "request", return_value={"status": "failed"}):
                with self.assertRaisesRegex(RuntimeError, "failed"):
                    api.sync("bling", job_id=8)


if __name__ == "__main__":
    unittest.main()
