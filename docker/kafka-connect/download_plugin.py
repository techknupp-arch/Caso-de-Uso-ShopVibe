"""Install a pinned official S3 plugin, verifying archive integrity."""
import hashlib
import io
import zipfile
from pathlib import Path
from urllib.request import urlopen

VERSION = "10.6.11"
SHA256 = "fb49ee1d6ca16bfbf8fbbfef38100f852a617b7c7ac7984529d92c2436a43190"
URL = ("https://hub-downloads.confluent.io/api/plugins/confluentinc/"
       f"kafka-connect-s3/versions/{VERSION}/confluentinc-kafka-connect-s3-{VERSION}.zip")

with urlopen(URL, timeout=120) as response:
    archive = response.read()
if hashlib.sha256(archive).hexdigest() != SHA256:
    raise RuntimeError("Checksum invalido no pacote S3")
root = Path("/plugins").resolve()
with zipfile.ZipFile(io.BytesIO(archive)) as package:
    for member in package.infolist():
        if not (root / member.filename).resolve().is_relative_to(root):
            raise RuntimeError("Caminho invalido no pacote S3")
    package.extractall(root)
print(f"S3 plugin {VERSION} instalado com checksum verificado")
