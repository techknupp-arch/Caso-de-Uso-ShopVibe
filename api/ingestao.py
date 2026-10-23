import os
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega o ficheiro .env da raiz do projeto
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path=dotenv_path)

# Configurações de conexão com o MinIO via S3 (padrão do professor)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER") or "admin"
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin"
BUCKET_BRONZE = "shopvibe-bronze"

# Validação de segurança das credenciais
if not MINIO_USER or not MINIO_PASSWORD:
    raise ValueError("[ERRO] As credenciais do MinIO não foram encontradas no .env!")

# Inicializa o cliente S3 (boto3) apontando para o MinIO local
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASSWORD,
    region_name='us-east-1' # Região padrão exigida pelo boto3 para S3 local
)

def garantir_bucket():
    """Garante que o bucket da Camada Bronze existe; cria-o apenas se necessário."""
    try:
        response = s3.list_buckets()
        nomes = [bucket["Name"] for bucket in response.get("Buckets", [])]
        if BUCKET_BRONZE not in nomes:
            s3.create_bucket(Bucket=BUCKET_BRONZE)
            print(f"[MINIO] Bucket '{BUCKET_BRONZE}' criado com sucesso!")
    except ClientError as e:
        print(f"[ERRO] Falha ao verificar/criar o bucket: {e}")

def salvar_na_bronze(pedido):
    """Transforma o pedido recebido e persiste-o na Camada Bronze com particionamento por data."""
    garantir_bucket()
    
    agora = datetime.now()
    pedido["recebido_em"] = agora.strftime("%Y-%m-%dT%H:%M:%S")
    
    pedido_id = pedido.get("id_pedido", pedido.get("order_id", "unknown"))
    caminho = f"pedidos/data_ingestao={agora.strftime('%Y-%m-%d')}/pedido_{pedido_id}_{agora.strftime('%H%M%S')}.json"
    
    s3.put_object(
        Bucket=BUCKET_BRONZE,
        Key=caminho,
        Body=json.dumps(pedido, ensure_ascii=False).encode("utf-8")
    )
    print(f"[SUCESSO] Pedido {pedido_id} salvo na Bronze em: {caminho}")
