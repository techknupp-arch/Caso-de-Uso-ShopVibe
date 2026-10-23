import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
from ingestao import salvar_na_bronze, garantir_bucket

# Carrega o ficheiro .env da raiz do projeto
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path=dotenv_path)

# Configurações do Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "shopvibe.orders.raw"

def iniciar_consumidor():
    """Inicia o consumo contínuo de eventos do Kafka em tempo real."""
    print("[KAFKA] A garantir a existência do bucket antes de iniciar...")
    garantir_bucket()
    
    print(f"[KAFKA] A conectar ao broker em {KAFKA_BROKER} (Tópico: {KAFKA_TOPIC})...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='shopvibe-bronze-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("[KAFKA] Consumidor ativo e à escuta de eventos de checkout...\n")
    
    for message in consumer:
        pedido_recebido = message.value
        # Chama a função de ingestão importada do ingestao.py
        salvar_na_bronze(pedido_recebido)
        # Acknowledge only after the object was successfully written to MinIO.
        consumer.commit()

if __name__ == "__main__":
    iniciar_consumidor()
