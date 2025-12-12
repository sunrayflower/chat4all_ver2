from datetime import datetime
import time
import json
import os
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import chat4all_pb2  # Necessário para desserializar a mensagem Protobuf
import metrics_setup

WORKER_NAME = "Persister"
METRICS_PORT = 8000

metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)

# --- Configurações ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_MESSAGES = 'messages'
KAFKA_GROUP_ID = 'chat4all_persister_group'

# --- Configurações MongoDB ---
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"
MONGO_COLLECTION = "messages"

# ----------------------------------------------------
# 1. Conexão com MongoDB
# ----------------------------------------------------
def setup_mongodb():
    print("Conectando ao MongoDB...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[MONGO_DATABASE]
        print("Conexão com MongoDB bem-sucedida.")
        return db
    except errors.ServerSelectionError as e:
        print(f"Falha ao conectar ao MongoDB: {e}")
        return None

# ----------------------------------------------------
# 2. Função de Persistência
# ----------------------------------------------------
def persist_message(db, message_pb):
    start_time = time.time()  # para métricas

    file_attachments_list = [
        {"file_id": attachment.file_id}
        for attachment in message_pb.file_attachments
    ]

    # timestamp
    received_at_ms = int(time.time() * 1000)

    # documento MongoDB
    message_dict = {
        "_id": message_pb.message_id,
        "conversation_id": message_pb.conversation_id,
        "from_user": message_pb.from_user,
        "to": list(message_pb.to),
        "channels": list(message_pb.channels),
        "payload": {"text": message_pb.payload.text},
        "metadata": dict(message_pb.metadata),
        "file_attachments": file_attachments_list,
        "received_at": received_at_ms,
        "status": chat4all_pb2.MessageStatus.SENT
    }

    try:
        collection = db[MONGO_COLLECTION]
        collection.insert_one(message_dict)

        print(f"  -> Mensagem {message_pb.message_id} salva no MongoDB com status SENT.")

        metrics_setup.MESSAGES_PROCESSED_TOTAL.labels(WORKER_NAME, 'SENT').inc()
        return True

    except errors.PyMongoError as e:
        if hasattr(e, "code") and e.code == 11000:
            print(f"  -> AVISO: Mensagem {message_pb.message_id} já existe. Ignorando.")
            return True

        print(f"  -> ERRO CRÍTICO NO MONGODB para {message_pb.message_id}: {e}")
        metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, 'mongo_error').inc()
        return False

    finally:
        metrics_setup.LATENCY_SECONDS.labels(WORKER_NAME).observe(time.time() - start_time)

# ----------------------------------------------------
# 3. Consumidor Kafka (Worker Principal)
# ----------------------------------------------------
def run_worker():
    db = setup_mongodb()
    if db is None:
        print("Worker encerrado devido à falha na conexão com o MongoDB.")
        return

    print("Conectando ao Kafka Consumer...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_MESSAGES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=None
        )
        print("Kafka Consumer pronto. Aguardando mensagens...")
    except Exception as e:
        print(f"Falha ao conectar ao Kafka: {e}")
        return

    for message in consumer:
        try:
            message_pb = chat4all_pb2.SendMessageRequest()
            message_pb.ParseFromString(message.value)

            print(f"\n[Worker] Recebida: Mensagem ID {message_pb.message_id} da Partição {message.partition}")

            persist_success = persist_message(db, message_pb)

        except Exception as e:
            print(f"ERRO DE PROCESSAMENTO: Falha ao desserializar/persistir mensagem. {e}")

if __name__ == '__main__':
    run_worker()
