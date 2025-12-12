# router_worker.py

import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
from datetime import datetime
import chat4all_pb2
import metrics_setup

# --- Configurações ---
WORKER_NAME = "RouterWorker"
METRICS_PORT = 8001

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_MESSAGES = 'messages'
KAFKA_GROUP_ID = 'chat4all_router_group'

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"
MONGO_COLLECTION = "messages"

# --- Iniciar métricas ---
metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)


# ============================================================
# MongoDB
# ============================================================
def setup_mongodb():
    try:
        print("[Router] Conectando ao MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("[Router] MongoDB conectado.")
        return client[MONGO_DATABASE]
    except Exception as e:
        print("[Router] ERRO ao conectar ao Mongo:", e)
        return None


# ============================================================
# Atualização de status (função crítica)
# ============================================================
def update_message_status(db, message_id, new_status, audit_log):
    collection = db[MONGO_COLLECTION]

    MAX_RETRIES = 5

    for attempt in range(MAX_RETRIES):

        msg = collection.find_one({"_id": message_id})

        if msg:
            audit_entry = {
                "timestamp": int(time.time() * 1000),
                "event": audit_log,
                "new_status": new_status,
                "worker": "router_worker"
            }

            try:
                collection.update_one(
                    {"_id": message_id},
                    {
                        "$set": {"status": new_status},
                        "$push": {"audit_trail": audit_entry}
                    }
                )

                metrics_setup.MESSAGES_PROCESSED_TOTAL.labels(WORKER_NAME, 'DELIVERED').inc()
                return True

            except errors.PyMongoError as e:
                print(f"[Router] ERRO MONGO ao atualizar {message_id}: {e}")
                metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, 'mongo_error').inc()
                return False

        else:
            print(f"[Router] Mensagem {message_id} NÃO encontrada (tentativa {attempt+1}/{MAX_RETRIES})")
            time.sleep(0.4)

    print(f"[Router] FALHA: Mensagem {message_id} não existe após {MAX_RETRIES} tentativas.")
    metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, 'not_found').inc()
    return False


# ============================================================
# Worker principal
# ============================================================
def run_router_worker():
    db = setup_mongodb()

    if db is None:
        print("[Router] ERRO: MongoDB não disponível. Encerrando.")
        return

    # Kafka Consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_MESSAGES,
            group_id=KAFKA_GROUP_ID,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=None
        )
        print("[Router] Kafka Consumer iniciado. Aguardando mensagens...")
    except Exception as e:
        print("[Router] ERRO ao iniciar KafkaConsumer:", e)
        return

    # Loop principal
    for message in consumer:
        try:
            # Desserializar mensagem Protobuf
            message_pb = chat4all_pb2.SendMessageRequest()
            message_pb.ParseFromString(message.value)

            msg_id = message_pb.message_id

            print("-" * 60)
            print(f"[Router] Mensagem recebida: {msg_id}")

            # Simulação de roteamento
            time.sleep(0.5)

            # Status DELIVERED
            new_status = chat4all_pb2.MessageStatus.DELIVERED
            audit_msg = "Entrega simulada pelo Router Worker."

            ok = update_message_status(db, msg_id, new_status, audit_msg)

            if ok:
                print(f"[Router] Status atualizado para DELIVERED em {msg_id}")
            else:
                print(f"[Router] ERRO ao atualizar status de {msg_id}")

        except Exception as e:
            print("[Router] ERRO crítico ao processar mensagem:", e)
            metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, 'processing_error').inc()


if __name__ == "__main__":
    run_router_worker()
