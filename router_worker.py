# router_worker.py

import time
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient, errors
from datetime import datetime
import chat4all_pb2
import metrics_setup

WORKER_NAME = "RouterWorker"
METRICS_PORT = 8001

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_MESSAGES = 'messages'
KAFKA_GROUP_ID = 'chat4all_router_group'

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"
MONGO_COLLECTION = "messages"
MONGO_USER_MAP_COLLECTION = "user_channel_map"  # <-- coleção usada para canal dinâmico

# Tópicos Kafka por canal
CHANNEL_TOPIC_MAP = {
    "whatsapp": "whatsapp_channel",
    "instagram": "instagram_channel",
}

# Kafka Producer
KAFKA_PRODUCER = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    retries=3,
    acks="all"
)
print("[RouterWorker] Kafka Producer OK")

# Métricas
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
        print("[Router] ERRO Mongo:", e)
        return None


# ============================================================
# Atualização de status
# ============================================================
def update_message_status(db, message_id, new_status, audit_text):
    collection = db[MONGO_COLLECTION]
    try:
        audit_entry = {
            "timestamp": int(time.time() * 1000),
            "event": audit_text,
            "new_status": new_status,
            "worker": WORKER_NAME
        }

        collection.update_one(
            {"_id": message_id},
            {"$set": {"status": new_status}, "$push": {"audit_trail": audit_entry}}
        )
        return True

    except errors.PyMongoError as e:
        print(f"[Router] ERRO MONGO ao atualizar {message_id}: {e}")
        return False


# ============================================================
# Worker Principal
# ============================================================
def run_router_worker():
    db = setup_mongodb()
    if db is None:
        print("[Router] ERRO: MongoDB indisponível.")
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
        print("[Router] Kafka Consumer iniciado.")
    except Exception as e:
        print("[Router] ERRO ao iniciar KafkaConsumer:", e)
        return

    # ---------------------------
    # LOOP PRINCIPAL
    # ---------------------------
    for message in consumer:
        try:
            # Desserializar protobuf
            message_pb = chat4all_pb2.SendMessageRequest()
            message_pb.ParseFromString(message.value)

            msg_id = message_pb.message_id
            print("--------------------------------------------------")
            print(f"[Router] Mensagem recebida: {msg_id}")

            # 1. Atualizar status
            update_message_status(db, msg_id, chat4all_pb2.MessageStatus.DELIVERED, "Roteamento iniciado")

            # 2. Identificar destinatário
            if len(message_pb.to) == 0:
                print("[Router] ERRO: mensagem sem destinatário.")
                continue

            target_user_id = message_pb.to[0]
            print(f"[Router] Destinatário: {target_user_id}")

            # 3. Consultar mapeamento no Mongo
            user_map_collection = db[MONGO_USER_MAP_COLLECTION]
            user_doc = user_map_collection.find_one({"user_id_interno": target_user_id})
            
            channels_to_send = []

            # 3.2. Verifica se o documento existe e se possui mapeamentos válidos
            if user_doc and user_doc.get('mappings'):
                
                # Itera sobre o array 'mappings' para extrair os nomes dos canais
                for mapping in user_doc['mappings']:
                    channel_name = mapping.get('canal', '').lower()
                    if channel_name:
                        channels_to_send.append(channel_name)

                print(f"[Router] Canais mapeados → {channels_to_send}")
            else:
                print(f"[Router] Nenhum mapeamento encontrado para {target_user_id}. Não será roteado.")
                # Se não houver mapeamento, a mensagem não deve prosseguir no roteamento
                continue

            # 4. Envio para tópicos
            for channel in channels_to_send:
                topic = CHANNEL_TOPIC_MAP.get(channel)

                if not topic:
                    print(f"[Router] Canal '{channel}' não existe no CHANNEL_TOPIC_MAP")
                    continue

                try:
                    KAFKA_PRODUCER.send(
                        topic,
                        value=message.value,
                        key=message_pb.conversation_id.encode()
                    ).get(timeout=2)

                    print(f"[RouterWorker] Roteado {msg_id} para tópico: {topic}")

                except Exception as e:
                    print(f"[RouterWorker] ERRO ao rotear para '{channel}': {e}")

        except Exception as e:
            print("[Router] ERRO crítico ao processar mensagem:", e)


if __name__ == "__main__":
    run_router_worker()
