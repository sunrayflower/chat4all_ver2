# status_update_worker.py

import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import chat4all_pb2
from datetime import datetime # Certifique-se de importar datetime

# --- Configurações Comuns ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_NOTIFICATIONS = 'notifications' 
KAFKA_GROUP_ID = 'chat4all_status_updater_v4' 

# --- Configurações MongoDB ---
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DATABASE = "chat4all_v2"
MONGO_COLLECTION = "messages"

# --- Inicialização MongoDB ---
def setup_mongodb():
    try:
        print("Status Update Worker: Conectando ao MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[MONGO_DATABASE]
        print("Status Update Worker: Conexão MongoDB OK.")
        return db
    except errors.ServerSelectionError as e:
        print(f"Status Update Worker: ERRO CRÍTICO no MongoDB: {e}")
        return None

# --- Lógica de Atualização de Status ---

def update_message_status(db, message_id, new_status, channel, audit_log):
    """Atualiza o status da mensagem e registra o log de auditoria no MongoDB."""
    
    collection = db[MONGO_COLLECTION]
    
    audit_entry = {
        "timestamp": int(time.time() * 1000),
        "event": audit_log,
        "channel": channel,
        "new_status": new_status,
        "worker": "status_update_worker"
    }
    
    try:
        collection.update_one(
            {"_id": message_id},
            {
                "$set": {"status": new_status}, 
                "$push": {"audit_trail": audit_entry}
            }
        )
        return True
    except errors.PyMongoError as e:
        print(f"  -> ERRO no MongoDB ao atualizar status de {message_id}: {e}")
        return False

def handle_notification(db, notification_pb):
    msg_id = notification_pb.message_id
    new_status_value = notification_pb.status
    channel = notification_pb.channel

    # Converte o valor ENUM para o objeto Protobuf (necessário para o status)
    status_enum = chat4all_pb2.MessageStatus.Name(new_status_value)
    
    log_msg = f"Status atualizado por notificação externa: {status_enum}"
    
    print("-" * 50)
    print(f"[Updater] Notificação recebida para {msg_id} - Novo Status: {status_enum}")
    
    if update_message_status(db, msg_id, new_status_value, channel, log_msg):
        print(f"  -> SUCESSO: Status {status_enum} persistido no MongoDB.")
    else:
        print(f"  -> FALHA: Não foi possível atualizar o status.")


def run_status_update_worker():
    db = setup_mongodb()
    if db is None: return

    try:
        print(f"Status Update Worker: Consumer pronto no tópico '{KAFKA_TOPIC_NOTIFICATIONS}' com Grupo: '{KAFKA_GROUP_ID}'...")
        consumer = KafkaConsumer(
            KAFKA_TOPIC_NOTIFICATIONS, 
            group_id=KAFKA_GROUP_ID, 
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=None
        )
        print(f"Status Update Worker: Kafka Consumer pronto no tópico '{KAFKA_TOPIC_NOTIFICATIONS}'...")
    except Exception as e:
        print(f"Status Update Worker: Erro ao inicializar o Kafka Consumer: {e}")
        return

    for message in consumer:
        try:
            notification_pb = chat4all_pb2.NotificationRequest()
            notification_pb.ParseFromString(message.value)
            
            handle_notification(db, notification_pb)
            
        except Exception as e:
            print(f"Status Update Worker: ERRO ao processar notificação: {e}")

if __name__ == '__main__':
    run_status_update_worker()