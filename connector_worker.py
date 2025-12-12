# connector_worker.py (Simulação de Envio para APIs Externas)

import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import chat4all_pb2

# --- Configurações Comuns ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_MESSAGES = 'messages'
# Grupo exclusivo para este Worker
KAFKA_GROUP_ID = 'chat4all_connector_group' 

# --- Configurações MongoDB ---
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DATABASE = "chat4all_v2"
MONGO_COLLECTION = "messages"

# --- Inicialização MongoDB ---
def setup_mongodb():
    # ... (mesmo código de setup do worker.py) ...
    try:
        print("Connector Worker: Conectando ao MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[MONGO_DATABASE]
        print("Connector Worker: Conexão MongoDB OK.")
        return db
    except errors.ServerSelectionError as e:
        print(f"Connector Worker: ERRO CRÍTICO no MongoDB: {e}")
        return None

# --- Lógica de Simulação de Envio e Log de Auditoria ---

def update_message_status(db, message_id, new_status, channel, audit_log):
    """Atualiza o status da mensagem e registra o log de auditoria no MongoDB."""
    
    collection = db[MONGO_COLLECTION]
    
    audit_entry = {
        "timestamp": int(time.time() * 1000),
        "event": audit_log,
        "channel": channel,
        "new_status": new_status,
        "worker": "connector_worker"
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

def handle_message(db, message_pb):
    msg_id = message_pb.message_id
    
    # Simulação: Itera sobre todos os canais de destino e simula o envio
    for channel in message_pb.channels:
        print("-" * 50)
        print(f"[Connector] Roteando {msg_id} para o canal: {channel}")
        
        # 1. Simulação da API Externa
        print(f"  -> SIMULANDO: Chamando API de envio para {channel}...")
        time.sleep(0.3) # Simula latência de chamada de API
        
        # 2. Roteamento de Regras (Mock Simples)
        if "sms" in channel.lower():
            log_msg = f"Mensagem enviada via Connector Mock (SMS) - Sucesso"
            new_status = chat4all_pb2.MessageStatus.SENT_TO_EXTERNAL # Status final para este worker
        elif "whatsapp" in channel.lower():
            log_msg = f"Mensagem enviada via Connector Mock (WhatsApp) - Sucesso"
            new_status = chat4all_pb2.MessageStatus.SENT_TO_EXTERNAL
        else:
            print(f"  -> AVISO: Canal {channel} não reconhecido. Ignorando.")
            continue

        # 3. Atualiza o Status no MongoDB
        if update_message_status(db, msg_id, new_status, channel, log_msg):
             print(f"  -> SUCESSO: Status {new_status} persistido para {channel}.")
        else:
             print(f"  -> FALHA: Não foi possível atualizar o status no MongoDB.")


def run_connector_worker():
    db = setup_mongodb()
    if db is None: return

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_MESSAGES,
            group_id=KAFKA_GROUP_ID, 
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest', # Começa a partir de novas mensagens
            enable_auto_commit=True,
            value_deserializer=None
        )
        print("Connector Worker: Kafka Consumer pronto. Aguardando mensagens...")
    except Exception as e:
        print(f"Connector Worker: Erro ao inicializar o Kafka Consumer: {e}")
        return

    for message in consumer:
        try:
            message_pb = chat4all_pb2.SendMessageRequest()
            message_pb.ParseFromString(message.value)
            
            handle_message(db, message_pb)
            
        except Exception as e:
            print(f"Connector Worker: ERRO ao processar mensagem: {e}")

if __name__ == '__main__':
    run_connector_worker()

