# connector_telegram.py
import requests
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import chat4all_pb2
import grpc
import chat4all_pb2_grpc
import metrics_setup

# --- CONFIGURAÇÕES ---
TELEGRAM_BOT_TOKEN = "8029339373:AAFTZGHRZ5yMKXaUX4MpBC3K1FlOzaKmsp0"
WORKER_NAME = "ConnectorTelegram"
METRICS_PORT = 8005
KAFKA_TOPIC = "telegram_channel"
GROUP_ID = "connector_telegram_group"

# MongoDB
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"

# gRPC Server (para enviar notificações de status)
GRPC_SERVER = "localhost:50051"

def send_telegram_message(chat_id, text, file_url=None):
    """Envia mensagem de texto ou arquivo via Telegram Bot API"""
    try:
        if file_url:
            # Enviar arquivo
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
            response = requests.post(url, json={
                "chat_id": chat_id,
                "document": file_url,
                "caption": text or ""
            }, timeout=10)
        else:
            # Enviar texto
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            response = requests.post(url, json={
                "chat_id": chat_id,
                "text": text
            }, timeout=10)
        
        response.raise_for_status()
        result = response.json()
        
        if result.get("ok"):
            print(f"[Telegram] Mensagem enviada com sucesso para {chat_id}")
            return True, result.get("result", {}).get("message_id")
        else:
            print(f"[Telegram] ERRO API: {result}")
            return False, None
            
    except Exception as e:
        print(f"[Telegram] ERRO ao enviar: {e}")
        return False, None

def notify_status_to_server(message_id, status, channel="telegram"):
    """Notifica o servidor gRPC sobre mudança de status"""
    try:
        with grpc.insecure_channel(GRPC_SERVER) as channel_grpc:
            stub = chat4all_pb2_grpc.ChatServiceStub(channel_grpc)
            
            notification = chat4all_pb2.NotificationRequest(
                message_id=message_id,
                status=status,
                channel=channel
            )
            
            response = stub.ReceiveNotification(notification)
            return response.success
            
    except Exception as e:
        print(f"[Telegram] ERRO ao notificar servidor: {e}")
        return False

def get_telegram_chat_id(user_id, db):
    """Busca o chat_id do Telegram para o usuário interno"""
    try:
        user_map = db["user_channel_map"].find_one({
            "user_id_interno": user_id,
            "mappings.canal": "telegram"
        })
        
        if user_map:
            for mapping in user_map.get("mappings", []):
                if mapping.get("canal") == "telegram":
                    return mapping.get("id_externo")
        
        print(f"[Telegram] Usuário {user_id} não tem mapeamento Telegram")
        return None
        
    except Exception as e:
        print(f"[Telegram] ERRO ao buscar mapeamento: {e}")
        return None

def process_message(message_pb, db):
    """Processa mensagem do Kafka e envia via Telegram"""
    msg_id = message_pb.message_id
    
    print(f"\n{'='*60}")
    print(f"[Telegram] Processando mensagem {msg_id}")
    
    # Pegar destinatário
    if len(message_pb.to) == 0:
        print(f"[Telegram] Mensagem sem destinatário")
        return False
    
    target_user = message_pb.to[0]
    print(f"[Telegram] Destinatário: {target_user}")
    
    # Buscar chat_id do Telegram
    chat_id = get_telegram_chat_id(target_user, db)
    print(f"[Telegram] Chat ID: {chat_id}")
    
    if not chat_id:
        notify_status_to_server(msg_id, chat4all_pb2.MessageStatus.FAILED)
        return False
    
    # Preparar conteúdo
    payload_type = message_pb.payload.type
    text = message_pb.payload.text or ""
    
    print(f"[Telegram] Tipo de payload: {payload_type}")
    print(f"[Telegram] Texto: {text}")
    print(f"[Telegram] Número de anexos: {len(message_pb.file_attachments)}")
    
    # Verificar se tem arquivo anexado
    file_url = None
    if message_pb.file_attachments and len(message_pb.file_attachments) > 0:
        file_id = message_pb.file_attachments[0].file_id
        print(f"[Telegram] Arquivo anexado. File ID: {file_id}")
        
        # Buscar metadados do arquivo no MongoDB
        try:
            file_doc = db["file_metadata"].find_one({"_id": file_id})
            
            if file_doc and file_doc.get("completed"):
                # Gerar URL de download do MinIO
                storage_bucket = file_doc["storage_bucket"]
                storage_key = file_doc["storage_key"]
                
                # URL público do MinIO (ajuste conforme sua config)
                file_url = f"http://localhost:9000/{storage_bucket}/{storage_key}"
                
                print(f"[Telegram] URL do arquivo: {file_url}")
                
                # IMPORTANTE: O Telegram precisa de URL PÚBLICO
                # Se localhost não funcionar, você precisa:
                # 1. Baixar o arquivo do MinIO
                # 2. Enviar como bytes para o Telegram
                
            else:
                print(f"[Telegram] ⚠️ Arquivo {file_id} não encontrado ou incompleto")
                
        except Exception as e:
            print(f"[Telegram] Erro ao buscar arquivo: {e}")
    
    # Enviar via Telegram
    if file_url:
        # Enviar arquivo
        success, telegram_msg_id = send_telegram_file(chat_id, text, file_url, file_doc)
    else:
        # Enviar apenas texto
        success, telegram_msg_id = send_telegram_message(chat_id, text)
    
    if success:
        notify_status_to_server(msg_id, chat4all_pb2.MessageStatus.DELIVERED)
        time.sleep(1)
        notify_status_to_server(msg_id, chat4all_pb2.MessageStatus.READ)
        metrics_setup.MESSAGES_PROCESSED_TOTAL.labels(WORKER_NAME, "SUCCESS").inc()
        return True
    else:
        notify_status_to_server(msg_id, chat4all_pb2.MessageStatus.FAILED)
        metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, "telegram_api").inc()
        return False

def send_telegram_file(chat_id, caption, file_url, file_doc):
    """Envia arquivo via Telegram Bot API"""
    try:
        print(f"[Telegram] Tentando enviar arquivo...")
        
        # MÉTODO 1: Download do MinIO e envio como bytes (RECOMENDADO)
        # Porque o Telegram não consegue acessar localhost:9000
        
        import boto3
        from botocore.config import Config
        
        # Cliente S3/MinIO
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadminpassword",
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        
        # Baixar arquivo do MinIO
        storage_bucket = file_doc["storage_bucket"]
        storage_key = file_doc["storage_key"]
        filename = file_doc["filename"]
        
        print(f"[Telegram] Baixando de MinIO: {storage_bucket}/{storage_key}")
        
        response = s3_client.get_object(Bucket=storage_bucket, Key=storage_key)
        file_bytes = response['Body'].read()
        
        print(f"[Telegram] Arquivo baixado: {len(file_bytes)} bytes")
        
        # Enviar para Telegram usando multipart/form-data
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        
        files = {
            'document': (filename, file_bytes)
        }
        
        data = {
            'chat_id': chat_id,
            'caption': caption or f"📎 {filename}"
        }
        
        print(f"[Telegram] Enviando para API do Telegram...")
        
        http_response = requests.post(url, files=files, data=data, timeout=60)
        http_response.raise_for_status()
        
        result = http_response.json()
        
        if result.get("ok"):
            print(f"[Telegram]Arquivo enviado com sucesso!")
            return True, result.get("result", {}).get("message_id")
        else:
            print(f"[Telegram] API retornou erro: {result}")
            return False, None
            
    except Exception as e:
        print(f"[Telegram] Erro ao enviar arquivo: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def run_connector():
    """Loop principal do conector"""
    # MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    
    # Métricas
    metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)
    
    # Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=["localhost:9092"],
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    print(f"[{WORKER_NAME}] Aguardando mensagens no tópico {KAFKA_TOPIC}...")
    
    for kafka_msg in consumer:
        try:
            message_pb = chat4all_pb2.SendMessageRequest()
            message_pb.ParseFromString(kafka_msg.value)
            
            print(f"\n[Telegram] Processando mensagem {message_pb.message_id}")
            process_message(message_pb, db)
            
        except Exception as e:
            print(f"[Telegram] ERRO crítico: {e}")

if __name__ == "__main__":
    run_connector()
