# connector_base.py

import time
import grpc
from kafka import KafkaConsumer
from pymongo import MongoClient
import chat4all_pb2
import chat4all_pb2_grpc

import metrics_setup   # << ADICIONAMOS

KAFKA_TOPIC_MESSAGES = "messages"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"


# ------------------------------------------------------------
# MongoDB
# ------------------------------------------------------------
def setup_mongodb():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client[MONGO_DATABASE]
    except Exception:
        return None


# ------------------------------------------------------------
# Envio de callback gRPC → ReceiveNotification
# ------------------------------------------------------------
def send_status_update_grpc(message_id, status_enum, channel, worker_name):
    # Não vamos medir o tempo aqui, pois é medido na função chamadora (run_connector).
    
    try:
        with grpc.insecure_channel("localhost:50051") as ch:
            stub = chat4all_pb2_grpc.ChatServiceStub(ch)

            status_name = chat4all_pb2.MessageStatus.Name(status_enum)

            req = chat4all_pb2.NotificationRequest(
                message_id=message_id,
                status=status_enum,
                channel=channel
            )

            res = stub.ReceiveNotification(req)

            # --- REGISTRO DE SUCESSO DO CALLBACK (NOVO) ---
            metrics_setup.MESSAGES_PROCESSED_TOTAL.labels(
                worker_name, f'CALLBACK_{status_name}_OK'
            ).inc()
            # ---------------------------------------------
            
            return res.success

    except Exception as e:
        print(f"[{channel}] ERRO no callback gRPC: {e}")
        # Métrica de erro
        metrics_setup.ERRORS_TOTAL.labels(worker_name, "grpc_callback_error").inc()

        return False

# ------------------------------------------------------------
# Função principal do conector
# ------------------------------------------------------------
def run_connector(channel_name, group_id, kafka_topic_consume, db, worker_name):

    # Kafka Consumer
    try:
        print(f"[{channel_name}] 🔍 Tentando consumir do Tópico: '{kafka_topic_consume}' com Grupo: '{group_id}'")
        consumer = KafkaConsumer(
            kafka_topic_consume,
            group_id=group_id,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=None
        )
        print(f"[{channel_name}] OK — Consumindo mensagens…")

    except Exception as e:
        print(f"[{channel_name}] ERRO Kafka: {e}")
        metrics_setup.ERRORS_TOTAL.labels(worker_name, "kafka_connection").inc()
        return

    # Loop principal
    for message in consumer:

        start_time = time.time()   # LATÊNCIA

        try:
            pb = chat4all_pb2.SendMessageRequest()
            pb.ParseFromString(message.value)
        
            print("-" * 50)
            print(f"[{channel_name}] Recebida {pb.message_id}")

            # Simula envio ao usuário
            time.sleep(0.5)
            print(f"[{channel_name}] Entregue ao usuário {pb.to[0]}")

            # -----------------------------
            # 1. RECEIVED
            # -----------------------------
            if send_status_update_grpc(
                pb.message_id,
                chat4all_pb2.MessageStatus.RECEIVED,
                channel_name,
                worker_name
            ):
                print(f"[{channel_name}] Callback RECEIVED enviado")

            # -----------------------------
            # 2. READ
            # -----------------------------
            time.sleep(1.5)

            if send_status_update_grpc(
                pb.message_id,
                chat4all_pb2.MessageStatus.READ,
                channel_name,
                worker_name
            ):
                print(f"[{channel_name}] Callback READ enviado")

        except Exception as e:
            print(f"[{channel_name}] ERRO processando msg: {e}")
            metrics_setup.ERRORS_TOTAL.labels(worker_name, "processing_error").inc()

        finally:
            # LATÊNCIA TOTAL DO PROCESSAMENTO DA MENSAGEM (CORRETO)
            metrics_setup.LATENCY_SECONDS.labels(worker_name).observe(
                time.time() - start_time
            )