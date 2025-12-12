from pydoc import doc
import grpc
import time
from concurrent import futures
import uuid
from kafka import KafkaProducer
from jose import jwt
from pymongo import MongoClient, errors
from datetime import datetime
import os
import boto3
from botocore.config import Config

import chat4all_pb2
import chat4all_pb2_grpc

import metrics_setup

# ----------------------------
# Metrics (port chosen to avoid conflicts)
# ----------------------------
WORKER_NAME = "gRPCServer"
METRICS_PORT = 8003  # altere se quiser outra porta
# Note: start_metrics_server called inside serve()

# ============================================================
# MINIO / S3 CONFIG
# ============================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_BUCKET = "chat4all-files"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadminpassword"

try:
    S3_CLIENT = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    try:
        S3_CLIENT.head_bucket(Bucket=MINIO_BUCKET)
        print(f"[MinIO] Bucket '{MINIO_BUCKET}' OK")
    except Exception:
        S3_CLIENT.create_bucket(Bucket=MINIO_BUCKET)
        print(f"[MinIO] Bucket '{MINIO_BUCKET}' criado")

except Exception as e:
    print("[MinIO] ERRO:", e)
    S3_CLIENT = None

# ============================================================
# MONGODB
# ============================================================
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "chat4all_v2"

try:
    MONGO_CLIENT = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    MONGO_CLIENT.admin.command("ping")
    MESSAGES_COLLECTION = MONGO_CLIENT[MONGO_DATABASE]["messages"]
    FILES_COLLECTION = MONGO_CLIENT[MONGO_DATABASE]["file_metadata"]
    print("[MongoDB] OK")
except Exception as e:
    print("[MongoDB] ERRO:", e)
    MESSAGES_COLLECTION = None
    FILES_COLLECTION = None

# ============================================================
# JWT
# ============================================================
SECRET_KEY = "sua-chave-secreta-estatica-e-segura"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 3600

CLIENT_CREDENTIALS = {"admin_client": "super_secret_key"}
CONVERSATIONS_DB = {}

def create_access_token(client_id):
    payload = {"sub": client_id, "exp": time.time() + ACCESS_TOKEN_EXPIRE_SECONDS}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_access_token(context, metadata):
    auth_header = [m.value for m in metadata if m.key == "authorization"]
    if not auth_header:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token ausente")
        return None

    try:
        token = auth_header[0].split("Bearer ")[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except Exception:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token inválido")
        return None

# ============================================================
# KAFKA PRODUCER & TOPICS
# ============================================================
KAFKA_TOPIC_MESSAGES = "messages"
KAFKA_TOPIC_NOTIFICATIONS = "notifications"

try:
    KAFKA_PRODUCER = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        retries=5,
        acks="all"
    )
    print("[Kafka] Producer OK")
except Exception as e:
    print("[Kafka] ERRO:", e)
    KAFKA_PRODUCER = None

# ============================================================
# CHAT SERVICE
# ============================================================
class ChatServiceServicer(chat4all_pb2_grpc.ChatServiceServicer):

    def _auth(self, context):
        return verify_access_token(context, context.invocation_metadata())

    def GetToken(self, request, context):
        # Debug logs to help with failing requests from gateway
        try:
            print(f"[GetToken] request.client_id = {repr(request.client_id)}")
            print(f"[GetToken] request.client_secret = {repr(request.client_secret)}")
        except Exception:
            pass

        if CLIENT_CREDENTIALS.get(request.client_id) != request.client_secret:
            print(f"[GetToken] Credenciais inválidas (expected={CLIENT_CREDENTIALS.get(request.client_id)})")
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Credenciais inválidas")

        token = create_access_token(request.client_id)
        return chat4all_pb2.TokenResponse(
            access_token=token,
            expires_in=ACCESS_TOKEN_EXPIRE_SECONDS
        )

    def CreateConversation(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        conv_id = str(uuid.uuid4())
        conv = chat4all_pb2.Conversation(
            id=conv_id,
            type=request.type,
            members=request.members,
            metadata=request.metadata
        )
        CONVERSATIONS_DB[conv_id] = conv
        print(f"[{client_id}] Conversa criada: {conv_id}")
        return conv
    

    def SendMessage(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        start_time = time.time()
        success_label = "SUCCESS"
        try:
            # Basic validation for payload (expect 'type' and possibly 'text')
            payload_type = getattr(request.payload, "type", "text")

            if payload_type == "file":
                if not request.file_attachments:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Tipo 'file' exige anexos.")
                    success_label = "VALIDATION_FAILED"
                    return
                for attachment in request.file_attachments:
                    file_doc = FILES_COLLECTION.find_one({"_id": attachment.file_id, "completed": True})
                    if not file_doc:
                        context.abort(grpc.StatusCode.NOT_FOUND, f"Arquivo {attachment.file_id} não encontrado ou upload incompleto.")
                        success_label = "VALIDATION_FAILED"
                        return
            elif payload_type == "text":
                if not getattr(request.payload, "text", ""):
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Mensagem tipo 'text' exige corpo de texto.")
                    success_label = "VALIDATION_FAILED"
                    return
            else:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Payload type inválido ('text' ou 'file').")
                success_label = "VALIDATION_FAILED"
                return

            if KAFKA_PRODUCER is None:
                context.abort(grpc.StatusCode.INTERNAL, "Kafka indisponível")
                success_label = "KAFKA_FAILED"
                return

            # publish to Kafka
            KAFKA_PRODUCER.send(
                KAFKA_TOPIC_MESSAGES,
                value=request.SerializeToString(),
                key=request.conversation_id.encode()
            ).get(timeout=5)

            # metrics success
            try:
                metrics_setup.MESSAGES_PROCESSED_TOTAL.labels(WORKER_NAME, success_label).inc()
            except Exception:
                # don't break RPC if metrics fail
                print("[Metrics] aviso: falha ao incrementar MESSAGES_PROCESSED_TOTAL")

            return chat4all_pb2.SendMessageResponse(
                status="accepted",
                message_id=request.message_id
            )

        except grpc.RpcError:
            # already aborted with context.abort, re-raise to propagate
            raise
        except Exception as e:
            print(f"[SendMessage] ERRO interno: {e}")
            try:
                metrics_setup.ERRORS_TOTAL.labels(WORKER_NAME, "gRPC_internal").inc()
            except Exception:
                pass
            context.abort(grpc.StatusCode.INTERNAL, "Erro interno no servidor.")
        finally:
            # Always observe latency
            latency = time.time() - start_time
            try:
                metrics_setup.LATENCY_SECONDS.labels(WORKER_NAME).observe(latency)
            except Exception:
                pass

    def ListMessages(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        if MESSAGES_COLLECTION is None:
            context.abort(grpc.StatusCode.INTERNAL, "MongoDB indisponível")

        query = {"conversation_id": request.conversation_id}
        if getattr(request, "since_timestamp", 0) and request.since_timestamp > 0:
            query["received_at"] = {"$gt": request.since_timestamp}

        try:
            cursor = MESSAGES_COLLECTION.find(query).sort("received_at", 1)
        except Exception as e:
            print(f"[ListMessages] ERRO ao consultar MongoDB: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Erro ao ler do banco de dados")

        for doc in cursor:
            # Normalize payload
            doc_payload = doc.get("payload", {})

            payload_type = "text"
            payload_text = ""

            if isinstance(doc_payload, dict):
                payload_type = doc_payload.get("type", "text")
                payload_text = doc_payload.get("text", "") or ""
            elif isinstance(doc_payload, str):
                payload_type = "text"
                payload_text = doc_payload
            else:
                try:
                    payload_text = str(doc_payload)
                except Exception:
                    payload_text = ""

            payload = chat4all_pb2.MessagePayload(
                type=payload_type,
                text=payload_text
            )

            attachments_pb = []
            for file_doc in doc.get("file_attachments", []):
                if isinstance(file_doc, dict):
                    fid = file_doc.get("file_id", "")
                else:
                    fid = str(file_doc)
                attachments_pb.append(chat4all_pb2.FileMetadata(file_id=fid))

            message_id = str(doc.get("_id"))
            msg_req = chat4all_pb2.SendMessageRequest(
                message_id=message_id,
                conversation_id=doc.get("conversation_id", ""),
                from_user=doc.get("from_user", ""),
                to=doc.get("to", []),
                channels=doc.get("channels", []),
                payload=payload,
                metadata=doc.get("metadata", {}),
                file_attachments=attachments_pb
            )

            msg = chat4all_pb2.Message(
                message_data=msg_req,
                timestamp=int(doc.get("received_at", 0)),
                status=int(doc.get("status", 0))
            )

            yield msg

    # ============================================================
    # ReceiveNotification — chamado pelos connectores (WhatsApp/Instagram)
    # ============================================================
    def ReceiveNotification(self, request, context):
        if MESSAGES_COLLECTION is None:
            context.abort(grpc.StatusCode.INTERNAL, "MongoDB indisponível")
            return chat4all_pb2.NotificationResponse(success=False, message="MongoDB indisponível")

        message_id = request.message_id
        new_status = request.status
        channel = request.channel.lower() if hasattr(request, "channel") else ""

        allowed = {
            chat4all_pb2.MessageStatus.RECEIVED,
            chat4all_pb2.MessageStatus.READ,
            chat4all_pb2.MessageStatus.FAILED,
            chat4all_pb2.MessageStatus.DELIVERED
        }

        if new_status not in allowed:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Status inválido para notificação.")
            return chat4all_pb2.NotificationResponse(success=False, message="Status inválido")

        msg_doc = MESSAGES_COLLECTION.find_one({"_id": message_id})
        if not msg_doc:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Mensagem {message_id} não encontrada")
            return chat4all_pb2.NotificationResponse(success=False, message="Mensagem não encontrada")

        # Canais da mensagem
        channels = [c.lower() for c in msg_doc.get("channels", [])]

        # ----------------------
        #   VALIDAÇÃO CORRETA
        # ----------------------
        if 'all' not in channels:
            if channel not in channels:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"Canal '{channel}' não corresponde à mensagem original"
                )
                return chat4all_pb2.NotificationResponse(success=False, message="Canal inválido")

        # ----------------------
        # Atualiza status no Mongo
        #------------------------
        try:
            now_ms = int(time.time() * 1000)
            audit_entry = {
                "status": int(new_status),
                "channel": channel,
                "timestamp": now_ms
            }

            MESSAGES_COLLECTION.update_one(
                {"_id": message_id},
                {
                    "$set": {"status": int(new_status), "status_updated_at": now_ms},
                    "$push": {"status_history": audit_entry}
                }
            )
        except Exception as e:
            print(f"[ReceiveNotification] ERRO ao atualizar Mongo: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Falha ao atualizar status no banco")
            return chat4all_pb2.NotificationResponse(success=False, message="Erro DB")

        # Kafka (opcional)
        if KAFKA_PRODUCER is not None:
            try:
                notif_bytes = request.SerializeToString()
                KAFKA_PRODUCER.send(
                    KAFKA_TOPIC_NOTIFICATIONS,
                    value=notif_bytes,
                    key=message_id.encode("utf-8")
                ).get(timeout=5)
            except Exception as e:
                print(f"[ReceiveNotification] WARN: falha ao publicar notificação no Kafka: {e}")

        return chat4all_pb2.NotificationResponse(success=True, message="Status atualizado")

# ============================================================
# FILE SERVICE (unchanged logic but kept tidy)
# ============================================================
class FileServiceServicer(chat4all_pb2_grpc.FileServiceServicer):

    def _auth(self, context):
        return verify_access_token(context, context.invocation_metadata())

    def GetUploadUrl(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        if S3_CLIENT is None:
            context.abort(grpc.StatusCode.INTERNAL, "MinIO indisponível")

        file_id = str(uuid.uuid4())
        ext = os.path.splitext(request.filename)[1]
        storage_key = f"{request.conversation_id}/{file_id}{ext}"

        try:
            mpu = S3_CLIENT.create_multipart_upload(
                Bucket=MINIO_BUCKET,
                Key=storage_key,
                ContentType=request.filename
            )
            upload_id = mpu["UploadId"]
        except Exception as e:
            print(f"[GetUploadUrl] ERRO MinIO: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Falha ao iniciar multipart upload")

        FILES_COLLECTION.insert_one({
            "_id": file_id,
            "conversation_id": request.conversation_id,
            "filename": request.filename,
            "mime_type": "application/octet-stream",
            "storage_key": storage_key,
            "storage_bucket": MINIO_BUCKET,
            "upload_session_id": upload_id,
            "size_bytes": request.file_size,
            "completed": False,
            "created_at": datetime.now()
        })

        parts = []
        for i in range(1, request.num_parts + 1):
            url = S3_CLIENT.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": MINIO_BUCKET,
                    "Key": storage_key,
                    "UploadId": upload_id,
                    "PartNumber": i
                },
                ExpiresIn=3600
            )
            parts.append(chat4all_pb2.UploadPartInfo(part_number=i, presigned_url=url))

        return chat4all_pb2.GetUploadUrlResponse(
            file_id=file_id,
            upload_session_id=upload_id,
            parts=parts
        )

    def CompleteUpload(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        file_doc = FILES_COLLECTION.find_one({"_id": request.file_id})
        if not file_doc:
            context.abort(grpc.StatusCode.NOT_FOUND, "Arquivo não encontrado")

        storage_key = file_doc["storage_key"]
        upload_id = file_doc["upload_session_id"]

        completed_parts = [
            {"ETag": p.part_etag, "PartNumber": p.part_number}
            for p in request.completed_parts
        ]

        try:
            S3_CLIENT.complete_multipart_upload(
                Bucket=MINIO_BUCKET,
                Key=storage_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": completed_parts}
            )
        except Exception as e:
            print(f"[CompleteUpload] ERRO MinIO: {e}")
            context.abort(grpc.StatusCode.INTERNAL, f"MinIO erro: {e}")

        FILES_COLLECTION.update_one(
            {"_id": request.file_id},
            {"$set": {
                "checksum_sha256": request.checksum_sha256,
                "size_bytes": request.file_size,
                "completed": True,
                "updated_at": datetime.now()
            }}
        )

        return chat4all_pb2.CompleteUploadResponse(
            file_id=request.file_id,
            success=True,
            message="Upload concluído",
            file_metadata=chat4all_pb2.FileMetadata(
                file_id=request.file_id,
                filename=file_doc["filename"],
                mime_type=file_doc["mime_type"],
                size_bytes=request.file_size,
                checksum_sha256=request.checksum_sha256,
                uploader_id=client_id,
                conversation_id=file_doc["conversation_id"],
                storage_bucket=file_doc["storage_bucket"],
                storage_key=file_doc["storage_key"]
            )
        )

    def GetDownloadUrl(self, request, context):
        client_id = self._auth(context)
        if not client_id:
            return

        file_doc = FILES_COLLECTION.find_one({"_id": request.file_id})
        if not file_doc:
            context.abort(grpc.StatusCode.NOT_FOUND, "Arquivo não encontrado")

        url = S3_CLIENT.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": file_doc["storage_bucket"],
                "Key": file_doc["storage_key"]
            },
            ExpiresIn=300
        )

        return chat4all_pb2.GetDownloadUrlResponse(
            presigned_url=url,
            filename=file_doc["filename"],
            mime_type=file_doc["mime_type"]
        )

# ============================================================
# SERVER bootstrap
# ============================================================
def serve():
    # start metrics once
    try:
        metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)
    except Exception as e:
        print(f"[Metrics] aviso: falha ao iniciar server de métricas: {e}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat4all_pb2_grpc.add_ChatServiceServicer_to_server(ChatServiceServicer(), server)
    chat4all_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)

    server.add_insecure_port("[::]:50051")
    server.start()
    print("\n[gRPC] Servidor iniciado na porta 50051...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
