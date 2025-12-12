# test_send_via_chat4all.py
import grpc
import uuid
import chat4all_pb2
import chat4all_pb2_grpc

# 1. Conectar e autenticar
channel = grpc.insecure_channel('localhost:50051')
stub = chat4all_pb2_grpc.ChatServiceStub(channel)

token_resp = stub.GetToken(
    chat4all_pb2.TokenRequest(
        client_id="admin_client",
        client_secret="super_secret_key"
    )
)

# 2. Enviar mensagem para user_giovanna via Telegram
metadata = [('authorization', f'Bearer {token_resp.access_token}')]

response = stub.SendMessage(
    chat4all_pb2.SendMessageRequest(
        message_id=str(uuid.uuid4()),
        conversation_id="test_telegram_conv",
        from_user="sistema",
        to=["user_giovanna"],  # ← Vai usar o mapeamento do MongoDB
        channels=["telegram"],   # ← Router vai rotear para telegram_channel
        payload=chat4all_pb2.MessagePayload(
            type="text",
            text="Teste do Chat4All! Mensagem chegando via Telegram Bot API"
        )
    ),
    metadata=metadata
)

print(f"Status: {response.status}")
print(f"Message ID: {response.message_id}")
