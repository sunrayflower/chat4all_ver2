# grpc_client_locust.py (O Wrapper que faz a chamada gRPC real)

import grpc
import uuid
import time
from datetime import datetime

# Importa os módulos gerados
import chat4all_pb2
import chat4all_pb2_grpc

# --- CONFIGURAÇÕES FIXAS ---
SERVER_ADDRESS = 'localhost:50051'
CLIENT_ID = "admin_client"
CLIENT_SECRET = "super_secret_key"

# Inicialização do Stub global (feito uma vez por processo)
# NOTA: Em Locust, o gRPC channel e stub devem ser gerenciados com cuidado para concorrência.
# Usaremos uma abordagem simples de inicialização no módulo.
try:
    CHANNEL = grpc.insecure_channel(SERVER_ADDRESS)
    CHAT_STUB = chat4all_pb2_grpc.ChatServiceStub(CHANNEL)
except Exception as e:
    print(f"Erro ao inicializar o canal gRPC: {e}")
    CHAT_STUB = None

# --- VARIÁVEIS DE ESTADO ---
GLOBAL_ACCESS_TOKEN = None
GLOBAL_CONV_ID = str(uuid.uuid4()) # ID de conversa estática para todos os testes

# --- FUNÇÕES CORE ---

def get_auth_metadata(access_token):
    return [('authorization', f'Bearer {access_token}')]

def setup_global_state():
    """Realiza autenticação e cria a conversa inicial (chamada única)."""
    global GLOBAL_ACCESS_TOKEN
    
    if GLOBAL_ACCESS_TOKEN is not None:
        return True # Já autenticado

    try:
        # 1. Autenticação
        token_request = chat4all_pb2.TokenRequest(
            client_id=CLIENT_ID, 
            client_secret=CLIENT_SECRET
        )
        response = CHAT_STUB.GetToken(token_request)
        GLOBAL_ACCESS_TOKEN = response.access_token
        
        # 2. Criação da Conversa
        # Usamos uma conversa estática para o teste de carga
        # No seu setup_conversation original, você pode fazer isso aqui.
        
        print("Estado Global gRPC configurado com sucesso.")
        return True
        
    except grpc.RpcError as e:
        print(f"ERRO CRÍTICO no setup gRPC: {e.details()}")
        return False
    except Exception as e:
        print(f"ERRO INESPERADO no setup gRPC: {e}")
        return False

# --- FUNÇÃO PRINCIPAL DE ENVIO ---

def send_message_request(user_id):
    """Executa a chamada SendMessage e lida com o erro RPC."""
    
    if CHAT_STUB is None or GLOBAL_ACCESS_TOKEN is None:
        return False, "Setup gRPC falhou."

    message_id = str(uuid.uuid4())
    metadata = get_auth_metadata(GLOBAL_ACCESS_TOKEN)
    
    message_request = chat4all_pb2.SendMessageRequest(
        message_id=message_id,
        conversation_id=GLOBAL_CONV_ID,
        from_user=user_id,
        to=["load_target"],
        channels=["whatsapp"],
        payload=chat4all_pb2.MessagePayload(
            type="text", 
            text=f"Load Test Msg @ {datetime.now().strftime('%H:%M:%S')}"
        )
    )
    
    try:
        # A chamada RPC síncrona
        CHAT_STUB.SendMessage(message_request, metadata=metadata)
        return True, None
    except grpc.RpcError as e:
        return False, e.details()
    except Exception as e:
        return False, str(e)

# Garante que o estado seja configurado quando o módulo é importado pelo Locust
setup_global_state()