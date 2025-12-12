# load_test.py - Simulação de Carga e Concorrência

import grpc
import time
import uuid
import threading
from datetime import datetime

# Importa os módulos gerados (necessário para o gRPC)
import chat4all_pb2
import chat4all_pb2_grpc

# --- CONFIGURAÇÕES ---
SERVER_ADDRESS = 'localhost:50051'
CLIENT_ID = "admin_client"
CLIENT_SECRET = "super_secret_key"

# Parâmetros do Teste de Carga
NUM_USERS = 10      # Número de usuários (threads) simulados
MESSAGES_PER_USER = 10 # Mensagens que cada usuário enviará


def get_stubs(channel):
    """Retorna o stub do ChatService."""
    return chat4all_pb2_grpc.ChatServiceStub(channel)

def get_auth_metadata(access_token):
    """Cria os metadados de autenticação."""
    return [('authorization', f'Bearer {access_token}')]

def authenticate(stub, user_id):
    """Obtém o token JWT."""
    try:
        response = stub.GetToken(
            chat4all_pb2.TokenRequest(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        )
        return response.access_token
    except grpc.RpcError as e:
        print(f"[{user_id}] ❌ ERRO: Falha na autenticação: {e.details()}")
        return None

def setup_conversation(stub, access_token, user_id):
    """Cria uma conversa para o usuário e retorna o ID."""
    try:
        metadata = get_auth_metadata(access_token)
        response = stub.CreateConversation(
            chat4all_pb2.ConversationCreateRequest(
                type=chat4all_pb2.ConversationCreateRequest.ConversationType.PRIVATE,
                members=[user_id, "load_test_target"],
                metadata={"test_type": "load_test"}
            ),
            metadata=metadata
        )
        return response.id
    except grpc.RpcError as e:
        # Se a conversa já existir (o que acontece em testes repetidos), é seguro ignorar.
        if "já existe" in str(e.details()):
             return str(uuid.uuid4()) # Retorna um ID mock para continuar o teste
        print(f"[{user_id}] ❌ ERRO: Falha ao criar conversa: {e.details()}")
        return None

# --- TAREFA PRINCIPAL DA THREAD (Simulação de Usuário) ---

def user_simulation_task(user_index):
    """Simula um único usuário (thread) enviando múltiplas mensagens."""
    
    user_id = f"User_{user_index:02d}" # User_01, User_02, etc.
    
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        stub = get_stubs(channel)
        
        # 1. Autenticação (cada thread se autentica)
        access_token = authenticate(stub, user_id)
        if not access_token: return
        
        # 2. Setup de Conversa
        conv_id = setup_conversation(stub, access_token, user_id)
        if not conv_id: return
        
        metadata = get_auth_metadata(access_token)
        start_time = time.time()
        
        # 3. Envio Concorrente de Mensagens
        print(f"[{user_id}] Iniciando envio de {MESSAGES_PER_USER} mensagens...")
        
        for i in range(MESSAGES_PER_USER):
            message_id = str(uuid.uuid4())
            message_text = f"Msg {i+1}/{MESSAGES_PER_USER} | Thread {user_index} @ {datetime.now().strftime('%H:%M:%S')}"
            
            message_request = chat4all_pb2.SendMessageRequest(
                message_id=message_id,
                conversation_id=conv_id,
                from_user=user_id,
                to=["user_ray"],
                channels=["all"],
                payload=chat4all_pb2.MessagePayload(type="text", text=message_text)
            )
            
            try:
                stub.SendMessage(message_request, metadata=metadata)
            except grpc.RpcError as e:
                print(f"[{user_id}] ERRO de Envio gRPC: {e.details()}")
                
        end_time = time.time()
        total_time = end_time - start_time
        print(f"[{user_id}] Concluído. Tempo total de envio: {total_time:.2f}s")
        return total_time

# --- FUNÇÃO PRINCIPAL DE CARGA ---

def run_load_test():
    
    threads = []
    total_messages = NUM_USERS * MESSAGES_PER_USER
    
    print("=" * 60)
    print(f"--- INICIANDO TESTE DE ESCALABILIDADE (gRPC & KAFKA) ---")
    print(f"Simulando: {NUM_USERS} usuários | Total de mensagens: {total_messages}")
    print("=" * 60)
    
    global_start_time = time.time()

    # Cria e inicia todas as threads
    for i in range(NUM_USERS):
        thread = threading.Thread(target=user_simulation_task, args=(i,))
        threads.append(thread)
        thread.start()
        
    # Espera que todas as threads terminem
    for thread in threads:
        thread.join()

    global_end_time = time.time()
    global_total_time = global_end_time - global_start_time

    print("\n" + "=" * 60)
    print(f"TESTE GLOBAL FINALIZADO")
    print(f"Tempo total de execução do cliente: {global_total_time:.2f} segundos.")
    print(f"Throughput aproximado: {total_messages / global_total_time:.2f} msgs/seg.")
    print("=" * 60)

if __name__ == '__main__':
    run_load_test()