import grpc
import time

# Importa os módulos gerados
import chat4all_pb2
import chat4all_pb2_grpc

# Endereço do Servidor
SERVER_ADDRESS = 'localhost:50051'

# Variável global para armazenar o token
ACCESS_TOKEN = ""
CONVERSATION_ID = ""

def get_auth_metadata(access_token):
    """Cria os metadados de autenticação no formato exigido pelo gRPC."""
    return [('authorization', f'Bearer {access_token}')]

def run():
    global ACCESS_TOKEN, CONVERSATION_ID
    
    # 1. Conexão com o Servidor
    # Usamos `insecure_channel` pois não estamos usando TLS/SSL no exemplo
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        stub = chat4all_pb2_grpc.ChatServiceStub(channel)
        
        print("--- 1. Teste de Autenticação (GetToken) ---")
        try:
            # 6.1 Autenticação
            token_request = chat4all_pb2.TokenRequest(
                client_id="admin_client", 
                client_secret="super_secret_key"
            )
            response = stub.GetToken(token_request)
            
            ACCESS_TOKEN = response.access_token
            print(f"Token recebido com sucesso. Expira em: {response.expires_in} segundos.")
            print("-" * 40)
            
        except grpc.RpcError as e:
            print(f"Erro de Autenticação: {e.details()}")
            return

        # ----------------------------------------------------
        
        print("--- 2. Teste de Criação de Conversa (CreateConversation) ---")
        try:
            # 6.2 Criar Conversa
            metadata = get_auth_metadata(ACCESS_TOKEN)
            
            conversation_request = chat4all_pb2.ConversationCreateRequest(
                type=chat4all_pb2.ConversationCreateRequest.ConversationType.PRIVATE,
                members=["userA", "userB"],
                metadata={"topic": "Project Chat4All"}
            )
            
            response = stub.CreateConversation(conversation_request, metadata=metadata)
            
            CONVERSATION_ID = response.id
            print(f"Conversa criada: {CONVERSATION_ID}")
            print(f"Membros: {list(response.members)}")
            print("-" * 40)
            
        except grpc.RpcError as e:
            print(f"Erro ao criar conversa: {e.details()}")
            return

        # ----------------------------------------------------

        print("--- 3. Teste de Envio de Mensagem (SendMessage) ---")
        try:
            # 6.3 Enviar Mensagem
            metadata = get_auth_metadata(ACCESS_TOKEN)
            
            message_payload = chat4all_pb2.MessagePayload(
                            type="text", # <--- ADICIONE ESTA LINHA PARA DEFINIR O TIPO
                            text="Olá! Testando envio via gRPC."            
            )

            message_request = chat4all_pb2.SendMessageRequest(
                message_id=str(uuid.uuid4()),
                conversation_id=CONVERSATION_ID,
                from_user="userA",
                to=["userB"],
                channels=["sms", "whatsapp"],
                payload=message_payload
            )
            
            response = stub.SendMessage(message_request, metadata=metadata)
            print(f"Mensagem enviada. Status: {response.status}, ID: {response.message_id}")
            
            # Envia uma segunda mensagem
            message_request.message_id = str(uuid.uuid4())
            message_request.payload.text = "Esta é a segunda mensagem."
            stub.SendMessage(message_request, metadata=metadata)
            
            print("Segunda mensagem enviada.")
            print("Aguardando 2 segundos para o worker persistir no MongoDB...")
            time.sleep(2)
            print("-" * 40)
            
        except grpc.RpcError as e:
            print(f"Erro ao enviar mensagem: {e.details()}")
            return
            
        # ----------------------------------------------------

        print("--- 4. Teste de Listagem de Mensagens (ListMessages) ---")
        try:
            # 6.2 Listar Mensagens
            metadata = get_auth_metadata(ACCESS_TOKEN)
            
            list_request = chat4all_pb2.ListMessagesRequest(
                conversation_id=CONVERSATION_ID,
                since_timestamp=0 # 0 para listar todas
            )
            
            # O Server Streaming retorna um iterador
            response = stub.ListMessages(list_request, metadata=metadata)
            
            print(f"Mensagens na conversa {CONVERSATION_ID}:")
            count = 0
            for message in response: 
                count += 1
                
                # Conversão do timestamp para legibilidade
                # É importante usar datetime.fromtimestamp e lidar com milissegundos
                from datetime import datetime
                dt_object = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

                # <<< NOVO CÓDIGO AQUI: OBTENDO O NOME LEGÍVEL DO STATUS >>>
                status_name = chat4all_pb2.MessageStatus.Name(message.status)

                # <<< NOVO PRINT PARA INCLUIR O STATUS >>>
                print(f"  [{dt_object}] De: {message.message_data.from_user} -> Texto: {message.message_data.payload.text} (Status: {status_name})")

            if count == 0:
                print("  Nenhuma mensagem encontrada.")
            print("-" * 40)
            
        except grpc.RpcError as e:
            print(f"Erro ao listar mensagens: {e.details()}")


if __name__ == '__main__':
    import uuid # Importa aqui para simplificar
    run()