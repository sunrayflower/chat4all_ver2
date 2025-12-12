# file_client.py

import grpc
import requests
import uuid
import time
import os
import hashlib
from datetime import datetime

import chat4all_pb2
import chat4all_pb2_grpc

# --- Configurações Comuns ---
SERVER_ADDRESS = 'localhost:50051'
# Usamos o mesmo cliente ID e Secret que o server.py espera
CLIENT_ID = "admin_client"
CLIENT_SECRET = "super_secret_key"
CONVERSATION_ID = "test_file_conversation_123"

# --- Dados de Simulação de Arquivo ---
# Simulação de um arquivo de 1MB (1024 * 1024 bytes)
FILE_SIZE = 15728640 
# Definimos 2 partes (Multipart Upload)
PART_SIZE = FILE_SIZE // 2
FILENAME = "documento_teste.pdf"
NUM_PARTS = 2

# Simulação: Criar um buffer de dados e calcular o SHA256
FILE_DATA = b"A" * FILE_SIZE
CHECKSUM = hashlib.sha256(FILE_DATA).hexdigest()

# --- Funções Auxiliares ---

def get_auth_metadata(access_token):
    """Cria o cabeçalho de autenticação para as chamadas gRPC."""
    return [('authorization', f'Bearer {access_token}')]

def get_stubs(channel):
    """Retorna os stubs dos dois serviços (Chat e File)."""
    return (
        chat4all_pb2_grpc.ChatServiceStub(channel),
        chat4all_pb2_grpc.FileServiceStub(channel)
    )

# --- Funções de Teste gRPC/S3 ---

def step_1_authenticate(chat_stub):
    """Autentica e retorna o token de acesso."""
    print("--- 1. Autenticação (GetToken) ---")
    try:
        response = chat_stub.GetToken(
            chat4all_pb2.TokenRequest(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        )
        print(f"Token recebido com sucesso. Expira em: {response.expires_in} segundos.")
        return response.access_token
    except grpc.RpcError as e:
        print(f"Erro de Autenticação: {e.details()}")
        return None

def step_2_get_upload_urls(file_stub, access_token):
    """Chama o servidor gRPC para iniciar o upload multipart e obter URLs pré-assinadas."""
    print("\n--- 2. Iniciar Upload Multipart (GetUploadUrl) ---")
    metadata = get_auth_metadata(access_token)
    try:
        request = chat4all_pb2.GetUploadUrlRequest(
            filename=FILENAME,
            conversation_id=CONVERSATION_ID,
            file_size=FILE_SIZE,
            num_parts=NUM_PARTS
        )
        response = file_stub.GetUploadUrl(request, metadata=metadata)
        
        print(f"Upload iniciado. File ID: {response.file_id}")
        print(f"Sessão Upload ID: {response.upload_session_id}")
        print(f"Total de Partes: {len(response.parts)}")
        return response
        
    except grpc.RpcError as e:
        print(f"Erro ao obter URLs de upload: {e.details()}")
        return None

def step_3_simulate_multipart_upload(upload_response):
    """Simula o cliente enviando partes via HTTP diretamente para o MinIO."""
    print("\n--- 3. Simular Upload de Dados (HTTP Direto para MinIO) ---")
    
    uploaded_parts = []
    start_time = time.time()
    
    for i, part_info in enumerate(upload_response.parts):
        part_number = part_info.part_number
        url = part_info.presigned_url
        
        # 1. Calcular o chunk de dados para esta parte
        start = (part_number - 1) * PART_SIZE
        end = start + PART_SIZE
        data_chunk = FILE_DATA[start:end]
        
        # 2. Enviar a parte via PUT HTTP
        # O MinIO/S3 espera o ETag no cabeçalho após o upload bem-sucedido
        try:
            http_response = requests.put(url, data=data_chunk, timeout=10)
            http_response.raise_for_status()
            
            # O ETag é retornado no cabeçalho após o upload para cada parte.
            etag = http_response.headers['ETag'].strip('"') 
            
            uploaded_parts.append(chat4all_pb2.CompletedPart(
                part_number=part_number,
                part_etag=etag
            ))
            print(f"  -> Parte {part_number}/{NUM_PARTS} enviada com sucesso. ETag: {etag[:10]}...")
            
        except requests.exceptions.RequestException as e:
            print(f"ERRO FATAL ao enviar a Parte {part_number}: {e}")
            return None
            
    print(f"Upload simulado finalizado em {time.time() - start_time:.2f}s")
    return uploaded_parts

def step_4_complete_upload(file_stub, access_token, upload_response, uploaded_parts):
    """Chama o servidor gRPC para finalizar o upload multipart e registrar os metadados."""
    print("\n--- 4. Finalizar Upload (CompleteUpload) ---")
    metadata = get_auth_metadata(access_token)
    
    try:
        request = chat4all_pb2.CompleteUploadRequest(
            file_id=upload_response.file_id,
            upload_session_id=upload_response.upload_session_id,
            completed_parts=uploaded_parts,
            checksum_sha256=CHECKSUM, # Enviamos o checksum final para validação (simulada)
            conversation_id=CONVERSATION_ID, 
            file_size=FILE_SIZE
        )
        response = file_stub.CompleteUpload(request, metadata=metadata)
        
        if response.success:
            print(f"✅ SUCESSO: Arquivo {response.file_id} finalizado e metadados persistidos.")
        else:
            print(f"❌ FALHA: {response.message}")
            
        return response
        
    except grpc.RpcError as e:
        print(f"Erro ao finalizar upload: {e.details()}")
        return None
    
def step_5_send_message_with_attachment(chat_stub, access_token, file_metadata):
    """
    Simula o envio de uma mensagem cujo corpo é o arquivo (Payload type: 'file').
    O Worker e o ListMessages no servidor devem processar e retornar isso corretamente.
    """
    print("\n--- 5. Enviar Mensagem com Anexo (SendMessage) ---")
    metadata = get_auth_metadata(access_token)
    
    # 1. Cria a referência do anexo (usando os metadados do CompleteUpload)
    attachment_ref = chat4all_pb2.FileMetadata(file_id=file_metadata.file_id)
    
    # 2. Cria a requisição SendMessage (Type: file, Anexos: [referência])
    request = chat4all_pb2.SendMessageRequest(
        message_id=str(uuid.uuid4()),
        conversation_id=CONVERSATION_ID,
        from_user=CLIENT_ID,
        to=["user_B"],
        channels=["inbox"], # Canal interno, não vai para o Connector Mock
        payload=chat4all_pb2.MessagePayload(
            type="file",
            text="" # O texto é ignorado se o tipo é "file"
        ),
        file_attachments=[attachment_ref]
    )
    
    try:
        response = chat_stub.SendMessage(request, metadata=metadata)
        print(f"✅ SUCESSO: Mensagem de anexo '{request.message_id}' publicada no Kafka. Status: {response.status}")
        return request.message_id
        
    except grpc.RpcError as e:
        print(f"❌ FALHA ao enviar mensagem com anexo: {e.details()}")
        return None
        

def step_6_get_download_url(file_stub, access_token, file_id):
    """Chama o servidor gRPC para obter a URL de download temporária."""
    print("\n--- 6. Obter URL de Download (GetDownloadUrl) ---")
    metadata = get_auth_metadata(access_token)
    
    try:
        request = chat4all_pb2.GetDownloadUrlRequest(file_id=file_id)
        response = file_stub.GetDownloadUrl(request, metadata=metadata)
        
        print(f"URL temporária gerada ({response.filename}): {response.presigned_url[:80]}...")
        
        # Simular o download do arquivo
        download_test = requests.get(response.presigned_url, timeout=10)
        download_test.raise_for_status()
        
        # Verificar o tamanho
        if len(download_test.content) == FILE_SIZE:
             print(f"✅ SUCESSO: Download simulado funcionou. Tamanho verificado ({len(download_test.content)} bytes).")
        else:
             print("❌ FALHA: Tamanho do download simulado está incorreto.")
        
    except grpc.RpcError as e:
        print(f"Erro ao obter URL de download: {e.details()}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao tentar simular download HTTP: {e}")

def step_7_simulate_read_notification(chat_stub, access_token, message_id):
    """Simula o Webhook de leitura final da plataforma externa."""
    print("\n--- 7. Simular Notificação de Leitura (READ) ---")
    
    # O server.py tem que chamar o ReceiveNotification RPC
    # Para o teste funcionar, o connector_whatsapp.py DEVE estar rodando e fazendo isso automaticamente.
    
    print("Aguardando 5 segundos para o Connector Mock enviar RECEIVED e READ ao Server...")
    time.sleep(5)
    
    # Você precisaria de um RPC para VERIFICAR o status final, mas por agora confiamos nos logs.
    print("Verifique os logs do 'status_update_worker.py'. O status final deve ser 'READ'.")
    print("Verifique o MongoDB: db.messages.findOne({'_id': 'id_da_mensagem_enviada'}).status deve ser 5.")
    return True

# --- Função Principal ---

def run_file_test():
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        chat_stub, file_stub = get_stubs(channel)
        
        # 1. Autenticação
        access_token = step_1_authenticate(chat_stub)
        if not access_token: return

        # 2. Iniciar Upload (gRPC)
        upload_response = step_2_get_upload_urls(file_stub, access_token)
        if not upload_response: return

        # 3. Simular Envio HTTP Multipart (Direto para MinIO)
        uploaded_parts = step_3_simulate_multipart_upload(upload_response)
        if not uploaded_parts: return

        # 4. Finalizar Upload (gRPC) - Agora retorna os metadados completos
        complete_response = step_4_complete_upload(file_stub, access_token, upload_response, uploaded_parts)
        if not complete_response or not complete_response.success: return
        
        # O Metadado completo da nossa nova versão do server.py
        file_metadata_final = complete_response.file_metadata
        
        # 5. ENVIAR MENSAGEM COM O ANEXO
        sent_message_id = step_5_send_message_with_attachment(chat_stub, access_token, file_metadata_final)
        if not sent_message_id: return

        # 6. Obter URL de Download (gRPC + HTTP)
        # Usamos o File ID para testar o download, mas agora é o PASSO 6
        step_6_get_download_url(file_stub, access_token, file_metadata_final.file_id)

        # 7. SIMULAR STATUS FINAL (READ)
        step_7_simulate_read_notification(chat_stub, access_token, sent_message_id) 

if __name__ == '__main__':
    # Cria o arquivo de simulação na memória (FILE_DATA)
    print(f"Iniciando teste de upload resumível. Tamanho total: {FILE_SIZE / 1024 / 1024:.2f} MB")
    run_file_test()