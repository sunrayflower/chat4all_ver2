# chat_terminal_files.py
import grpc
import uuid
import os
import hashlib
import requests
from pathlib import Path
import chat4all_pb2
import chat4all_pb2_grpc

# --- Configurações ---
SERVER_ADDRESS = 'localhost:50051'
CLIENT_ID = "admin_client"
CLIENT_SECRET = "super_secret_key"
PART_SIZE = 5 * 1024 * 1024  # 5MB por parte

def autenticar():
    """Autentica e retorna stubs e token"""
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    chat_stub = chat4all_pb2_grpc.ChatServiceStub(channel)
    file_stub = chat4all_pb2_grpc.FileServiceStub(channel)
    
    response = chat_stub.GetToken(
        chat4all_pb2.TokenRequest(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
    )
    
    print("Autenticado com sucesso!\n")
    return channel, chat_stub, file_stub, response.access_token

def calcular_sha256(filepath):
    """Calcula o checksum SHA256 do arquivo"""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(65536)  # 64KB chunks
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def upload_arquivo(file_stub, token, filepath, conversation_id):
    """Faz upload de um arquivo para o servidor"""
    
    if not os.path.exists(filepath):
        print(f"Arquivo não encontrado: {filepath}")
        return None
    
    file_size = os.path.getsize(filepath)
    filename = os.path.basename(filepath)
    
    # Calcular número de partes
    num_parts = (file_size + PART_SIZE - 1) // PART_SIZE
    
    print(f"\nIniciando upload de '{filename}'")
    print(f"   Tamanho: {file_size / 1024 / 1024:.2f} MB")
    print(f"   Partes: {num_parts}")
    
    metadata = [('authorization', f'Bearer {token}')]
    
    # 1. Iniciar upload multipart
    try:
        upload_response = file_stub.GetUploadUrl(
            chat4all_pb2.GetUploadUrlRequest(
                filename=filename,
                conversation_id=conversation_id,
                file_size=file_size,
                num_parts=num_parts
            ),
            metadata=metadata
        )
        
        file_id = upload_response.file_id
        print(f"   File ID: {file_id}")
        
    except grpc.RpcError as e:
        print(f"Erro ao iniciar upload: {e.details()}")
        return None
    
    # 2. Upload das partes via HTTP
    uploaded_parts = []
    
    with open(filepath, 'rb') as f:
        for i, part_info in enumerate(upload_response.parts, 1):
            print(f"   Enviando parte {i}/{num_parts}...", end=' ')
            
            # Ler chunk do arquivo
            chunk_data = f.read(PART_SIZE)
            
            try:
                # Upload via PUT HTTP
                response = requests.put(
                    part_info.presigned_url,
                    data=chunk_data,
                    timeout=30
                )
                response.raise_for_status()
                
                # Pegar ETag
                etag = response.headers['ETag'].strip('"')
                
                uploaded_parts.append(
                    chat4all_pb2.CompletedPart(
                        part_number=part_info.part_number,
                        part_etag=etag
                    )
                )
                
                print("Concluido!")
                
            except Exception as e:
                print(f"Erro: {e}")
                return None
    
    # 3. Calcular checksum
    print(f"   Calculando checksum...", end=' ')
    checksum = calcular_sha256(filepath)
    
    # 4. Finalizar upload
    print(f"   Finalizando upload...", end=' ')
    
    try:
        complete_response = file_stub.CompleteUpload(
            chat4all_pb2.CompleteUploadRequest(
                file_id=file_id,
                upload_session_id=upload_response.upload_session_id,
                completed_parts=uploaded_parts,
                checksum_sha256=checksum,
                conversation_id=conversation_id,
                file_size=file_size
            ),
            metadata=metadata
        )
        
        if complete_response.success:
            print(f"\nUpload concluído! File ID: {file_id}\n")
            return file_id
        else:
            print(f"Falha: {complete_response.message}")
            return None
            
    except grpc.RpcError as e:
        print(f"Erro ao finalizar: {e.details()}")
        return None

def enviar_mensagem(chat_stub, token, destinatario, canal, texto, file_id=None):
    """Envia mensagem de texto ou com arquivo anexado"""
    metadata = [('authorization', f'Bearer {token}')]
    message_id = str(uuid.uuid4())
    
    # Preparar anexos
    attachments = []
    if file_id:
        attachments.append(chat4all_pb2.FileMetadata(file_id=file_id))
    
    # Determinar tipo de payload
    if file_id:
        payload = chat4all_pb2.MessagePayload(type="file", text=texto or "")
    else:
        payload = chat4all_pb2.MessagePayload(type="text", text=texto)
    
    try:
        response = chat_stub.SendMessage(
            chat4all_pb2.SendMessageRequest(
                message_id=message_id,
                conversation_id="terminal_chat",
                from_user="usuario_terminal",
                to=[destinatario],
                channels=[canal],
                payload=payload,
                file_attachments=attachments
            ),
            metadata=metadata
        )
        
        print(f"Mensagem enviada! ID: {message_id}")
        print(f"   Status: {response.status}\n")
        return True
        
    except grpc.RpcError as e:
        print(f"Erro ao enviar: {e.details()}\n")
        return False

def main():
    print("=" * 60)
    print("CHAT4ALL")
    print("=" * 60)
    
    # Autenticar
    try:
        channel, chat_stub, file_stub, token = autenticar()
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return
    
    # Escolher destinatário
    destinatarios = {
        "1": "user_giovanna",
        "2": "user_ray",
        "3": "user_fabo",
    }
    
    print("DESTINATÁRIOS:")
    for key, user in destinatarios.items():
        print(f"   {key}. {user}")
    
    dest_escolha = input("\nEscolha o destinatário: ").strip()
    destinatario = destinatarios.get(dest_escolha)
    
    if not destinatario:
        print("Destinatário inválido!")
        return
    
    # Escolher canal
    canais = {
        "1": "telegram",
        "2": "whatsapp",
        "3": "instagram",
    }
    
    print(f"\nCANAIS:")
    for key, canal in canais.items():
        print(f"   {key}. {canal}")
    
    canal_escolha = input("\nEscolha o canal: ").strip()
    canal = canais.get(canal_escolha)
    
    if not canal:
        print("Canal inválido!")
        return
    
    print(f"\nEnviando para: {destinatario} via {canal}")
    
    # Loop de mensagens
    print("COMANDOS:")
    print("   - Digite texto para enviar mensagem")
    print("   - /file <caminho> - Enviar arquivo")
    print("   - /attach <caminho> <mensagem> - Arquivo com mensagem")
    print("   - sair - Encerrar")
    
    while True:
        try:
            comando = input(f"Você → {destinatario}: ").strip()
            
            if comando.lower() in ['sair', 'exit', 'quit']:
                print("\nBye!")
                break
            
            if not comando:
                continue
            
            # Comando: enviar arquivo
            if comando.startswith('/file '):
                filepath = comando[6:].strip().strip('"').strip("'")
                
                # Upload do arquivo
                file_id = upload_arquivo(
                    file_stub, token, filepath, "terminal_chat"
                )
                
                if file_id:
                    # Enviar mensagem com arquivo
                    enviar_mensagem(
                        chat_stub, token, destinatario, canal,
                        f"📎 {os.path.basename(filepath)}",
                        file_id=file_id
                    )
            
            # Comando: arquivo com mensagem
            elif comando.startswith('/attach '):
                partes = comando[8:].strip().split(' ', 1)
                
                if len(partes) < 2:
                    print("Uso: /attach <arquivo> <mensagem>")
                    continue
                
                filepath = partes[0].strip('"').strip("'")
                mensagem = partes[1]
                
                # Upload do arquivo
                file_id = upload_arquivo(
                    file_stub, token, filepath, "terminal_chat"
                )
                
                if file_id:
                    # Enviar com mensagem personalizada
                    enviar_mensagem(
                        chat_stub, token, destinatario, canal,
                        mensagem, file_id=file_id
                    )
            
            # Mensagem de texto simples
            else:
                enviar_mensagem(
                    chat_stub, token, destinatario, canal, comando
                )
            
        except KeyboardInterrupt:
            print("\n\nInterrompido. Até!")
            break
        except Exception as e:
            print(f"Erro: {e}\n")
    
    channel.close()

if __name__ == "__main__":
    main()
