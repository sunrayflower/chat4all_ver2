# test_failover.py
import subprocess
import time
import grpc
import uuid
from pymongo import MongoClient
import chat4all_pb2
import chat4all_pb2_grpc

def contar_mensagens():
    """Conta mensagens no MongoDB"""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["chat4all_v2"]
    return db.messages.count_documents({})

def enviar_mensagens_teste(num_msgs=50):
    """Envia mensagens de teste"""
    channel = grpc.insecure_channel('localhost:50051')
    stub = chat4all_pb2_grpc.ChatServiceStub(channel)
    
    # Autenticar
    token = stub.GetToken(
        chat4all_pb2.TokenRequest(
            client_id="admin_client",
            client_secret="super_secret_key"
        )
    ).access_token
    
    metadata = [('authorization', f'Bearer {token}')]
    
    print(f"Enviando {num_msgs} mensagens de teste...")
    
    for i in range(num_msgs):
        try:
            stub.SendMessage(
                chat4all_pb2.SendMessageRequest(
                    message_id=f"failover-test-{uuid.uuid4()}",
                    conversation_id="failover_test",
                    from_user="test_user",
                    to=["user_giovanna"],
                    channels=["telegram"],
                    payload=chat4all_pb2.MessagePayload(
                        type="text",
                        text=f"Mensagem de teste failover #{i+1}"
                    )
                ),
                metadata=metadata
            )
            
            if (i + 1) % 10 == 0:
                print(f" {i+1}/{num_msgs} enviadas")
                
        except Exception as e:
            print(f" Erro na mensagem {i+1}: {e}")
    
    channel.close()
    return num_msgs

def main():
    print("TESTE DE FAILOVER - CHAT4ALL")
    
    # 1. Contagem inicial
    print("\nFase 1: Contagem inicial")
    count_inicial = contar_mensagens()
    print(f"   Mensagens no banco: {count_inicial}")
    
    # 2. Enviar mensagens
    print("\nFase 2: Enviando mensagens em lote")
    num_enviadas = enviar_mensagens_teste(10)
    time.sleep(2)  # Aguardar processamento inicial
    
    # 3. Simular falha do worker
    print("\nFase 3: Simulando FALHA do worker")
    print(" MATE O PROCESSO worker.py MANUALMENTE AGORA!")
    print("(Pressione Ctrl+C no terminal do worker.py)")
    input(" Pressione ENTER quando tiver matado o worker...")
    
    # 4. Enviar mais mensagens durante a "falha"
    print("\nFase 4: Enviando mensagens durante a falha")
    print("   (Essas mensagens ficam no Kafka esperando)")
    enviar_mensagens_teste(5)
    time.sleep(1)
    
    # 5. Reiniciar worker
    print("\n Fase 5: Recuperação")
    print("   REINICIE O worker.py AGORA!")
    print("   Execute: python worker.py")
    input("   Pressione ENTER quando o worker estiver rodando...")
    
    # 6. Aguardar processamento
    print("\nFase 6: Aguardando reprocessamento...")
    print("   (Kafka vai reprocessar as mensagens pendentes)")
    time.sleep(10)
    
    # 7. Contagem final
    print("\nFase 7: Verificação final")
    count_final = contar_mensagens()
    print(f"   Mensagens no banco: {count_final}")
    
    # 8. Validação
    total_esperado = count_inicial + num_enviadas + 20
    
    print("RESULTADO DO TESTE:")
    print(f"   Mensagens iniciais:  {count_inicial}")
    print(f"   Mensagens enviadas:  {num_enviadas + 20}")
    print(f"   Total esperado:      {total_esperado}")
    print(f"   Total no banco:      {count_final}")
    
    if count_final >= total_esperado:
        print("\n Zero perda de mensagens!")
        print(" Failover funcionou corretamente!")
    else:
        print(f"\n TESTE FALHOU! Perdeu {total_esperado - count_final} mensagens")
    
if __name__ == "__main__":
    main()
