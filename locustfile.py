# locustfile.py (O Script de Teste de Carga do Locust)

from locust import FastHttpUser, task, tag, between
import random
import grpc
import time

# Importa o nosso cliente wrapper gRPC
import grpc_client_locust as grpc_client

class GrpcLoadTestUser(FastHttpUser):
    # Simula 1 segundo de espera entre as tarefas
    host = 'http://localhost:50051' 
    wait_time = between(0.5, 1.5) 
    
    # Cada usuário terá seu próprio ID para a simulação
    user_id = f"User_{random.randint(1, 1000)}"

    def on_start(self):
        """Executado quando a thread do usuário inicia."""
        if not grpc_client.setup_global_state():
             print(f"[{self.user_id}] ERRO: Setup gRPC falhou. Parando thread.")
             self.environment.runner.quit()

    @task(10) # Peso 10: esta é a tarefa principal e mais frequente
    @tag('send_message')
    def send_message_task(self):
        """Simula o envio de uma mensagem gRPC e registra a métrica."""
        
        # 1. Inicia o Timer e executa a chamada gRPC
        start_time = time.time()
        success, error_detail = grpc_client.send_message_request(self.user_id)
        end_time = time.time()
        response_time = (end_time - start_time) * 1000  # Tempo em milissegundos
        
        # 2. Informa o Locust sobre o resultado da requisição
        
        # O nome da requisição que aparecerá no dashboard
        request_name = "SendMessage_gRPC" 

        if success:
            # Reporta sucesso: response_time é medido
            self.environment.events.request.fire(
                request_type="gRPC", 
                name=request_name, 
                response_time=response_time, 
                response_length=0
            )
        else:
            # Reporta falha
            self.environment.events.request.fire(
                request_type="gRPC", 
                name=request_name, 
                response_time=response_time, 
                response_length=0,
                exception=error_detail # Registra o detalhe do erro
            )

# NOTA: O Locust usará o endereço HTTP da classe FastHttpUser (que ignoramos aqui),
# mas as métricas serão geradas a partir do nosso wrapper gRPC.