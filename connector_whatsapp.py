# connector_whatsapp.py

import connector_base
import time
import metrics_setup

# --- CONFIGURAÇÕES DO CANAL ---
CHANNEL = "WhatsApp"
GROUP_ID = "connector_whatsapp_group_v4"

# --- CONFIGURAÇÕES DE OBSERVALIDADE ---
WORKER_NAME = "ConnectorWhatsApp"
METRICS_PORT = 8002

# --- TÓPICO DE CONSUMO ---
# Deve ser o mesmo tópico para o qual o router_worker.py publica
KAFKA_TOPIC_CONSUME = "whatsapp_channel" 


# ============================================================
# INICIALIZAÇÃO
# ============================================================

# 1. Iniciar o servidor de métricas ANTES de rodar o worker
metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)


if __name__ == '__main__':
    print(f"[{WORKER_NAME}] Iniciando Worker...")
    
    # 2. Setup do MongoDB
    db = connector_base.setup_mongodb()
    
    if db is not None:
        # 3. Rodar o loop principal, passando o NOME DO TÓPICO
        connector_base.run_connector(
            CHANNEL, 
            GROUP_ID, 
            KAFKA_TOPIC_CONSUME,  # <--- NOVA VARIÁVEL PASSADA
            db, 
            WORKER_NAME
        )
    else:
        print(f"[{WORKER_NAME}] ERRO CRÍTICO: Não foi possível conectar ao MongoDB.")