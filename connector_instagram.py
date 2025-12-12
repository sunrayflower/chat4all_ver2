# connector_instagram.py

import connector_base
import time
import metrics_setup

# --- CONFIGURAÇÕES DO CANAL ---
CHANNEL = "Instagram"
GROUP_ID = "connector_instagram_group_v4"

# --- CONFIGURAÇÕES DE OBSERVALIDADE ---
WORKER_NAME = "ConnectorInstagram"
METRICS_PORT = 8003 # <--- Porta de Métricas Exclusiva (8003)

# --- TÓPICO DE CONSUMO ---
# Deve ser o mesmo tópico para o qual o router_worker.py publica
KAFKA_TOPIC_CONSUME = "instagram_channel" 


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
            KAFKA_TOPIC_CONSUME,
            db, 
            WORKER_NAME
        )
    else:
        print(f"[{WORKER_NAME}] ERRO CRÍTICO: Não foi possível conectar ao MongoDB.")