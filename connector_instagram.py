# connector_instagram.py
import connector_base
import time
import metrics_setup

CHANNEL = "Instagram"
GROUP_ID = "connector_instagram_group_v4"

WORKER_NAME = "ConnectorInstagram"
METRICS_PORT = 8003

KAFKA_TOPIC_CONSUME = "instagram_channel" 

metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)

if __name__ == '__main__':
    print(f"[{WORKER_NAME}] Iniciando Worker...")
    db = connector_base.setup_mongodb()    
    if db is not None:
        connector_base.run_connector(
            CHANNEL, 
            GROUP_ID, 
            KAFKA_TOPIC_CONSUME,
            db, 
            WORKER_NAME
        )
    else:
        print(f"[{WORKER_NAME}] ERRO CRÍTICO: Não foi possível conectar ao MongoDB.")