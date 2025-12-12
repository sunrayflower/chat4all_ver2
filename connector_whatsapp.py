# connector_whatsapp.py
import connector_base
import time
import metrics_setup

CHANNEL = "WhatsApp"
GROUP_ID = "connector_whatsapp_group"

WORKER_NAME = "ConnectorWhatsApp"
METRICS_PORT = 8002

metrics_setup.start_metrics_server(METRICS_PORT, WORKER_NAME)

if __name__ == '__main__':
    db = connector_base.setup_mongodb()
    if db is not None:
        connector_base.run_connector(CHANNEL, GROUP_ID, db, WORKER_NAME)