# metrics_setup.py

from prometheus_client import Counter, Histogram, start_http_server

# --- MÉTRICAS GERAIS (Aplicáveis a todos os Workers) ---
MESSAGES_PROCESSED_TOTAL = Counter(
    'messages_processed_total', 
    'Total de mensagens processadas', 
    ['worker_name', 'status'] # Labels para rastrear por worker e resultado (ex: 'SENT', 'DELIVERED', 'FAILED')
)

LATENCY_SECONDS = Histogram(
    'processing_latency_seconds', 
    'Latência do processamento por Worker', 
    ['worker_name']
)

ERRORS_TOTAL = Counter(
    'errors_total', 
    'Total de erros capturados', 
    ['worker_name', 'error_type']
)

def start_metrics_server(port, worker_name):
    """Inicia o servidor HTTP para expor métricas do Prometheus."""
    try:
        start_http_server(port, addr='0.0.0.0')
        print(f"[{worker_name}] 📈 Servidor de Métricas Prometheus iniciado na porta {port}.")
    except Exception as e:
        print(f"[{worker_name}] ERRO: Não foi possível iniciar o servidor de métricas: {e}")