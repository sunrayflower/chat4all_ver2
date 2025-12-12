# Dockerfile
FROM python:3.11-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o restante do código
COPY . .

# Expõe a porta gRPC
EXPOSE 50051

# Comando padrão para rodar o servidor gRPC (pode ser sobrescrito)
# CMD ["python", "server.py"]