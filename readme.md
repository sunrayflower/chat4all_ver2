# Chat4All v2 – README Oficial

Plataforma distribuída para processamento de mensagens utilizando **gRPC**, **Kafka**, **workers**, **conectores externos** e **pipelines assíncronos**. Este README substitui completamente o anterior e reflete a estrutura atual do repositório.

---

# 📌 Visão Geral

O Chat4All v2 é um sistema modular construído para receber mensagens, processá-las e encaminhá-las para diferentes plataformas através de conectores desacoplados. A arquitetura combina performance (gRPC), escalabilidade (Kafka) e extensibilidade (conectores independentes).

---

# 🧩 Arquitetura Geral

A plataforma é composta por:

### 🔹 **API gRPC (Router)**

Recebe mensagens e distribui para os tópicos do Kafka.

### 🔹 **Workers Kafka**

Processam mensagens de tópicos específicos e chamam os conectores.

### 🔹 **Conectores**

Enviam mensagens para sistemas externos (WhatsApp/Instagram no projeto atual).

### 🔹 **Kafka + Zookeeper**

Barramento de eventos distribuído.

### 🔹 **Prometheus + Grafana**

Monitoramento e observabilidade.

### 🔹 **Locust**

Testes de carga e simulação de múltiplos usuários.

---

# 🚀 Como Executar o Projeto

## 1. Clonar o repositório

```bash
git clone https://github.com/sunrayflower/chat4all_ver2.git
cd chat4all_ver2
```

## 2. Acessar o ambiente virtual

```bash
.\.venv\Scripts\activate
```
## 3. Instalar as ferramentas necessárias

```bash
pip install -r requirements.txt
```

## 4. Subir toda a stack com Docker Compose

```bash
docker-compose up -d --build
```

Isso iniciará:

* API gRPC
* Workers
* Kafka
* Zookeeper
* Prometheus
* Grafana

## 5. Acessar o Grafana

```
http://localhost:3000
```

Usuário padrão: **admin / admin**

## 6. Rodar testes de carga (Locust)

```bash
locust -f locustfile.py
```

Interface:

```
http://localhost:8089
```

---

# 📡 Comunicação gRPC

O arquivo principal da API é:

```
chat.proto
```

Para recompilar os stubs gRPC:

```bash
python -m grpc_tools.protoc -I=api --python_out=api --grpc_python_out=api chat.proto
```

---

# 🔌 Conectores

Atualmente existem dois conectores ativos:

* **WhatsApp** (simulado)
* **Instagram** (simulado)

Eles são chamados automaticamente pelos workers.

---

# 📈 Observabilidade

O sistema expõe métricas para Prometheus:

```
http://localhost:8000/metrics
```

E os dashboards podem ser visualizados no Grafana.

👉 **Inserir captura de tela aqui** (dashboard geral)

---

# 🔥 Testes de Carga com Locust

Os testes simulam usuários enviando mensagens via gRPC.




---

# 🛠 Tecnologias Utilizadas

* Python 3.12
* Kafka / Zookeeper
* gRPC
* Docker + Docker Compose
* Prometheus
* Grafana
* Locust


