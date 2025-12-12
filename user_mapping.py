# seed_script.py (Versão Corrigida para Roteamento)

from pymongo import MongoClient

# Conexão com MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["chat4all_v2"]

# Usando uma coleção que armazena todos os canais de um usuário
mapping = db["user_channel_map"] # <-- RECOMENDADO USAR ESSE NOME DE COLEÇÃO

print("🔄 Limpando collection user_channel_map...")
mapping.delete_many({})

print("📌 Inserindo mapeamentos de teste...")

user_mappings = [
    {
        "user_id_interno": "user_giovanna",
        "name": "Giovanna",
        "mappings": [
            {"canal": "whatsapp", "id_externo": "+551199999999"},
            {"canal": "instagram", "id_externo": "giovanna_ig"}
        ]
    },
    {
        "user_id_interno": "user_ray",
        "name": "Rayssa",
        "mappings": [
            {"canal": "whatsapp", "id_externo": "+5562993077188"},
            {"canal": "instagram", "id_externo": "rayodesol"}

        ]
    },
    {
        "user_id_interno": "user_jao",
        "name": "Jão",
        "mappings": [
            {"canal": "instagram", "id_externo": "joao"}
        ]
    }
]

mapping.insert_many(user_mappings)

print("SEED COMPLETO!")
print(f"Total de {len(user_mappings)} usuários mapeados.")