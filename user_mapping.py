# seed_script.py

from pymongo import MongoClient

# Conexão com MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["chat4all_v2"]

mapping = db["user_channel_map"]

print("Limpando collection user_channel_map...")
mapping.delete_many({})

print("Inserindo mapeamentos de teste...")

user_mappings = [
    {
        "user_id_interno": "user_giovanna",
        "name": "Giovanna",
        "mappings": [
            {"canal": "whatsapp", "id_externo": "+556299999999"},
            {"canal": "instagram", "id_externo": "giovanna_ig"},
            {"canal": "telegram", "id_externo": "1628432250", "username": "@nannafmgn"}
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
        "user_id_interno": "user_fabo",
        "name": "Fabio",
        "mappings": [
            {"canal": "instagram", "id_externo": "fabinho"}
        ]
    },
    {
        "user_id_interno": "load_target",
        "name": "locust",
        "mappings": [
            {"canal": "whatsapp", "id_externo": "+5511988887777"}
        ]
    }
]

mapping.insert_many(user_mappings)

print("SEED COMPLETO!")
print(f"Total de {len(user_mappings)} usuários mapeados.")