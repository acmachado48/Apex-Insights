from urllib.request import urlopen
import json
import pandas as pd
import os
import requests
import psycopg2

# Configurações do banco de dados usando variáveis de ambiente
DB_NAME = os.getenv("DB_NAME", "f1_stats")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "403800")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Conectar ao banco de dados
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST
)
cursor = conn.cursor()

# Requisição à API OpenF1
url_drivers = "https://api.openf1.org/v1/drivers"
response = requests.get(url_drivers)

if response.status_code == 200:
    drivers = response.json()

    for driver in drivers:
        driver_id = driver.get("driver_number")  # ID do piloto
        name = driver.get("full_name")  # Nome completo
        nationality = driver.get("country_code")  # Código do país
        birthdate = driver.get("dob")  # Data de nascimento

        cursor.execute(
            """
            INSERT INTO drivers (driver_id, name, nationality, birthdate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING
            """,
            (driver_id, name, nationality, birthdate)
        )

    conn.commit()
    print("Dados inseridos com sucesso!")

else:
    print(f"Erro ao acessar a API. Código: {response.status_code}")

# Fechar conexão
cursor.close()
conn.close()




