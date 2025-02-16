from urllib.request import urlopen
import json
import os
import requests
import psycopg2

# Configurações do banco de dados
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

# 🔹 Inserção dos Drivers
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
    print("✅ Dados dos drivers inseridos com sucesso!")

     # 🔹 Agora, buscar os dados apenas uma vez
    cursor.execute("SELECT * FROM drivers;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)


else:
    print(f"❌ Erro ao acessar a API de drivers. Código: {response.status_code}")

# 🔹 Inserção das Posições
url_position = "https://api.openf1.org/v1/position"
response = requests.get(url_position)

if response.status_code == 200:
    positions = response.json()
    for position in positions:
        date = position.get("date")
        driver_number = position.get("driver_number")
        meeting_key = position.get("meeting_key")
        pos = position.get("position")  # Renomear para evitar conflito com nome de variável
        session_key = position.get("session_key")

        cursor.execute(
            """
            INSERT INTO positions (date, driver_number, meeting_key, position, session_key)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (date, driver_number, meeting_key, pos, session_key)
        )

    conn.commit()
    print("✅ Dados das posições inseridos com sucesso!")

else:
    print(f"❌ Erro ao acessar a API de posições. Código: {response.status_code}")

# 🔹 Fechar conexão
cursor.close()
conn.close()



