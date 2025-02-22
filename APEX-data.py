from urllib.request import urlopen
import json
import os
import requests
import psycopg2
import pandas as pd
import io

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


else:
    print(f"❌ Erro ao acessar a API de drivers. Código: {response.status_code}")

# 🔹 Inserção das Sessões
url_sessions = "https://api.openf1.org/v1/sessions"
response = requests.get(url_sessions)

if response.status_code == 200:
    sessions = response.json()
    for session in sessions:
        cursor.execute(
            """
            INSERT INTO sessions (session_key, meeting_key, circuit_key, circuit_short_name,
                                  country_code, country_key, country_name, date_start, date_end,
                                  gmt_offset, location, session_name, session_type, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_key) DO NOTHING
            """,
            (
                session.get("session_key"),
                session.get("meeting_key"),
                session.get("circuit_key"),
                session.get("circuit_short_name"),
                session.get("country_code"),
                session.get("country_key"),
                session.get("country_name"),
                session.get("date_start"),
                session.get("date_end"),
                session.get("gmt_offset"),
                session.get("location"),
                session.get("session_name"),
                session.get("session_type"),
                session.get("year")
            )
        )

    conn.commit()
    print("✅ Dados das sessões inseridos com sucesso!")

else:
    print(f"❌ Erro ao acessar a API de sessões. Código: {response.status_code}")



# 🔹 Inserção das Posições
url_position = "https://api.openf1.org/v1/position?csv=true"
response = requests.get(url_position)

if response.status_code == 200:
    # Lendo CSV da API
    positions_df = pd.read_csv(io.StringIO(response.text))
    
    # Convertendo data para formato correto
    positions_df["date"] = pd.to_datetime(positions_df["date"], format='ISO8601')

    # Query de inserção
    insert_query = """
    INSERT INTO positions (date, driver_number, meeting_key, position, session_key)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    # Converter DataFrame para lista de tuplas
    records = [tuple(row) for row in positions_df.itertuples(index=False, name=None)]

    # Inserir no banco de dados
    cursor.executemany(insert_query, records)
    conn.commit()
    print("✅ Dados das posições inseridos com sucesso!")

else:
    print(f"❌ Erro ao acessar a API de posições. Código: {response.status_code}")


# 🏆 Piloto com Mais Vitórias, Pódios e Pole Positions
query = """
WITH race_sessions AS (
    SELECT session_key::INTEGER AS session_key
    FROM sessions
    WHERE session_type = 'Race' AND year IN (2023, 2024)
),
qualifying_sessions AS (
    SELECT session_key::INTEGER AS session_key
    FROM sessions
    WHERE session_type = 'Qualifying' AND year IN (2023, 2024)
),
race_results AS (
    SELECT 
        p.driver_number,
        COUNT(*) FILTER (WHERE p.position = 1) AS victories,
        COUNT(*) FILTER (WHERE p.position IN (1, 2, 3)) AS podiums
    FROM positions p
    JOIN race_sessions rs ON p.session_key = rs.session_key
    GROUP BY p.driver_number
),
pole_positions AS (
    SELECT 
        p.driver_number,
        COUNT(*) AS pole_positions
    FROM positions p
    JOIN qualifying_sessions qs ON p.session_key = qs.session_key
    WHERE p.position = 1
    GROUP BY p.driver_number
)
SELECT 
    d.name AS piloto,
    COALESCE(r.victories, 0) AS vitorias,
    COALESCE(r.podiums, 0) AS podios,
    COALESCE(p.pole_positions, 0) AS pole_positions
FROM drivers d
LEFT JOIN race_results r ON d.driver_id = r.driver_number
LEFT JOIN pole_positions p ON d.driver_id = p.driver_number
ORDER BY vitorias DESC, pole_positions DESC, podios DESC
LIMIT 1;
"""

cursor.execute(query)
top_driver = cursor.fetchone()

if top_driver:
    print(f"🏆 Piloto com mais vitórias, pódios e poles: {top_driver[0]}")
    print(f"   - Vitórias: {top_driver[1]}")
    print(f"   - Pódios: {top_driver[2]}")
    print(f"   - Pole Positions: {top_driver[3]}")
else:
    print("❌ Nenhum piloto encontrado")

# 🔹 Fechar conexão
cursor.close()
conn.close()






















# 🔹 Inserção das Posições
#url_position = "https://api.openf1.org/v1/position" #API de posicoes esta fora do ar

#response = requests.get(url_position)

#if response.status_code == 200:
 #   positions = response.json()
 #   for position in positions:
 #       date = position.get("date")
 #       driver_number = position.get("driver_number")
 #       meeting_key = position.get("meeting_key")
 #       pos = position.get("position")  # Renomear para evitar conflito com nome de variável
 #       session_key = position.get("session_key")

 #       cursor.execute(
 #           """
 #           INSERT INTO positions (date, driver_number, meeting_key, position, session_key)
 #           VALUES (%s, %s, %s, %s, %s)
  #          """,
 #           (date, driver_number, meeting_key, pos, session_key)
   #     )

  #  conn.commit()
 #   print("✅ Dados das posições inseridos com sucesso!")

#else:
#    print(f"❌ Erro ao acessar a API de posições. Código: {response.status_code}")





