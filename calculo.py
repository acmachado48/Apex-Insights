import psycopg2
import os

# Configurações do banco de dados
DB_NAME = os.getenv("DB_NAME", "f1_stats")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "403800")
DB_HOST = os.getenv("DB_HOST", "localhost")

def get_f1_statistics():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    cursor = conn.cursor()

    # 🔹 Piloto com mais vitórias (posição 1 na corrida)
    cursor.execute("""
        SELECT d.name, COUNT(*) as victories 
        FROM positions p
        JOIN drivers d ON p.driver_number = d.driver_id
        WHERE p.position = 1
        GROUP BY d.name
        ORDER BY victories DESC
        LIMIT 1
    """)
    top_winner = cursor.fetchone()

    # 🔹 Piloto com mais pódios (posição 1, 2 ou 3 na corrida)
    cursor.execute("""
        SELECT d.name, COUNT(*) as podiums 
        FROM positions p
        JOIN drivers d ON p.driver_number = d.driver_id
        WHERE p.position IN (1, 2, 3)
        GROUP BY d.name
        ORDER BY podiums DESC
        LIMIT 1
    """)
    top_podium = cursor.fetchone()

    # 🔹 Piloto com mais pole positions (posição 1 na sessão de qualificação)
    cursor.execute("""
        SELECT d.name, COUNT(*) as poles 
        FROM positions p
        JOIN drivers d ON p.driver_number = d.driver_id
        WHERE p.position = 1 AND p.session_key = 'Qualifying'
        GROUP BY d.name
        ORDER BY poles DESC
        LIMIT 1
    """)
    top_pole = cursor.fetchone()

    # Fechar conexão
    cursor.close()
    conn.close()

    # Retornar os resultados em um dicionário
    return {
        "Piloto com mais vitórias": top_winner,
        "Piloto com mais pódios": top_podium,
        "Piloto com mais poles": top_pole
    }

# Testando a função
stats = get_f1_statistics()
for key, value in stats.items():
    print(f"{key}: {value[0]} ({value[1]})")
