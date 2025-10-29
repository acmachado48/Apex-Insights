import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
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


def highlight_max(s):
    """Destaca o valor máximo em uma Série."""
    is_max = s == s.max()
    return ['background-color: #4CAF50' if v else '' for v in is_max]

def highlight_min(s):
    """Destaca o valor mínimo em uma Série."""
    is_min = s == s.min()
    return ['background-color: #f44336' if v else '' for v in is_min]

def format_time(td):
    """Formata um timedelta para 'M:SS.fff'."""
    if pd.isna(td):
        return 'N/A'
    total_seconds = td.total_seconds()
    minutes = int(total_seconds // 60)
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:06.3f}"

def calculate_points(row):
    """Calcula pontos com base na posição (lógica do seu 'APEX-data.py')."""
    pos = row['Position']
    sprint = row['Sprint']
    fastest_lap = row['Fastest Lap']
    
    points = 0
    
    # Pontos da corrida principal
    if pos == 1: points = 25
    elif pos == 2: points = 18
    elif pos == 3: points = 15
    elif pos == 4: points = 12
    elif pos == 5: points = 10
    elif pos == 6: points = 8
    elif pos == 7: points = 6
    elif pos == 8: points = 4
    elif pos == 9: points = 2
    elif pos == 10: points = 1
        
    # Pontos da Sprint
    if sprint:
        if pos == 1: points += 8
        elif pos == 2: points += 7
        elif pos == 3: points += 6
        elif pos == 4: points += 5
        elif pos == 5: points += 4
        elif pos == 6: points += 3
        elif pos == 7: points += 2
        elif pos == 8: points += 1
            
    # Ponto de volta mais rápida
    if fastest_lap and pos <= 10:
        points += 1
        
    return points

# --- Funções de Processamento e Plotagem ---

def get_driver_performance(df):
    """
    Recebe o DataFrame limpo e calcula o desempenho dos pilotos.
    Retorna um novo DataFrame com os resultados.
    (Lógica do seu 'APEX-data.py')
    """
    df_copy = df.copy()
    
    # Assumindo que 'Sprint' e 'Fastest Lap' precisam ser criadas
    # Adicione sua lógica real aqui, por enquanto vou simular
    if 'Sprint' not in df_copy.columns:
        # Simulação: Apenas corridas com 'Sprint' no nome
        df_copy['Sprint'] = df_copy['Circuit'].str.contains('Sprint', case=False, na=False)
    if 'Fastest Lap' not in df_copy.columns:
        # Simulação: Piloto na P1 ou P2
        df_copy['Fastest Lap'] = df_copy['Position'].isin([1, 2])
    
    
    df_copy['pontos'] = df_copy.apply(calculate_points, axis=1)
    
    df_pilotos = df_copy.groupby('Driver').agg(
        total_pontos=('pontos', 'sum'),
        media_posicao=('Position', 'mean'),
        melhor_posicao=('Position', 'min'),
        pior_posicao=('Position', 'max'),
        poles=('Position', lambda x: (x == 1).sum()),
        podiums=('Position', lambda x: (x <= 3).sum()),
        corridas_disputadas=('Circuit', 'nunique'),
        media_tempo_qualify=('Time', 'mean') # Média do timedelta
    ).sort_values(by='total_pontos', ascending=False)
    
    # Formata a média de tempo para exibição
    df_pilotos['media_tempo_qualify_str'] = df_pilotos['media_tempo_qualify'].apply(format_time)
    
    return df_pilotos

def plot_driver_performance_grid(df_pilotos):
    """
    Recebe o DataFrame de desempenho e retorna uma Figura Matplotlib.
    (Lógica de plotagem do seu 'APEX-data.py')
    """
    # Seleciona apenas as colunas numéricas para o heatmap
    df_heatmap = df_pilotos[['total_pontos', 'media_posicao', 'melhor_posicao', 'pior_posicao', 'poles', 'podiums', 'corridas_disputadas']]
    
    # Normaliza os dados para o heatmap
    df_normalized = (df_heatmap - df_heatmap.min()) / (df_heatmap - df_heatmap.max())
    
    # Cores personalizadas (Ex: Vermelho para ruim, Verde para bom)
    # Invertemos: Pior Posição e Média Posição (quanto menor, melhor)
    df_normalized['media_posicao'] = 1 - df_normalized['media_posicao']
    df_normalized['pior_posicao'] = 1 - df_normalized['pior_posicao']
    
    cmap = LinearSegmentedColormap.from_list("custom_cmap", ["#f44336", "#FFEB3B", "#4CAF50"])
    
    fig, ax = plt.subplots(figsize=(15, 10))
    sns.heatmap(
        df_normalized, 
        annot=df_heatmap,  # Mostra os valores reais
        fmt=".0f",         # Formato dos números (inteiro)
        cmap=cmap, 
        linewidths=.5, 
        ax=ax
    )
    ax.set_title('Grid de Desempenho dos Pilotos (Análise position.csv)', fontsize=16)
    ax.set_xlabel('Métricas')
    ax.set_ylabel('Pilotos')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    # Em vez de plt.savefig(...), retorne a figura
    return fig

def plot_temporal_evolution(df, driver_name):
    """
    Recebe o DataFrame limpo e o nome de um piloto, retorna a figura da evolução.
    (Lógica de plotagem do seu 'APEX-data.py')
    """
    df_piloto = df[df['Driver'] == driver_name].sort_values(by='Circuit') # Simplificado
    
    if df_piloto.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, f'Piloto "{driver_name}" não encontrado.', horizontalalignment='center', verticalalignment='center')
        return fig

    fig, ax1 = plt.subplots(figsize=(15, 7))
    
    # Eixo 1: Posição
    color = 'tab:blue'
    ax1.set_xlabel('Corrida (Circuito)')
    ax1.set_ylabel('Posição', color=color)
    ax1.plot(df_piloto['Circuit'], df_piloto['Position'], color=color, marker='o', label='Posição')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.invert_yaxis() # Posição 1 é melhor (em cima)
    
    # Eixo 2: Tempo (convertido para segundos para plotar)
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Tempo de Qualify (segundos)', color=color)
    ax2.plot(df_piloto['Circuit'], df_piloto['Time'].dt.total_seconds(), color=color, linestyle='--', marker='x', label='Tempo (s)')
    ax2.tick_params(axis='y', labelcolor=color)
    
    ax1.set_title(f'Evolução Temporal de {driver_name}')
    ax1.set_xticklabels(df_piloto['Circuit'], rotation=45, ha='right')
    fig.tight_layout()
    
    return fig
