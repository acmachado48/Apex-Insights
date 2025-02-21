import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates

# Configurações do banco de dados
DB_NAME = os.getenv("DB_NAME", "f1_stats")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "403800")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Conectar ao banco de dados
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    cursor = conn.cursor()
    print("Conexão com o banco de dados estabelecida com sucesso.")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

# Carregar o arquivo CSV
try:
    df = pd.read_csv('Apex-Insights/position.csv')
    print("Arquivo CSV carregado com sucesso.")
except Exception as e:
    print(f"Erro ao carregar o arquivo CSV: {e}")
    exit()

# Converter a coluna 'date' para datetime
df['date'] = pd.to_datetime(df['date'], format='ISO8601')  # ou format='mixed'

# Criar a pasta para salvar os gráficos, se não existir
output_dir = "graficos_f1"
os.makedirs(output_dir, exist_ok=True)

# Função para evolução temporal das posições de Max Verstappen
def evolucao_temporal_max_verstappen(df):
    dados_max = df[df['driver_number'] == 1].copy()

    plt.figure(figsize=(18, 6))

    # Gráfico de linha da posição na corrida
    sns.lineplot(x='date', y='position', data=dados_max, label='Posição na Corrida',
                 color='blue', marker='o', linestyle='-', markersize=4)

    # Média móvel de 10 corridas
    dados_max['media_movel'] = dados_max['position'].rolling(window=10, min_periods=1).mean()
    sns.lineplot(x='date', y='media_movel', data=dados_max, label='Média Móvel (10 corridas)',
                 color='red', linestyle='--', linewidth=2)

    # Destacar vitórias
    vitorias = dados_max[dados_max['position'] == 1]
    plt.scatter(vitorias['date'], vitorias['position'], color='gold', s=80,
                label='Vitórias (1ª posição)', edgecolor='black', zorder=3)

    # Ajuste do eixo X
    plt.xticks(rotation=30, fontsize=10)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    # Personalizações finais
    plt.title('Evolução Temporal das Posições de Max Verstappen', fontsize=16)
    plt.xlabel('Data', fontsize=12)
    plt.ylabel('Posição na Corrida', fontsize=12)
    plt.gca().invert_yaxis()
    plt.grid(True, linestyle='--', alpha=0.7)

    # Ajuste da legenda
    plt.legend(title='Legenda', fontsize=10, loc='upper left', bbox_to_anchor=(1, 1))
    plt.tight_layout()

    # Salvar gráfico
    output_path = os.path.join(output_dir, "evolucao_temporal_max_verstappen.png")
    plt.savefig(output_path, dpi=300)
    plt.show()

# Análise de desempenho dos pilotos
def analisar_desempenho_pilotos(df):
    desempenho_pilotos = df.groupby('driver_number')['position'].agg(['min', 'max', 'mean', 'count'])
    desempenho_pilotos.columns = ['Posição Mínima', 'Posição Máxima', 'Posição Média', 'Número de Registros']
    return desempenho_pilotos

# Comparação entre sessões
def comparar_sessoes(df):
    comparacao_sessoes = df.groupby(['session_key', 'driver_number'])['position'].mean().unstack()
    return comparacao_sessoes

# Evolução temporal das posições de um piloto específico
def evolucao_temporal(df, driver_number):
    dados_piloto = df[df['driver_number'] == driver_number]

    try:
        plt.figure(figsize=(10, 6))
        sns.lineplot(x='date', y='position', data=dados_piloto)
        plt.title(f'Evolução Temporal das Posições do Piloto {driver_number}')
        plt.xlabel('Data e Hora')
        plt.ylabel('Posição')

        # Salvar gráfico
        output_path = os.path.join(output_dir, f"evolucao_temporal_piloto_{driver_number}.png")
        plt.savefig(output_path, dpi=300)
        plt.show()
    except Exception as e:
        print(f"Erro ao gerar o gráfico: {e}")
        plt.close()

# Executar as análises
try:
    evolucao_temporal_max_verstappen(df)

    desempenho_pilotos = analisar_desempenho_pilotos(df)
    print("\nDesempenho dos Pilotos:")
    print(desempenho_pilotos)

    comparacao_sessoes = comparar_sessoes(df)
    print("\nComparação entre Sessões:")
    print(comparacao_sessoes)

    # Salvar os resultados das análises em arquivos CSV

    # Salvar desempenho dos pilotos
    desempenho_pilotos.to_csv("desempenho_pilotos.csv", index=True)
    print("Desempenho dos pilotos salvo em 'desempenho_pilotos.csv'.")

    # Salvar comparação entre sessões
    comparacao_sessoes.to_csv("comparacao_sessoes.csv", index=True)
    print("Comparação entre sessões salva em 'comparacao_sessoes.csv'.")

except Exception as e:
    print(f"Erro ao salvar os arquivos CSV: {e}")


    # Escolher piloto específico
    piloto_especifico = 1
    evolucao_temporal(df, piloto_especifico)

except Exception as e:
    print(f"Erro durante a execução das análises: {e}")

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados fechada.")