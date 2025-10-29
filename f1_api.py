import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import openf1.utils as f1_utils # Dependência do seu código original

# URL base da API OpenF1
BASE_API_URL = "https://api.openf1.org/v1"

def _get_session_key(year: int, location: str) -> int | None:
    """
    Busca o session_key de uma corrida (ex: 9158)
    usando o ano e o nome da localização/GP.
    """
    print(f"Buscando session_key para: {location} {year}")
    try:
        # Busca por sessões daquele ano
        response = requests.get(f"{BASE_API_URL}/sessions?year={year}", timeout=10)
        response.raise_for_status()
        sessions = response.json()
        
        # Filtra pela localização e tipo 'Race'
        for session in sessions:
            if (session['location'].lower() == location.lower() or 
                session['session_name'].lower() == location.lower() or 
                session['circuit_short_name'].lower() == location.lower()) and \
               session['session_type'] == 'Race':
                
                key = session['session_key']
                print(f"Encontrado session_key: {key}")
                return key
        
        print(f"Nenhuma sessão 'Race' encontrada para '{location}' em {year}.")
        return None
    except Exception as e:
        print(f"Erro ao buscar session_key: {e}")
        return None

def _fetch_position_data(session_key: int) -> pd.DataFrame | None:
    """
    Busca dados de posição para uma session_key.
    (Baseado no seu 'data_fetcher.py')
    """
    print(f"Buscando dados de posição para session_key: {session_key}...")
    try:
        response = requests.get(f"{BASE_API_URL}/position?session_key={session_key}", timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data:
            print("Nenhum dado de posição retornado.")
            return None
            
        df = pd.DataFrame(data)
        # Converte data para datetime (necessário para o utils)
        df['date'] = pd.to_datetime(df['date'])
        print("Dados de posição carregados.")
        return df
    except Exception as e:
        print(f"Erro ao buscar dados de posição: {e}")
        return None

def _fetch_overtakes_data(session_key: int) -> pd.DataFrame | None:
    """
    Busca dados de ultrapassagens para uma session_key.
    """
    print(f"Buscando dados de ultrapassagem para session_key: {session_key}...")
    try:
        # O endpoint 'pit' não existe, 'stints' é melhor.
        # Vamos usar 'laps' para ver as posições
        response = requests.get(f"{BASE_API_URL}/laps?session_key={session_key}&is_pit_out_lap=false", timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data:
            print("Nenhum dado de voltas retornado para análise de ultrapassagem.")
            return None
        
        df = pd.DataFrame(data)
        print("Dados de voltas (para ultrapassagens) carregados.")
        return df
    except Exception as e:
        print(f"Erro ao buscar dados de voltas: {e}")
        return None

# --- CORREÇÃO APLICADA AQUI ---
# A função agora também recebe year e location para passar para a biblioteca f1_utils
def _plot_position_changes(pos_data: pd.DataFrame, year: int, location: str) -> plt.Figure | None:
    """
    Plota o gráfico de mudança de posições.
    (Baseado no seu 'position_graph.py')
    """
    print("Iniciando plotagem de posições...")
    try:
        # Pega os números dos pilotos
        driver_numbers = pos_data['driver_number'].unique()
        
        # Prepara o plot
        fig, ax = plt.subplots(figsize=(15, 10))
        
        for driver in driver_numbers:
            driver_data = pos_data[pos_data['driver_number'] == driver]
            if driver_data.empty:
                continue

            # --- CORREÇÃO DA CHAMADA ---
            # A biblioteca f1_utils espera year e location, não session_key, para encontrar os dados.
            try:
                driver_color = f"#{f1_utils.get_driver_color(driver, year=year, location=location, session_type='Race')}"
            except:
                driver_color = None # Usa cor padrão do Seaborn
            
            try:
                driver_tla = f1_utils.get_driver_tla(driver, year=year, location=location, session_type='Race')
            except:
                driver_tla = f"Piloto {driver}"

            sns.lineplot(
                data=driver_data,
                x='date', 
                y='position',
                color=driver_color,
                label=driver_tla,
                ax=ax
            )

        ax.set_ylim(0.5, 20.5) # Limites de posição
        ax.set_yticks(range(1, 21))
        ax.invert_yaxis() # P1 no topo
        ax.set_title(f'Mudanças de Posição Durante a Corrida ({location} {year})')
        ax.set_xlabel('Tempo de Corrida')
        ax.set_ylabel('Posição')
        ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.tight_layout()
        print("Gráfico de posições criado.")
        return fig
    except Exception as e:
        print(f"Erro ao plotar gráfico de posições: {e}")
        return None

# --- CORREÇÃO APLICADA AQUI ---
# A função agora também recebe year e location
def _plot_overtakes(laps_data: pd.DataFrame, year: int, location: str) -> plt.Figure | None:
    """
    Plota um gráfico simples de posições por volta.
    (Baseado no seu 'overtakes.py' mas simplificado)
    """
    print("Iniciando plotagem de ultrapassagens (pos. por volta)...")
    try:
        # Foca apenas na posição ao final de cada volta
        df_laps = laps_data.dropna(subset=['lap_number', 'position', 'driver_number'])
        df_laps['lap_number'] = df_laps['lap_number'].astype(int)
        df_laps['position'] = df_laps['position'].astype(int)
        
        drivers = df_laps['driver_number'].unique()
        
        fig, ax = plt.subplots(figsize=(15, 10))
        
        for driver in drivers:
            driver_laps = df_laps[df_laps['driver_number'] == driver]
            if driver_laps.empty:
                continue
                
            # --- CORREÇÃO DA CHAMADA ---
            try:
                driver_color = f"#{f1_utils.get_driver_color(driver, year=year, location=location, session_type='Race')}"
                driver_tla = f1_utils.get_driver_tla(driver, year=year, location=location, session_type='Race')
            except:
                driver_color = None
                driver_tla = f"Piloto {driver}"
                
            sns.lineplot(
                data=driver_laps,
                x='lap_number',
                y='position',
                label=driver_tla,
                color=driver_color,
                marker='o',
                markersize=4,
                ax=ax
            )
            
        ax.set_ylim(0.5, 20.5)
        ax.set_yticks(range(1, 21))
        ax.invert_yaxis()
        ax.set_title(f'Posição dos Pilotos por Volta ({location} {year})')
        ax.set_xlabel('Número da Volta')
        ax.set_ylabel('Posição')
        ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.tight_layout()
        print("Gráfico de posições por volta criado.")
        return fig
    except Exception as e:
        print(f"Erro ao plotar gráfico de posições por volta: {e}")
        return None

# --- Funções Públicas ---

def get_position_plot(year: int, location: str) -> plt.Figure | None:
    """
    Função principal para buscar dados de posição e retornar o gráfico.
    """
    session_key = _get_session_key(year, location)
    if not session_key:
        print(f"Não foi possível encontrar uma session_key para {location} {year}.")
        return None
        
    pos_data = _fetch_position_data(session_key)
    if pos_data is None:
        print("Falha ao buscar dados de posição.")
        return None
        
    # --- CORREÇÃO DA CHAMADA ---
    # Passa year e location para a função de plotagem
    fig = _plot_position_changes(pos_data, year, location)
    return fig

def get_overtakes_plot(year: int, location: str) -> plt.Figure | None:
    """
    Função principal para buscar dados de voltas e retornar o gráfico de pos/volta.
    """
    session_key = _get_session_key(year, location)
    if not session_key:
        print(f"Não foi possível encontrar uma session_key para {location} {year}.")
        return None
        
    laps_data = _fetch_overtakes_data(session_key)
    if laps_data is None:
        print("Falha ao buscar dados de voltas.")
        return None
        
    # --- CORREÇÃO DA CHAMADA ---
    # Passa year e location para a função de plotagem
    fig = _plot_overtakes(laps_data, year, location)
    return fig
