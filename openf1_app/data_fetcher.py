import pandas as pd

def load_data(race_name):
    # URL da API OpenF1 para os resultados da corrida do Bahrein 2024 em formato CSV
    url = "https://api.openf1.org/v1/races/bahrain-2024/results.csv"
    
    # Carregar os dados diretamente da URL
    df = pd.read_csv(url)
    
    return df
