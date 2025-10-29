import pandas as pd

DATA_FILE = 'position.csv'

def get_cleaned_data():
    """
    Lê o CSV 'position.csv' e aplica a limpeza básica.
    Retorna um DataFrame pronto para análise.
    (Lógica baseada no seu 'calculo.py' original)
    """
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{DATA_FILE}' não foi encontrado.")
        print("Certifique-se de que ele está na mesma pasta que 'app.py'.")
        raise
        
    # Remove caracteres não numéricos, exceto ':' e '.'
    df['Time'] = df['Time'].str.replace(r'[^\d:.]', '', regex=True)
    
    # Converte o tempo para timedelta (assumindo formato M:SS.fff)
    # O prefixo '00:' o transforma em 'HH:MM:SS.fff'
    try:
        df['Time'] = pd.to_timedelta('00:' + df['Time'])
    except Exception as e:
        print(f"Erro ao converter coluna 'Time': {e}")
        print("Verifique o formato dos tempos no CSV. Esperado algo como '1:23.456'.")
        # Continua mesmo assim, mas a coluna 'Time' pode estar com problemas
        df['Time'] = pd.NaT 

    # Ordena e remove duplicatas (pegando o melhor tempo por piloto/circuito)
    df.sort_values(by=['Circuit', 'Driver', 'Time'], inplace=True)
    df.drop_duplicates(subset=['Circuit', 'Driver'], keep='first', inplace=True)
    
    # Garante que a Posição é numérica
    df['Position'] = pd.to_numeric(df['Position'], errors='coerce')
    
    return df
