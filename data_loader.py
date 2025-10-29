import pandas as pd

DATA_FILE = 'position.csv'

def get_cleaned_data():
    """
    Lê o CSV 'position.csv' e aplica a limpeza básica.
    Retorna um DataFrame pronto para análise.
    """
    df = pd.read_csv(DATA_FILE)
    # ... (toda a lógica de limpeza de 'calculo.py' e 'APEX-data.py') ...
    # Ex: df['Time'] = df['Time'].str.replace(...)
    # Ex: df['Time'] = pd.to_datetime(...)
    return df
