import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# ... (funções format_time, highlight_max, etc.) ...

def calculate_points(row):
    # ... (lógica exata como está hoje) ...
    return points

def get_driver_performance(df):
    """
    Recebe o DataFrame limpo e calcula o desempenho dos pilotos.
    Retorna um novo DataFrame com os resultados.
    """
    df_copy = df.copy()
    df_copy['pontos'] = df_copy.apply(calculate_points, axis=1)
    # ... (toda a lógica de 'groupby', 'agg', etc.) ...
    return df_pilotos 

def plot_driver_performance_grid(df_pilotos):
    """
    Recebe o DataFrame de desempenho e retorna uma Figura Matplotlib.
    """
    # ... (toda a sua lógica de plotagem) ...
    # Em vez de plt.savefig(...), retorne a figura
    return fig # Onde 'fig' é o objeto Figure do Matplotlib