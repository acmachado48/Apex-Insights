import plotly.graph_objects as go
from data_fetcher import get_live_position, get_live_positions

# Função para mostrar o grid ao vivo com o mapa da pista
def show_live_grid(race_id):
    positions = get_live_positions(race_id)
    
    # Criando o layout da pista (exemplo simples com círculos representando posições)
    track_layout = go.Figure()

    # Adicionando a pista como um caminho (exemplo simplificado)
    track_layout.add_trace(go.Scatter(
        x=[0, 1, 2, 3, 4],  # Coordenadas X da pista
        y=[0, 1, 0, -1, 0],  # Coordenadas Y da pista
        mode="lines+markers",  # Exibe linha e marcações
        line=dict(color="gray", width=2)
    ))

    # Adicionando as posições dos carros
    for position in positions:
        driver = position['driver']
        car_position = position['position']
        car_x = car_position['x']  # Ajuste conforme a API real
        car_y = car_position['y']  # Ajuste conforme a API real

        track_layout.add_trace(go.Scatter(
            x=[car_x],
            y=[car_y],
            mode='markers',
            marker=dict(size=10, color='blue'),
            name=driver
        ))

    # Layout da figura (tamanho da pista, título, etc.)
    track_layout.update_layout(
        title="🏎️ Grid ao Vivo",
        xaxis=dict(range=[-1, 5], title="Coordenada X da Pista"),
        yaxis=dict(range=[-2, 2], title="Coordenada Y da Pista"),
        showlegend=True
    )

    # Exibindo o gráfico
    track_layout.show()
