import plotly.express as px

def plot_positions(data, lap_range, selected_pilots):
    filtered = data[
        (data['lap'] >= lap_range[0]) &
        (data['lap'] <= lap_range[1]) &
        (data['driver_number'].isin(selected_pilots))
    ]

    fig = px.line(
        filtered,
        x="lap",
        y="position",
        color="driver_number",
        title="Evolução de Posição por Volta",
        labels={"lap": "Volta", "position": "Posição"}
    )

    fig.update_yaxes(autorange="reversed")  # 1º lugar no topo
    fig.update_layout(height=600)

    return fig
