from turtle import st
from data_fetcher import get_lap_data

# Função para calcular as ultrapassagens
def calculate_overtakes(lap_data):
    overtakes = {}
    previous_positions = {}

    # Para cada volta, vamos analisar as posições
    for lap, data in lap_data.items():
        for driver, position in data.items():
            if driver not in previous_positions:
                previous_positions[driver] = position
                continue

            # Detecta mudança de posição (ultrapassagem)
            if position != previous_positions[driver]:
                if driver not in overtakes:
                    overtakes[driver] = 0
                overtakes[driver] += 1

            # Atualiza posição anterior do piloto
            previous_positions[driver] = position
    return overtakes

# Função para exibir o ranking de ultrapassagens
def show_overtake_ranking(race_id):
    lap_data = get_lap_data(race_id)
    overtakes = calculate_overtakes(lap_data)

    # Ordenando os pilotos pelo número de ultrapassagens
    sorted_overtakes = sorted(overtakes.items(), key=lambda x: x[1], reverse=True)

    # Exibindo o ranking
    st.subheader("Ranking de Ultrapassagens")
    for idx, (driver, overtakes_count) in enumerate(sorted_overtakes, start=1):
        st.write(f"{idx}. {driver}: {overtakes_count} ultrapassagens")
