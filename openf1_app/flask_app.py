from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import requests
import os
import pandas as pd
import io

app = Flask(__name__) 
socketio = SocketIO(app)

# URLs dos endpoints da OpenF1 API
drivers_url = "https://api.openf1.org/v1/drivers"
sessions_url = "https://api.openf1.org/v1/sessions"
positions_url = "https://api.openf1.org/v1/positions?csv=true"
lap_times_url = "https://api.openf1.org/v1/lap_times?driver_number=44&csv=true"
results_base_url = "https://api.openf1.org/v1/results?csv=true"

# Funções para buscar dados da API OpenF1

def fetch_drivers():
    response = requests.get(drivers_url)
    if response.status_code == 200:
        return response.json()
    else:
        return []

def fetch_sessions():
    response = requests.get(sessions_url)
    if response.status_code == 200:
        return response.json()
    else:
        return []

def fetch_positions():
    try:
        response = requests.get(positions_url)
        if response.status_code == 200:
            positions_df = pd.read_csv(io.StringIO(response.text))
            return positions_df.to_dict(orient="records")
    except Exception as e:
        print(f"Erro ao buscar posições: {e}")
    return []

    
def clean_driver_list(driver_entries):
    seen = set()
    cleaned = []

    for entry in driver_entries:
        name = entry.get("full_name", "").strip()
        country = (entry.get("country_code") or "").strip()
        key = (name.upper(), country.upper())

        if key not in seen:
            seen.add(key)
            cleaned.append(entry)

    return cleaned


# Rota principal para renderizar a página com os dados iniciais
@app.route('/')
def index():
    drivers = fetch_drivers()
    sessions = fetch_sessions()
    return render_template('index.html', drivers=drivers, sessions=sessions)

# Evento para enviar dados de pilotos em tempo real via SocketIO
@socketio.on('connect')
def handle_connect():
    print('Client connected')
    drivers = fetch_drivers()
    cleaned_drivers = clean_driver_list(drivers)
    emit('drivers_data', cleaned_drivers)


# Evento para enviar dados de posições em tempo real
@socketio.on('request_positions')
def handle_positions_request():
    positions = fetch_positions()
    emit('positions_data', positions)

def fetch_race_results(driver_number):
    url = f"{results_base_url}&driver_number={driver_number}"
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))
        return df[['session_name', 'date', 'position', 'status']].to_dict(orient='records')
    return []    

@socketio.on('request_race_results')
def handle_race_results(data):
    driver_number = data.get("driver_number")
    results = fetch_race_results(driver_number)
    emit('race_results_data', results)    

# Inicia o servidor Flask
if __name__ == '__main__':
    socketio.run(app, debug=True)
