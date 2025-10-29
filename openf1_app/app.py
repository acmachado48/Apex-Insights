from flask import Flask, render_template
import requests
import pandas as pd

app = Flask(__name__)

def get_race_results(race_id):
    url = f"https://api.openf1.org/v1/races/{race_id}/results"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['results'])
        return df
    else:
        return None
    
def clean_driver_list(driver_entries):
    seen = set()
    cleaned = []

    for entry in driver_entries:
        # Normalize entry (name and country), handle None/null as empty string
        name, country = entry
        key = (name.strip().upper(), (country or "").strip().upper())

        if key not in seen:
            seen.add(key)
            cleaned.append((name.strip(), country.strip() if country else None))

    return cleaned


@app.route('/')
def index():
    race_id = '2024-bahrain'  # Exemplo de ID da corrida
    df = get_race_results(race_id)
    if df is not None:
        top_5 = df[['driver', 'position', 'time']].sort_values(by='position')
        return render_template('index.html', tables=[top_5.to_html(classes='data')], titles=top_5.columns.values)
    else:
        return "Erro ao obter dados."

if __name__ == '__main__':
    app.run(debug=True)
