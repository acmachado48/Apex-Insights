import requests

BASE_URL = "[https://ergast.com/api/f1](https://ergast.com/api/f1)"

def get_world_champion_by_year(year):
    """
    Busca o campeão mundial de um ano específico.
    """
    try:
        url = f"{BASE_URL}/{year}/driverStandings/1.json"
        response = requests.get(url)
        response.raise_for_status() # Lança erro se a requisição falhar
        
        data = response.json()
        
        # Extrai a informação
        standings = data['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings'][0]
        driver = standings['Driver']
        constructor = standings['Constructors'][0]
        
        return {
            "year": year,
            "driver": f"{driver['givenName']} {driver['familyName']}",
            "nationality": driver['nationality'],
            "constructor": constructor['name'],
            "points": standings['points']
        }
    except Exception as e:
        print(f"Erro ao buscar campeão: {e}")
        return None

def get_fastest_lap_by_race(year, round_num):
    """
    Busca a volta mais rápida de uma corrida específica.
    """
    try:
        # Nota: A API é um pouco complexa para voltas rápidas. 
        # Esta consulta busca o resultado da corrida e filtra pela volta mais rápida.
        url = f"{BASE_URL}/{year}/{round_num}/results.json?limit=1"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # A API pode não ter dados de volta rápida para todas as corridas
        results = data['MRData']['RaceTable']['Races'][0]['Results']
        
        # Encontra o piloto com a volta mais rápida (rank 1)
        for result in results:
            if result.get('FastestLap') and result['FastestLap'].get('rank') == '1':
                driver = result['Driver']
                fastest_lap = result['FastestLap']
                return {
                    "year": year,
                    "round": round_num,
                    "driver": f"{driver['givenName']} {driver['familyName']}",
                    "time": fastest_lap['Time']['time'],
                    "avg_speed": fastest_lap['AverageSpeed']['speed'] + " " + fastest_lap['AverageSpeed']['units']
                }
        return {"error": "Dados de volta mais rápida não encontrados para esta corrida."}
        
    except Exception as e:
        print(f"Erro ao buscar volta rápida: {e}")
        return None
