import requests
import psycopg2
from collections import defaultdict
import os
import concurrent.futures
import requests_cache

# Enable caching
requests_cache.install_cache('f1_cache', expire_after=86400)  # Cache expires after 1 day

DB_NAME = os.getenv("DB_NAME", "f1_stats")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "403800")
DB_HOST = os.getenv("DB_HOST", "localhost")

def create_database():
    conn = psycopg2.connect(f"dbname=postgres user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS f1_stats;")
    cursor.execute("CREATE DATABASE f1_stats;")
    conn.close()

def create_tables():
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id TEXT PRIMARY KEY,
            name TEXT,
            wins INTEGER,
            podiums INTEGER,
            pole_positions INTEGER
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            wins INTEGER DEFAULT 0
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS circuits (
            circuit_id TEXT PRIMARY KEY,
            name TEXT,
            location TEXT,
            country TEXT
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS races (
            race_id SERIAL PRIMARY KEY,
            season INTEGER,
            round INTEGER,
            circuit_id TEXT REFERENCES circuits(circuit_id),
            winner TEXT REFERENCES drivers(driver_id),
            fastest_lap_driver TEXT REFERENCES drivers(driver_id),
            team_id INTEGER REFERENCES teams(team_id)
        );
    """)
    
    conn.commit()
    conn.close()

def insert_driver_stats(drivers):
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")
    cursor = conn.cursor()
    
    for driver_id, stats in drivers.items():
        cursor.execute("""
            INSERT INTO drivers (driver_id, name, wins, podiums, pole_positions)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO UPDATE 
            SET wins = EXCLUDED.wins, podiums = EXCLUDED.podiums, pole_positions = EXCLUDED.pole_positions;
        """, (driver_id, stats['name'], stats['wins'], stats['podiums'], stats.get('pole_positions', 0)))
    
    conn.commit()
    conn.close()

def get_driver_stats():
    url = "http://ergast.com/api/f1/drivers.json?limit=1000"
    response = requests.get(url)
    data = response.json()
    
    drivers = {}
    for driver in data["MRData"]["DriverTable"]["Drivers"]:
        driver_id = driver["driverId"]
        drivers[driver_id] = {
            "name": f"{driver['givenName']} {driver['familyName']}",
            "wins": 0,
            "podiums": 0,
            "pole_positions": 0
        }
    
    return drivers

def fetch_season_data(season):
    drivers = {}
    races_url = f"http://ergast.com/api/f1/{season}/results.json?limit=1000"
    qualifying_url = f"http://ergast.com/api/f1/{season}/qualifying.json?limit=1000"
    
    # Fetch race results
    races_response = requests.get(races_url)
    races_data = races_response.json()
    if "RaceTable" in races_data["MRData"]:
        for race in races_data["MRData"]["RaceTable"]["Races"]:
            for result in race["Results"]:
                driver_id = result["Driver"]["driverId"]
                position = result.get("positionText", "0")
                
                if driver_id not in drivers:
                    drivers[driver_id] = {"wins": 0, "podiums": 0, "pole_positions": 0}
                
                if position.isdigit() and int(position) == 1:
                    drivers[driver_id]["wins"] += 1
                if position.isdigit() and int(position) in [1, 2, 3]:
                    drivers[driver_id]["podiums"] += 1
    
    # Fetch qualifying results
    qualifying_response = requests.get(qualifying_url)
    qualifying_data = qualifying_response.json()
    if "RaceTable" in qualifying_data["MRData"]:
        for race in qualifying_data["MRData"]["RaceTable"]["Races"]:
            if "QualifyingResults" in race:
                pole_driver_id = race["QualifyingResults"][0]["Driver"]["driverId"]
                if pole_driver_id in drivers:
                    drivers[pole_driver_id]["pole_positions"] += 1
    
    return drivers

def get_wins_podiums_poles(drivers):
    seasons = range(2000, 2025)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:  # Limit concurrent requests
        futures = [executor.submit(fetch_season_data, season) for season in seasons]
        for future in concurrent.futures.as_completed(futures):
            season_drivers = future.result()
            for driver_id, stats in season_drivers.items():
                if driver_id in drivers:
                    drivers[driver_id]["wins"] += stats["wins"]
                    drivers[driver_id]["podiums"] += stats["podiums"]
                    drivers[driver_id]["pole_positions"] += stats["pole_positions"]
                else:
                    drivers[driver_id] = stats
    
    return drivers

def get_top_driver(drivers, stat):
    return max(drivers.items(), key=lambda x: x[1][stat])

def get_top_team():
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, (wins * 1.0 / (SELECT COUNT(*) FROM races WHERE team_id IS NOT NULL)) AS win_rate
        FROM teams
        ORDER BY win_rate DESC
        LIMIT 1;
    """)
    result = cursor.fetchone()
    conn.close()
    return result

if __name__ == "__main__":
    create_database()
    create_tables()
    
    drivers = get_driver_stats()
    drivers = get_wins_podiums_poles(drivers)
    insert_driver_stats(drivers)
    
    top_wins = get_top_driver(drivers, "wins")
    top_podiums = get_top_driver(drivers, "podiums")
    top_poles = get_top_driver(drivers, "pole_positions")
    top_team = get_top_team()
    
    print(f"Piloto com mais vitórias: {top_wins[1]['name']} ({top_wins[1]['wins']} vitórias)")
    print(f"Piloto com mais pódios: {top_podiums[1]['name']} ({top_podiums[1]['podiums']} pódios)")
    print(f"Piloto com mais pole positions: {top_poles[1]['name']} ({top_poles[1]['pole_positions']} poles)")
    if top_team:
        print(f"Equipe com maior taxa de vitórias: {top_team[0]} ({top_team[1]:.2%} de vitórias)")
        