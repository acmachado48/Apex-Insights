import requests
import psycopg2
from collections import defaultdict
import os
import concurrent.futures
import requests_cache
from psycopg2 import pool
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # Importação corrigida

# Enable caching
requests_cache.install_cache('f1_cache', expire_after=86400)  # Cache expires after 1 day

DB_NAME = os.getenv("DB_NAME", "f1_stats")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "403800")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Initialize connection pool
connection_pool = pool.SimpleConnectionPool(1, 10, f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")

def get_connection():
    return connection_pool.getconn()

def release_connection(conn):
    connection_pool.putconn(conn)

def create_database():
    try:
        conn = psycopg2.connect(f"dbname=postgres user={DB_USER} password={DB_PASSWORD} host={DB_HOST}")
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the database already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'f1_stats';")
        if cursor.fetchone():
            print("Database already exists. Skipping creation.")
            return

        cursor.execute("DROP DATABASE IF EXISTS f1_stats;")
        cursor.execute("CREATE DATABASE f1_stats;")
        print("Database created successfully!")

        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

def create_tables():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
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
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")

def insert_driver_stats(drivers, driver_names):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for driver_id, stats in drivers.items():
                    name = driver_names.get(driver_id, "Desconhecido")  # Get name from mapping
                    cursor.execute("""
                        INSERT INTO drivers (driver_id, name, wins, podiums, pole_positions)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (driver_id) DO UPDATE 
                        SET wins = EXCLUDED.wins, podiums = EXCLUDED.podiums, pole_positions = EXCLUDED.pole_positions;
                    """, (driver_id, name, stats['wins'], stats['podiums'], stats.get('pole_positions', 0)))

                conn.commit()
        print("✅ Driver stats inserted successfully!")
    except Exception as e:
        print(f"❌ Error inserting driver stats: {e}")

def get_driver_stats():
    try:
        url = "http://ergast.com/api/f1/drivers.json?limit=1000"
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        driver_names = {}  # Dictionary to map driver_id to name
        drivers = {}  # Dictionary to store stats using driver_id as key

        for driver in data["MRData"]["DriverTable"]["Drivers"]:
            driver_id = driver["driverId"]
            name = f"{driver['givenName']} {driver['familyName']}"
            driver_names[driver_id] = name  # Map driver_id to name
            drivers[driver_id] = {  # Initialize stats
                "wins": 0,
                "podiums": 0,
                "pole_positions": 0
            }

        print("Driver stats fetched successfully!")
        return drivers, driver_names
    except Exception as e:
        print(f"Error fetching driver stats: {e}")
        return {}, {}


def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_season_data(season, driver_names):
    try:
        session = create_session()
        races_url = f"http://ergast.com/api/f1/{season}/results.json?limit=1000"
        qualifying_url = f"http://ergast.com/api/f1/{season}/qualifying.json?limit=1000"

        # Fetch race results
        races_response = session.get(races_url)
        races_response.raise_for_status()
        races_data = races_response.json()

        # Fetch qualifying results
        qualifying_response = session.get(qualifying_url)
        qualifying_response.raise_for_status()
        qualifying_data = qualifying_response.json()

# Process data
        drivers = {}
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

        if "RaceTable" in qualifying_data["MRData"]:
            for race in qualifying_data["MRData"]["RaceTable"]["Races"]:
                if "QualifyingResults" in race and race["QualifyingResults"]:
                    try:
                        pole_driver_id = race["QualifyingResults"][0]["Driver"]["driverId"]
                        if pole_driver_id in drivers:
                            drivers[pole_driver_id]["pole_positions"] += 1
                        else:
                            drivers[pole_driver_id] = {"wins": 0, "podiums": 0, "pole_positions": 1}
                    except (IndexError, KeyError) as e:
                        print(f"Erro ao processar pole position para a corrida {race.get('raceName', 'desconhecida')}: {e}")
                        print(f"Data fetched for season {season}!")
        return drivers
    except Exception as e:
        print(f"Error fetching data for season {season}: {e}")
        return {}


def get_wins_podiums_poles(drivers, driver_names):
    try:
        seasons = range(2000, 2025)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_season_data, season, driver_names) for season in seasons]
            for future in concurrent.futures.as_completed(futures):
                try:
                    season_drivers = future.result()
                    for driver_id, stats in season_drivers.items():
                        if driver_id in drivers:
                            drivers[driver_id]["wins"] += stats["wins"]
                            drivers[driver_id]["podiums"] += stats["podiums"]
                            drivers[driver_id]["pole_positions"] += stats["pole_positions"]
                        else:
                            drivers[driver_id] = stats
                    gc.collect()  # Manually trigger garbage collection
                except Exception as e:
                    print(f"Error processing season data: {e}")

        print("Wins, podiums, and poles calculated successfully!")
        return drivers
    except Exception as e:
        print(f"Error calculating wins, podiums, and poles: {e}")
        return {}

def get_top_driver(drivers, driver_names, stat):
    try:
        top_driver_id, stats = max(drivers.items(), key=lambda x: x[1][stat])
        name = driver_names.get(top_driver_id, "Desconhecido")  # Get name from mapping
        return top_driver_id, name, stats[stat]
    except Exception as e:
        print(f"Error finding top driver: {e}")
        return None, None, None

def get_top_team():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT t.name, (t.wins * 1.0 / total_races) AS win_rate
                    FROM teams t
                    JOIN (
                        SELECT COUNT(*) AS total_races
                        FROM races
                        WHERE winner IS NOT NULL
                    ) AS race_counts ON 1=1
                    ORDER BY win_rate DESC
                    LIMIT 1;
                """)
                result = cursor.fetchone()
        print("Top team fetched successfully!")
        return result
    except Exception as e:
        print(f"Error fetching top team: {e}")
        return None

if __name__ == "__main__":
    create_database()
    create_tables()

    drivers, driver_names = get_driver_stats()
    drivers = get_wins_podiums_poles(drivers, driver_names)  # Pass driver_names here
    insert_driver_stats(drivers, driver_names)

    top_wins_id, top_wins_name, top_wins_count = get_top_driver(drivers, driver_names, "wins")
    top_podiums_id, top_podiums_name, top_podiums_count = get_top_driver(drivers, driver_names, "podiums")
    top_poles_id, top_poles_name, top_poles_count = get_top_driver(drivers, driver_names, "pole_positions")
    top_team = get_top_team()

    if top_wins_id:
        print(f"Piloto com mais vitórias: {top_wins_name} ({top_wins_count} vitórias)")
    else:
        print("No valid driver found for most wins.")

    if top_podiums_id:
        print(f"Piloto com mais pódios: {top_podiums_name} ({top_podiums_count} pódios)")
    else:
       print("No valid driver found for most podiums.")

    if top_poles_id:
        print(f"Piloto com mais pole positions: {top_poles_name} ({top_poles_count} poles)")
    else:
        print("No valid driver found for most pole positions.")

    if top_team:
        print(f"Equipe com maior taxa de vitórias: {top_team[0]} ({top_team[1]:.2%} de vitórias)")
    else:
        print("No valid team found for highest win rate.")