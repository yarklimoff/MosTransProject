import psycopg2
import pandas as pd
import re
from psycopg2 import OperationalError


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")

connection = create_connection("buildings", "postgres", "29072001", "localhost", "5432")

create_bus_stations_table = """
CREATE TABLE IF NOT EXISTS bus_stations (
    ID INTEGER PRIMARY KEY,
    Name TEXT,
    Longitude_WGS84 REAL,
    Latitude_WGS84 REAL,
    Street TEXT,
    AdmArea TEXT,
    District TEXT,
    RouteNumbers TEXT,
    StationName TEXT,
    Direction TEXT,
    Pavilion TEXT,
    OperatingOrgName TEXT,
    EntryState TEXT,
    global_id TEXT,
    geodata_center POINT,
    geoarea TEXT
);
"""

create_test_complexes_table = """
CREATE TABLE IF NOT EXISTS test_complexes (
    name TEXT,
    geodata_center POINT
);
"""

execute_query(connection, create_bus_stations_table)
execute_query(connection, create_test_complexes_table)

stops = pd.read_csv("bus_stations.csv")
complexes = pd.read_csv("test_complexes.csv")

stops_array = []
complexes_array = []

for index, row in stops.iterrows():
    temp = row.values
    m = re.search('\[(\S*\s*\S*)\]', row.values[-2])
    m = m.group(0)[1:-1].split(",")
    temp[-2] = f"({m[0]}, {m[1]})"
    stops_array.append(tuple(temp))

for index, row in complexes.iterrows():
    temp = row.values
    m = re.search('\(\S*\s*\S*\)', row.values[-1])
    m = m.group(0)[1: -1].split()
    temp[-1] = f"({m[0]}, {m[1]})"
    complexes_array.append(tuple(temp))

stops_records = ", ".join(["%s"] * len(stops_array))
complexes_records = ", ".join(["%s"] * len(complexes_array))

insert_query_stops = (
    f"INSERT INTO bus_stations VALUES {stops_records}"
)

insert_query_complexes = (
    f"INSERT INTO test_complexes VALUES {complexes_records}"
)


connection.autocommit = True
cursor = connection.cursor()
#cursor.execute(insert_query_stops, stops_array)
#cursor.execute(insert_query_complexes, complexes_array)

#cursor.execute(f"ALTER TABLE bus_stations ALTER COLUMN geodata_center TYPE geometry(Point,4326) USING ST_SetSRID(geodata_center::GEOMETRY(POINT), 4326);")
poi = (37.49988, 55.767798)
cursor.execute("""\
    SELECT id, ST_AsGeoJSON(geodata_center), ST_Distance(geodata_center, poi)
FROM bus_stations, (SELECT ST_MakePoint(%s, %s)::geography AS poi) AS f
WHERE ST_DWithin(geodata_center, poi, 1000);""", poi)

for row in cursor.fetchall():
    print(row)