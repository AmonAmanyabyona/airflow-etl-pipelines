# pip install overpy psycopg2-binary
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task  
from datetime import datetime
import overpy

# Constants
LIMIT = 50
POSTGRES_CONN_ID = "postgres_default"

def fetch_berlin_cafes(limit: int = LIMIT):
    """
    Query Overpass for cafés within Berlin administrative boundary.
    Returns a list of dicts with core fields and available tags.
    """
    api = overpy.Overpass()
    query = f"""
    [out:json][timeout:25];
    area["name"="Berlin"]["boundary"="administrative"]->.berlin;
    node(area.berlin)["amenity"="cafe"];
    out {limit};
    """
    result = api.query(query)

    cafes = []
    for node in result.nodes[:limit]:
        cafes.append(
            {
                "osm_id": node.id,
                "name": node.tags.get("name", "Unnamed"),
                "lat": node.lat,
                "lon": node.lon,
                "phone": node.tags.get("phone") or node.tags.get("contact:phone"),
                "website": node.tags.get("website") or node.tags.get("contact:website"),
                "addr_street": node.tags.get("addr:street"),
                "addr_housenumber": node.tags.get("addr:housenumber"),
                "addr_postcode": node.tags.get("addr:postcode"),
                "addr_city": node.tags.get("addr:city"),
            }
        )
    return cafes


# Define DAG
with DAG(
    dag_id="berlin_cafes_pipeline",
    start_date=datetime(2025, 11, 28),
    schedule="@daily",
    catchup=False,
) as dag:

    @task()
    def extract_cafes():
        return fetch_berlin_cafes(limit=LIMIT)

    @task()
    def load_to_postgres(rows):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS berlin_cafes (
            osm_id BIGINT PRIMARY KEY,
            name TEXT,
            lat FLOAT,
            lon FLOAT,
            phone TEXT,
            website TEXT,
            addr_street TEXT,
            addr_housenumber TEXT,
            addr_postcode TEXT,
            addr_city TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert rows
        for r in rows:
            cursor.execute("""
            INSERT INTO berlin_cafes (osm_id, name, lat, lon, phone, website,
                                      addr_street, addr_housenumber, addr_postcode, addr_city)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (osm_id) DO NOTHING;
            """, (
                r["osm_id"], r["name"], r["lat"], r["lon"], r["phone"], r["website"],
                r["addr_street"], r["addr_housenumber"], r["addr_postcode"], r["addr_city"]
            ))

        conn.commit()
        cursor.close()

    # DAG workflow
    cafes = extract_cafes()
    load_to_postgres(cafes)
