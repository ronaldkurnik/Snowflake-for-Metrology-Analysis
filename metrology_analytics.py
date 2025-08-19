# This script connects to a live Snowflake account, creates a table,
# ingests simulated metrology data, and runs an analytical query.

import snowflake.connector
import pandas as pd
import random
from datetime import datetime

# --- Your Snowflake Account Credentials ---
# IMPORTANT: Replace these with your actual details.
# For production, use environment variables or a key-pair authentication.
SNOWFLAKE_USER = "UserName"
SNOWFLAKE_PASSWORD = "UserPassword"
SNOWFLAKE_ACCOUNT = "UserAccount"
SNOWFLAKE_WAREHOUSE = "METROLOGY_WH"
SNOWFLAKE_DATABASE = "FAB_DATA_ANALYTICS"
SNOWFLAKE_SCHEMA = "METROLOGY"

def connect_to_snowflake():
    """
    Establishes a connection to Snowflake and returns the connection object.
    Handles potential connection errors with a try-except block.
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("✅ Connection to Snowflake successful!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        return None


def use_context(conn, database, schema):
    """
    Explicitly sets the session context to the specified database and schema.
    This is a best practice to avoid 'no current schema' errors.
    
    NOTE: Identifiers are enclosed in double quotes to handle case-sensitivity.
    """
    try:
        with conn.cursor() as cur:
            # Enclose the database and schema names in double quotes.
            cur.execute(f'USE DATABASE "{database}"')
            cur.execute(f'USE SCHEMA "{schema}"')
        print(f"✅ Session context set to {database}.{schema}")
    except Exception as e:
        print(f"❌ Error setting context: {e}")
        return False
    return True


def create_metrology_table(conn):
    """
    Creates a sample metrology data table if it doesn't already exist.
    """
    query = """
    CREATE OR REPLACE TABLE wafer_metrology (
        WAFER_ID VARCHAR(255),
        LOT_ID VARCHAR(255),
        PROCESS_STEP VARCHAR(255),
        EVENT_TIMESTAMP TIMESTAMP_NTZ,
        LINE_WIDTH_NM FLOAT,
        DEFECT_COUNT INTEGER
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        print("✅ Table 'wafer_metrology' created or replaced.")
    except Exception as e:
        print(f"❌ Error creating table: {e}")


def generate_wafer_metrology_data(wafer_id, lot_id, num_steps=10):
    """
    Generates a list of tuples simulating metrology data for a single wafer,
    ready for insertion into Snowflake.
    """
    data = []
    for step in range(num_steps):
        process_step = f"Step_{step:02d}"
        measurement = round(random.uniform(50.5, 51.5), 2)
        defect_count = random.randint(0, 5)
        data.append((wafer_id, lot_id, process_step, datetime.utcnow(), measurement, defect_count))
    return data


def ingest_data_into_snowflake(conn, data):
    """
    Ingests a list of data records into the Snowflake table using `executemany`
    for efficient bulk insertion.
    """
    query = """
    INSERT INTO wafer_metrology (
        WAFER_ID,
        LOT_ID,
        PROCESS_STEP,
        EVENT_TIMESTAMP,
        LINE_WIDTH_NM,
        DEFECT_COUNT
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    try:
        with conn.cursor() as cur:
            cur.executemany(query, data)
        print(f"✅ Successfully ingested {len(data)} records.")
    except Exception as e:
        print(f"❌ Error ingesting data: {e}")


def run_metrology_analysis(conn, lot_id):
    """
    Runs an analytical SQL query on the ingested data and displays the results
    in a readable Pandas DataFrame.
    """
    print(f"\n--- Running Analysis for Lot '{lot_id}' ---")
    query = f"""
    SELECT
        PROCESS_STEP,
        AVG(DEFECT_COUNT) AS AVG_DEFECTS,
        AVG(LINE_WIDTH_NM) AS AVG_LINE_WIDTH
    FROM
        wafer_metrology
    WHERE
        LOT_ID = '{lot_id}'
    GROUP BY
        PROCESS_STEP
    ORDER BY
        AVG_DEFECTS DESC;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            # Fetch results and load into a Pandas DataFrame for easy viewing
            df = cur.fetch_pandas_all()
            print("📈 Simulated Analysis Results:")
            print(df)
    except Exception as e:
        print(f"❌ Error running analysis query: {e}")


# --- Main Execution Flow ---
if __name__ == "__main__":
    conn = connect_to_snowflake()
    
    if conn:
        # Explicitly set the session context after a successful connection.
        if use_context(conn, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA):
            create_metrology_table(conn)
            
            # 1. Generate and ingest data for multiple wafers in a lot.
            print("\n--- Ingesting Simulated Data ---")
            lot_id = "LOT_A_2025"
            num_wafers_to_simulate = 5
            
            for i in range(num_wafers_to_simulate):
                wafer_id = f"WAFER_{i+1:02d}"
                wafer_data = generate_wafer_metrology_data(wafer_id, lot_id)
                ingest_data_into_snowflake(conn, wafer_data)
            
            # 2. Run the analytical query on the ingested data.
            run_metrology_analysis(conn, lot_id)
            
            # 3. Close the connection.
            conn.close()

            print("\nConnection closed. Have a great day! 👋")

