#!/usr/bin/env python3
import os
import time
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values


def wait_for_db(host, port, user, password, db, timeout=60):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
            conn.close()
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise
            print("Waiting for database...", e)
            time.sleep(2)


def main():
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", 5432))
    user = os.getenv("DB_USER", "evuser")
    password = os.getenv("DB_PASSWORD", "evpass")
    db = os.getenv("DB_NAME", "evdb")
    csv_path = os.getenv("CSV_PATH", "ev_sales.csv")

    print(f"Loader: waiting for DB {host}:{port} (db={db})")
    wait_for_db(host, port, user, password, db)

    print(f"Reading CSV from {csv_path}...")
    df = pd.read_csv(csv_path, dtype=str)
    df = df.fillna('')

    def to_int(x):
        try:
            return int(float(x))
        except Exception:
            return None

    df['year_int'] = df.get('year').apply(lambda x: to_int(x) if x else None)

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ev_sales (
      id SERIAL PRIMARY KEY,
      region TEXT,
      category TEXT,
      parameter TEXT,
      mode TEXT,
      powertrain TEXT,
      year INTEGER,
      unit TEXT,
      value TEXT,
      percentage TEXT
    );
    """)
    conn.commit()

    records = []
    for _, row in df.iterrows():
        records.append((
            row.get('region'),
            row.get('category'),
            row.get('parameter'),
            row.get('mode'),
            row.get('powertrain'),
            row.get('year_int'),
            row.get('unit'),
            row.get('value'),
            row.get('percentage'),
        ))

    if records:
        insert_sql = (
            "INSERT INTO ev_sales (region, category, parameter, mode, powertrain, year, unit, value, percentage)"
            " VALUES %s"
        )
        execute_values(cur, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"Inserted {len(records)} rows into ev_sales")
    else:
        print("No records to insert")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
