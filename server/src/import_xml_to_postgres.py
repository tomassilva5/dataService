import os
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from lxml import etree  # para ler o XML


def wait_for_db(host, port, user, password, db, timeout=60):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=db
            )
            conn.close()
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise
            print("Waiting for database...", e)
            time.sleep(2)


def to_int(x):
    try:
        return int(x)
    except Exception:
        return None


def main():
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", 5432))
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "123456789")
    db = os.getenv("DB_NAME", "db_TP2B")

    xml_path = Path(os.getenv("XML_PATH", "/app/data/output.xml"))

    print(f"Loader (XML): waiting for DB {host}:{port} (db={db})")
    wait_for_db(host, port, user, password, db)

    if not xml_path.exists():
        raise FileNotFoundError(f"XML file not found: {xml_path}")

    print(f"Reading XML from {xml_path}...")

    tree = etree.parse(str(xml_path))
    root = tree.getroot()  # <ev_sales>

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=db
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ev_sales (
          id SERIAL PRIMARY KEY,
          region      TEXT,
          category    TEXT,
          parameter   TEXT,
          mode        TEXT,
          powertrain  TEXT,
          year        INTEGER,
          unit        TEXT,
          value       INTEGER,
          percentage  TEXT
        );
        """
    )
    conn.commit()

    records = []
    for sale in root.findall("ev_sale"):
        region = sale.findtext("region") or ""
        category = sale.findtext("category") or ""
        parameter = sale.findtext("parameter") or ""
        mode = sale.findtext("mode") or ""
        powertrain = sale.findtext("powertrain") or ""
        year = to_int(sale.findtext("year"))
        unit = sale.findtext("unit") or ""
        value = to_int(sale.findtext("value"))
        percentage = sale.findtext("percentage") or ""

        records.append(
            (
                region,
                category,
                parameter,
                mode,
                powertrain,
                year,
                unit,
                value,
                percentage,
            )
        )

    if records:
        insert_sql = """
            INSERT INTO ev_sales
            (region, category, parameter, mode, powertrain, year, unit, value, percentage)
            VALUES %s
        """
        execute_values(cur, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"Inserted {len(records)} rows into ev_sales from XML")
    else:
        print("No records to insert from XML")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
