import os
import time
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from lxml import etree as ET 
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def to_int(x):
    try: 
        return int(x)
    except Exception: 
        return None

def to_float(x):
    try: 
        if isinstance(x, str):
            x = x.replace(',', '.')
        return float(x)
    except Exception: 
        return None


def infer_sql_type(column_name: str) -> str:
    name_lower = column_name.lower().replace(' ', '_')
    if 'year' in name_lower or 'id' in name_lower or 'age' in name_lower or 'quantity' in name_lower:
        return "INTEGER"
    if 'amount' in name_lower or 'price' in name_lower or 'cost' in name_lower or 'profit' in name_lower or 'revenue' in name_lower or 'distance' in name_lower:
        return "NUMERIC"
    return "TEXT"


def wait_for_db(host, port, user, password, db, timeout=60):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
            conn.close()
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise TimeoutError("Database did not become available within the timeout.") from e
            logging.warning(f"Waiting for database... Retrying in 2s. ({e})")
            time.sleep(2)



def main(column_names: list[str], table_name: str):
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", 5432))
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "123456789")
    db = os.getenv("DB_NAME", "db_TP2B")
    
    xml_path = Path(os.getenv("XML_PATH", "/app/data/output.xml")) 

    logging.info(f"Loader (XML): waiting for DB {host}:{port} (db={db})")
    wait_for_db(host, port, user, password, db)

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    cur = conn.cursor()

    col_defs = []
    
    for col_name in column_names:
        sql_type = infer_sql_type(col_name)
        safe_col_name = f'"{col_name}"'.replace(' ', '_') 
        col_defs.append(f"{safe_col_name} {sql_type}")

    cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
    conn.commit()
    
    create_sql = f"""
        CREATE TABLE "{table_name}" (
          id SERIAL PRIMARY KEY,
          {', '.join(col_defs)}
        );
    """
    cur.execute(create_sql)
    conn.commit()
    logging.info(f"Table '{table_name}' created/recreated dynamically.")

    
    if not xml_path.exists():
        logging.error(f"XML file not found: {xml_path}")
        return 

    logging.info(f"Reading XML from {xml_path}...")
    tree = ET.parse(str(xml_path))
    root = tree.getroot() 
    
    records = []
    
    for sale in root.findall("row"): 
        row_values = []
        for col_name in column_names:
            value = sale.findtext(col_name)
            
            sql_type = infer_sql_type(col_name)

            if sql_type == "INTEGER":
                row_values.append(to_int(value))
            elif sql_type == "NUMERIC":
                row_values.append(to_float(value))
            else:
                row_values.append(value or "")
                
        records.append(tuple(row_values))

    if records:
        safe_column_list = [f'"{col.replace(" ", "_")}"' for col in column_names]
        
        insert_sql = f"""
            INSERT INTO "{table_name}" ({', '.join(safe_column_list)})
            VALUES %s
        """
        execute_values(cur, insert_sql, records, page_size=1000)
        conn.commit()
        logging.info(f"Inserted {len(records)} rows into table '{table_name}'.")
    else:
        logging.warning("No records to insert from XML")

    cur.close()
    conn.close()


if __name__ == "__main__":
    logging.error("This module should be executed via the gRPC server's ETL pipeline.")