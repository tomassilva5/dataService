import pandas as pd
from pathlib import Path
from typing import List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


DATA_DIR = Path("/app/data") 
XML_PATH = DATA_DIR / "output.xml" 


def convert_csv_to_xml(input_csv_path: Path) -> List[str]:
    if not input_csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at: {input_csv_path}")

    df = pd.read_csv(input_csv_path) 
    
    column_names = list(df.columns)

    xml_raw = df.to_xml(
        root_name="dataset",
        row_name="row",
        index=False
    )

    try:
        with open(XML_PATH, "w", encoding="utf-8") as f:
            f.write(xml_raw)
        logging.info(f"XML created: {XML_PATH}")
    except Exception as e:
        logging.error(f"Error writing XML file: {e}")
        raise

    return column_names

if __name__ == "__main__":
    DEFAULT_CSV_PATH = DATA_DIR / "test.csv" 
    try:
        convert_csv_to_xml(DEFAULT_CSV_PATH)
    except FileNotFoundError as e:
        logging.error(f"Could not run local test: {e}")