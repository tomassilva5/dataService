import pandas as pd
from pathlib import Path

# ========== PATH DATA ==========

# VERSÃO DOCKER:
DATA_DIR = Path("/app/data")

# PARA TESTAR SEM DOCKER:
# BASE_DIR = Path(__file__).resolve().parent.parent  # .../TP2-B/server
# DATA_DIR = BASE_DIR / "data"

CSV_PATH = DATA_DIR / "ev_sales.csv"
XML_PATH = DATA_DIR / "output.xml"


def convert_csv_to_xml():
    df = pd.read_csv(CSV_PATH, dtype=str)

    xml_raw = df.to_xml(
        root_name="ev_sales",
        row_name="ev_sale",
        index=False
    )

    with open(XML_PATH, "w", encoding="utf-8") as f:
        f.write(xml_raw)

    print(f"XML created: {XML_PATH}")


if __name__ == "__main__":
    convert_csv_to_xml()
