import pandas as pd
import os

df = pd.read_csv('../data/ev_sales.csv', dtype=str)

xml_raw = df.to_xml(root_name='ev_sales', row_name='sale', index=False)

xml_path = os.path.join('..', 'data', 'ev_sales.xml')
with open(xml_path, 'w', encoding='utf-8') as f:
    f.write(xml_raw)

print(f"XML created: {xml_path}")


