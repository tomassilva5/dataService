from lxml import etree
from pathlib import Path

xml_path = Path(__file__).parent.parent / "data" / "output.xml" 
tree = etree.parse(str(xml_path))
root = tree.getroot()

filtered_root = etree.Element("ev_sales")

for sale in root.xpath("./sale[region='Austria']"):
    filtered_root.append(sale)

output_path = Path(__file__).parent.parent / "data" / "filtered_sales_Austria.xml"
filtered_tree = etree.ElementTree(filtered_root)
filtered_tree.write(str(output_path), encoding='utf-8', xml_declaration=True)

print(f"Filtered XML created: {output_path}")
