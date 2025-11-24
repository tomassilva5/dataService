from lxml import etree

tree = etree.parse('ev_sales.xml')
root = tree.getroot()

filtered_root = etree.Element("ev_sales")

for sale in root.xpath("./sale[region='Austria']"):
    filtered_root.append(sale)

filtered_tree = etree.ElementTree(filtered_root)
filtered_tree.write('ev_sales_Austria.xml', encoding='utf-8', xml_declaration=True)

print("Filtered XML created: ev_sales_Austria.xml")

