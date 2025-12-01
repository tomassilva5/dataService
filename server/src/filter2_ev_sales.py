from lxml import etree

tree = etree.parse('ev_sales.xml')
root = tree.getroot()

sales_ev_stock = root.xpath("./sale[parameter='EV stock']")

filtered_root = etree.Element("ev_sales")

for sale in sales_ev_stock:
    filtered_root.append(sale)

filtered_tree = etree.ElementTree(filtered_root)
filtered_tree.write('ev_sales_EV_stock.xml', encoding='utf-8', xml_declaration=True)

print("Filtered XML created: ev_sales_EV_stock.xml")




