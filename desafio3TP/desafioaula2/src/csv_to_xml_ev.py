import pandas as pd
import os
import xml.etree.ElementTree as ET

CSV_PATH = "../data/ev_sales.csv"       
XML_PATH = "../data/output.xml"
XSD_PATH = "../data/output.xsd"

file_name = os.path.basename(CSV_PATH).split('.')[0]

ROOT_NAME = file_name

if ROOT_NAME.endswith('s'):
    ROW_NAME = ROOT_NAME[:-1]     
else:
    ROW_NAME = "row"              

df = pd.read_csv(CSV_PATH, dtype=str)

xml_string = df.to_xml(
    root_name=ROOT_NAME,
    row_name=ROW_NAME,
    index=False
)

with open(XML_PATH, "w", encoding="utf-8") as f:
    f.write(xml_string)

print(f"XML created: {XML_PATH}")


def infer_xsd_type(value: str) -> str:
    if value is None or value == "":
        return "xs:string"
    try:
        int(value)
        return "xs:integer"
    except:
        pass
    try:
        float(value)
        return "xs:float"
    except:
        pass
    return "xs:string"

def indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            indent(child, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def generate_xsd(df: pd.DataFrame, xsd_path: str):
    xs = "http://www.w3.org/2001/XMLSchema"

    schema = ET.Element("xs:schema", attrib={
        "xmlns:xs": xs,
        "elementFormDefault": "qualified"
    })

    root_element = ET.SubElement(schema, "xs:element", name=ROOT_NAME)
    root_complex = ET.SubElement(root_element, "xs:complexType")
    root_sequence = ET.SubElement(root_complex, "xs:sequence")

    row_element = ET.SubElement(root_sequence, "xs:element", name=ROW_NAME, maxOccurs="unbounded")
    row_complex = ET.SubElement(row_element, "xs:complexType")
    row_sequence = ET.SubElement(row_complex, "xs:sequence")

    for col in df.columns:
        non_empty = df[col].dropna()
        sample = str(non_empty.iloc[0]) if not non_empty.empty else ""
        xsd_type = infer_xsd_type(sample)
        ET.SubElement(row_sequence, "xs:element", name=col, type=xsd_type)
        
    indent(schema)
    tree = ET.ElementTree(schema)
    tree.write(xsd_path, encoding="utf-8", xml_declaration=True)


generate_xsd(df, XSD_PATH)
print(f"XSD created: {XSD_PATH}")
