from pathlib import Path
import xml.etree.ElementTree as ET
import xmlschema

DATA_DIR = Path("/app/data")
XML_PATH = DATA_DIR / "output.xml"
XSD_PATH = DATA_DIR / "output.xsd"


def indent(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def generate_xsd_from_xml():
    tree = ET.parse(str(XML_PATH))
    root = tree.getroot()          # <ev_sales>
    row = root.find("./*")         # primeiro filho, ex.: <ev_sale>

    if row is None:
        raise RuntimeError("XML sem elementos de linha (ev_sale).")

    xs = "http://www.w3.org/2001/XMLSchema"

    schema = ET.Element("xs:schema", attrib={
        "xmlns:xs": xs,
        "elementFormDefault": "qualified"
    })

    # raiz ev_sales
    root_element = ET.SubElement(schema, "xs:element", name=root.tag)
    root_complex = ET.SubElement(root_element, "xs:complexType")
    root_sequence = ET.SubElement(root_complex, "xs:sequence")

    # elemento repetido ev_sale
    row_element = ET.SubElement(
        root_sequence,
        "xs:element",
        name=row.tag,
        maxOccurs="unbounded",
    )
    row_complex = ET.SubElement(row_element, "xs:complexType")
    row_sequence = ET.SubElement(row_complex, "xs:sequence")

    # campos simples dentro de ev_sale
    for child in row:
        name = child.tag
        if name == "year":
            xsd_type = "xs:integer"
        else:
            xsd_type = "xs:string"
        ET.SubElement(row_sequence, "xs:element", name=name, type=xsd_type)

    indent(schema)
    tree_xsd = ET.ElementTree(schema)
    tree_xsd.write(str(XSD_PATH), encoding="utf-8", xml_declaration=True)
    print(f"XSD created from XML: {XSD_PATH}")


def validate_xml_with_xsd() -> bool:
    schema = xmlschema.XMLSchema(str(XSD_PATH))
    is_valid = schema.is_valid(str(XML_PATH))
    print("XML is validated :", is_valid)
    return is_valid


if __name__ == "__main__":
    generate_xsd_from_xml()
    validate_xml_with_xsd()
