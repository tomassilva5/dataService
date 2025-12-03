from pathlib import Path
import xml.etree.ElementTree as ET
import xmlschema
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    try:
        if not XML_PATH.exists():
            logging.error(f"Cannot generate XSD: XML file not found at {XML_PATH}")
            return False
            
        tree = ET.parse(str(XML_PATH))
        root = tree.getroot() 
        row = root.find("row") 
        
        if row is None:
            logging.error("XML does not contain generic <row> elements. Cannot generate XSD.")
            return False

        xs = "http://www.w3.org/2001/XMLSchema"

        schema = ET.Element("xs:schema", attrib={
            "xmlns:xs": xs,
            "elementFormDefault": "qualified"
        })

        root_element = ET.SubElement(schema, "xs:element", name="dataset")
        root_complex = ET.SubElement(root_element, "xs:complexType")
        root_sequence = ET.SubElement(root_complex, "xs:sequence")

        row_element = ET.SubElement(
            root_sequence,
            "xs:element",
            name="row",
            maxOccurs="unbounded",
            minOccurs="0"
        )
        row_complex = ET.SubElement(row_element, "xs:complexType")
        row_sequence = ET.SubElement(row_complex, "xs:sequence")

        for child in row:
            name = child.tag
            xsd_type = "xs:string" 
            ET.SubElement(row_sequence, "xs:element", name=name, type=xsd_type)

        indent(schema)
        
        tree_xsd = ET.ElementTree(schema)
        tree_xsd.write(str(XSD_PATH), encoding="utf-8", xml_declaration=True)
        
        logging.info(f"XSD created dynamically: {XSD_PATH}")
        return True
        
    except Exception as e:
        logging.error(f"Error during XSD generation: {e}")
        return False


def validate_xml_with_xsd() -> bool:
    if not XSD_PATH.exists():
        logging.error("[Error] XSD not found for validation. Skipping validation.")
        return False
        
    try:
        schema = xmlschema.XMLSchema(str(XSD_PATH))
        is_valid = schema.is_valid(str(XML_PATH))
        logging.info(f"XML is validated : {is_valid}")
        return is_valid
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        return False


if __name__ == "__main__":
    generate_xsd_from_xml() 
    validate_xml_with_xsd()