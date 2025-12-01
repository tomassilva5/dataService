import xmlschema

XSD_FILE = "ev_sales.xsd"
XML_FILE = "ev_sales.xml"

def main():
    schema = xmlschema.XMLSchema(XSD_FILE)
    is_valid = schema.is_valid(XML_FILE)
    print("XML is validated:", is_valid)

if __name__ == "__main__":
    main()


