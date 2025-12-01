import grpc
from concurrent import futures
from pathlib import Path
from lxml import etree
import sys

# ========== PATH PROTO ==========
BASE_DIR = Path(__file__).resolve().parent.parent  # /app/src -> /app (no Docker)
PROTO_DIR = BASE_DIR / "proto"
if str(PROTO_DIR) not in sys.path:
    sys.path.append(str(PROTO_DIR))

import ev_pb2
import ev_pb2_grpc
from csv_to_xml_ev import convert_csv_to_xml
from schema_generator import generate_xsd_from_xml, validate_xml_with_xsd
from import_xml_to_postgres import main as load_xml_to_db


class GenericXMLServicer(ev_pb2_grpc.EVSalesServicer):
    # ========== CAMINHO XML ==========
    XML_FILE = Path("/app/data/output.xml")

    def __init__(self):
        # gera/atualiza o XML a partir do CSV
        convert_csv_to_xml()
        # gera/atualiza o XSD a partir do XML
        generate_xsd_from_xml()
        # valida XML com XSD
        self.xml_valid = validate_xml_with_xsd()
        if not self.xml_valid:
            print("[Error] XML not valid against XSD")
        else:
            print("[Info] XML is valid, loading into DB...")
            try:
                load_xml_to_db()
                print("[Info] Database db_TP2B created/updated from XML.")
            except Exception as e:
                print(f"[Error] Failed to load XML into DB: {e}")

    def GetSalesFiltered(self, request, context):
        if not self.xml_valid:
            context.set_details("XML not valid against XSD")
            context.set_code(grpc.StatusCode.INTERNAL)
            return ev_pb2.SalesReply()

        if not self.XML_FILE.exists():
            context.set_details(f"File not found: {self.XML_FILE}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return ev_pb2.SalesReply()

        try:
            tree = etree.parse(str(self.XML_FILE))
        except Exception as e:
            context.set_details(f"Error parsing XML: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return ev_pb2.SalesReply()

        root = tree.getroot()
        if len(root) == 0:
            context.set_details("XML file is empty")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return ev_pb2.SalesReply()

        first_row = root[0]
        field_names = [child.tag for child in first_row]

        filters = request.filters
        warnings = []
        valid_filters = {}

        for field, value in filters.items():
            if field not in field_names:
                warnings.append(
                    f"Field '{field}' does not exist and will be ignored"
                )
            else:
                valid_filters[field] = value

        xpath_conditions = []
        for field, value in valid_filters.items():
            values_in_xml = [
                elem.text for elem in root.xpath(f".//{first_row.tag}/{field}")
            ]
            if value not in values_in_xml:
                warnings.append(
                    f"Value '{value}' for field '{field}' not found and will be ignored"
                )
            else:
                xpath_conditions.append(f"{field}='{value}'")

        if xpath_conditions:
            xpath_query = (
                f".//{first_row.tag}[" + " and ".join(xpath_conditions) + "]"
            )
            matched_nodes = root.xpath(xpath_query)
            sales = [
                etree.tostring(sale, encoding="unicode")
                for sale in matched_nodes
            ]
        else:
            matched_nodes = []
            sales = []

        for w in warnings:
            print(f"[Warning] {w}")

        print(f"[Info] Filters: {dict(filters)} -> {len(sales)} records found")

        if matched_nodes:
            results_root = etree.Element(root.tag)  # ex.: ev_sales
            for node in matched_nodes:
                results_root.append(node)

            result_tree = etree.ElementTree(results_root)

            result_path = Path("/app/data/filtered_results.xml")

            result_tree.write(
                str(result_path),
                encoding="utf-8",
                xml_declaration=True,
                pretty_print=True,
            )
            print(f"[Info] Filtered XML saved to: {result_path}")

        return ev_pb2.SalesReply(sales_xml=sales)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ev_pb2_grpc.add_EVSalesServicer_to_server(GenericXMLServicer(), server)
    server.add_insecure_port("0.0.0.0:50051")
    server.start()
    print("gRPC server running on port 50051...")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
