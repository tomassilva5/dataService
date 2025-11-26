import grpc
from concurrent import futures
import ev_pb2
import ev_pb2_grpc
from lxml import etree
from pathlib import Path

class GenericXMLServicer(ev_pb2_grpc.EVSalesServicer):
    XML_FILE = Path(__file__).parent.parent / "desafioaula2" / "data" / "output.xml"

    def GetSalesFiltered(self, request, context):
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
                warnings.append(f"Field '{field}' does not exist and will be ignored")
            else:
                valid_filters[field] = value

        xpath_conditions = []
        for field, value in valid_filters.items():
            values_in_xml = [elem.text for elem in root.xpath(f".//{first_row.tag}/{field}")]
            if value not in values_in_xml:
                warnings.append(f"Value '{value}' for field '{field}' not found and will be ignored")
            else:
                xpath_conditions.append(f"{field}='{value}'")

        if xpath_conditions:
            xpath_query = f".//{first_row.tag}[" + " and ".join(xpath_conditions) + "]"
            sales = [etree.tostring(sale, encoding="unicode") for sale in root.xpath(xpath_query)]
        else:
            sales = []

        for w in warnings:
            print(f"[Warning] {w}")

        return ev_pb2.SalesReply(sales_xml=sales)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ev_pb2_grpc.add_EVSalesServicer_to_server(GenericXMLServicer(), server)
    server.add_insecure_port('0.0.0.0:50051')
    server.start()
    print("gRPC server running on port 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
