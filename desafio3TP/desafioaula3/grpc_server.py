#!/usr/bin/env python3
import grpc
from concurrent import futures
import ev_pb2
import ev_pb2_grpc
from lxml import etree
from pathlib import Path

class EVSalesServicer(ev_pb2_grpc.EVSalesServicer):
    def GetSalesFiltered(self, request, context):
        print(f"[Server] Request for: region={request.country}, year={request.year}, mode={request.mode}")

        xml_file = Path("/app/data/ev_sales.xml") 
        if not xml_file.exists():
            xml_file = Path(__file__).parent / "data" / "ev_sales.xml"  
        if not xml_file.exists():
            context.set_details(f"File not found: {xml_file}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return ev_pb2.SalesReply()

        try:
            tree = etree.parse(str(xml_file))
        except Exception as e:
            context.set_details(f"Error parsing XML: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return ev_pb2.SalesReply()

        root = tree.getroot()
        print(f"Total sales in XML: {len(root.xpath('.//sale'))}")

        filter_xpath = f".//sale[region='{request.country}' and year='{str(request.year)}' and mode='{request.mode}']"
        sales = [etree.tostring(sale, encoding="unicode") for sale in root.xpath(filter_xpath)]

        print(f"[Server] Sent: {len(sales)} records")
        return ev_pb2.SalesReply(sales_xml=sales)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ev_pb2_grpc.add_EVSalesServicer_to_server(EVSalesServicer(), server)
    
    server.add_insecure_port('0.0.0.0:50051')
    
    server.start()
    print("gRPC server running on port 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
