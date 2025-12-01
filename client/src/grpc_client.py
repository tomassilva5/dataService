import grpc
import sys
from pathlib import Path

# Define paths para importar os módulos gerados pelo protoc
BASE_DIR = Path(__file__).resolve().parent.parent.parent  
PROTO_DIR = BASE_DIR / "client" / "proto"
if str(PROTO_DIR) not in sys.path:
    sys.path.append(str(PROTO_DIR))

import ev_pb2
import ev_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = ev_pb2_grpc.EVSalesStub(channel)

    filters = {}
    print("Enter filters (leave field empty to finish):")
    while True:
        field = input("Field: ").strip()
        if not field:
            break
        value = input(f"Value for {field}: ").strip()
        filters[field] = value

    response = stub.GetSalesFiltered(ev_pb2.SalesFilterRequest(filters=filters))

    if response.sales_xml:
        xml_content = "\n".join(response.sales_xml)
        file_path = BASE_DIR / "server" / "data" / "filtered_results.xml"
        file_path.parent.mkdir(parents=True, exist_ok=True)  
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        print(f"\nTotal records: {len(response.sales_xml)}\n")
        for i, sale in enumerate(response.sales_xml, 1):
            print(f"--- Record {i} ---")
            print(sale)
            print()
        print(f"Filtered XML saved to: server/data/filtered_results.xml")
    else:
        print("No data received for these filters.")

if __name__ == '__main__':
    run()
