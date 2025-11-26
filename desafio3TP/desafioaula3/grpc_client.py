import grpc
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
        print(f"\nTotal records: {len(response.sales_xml)}\n")
        for i, sale in enumerate(response.sales_xml, 1):
            print(f"--- Record {i} ---")
            print(sale)
            print()
    else:
        print("No data received for these filters.")

if __name__ == '__main__':
    run()
