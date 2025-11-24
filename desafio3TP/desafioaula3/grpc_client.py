import grpc
import ev_pb2
import ev_pb2_grpc
from lxml import etree
from pathlib import Path  

def save_sales_xml(sales, filename="filtered_sales.xml"):
    data_path = Path(__file__).parent / "data" / filename
    root = etree.Element("sales")
    for sale_str in sales:
        root.append(etree.fromstring(sale_str))
    tree = etree.ElementTree(root)
    tree.write(str(data_path), pretty_print=True, xml_declaration=True, encoding="UTF-8")
    print(f"Results exported to {data_path}")

def run():
    try:
        channel = grpc.insecure_channel('localhost:50051')
        stub = ev_pb2_grpc.EVSalesStub(channel)

        country = input("Enter country: ").strip()
        year_input = input("Enter year: ").strip()
        mode = input("Enter mode: ").strip()

        try:
            year = int(year_input)
        except ValueError:
            print("Year must be a number. Exiting...")
            return

        print(f"Requesting sales for country={country}, year={year}, mode={mode} ...")
        response = stub.GetSalesFiltered(
            ev_pb2.SalesFilterRequest(country=country, year=year, mode=mode)
        )

        if response.sales_xml:
            print(f"\nTotal records: {len(response.sales_xml)}\n")
            for i, sale in enumerate(response.sales_xml, 1):
                print(f"--- Record {i} ---")
                print(sale)
                print()
            save_sales_xml(response.sales_xml)
        else:
            print("No data received")
    except grpc.RpcError as e:
        print(f"gRPC Error: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    run()


