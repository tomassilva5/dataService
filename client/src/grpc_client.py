import grpc
import sys
from pathlib import Path
from typing import Iterator
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


try:
    LOCAL_PROTO_DIR = Path(__file__).resolve().parent.parent / "proto"
    DOCKER_PROTO_DIR = Path("/app") / "proto" 
    
    CLIENT_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

    if LOCAL_PROTO_DIR.exists():
        if str(LOCAL_PROTO_DIR) not in sys.path:
            sys.path.append(str(LOCAL_PROTO_DIR))
    elif DOCKER_PROTO_DIR.exists():
        if str(DOCKER_PROTO_DIR) not in sys.path:
            sys.path.append(str(DOCKER_PROTO_DIR))
    else:
        if str(Path.cwd() / "proto") not in sys.path:
            sys.path.append(str(Path.cwd() / "proto"))

except Exception as e:
    logging.error(f"Critical error setting PATH: {e}")

try:
    import ev_pb2
    import ev_pb2_grpc
except ImportError as e:
    logging.error(f"Failed to import stubs (ev_pb2/ev_pb2_grpc). Check local compilation: {e}")


def filter_sales_logic(stub: ev_pb2_grpc.EVSalesStub):
    print("\n--- Start Sales Query (Filters) ---")
    
    filters = {}
    print("Enter filters (leave 'Field' empty to finish):")
    
    while True:
        field = input("Field: ").strip()
        if not field:
            break
        value = input(f"Value for {field}: ").strip()
        filters[field] = value

    try:
        response = stub.GetSalesFiltered(ev_pb2.SalesFilterRequest(filters=filters))
    except grpc.RpcError as e:
        print(f"gRPC Communication Failure. Code: {e.code()}")
        print(f"Details: {e.details()}")
        return

    if response.sales_xml:
        total_records = len(response.sales_xml)
        xml_content = "\n".join(response.sales_xml)
        
        CLIENT_DATA_DIR.mkdir(parents=True, exist_ok=True) 
        file_path = CLIENT_DATA_DIR / "filtered_results.xml"
        
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(xml_content)
        except Exception as e:
            print(f"Error saving file locally: {e}")

        print(f"\nTotal records: {total_records}\n")
        
        num_to_display = 3
        
        for i, sale in enumerate(response.sales_xml[:num_to_display], 1):
            print(f"--- Record {i} ---")
            print(sale)
        
        if total_records > num_to_display:
            print(f"...")
            print(f"({total_records - num_to_display} more records not shown here)")

        print(f"\nFiltered XML saved successfully.")
        print(f"File path: {file_path}") 
        
    else:
        if filters:
             print("\nNo data matched the filters.")
             print("Check the Server log for warnings on fields or values that might have been ignored.")
        else:
             print("No data received for these filters.")


def run():
    try:
        channel = grpc.insecure_channel('localhost:50051')
        stub = ev_pb2_grpc.EVSalesStub(channel)
        grpc.channel_ready_future(channel).result(timeout=5) 
    except grpc.FutureTimeoutError:
        print("\nERROR: gRPC Server is unavailable at localhost:50051.")
        print("Check if the 'grpc_server' container is running.")
        return
    except Exception as e:
        print(f"Failed to connect to the server: {e}")
        return

    while True:
        print("\n==================================")
        print("1. Start Sales Query (Filters)")
        print("2. Exit")
        print("==================================")
        
        option = input("Option: ").strip()

        if option == "1":
            filter_sales_logic(stub)
        elif option == "2":
            print("Shutting down client...")
            break
        else:
            print("Invalid option. Try again.")

if __name__ == '__main__':
    run()