import grpc
from concurrent import futures
from pathlib import Path
from lxml import etree
import sys
import logging
from typing import Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).resolve().parent.parent 
PROTO_DIR = BASE_DIR / "proto"

DOCKER_PROTO_DIR = Path("/app") / "proto" 
if str(DOCKER_PROTO_DIR) not in sys.path:
    sys.path.append(str(DOCKER_PROTO_DIR))

import ev_pb2
import ev_pb2_grpc
from csv_to_xml_ev import convert_csv_to_xml 
from schema_generator import generate_xsd_from_xml, validate_xml_with_xsd
from import_xml_to_postgres import main as load_xml_to_db


class GenericXMLServicer(ev_pb2_grpc.EVSalesServicer):
    XML_FILE = Path("/app/data/output.xml")
    last_table_name = "default_table"
    xml_valid = False
    
    def __init__(self):
        self.run_etl_pipeline(Path("/app/data/test.csv"), "test_data_default")


    def run_etl_pipeline(self, csv_path: Path, table_name: str):
        self.xml_valid = False
        
        try:
            column_names = convert_csv_to_xml(csv_path)
            logging.info(f"XML created from {csv_path.name}. Columns: {column_names}")
            
            generate_xsd_from_xml() 
            self.xml_valid = validate_xml_with_xsd()
            
            if not self.xml_valid:
                logging.error("[Error] XML not valid against XSD. DB load aborted.")
                return

            logging.info(f"[Info] XML is valid, loading into DB '{table_name}'...")
            
            load_xml_to_db(column_names, table_name) 
            
            self.last_table_name = table_name
            logging.info(f"[Info] Database table '{table_name}' created/updated successfully.")

        except FileNotFoundError:
             logging.warning(f"[Warning] Initialization skipped: File not found at {csv_path}")
        except Exception as e:
            logging.error(f"[Error] Failed during ETL pipeline: {e}")
            self.xml_valid = False


    def GetSalesFiltered(self, request, context):
        if not self.xml_valid:
            context.set_details("XML not valid against XSD or DB not loaded.")
            context.set_code(grpc.StatusCode.INTERNAL)
            return ev_pb2.SalesReply()

        if not self.XML_FILE.exists():
            context.set_details(f"File not found: {self.XML_FILE}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return ev_pb2.SalesReply()

        try:
            tree = etree.parse(str(self.XML_FILE))
            root = tree.getroot()
        except Exception as e:
            context.set_details(f"Error parsing XML: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return ev_pb2.SalesReply()

        first_row_tag = "row" 
        
        xpath_conditions = []
        warnings = [] 
        
        if root.find(first_row_tag) is not None:
            valid_field_names = [child.tag for child in root.find(first_row_tag)]
        else:
             valid_field_names = [] 

        for field, value in request.filters.items():
            safe_field = field.replace(' ', '_') 
            safe_value = value.replace("'", "") 
            
            if safe_field not in valid_field_names:
                 warnings.append(f"Field '{field}' does not exist and was ignored.")
                 continue

            xpath_check = root.xpath(f"./{first_row_tag}/{safe_field}[.='{safe_value}']")
            if not xpath_check:
                 warnings.append(f"Value '{value}' for field '{field}' not found and was ignored.")
                 continue
            
            xpath_conditions.append(f"{safe_field}='{safe_value}'")

        if xpath_conditions:
            xpath_query = f"./{first_row_tag}[" + " and ".join(xpath_conditions) + "]"
            matched_nodes = root.xpath(xpath_query)
            sales = [
                etree.tostring(sale, encoding="unicode")
                for sale in matched_nodes
            ]
        else:
            matched_nodes = []
            sales = []

        for w in warnings:
            logging.warning(f"[Query Warning] {w}")

        logging.info(f"[Info] Filters: {dict(request.filters)} -> {len(sales)} records found")

        if matched_nodes:
             logging.info(f"[Info] Returning {len(matched_nodes)} filtered records.")

        return ev_pb2.SalesReply(sales_xml=sales)


    def UploadDataset(self, request_iterator: Iterator[ev_pb2.UploadRequest], context) -> ev_pb2.UploadStatus:
        data = bytearray()
        filename = "uploaded_temp.csv"
        
        for request in request_iterator:
            if request.HasField("info"):
                filename = request.info.filename
            elif request.HasField("chunk_data"):
                data.extend(request.chunk_data)

        logging.info(f"File {filename} received. Total size: {len(data)} bytes.")

        temp_csv_path = Path("/app/data/") / filename
        table_name = filename.replace('.', '_').replace('-', '_').lower() 
        
        try:
            with open(temp_csv_path, "wb") as f:
                f.write(data)
            
            self.run_etl_pipeline(csv_path=temp_csv_path, table_name=table_name)

            if self.xml_valid:
                return ev_pb2.UploadStatus(success=True, message=f"Dataset '{filename}' processed. DB table: {table_name}.")
            else:
                return ev_pb2.UploadStatus(success=False, message="Upload successful but validation/DB load failed. Check server logs.")

        except Exception as e:
            logging.error(f"Upload or processing failed for {filename}: {e}")
            return ev_pb2.UploadStatus(success=False, message=f"Upload failed on server: {str(e)}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ev_pb2_grpc.add_EVSalesServicer_to_server(GenericXMLServicer(), server)
    server.add_insecure_port("0.0.0.0:50051")
    server.start()
    logging.info("gRPC server running on port 50051...")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()