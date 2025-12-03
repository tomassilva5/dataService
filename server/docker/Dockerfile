FROM python:3.10

WORKDIR /app

COPY ../src /app/src
COPY ../data /app/data
COPY ../proto /app/proto

RUN pip install grpcio grpcio-tools lxml psycopg2-binary pandas xmlschema xmltoxsd

RUN python -m grpc_tools.protoc -I/app/proto --python_out=/app/proto --grpc_python_out=/app/proto /app/proto/ev.proto

EXPOSE 50051

CMD ["python3", "-u", "src/grpc_server.py"]