FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install grpcio grpcio-tools lxml psycopg2-binary

EXPOSE 50051

CMD ["python3", "-u", "grpc_server.py"]



