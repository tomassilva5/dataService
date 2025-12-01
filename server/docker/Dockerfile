FROM python:3.10

# diretório de trabalho dentro do container
WORKDIR /app

# copiar código e dados (a partir da pasta server)
COPY ../src /app/src
COPY ../data /app/data
COPY ../proto /app/proto

# instalar dependências
RUN pip install grpcio grpcio-tools lxml psycopg2-binary pandas xmlschema xmltoxsd

# porta do servidor gRPC
EXPOSE 50051

# comando para arrancar o servidor
CMD ["python3", "-u", "src/grpc_server.py"]
