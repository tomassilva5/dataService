Postgres + pgAdmin + CSV loader (quick guide)

This setup brings up a Postgres server, pgAdmin UI and a CSV loader that imports `ev_sales.csv` into table `ev_sales`.

How to run (with Docker Compose)

1. From `desafioaula3` run:

```powershell
cd "c:\Users\tomas\OneDrive - Instituto Politécnico de Viana do Castelo\Desktop\3anoEI_IPVC\IS\desafio3TP\desafioaula3"
docker-compose up --build
```

2. Once started, pgAdmin will be available at `http://localhost:8080` with credentials:
   - Email: `tomas.silva@ipvc.pt`  
   - Password: `Tominho2005##`

3. Add a server in pgAdmin using these connection details:
   - Host: `db`
   - Port: `5432`
   - Maintenance DB: `evdb`
   - Username: `postgres`
   - Password: `123456789`

4. The `csv_loader` service reads `../desafioaula2/data/ev_sales.csv` (the repo path) and inserts rows into `ev_sales` table.

Notes and troubleshooting
- The CSV loader installs `pandas` and `psycopg2-binary` at container start. On slow networks this may take a minute.
- If loader times out waiting for DB, re-run the loader service manually:

```powershell
docker-compose run --rm csv_loader
```
