# Start the Docker with Postgres
docker run --name DB1 -e POSTGRES_PASSWORD=DB1 -p 5432:5432 -d postgres-with-python:v1