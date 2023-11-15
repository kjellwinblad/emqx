#!/bin/bash

docker run --rm --name PostgreSQL -p 5432:5432 -e POSTGRES_PASSWORD=public -d postgres

# Wait until port 5432 is open

while ! nc -z localhost 5432; do
  sleep 0.1
done

# Create DB
# Try until DB is created (pipe error output to /dev/null)

until docker exec -it PostgreSQL bash -c "PGPASSWORD=public psql -U postgres -c \"CREATE DATABASE emqx_data;\"" > /dev/null; do
  sleep 0.1
done


#docker exec -it PostgreSQL bash -c "PGPASSWORD=public psql -U postgres -c \"CREATE DATABASE emqx_data;\""

# Create Table
docker exec -it PostgreSQL bash -c "PGPASSWORD=public psql -U postgres -d emqx_data -c \"CREATE TABLE client_events (id SERIAL PRIMARY KEY, clientid VARCHAR(255), event VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);\""


# Trap SIGINT and SIGTERM signals and stop PostgreSQL
trap "echo stopping ; docker stop PostgreSQL" SIGINT SIGTERM


# Wait indefinetely
sleep infinity
