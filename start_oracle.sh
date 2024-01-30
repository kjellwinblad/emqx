#!/bin/bash

# Function to clean up Docker containers on exit
cleanup() {
    echo "Stopping and removing the Oracle DB and Toxiproxy containers..."
    docker stop oracledb toxiproxy
    docker rm oracledb toxiproxy
    exit 0
}

# Trap CTRL-C (SIGINT) and call the cleanup function
trap cleanup SIGINT

# Start Oracle Database docker image locally
docker run --name oracledb -p 1521:1521 -d oracleinanutshell/oracle-xe-11g:1.0.0

# Start Toxiproxy
docker run --name toxiproxy -p 8474:8474 -p 1522:1522 -d shopify/toxiproxy

# Wait for the database and Toxiproxy to start up
echo "Waiting for the Oracle DB and Toxiproxy to start up..."
sleep 10 # Adjust the sleep time if necessary

# Configure Toxiproxy to create a proxy for Oracle server
echo "Configuring Toxiproxy..."
curl -X POST http://localhost:8474/proxies -d '{
    "name": "oracle",
    "listen": "0.0.0.0:1522",
    "upstream": "oracledb:1521",
    "enabled": true
}'

# SQL script to create the table
SQL_SCRIPT="
CREATE TABLE t_mqtt_msgs (
  msgid VARCHAR2(64),
  sender VARCHAR2(64),
  topic VARCHAR2(255),
  qos NUMBER(1),
  retain NUMBER(1),
  payload NCLOB,
  arrived TIMESTAMP
);
exit;
"

# Save SQL script to a file
echo "$SQL_SCRIPT" > create_table.sql

# Copy SQL script into the Docker container
docker cp create_table.sql oracledb:/create_table.sql

# Connect to the database and execute the script
echo "Creating the table t_mqtt_msgs in the Oracle DB..."
docker exec -it oracledb bash -c '
    ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe/
    echo "ORACLE_HOME=$ORACLE_HOME"
    export ORACLE_HOME
    export ORACLE_SID=XE
    export PATH=$PATH:$ORACLE_HOME/bin
    sqlplus system/oracle@//localhost:1521/XE @/create_table.sql
'

# Remove the SQL script file
rm create_table.sql

# Keep querying the table and display its contents
echo "Oracle DB is running through Toxiproxy. Press CTRL-C to stop and remove the containers."
while true; do
    echo "Querying the t_mqtt_msgs table..."

    # docker exec -it oracledb bash -c '
    #     ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe/
    #     export ORACLE_HOME
    #     export PATH=$PATH:$ORACLE_HOME/bin
    #     echo "
    #     SET PAGESIZE 50
    #     SELECT * FROM t_mqtt_msgs;
    #     exit;
    #     " | sqlplus -S system/oracle@//localhost:1521/XE
    # '

    # Wait for a bit before the next query
    sleep 10 # Adjust this for how often you want to refresh the data
done
