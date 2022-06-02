# Transactions-Course-Project

A client makes a connection to the leader to send transactions.

# Setting up
Clone this repository

## Install Docker and Build the image
Install docker as per your operating system

```bash
cd docker-files
docker build -t postgres-with-python:v1 .
```

## Run Setup
This step creates three docker containers each containing an agent and postgresql.
The agents can be interacted at ports 8000, 8001 and 8002. The leader is the randomly 
chosen and printed to console (with port). The client is also started and can be 
interacted from "http://127.0.0.1:5000/create/"
```bash
cd ..
./setup.sh
```

## Failing an DB
Set a POST request at http://127.0.0.1:<leader_port>/stop/ to fail the agent. 
A random leader is chosen 

Completed
1. Postgres created on 3 different docker containers using a single docker image

TODO
1. Agent
    a. Server
    b. SQL queries for interactions with db 
    c. validation protocol
    d. replication log