# Transactions-Course-Project

Middleware for PostgreSQL database replication, fail over and recovery

## Setting up
Clone this repository

### Install Docker and Build the image
Install docker as per your operating system

### Create a postgres docker image using the docker file in the project
```bash
cd <PROJECT-DIRECTORY>
cd docker-files
docker build -t postgres-with-python:v1 .
```

### Run Setup
This step creates three docker containers each containing an agent and postgresql.
The agents can be interacted at ports 8000, 8001 and 8002. The leader is the randomly 
chosen and printed to console (with port). The client is also started and can be 
interacted from "http://127.0.0.1:5000/create/". Multiple clients can also be created
by just copying the code and changing the port in client/config.json
```bash
cd ..
./setup.sh
```

### Setup Python
The project requires Python version >= 3.9. Additionally install required
dependencies using pip install -r requirements.txt after python is installed.

### Testing
In order to test the replication, we need >= 2 agents and >= 1 client. The
number of database servers should be equal to the number of agents.

We can create multiple agents by just duplicating the agent folder with different names
and changing the config.json to point to a different port. Additionally, we 
also need to change the database port and password in agent.py as per setup.sh
while connecting to the postgres instance using "psycopg2.connect"

#### Starting the components
You can start the components by going to the agent's, client's and service(supervisor)'s folder
and running "python app.py" (Make sure to run with python3). We recommend using
an IDE like PyCharm for better experience.


#### Interacting with the application
Please visit http://127.0.0.1:5000/create/ to interact with the application.
We have defined a transaction as a set of reads and writes. Agents upon starting,
create a bank_balance table with account_number and balance (both integers) and 
updated_at and created_at timestamps in nanoseconds (for debugging and concurrency control)

While writing using w(x=y), if entry `x` doesn't exists te application will create a new insert
in the table. Otherwise, `x` is updated with value `y`.

Whenever we run a Xaction with write set, it should be replicated across all servers.

By default read-only transactions are enabled in all the replicas.

In addtion to using the client, agents can be interacted with API. Note that
all the messages between the agents, clients and service(Supervisor)
is through HTTP requests/responses.

For example: A read only transaction can be done using 
```shell
curl -X POST http://127.0.0.1:8000/read/ -H 'Content-Type: application/json' -d '{"transaction": {"read_set": [1, 2], "write_set": {}}}'
```
#### Fail over testing
The current leader can be brought down using `http://127.0.0.1:8000/down_leader/`(8000 is the port of current leader)  (Just run it in a browser)
This triggers a failover and the Supervisor service chooses a new leader
in a roundrobin method. Any ongoing transactions are temporarily paused to avoid
any inconsistencies during the failover. Once the new leader is up, the client is 
informed to communicate with the new leader for future transactions.

#### Recovery testing
A failed node can be brought up using `http://127.0.0.1:8000/up_node/` (8000 is the port of failed node).
The supervisor aborts ongoing transactions and temporarily stops future
transactions to avoid inconsistencies between the recovered node and other nodes.
During recovery, the node fetches the missing transactions through the leader
recovery log. It uses them to sync it database as well as its recovery log.
Note that we keep both the recover_log.txt and the database in the same state.
Once the node is recovered, the supervisor resumes the transactions and the
recovered node is attached as a replica to the current leader.


### Protocols used
We used Optimistic concurrency control in the client, Time stamp ordering, locking for
concurrency control and a modified version of 2 Phase commit protocol for replication.
We use Write Ahead Logging for writing recovery logs which are used for node 
recovery. More details about the system design are in the Project Report.
