from flask import Flask, request
import json
from agent import Agent
from client.transation import Transaction

app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True

url = "https://" + IP 
agent_instance = Agent(url, PORT)

@app.route('/read/', methods=['POST'])
# APIs from client
def read():
    json_data = request.get_json()
    # read-set -- list of account numbers
    transaction = json_data['transaction']
    # validate for any ongoing commits
    status, return_data = agent_instance.read_transaction(transaction)
    # need to return the timestamp of the resource
    return {'read_status': status, 'data': return_data}


@app.route('/commit/', method=['POST'])
def commit():
    json_data = request.get_json()
    # write_set = json_data['write_set']
    # read_set = json_data['read_set']
    # read_time = json_data['read_time']
    transaction = json_data['transaction']
    # commit_status = agent_instance.commit_transaction(read_set, write_set, read_time)
    commit_status = agent_instance.commit_transaction(transaction)
    return {'commit': commit_status}

@app.route('/commit_message/', method=['POST'])
def commit_message():
    json_data = request.get_json()
    write_set = json_data['write-set']
    status = agent_instance.log_commit_transaction(write_set)
    return {'commit_status' : status}


@app.route('/prepare_message/', method=['POST'])
def prepare_message():
    json_data = request.get_json()
    write_set = json_data['write_set']
    status = agent_instance.prepare_for_commit(write_set)
    return {'prepare_status' : status}

# APIs from External source for clock synchronize and leader selection

@app.route('/poll/', method=['POST'])
def poll():
    return {'response':'OK'}

@app.route('/become_leader/', method=['POST'])
def become_leader():
    json_data = request.get_json()
    if json_data['leader']:
        agent_instance.become_leader()
    return {'response':'OK'}

def main():
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
