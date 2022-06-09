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

url = "http://" + IP
agent_instance = Agent(url, PORT)


# APIs from client
@app.route('/read/', methods=['POST'])
def read():
    json_data = request.get_json()
    # read-set -- list of account numbers
    transaction = json_data['transaction']
    # validate for any ongoing commits
    print("Received READ: ", transaction)
    status, return_data, timestamp = agent_instance.read_transaction(transaction)
    # need to return the timestamp of the resource
    return {'read_status': status, 'data': return_data, 'timestamp': timestamp}


@app.route('/commit/', methods=['POST'])
def commit():
    json_data = request.get_json()
    # write_set = json_data['write_set']
    # read_set = json_data['read_set']
    # read_time = json_data['read_time']
    transaction = json_data['transaction']
    # commit_status = agent_instance.commit_transaction(read_set, write_set, read_time)
    commit_status = agent_instance.commit_transaction(transaction)
    print("commit_status", commit_status)
    return {'commit': commit_status}


@app.route('/commit_message/', methods=['POST'])
def commit_message():
    json_data = request.get_json()
    tx = json_data['transaction']
    status = agent_instance.log_commit_transaction(tx)
    return {'commit_status': status}


@app.route('/prepare_message/', methods=['POST'])
def prepare_message():
    json_data = request.get_json()
    tx = json_data['transaction']
    status = agent_instance.prepare_for_commit(tx)
    return {'prepare_status': status}


# APIs from External source for clock synchronize and leader selection

@app.route('/poll/', methods=['POST'])
def poll():
    return {'response': 'OK'}


@app.route('/become_leader/', methods=['POST'])
def become_leader():
    json_data = request.get_json()
    if json_data['leader']:
        agent_instance.become_leader()
    return {'response': 'OK'}


def main():
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
