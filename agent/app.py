from flask import Flask, render_template, request
import json
from agent_dummy import Agent

app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

agent_instance = Agent()

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True

@app.route('/read/', methods=['POST'])

# APIs from client
def read():
    json_data = request.get_json()
    # read-set
    account_number = json_data['acc_num']
    # validate for any ongoing commits
    account_balance = agent_instance.get_account_balance(account_number)
    # need to return the timestamp of the resource
    return {'balance': account_balance}


@app.route('/commit/', method=['POST'])
def commit():
    json_data = request.get_json()
    transaction = json_data['transaction']
    # write_set = json_data['write_set']
    # read_set = json_data[read_set]
    commit_status = agent_instance.commit_transaction(transaction)
    return {'commit': commit_status}

# APIs from External source for clock synchronize and leader selection