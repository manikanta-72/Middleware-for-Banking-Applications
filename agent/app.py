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
def read():
    json_data = request.get_json()
    account_number = json_data['acc_num']
    account_balance = agent_instance.get_account_balance(account_number)
    return {'balance': account_balance}


@app.route('/commit/', method=['POST'])
def commit():
    json_data = request.get_json()
    transaction_id = json_data['transaction_id']
    write_set = json_data['write_set']
    read_set = json_data[read_set]
    commit_status = agent_instance.commit_transaction(transaction_id, write_set, read_set)
    return {'commit': commit_status}