from flask import Flask, request
import json
from agent import Agent

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
    # read-set -- list of account numbers
    account_numbers = json_data['acc_nums']
    # validate for any ongoing commits
    account_balances = agent_instance.get_account_balances(account_numbers)
    # need to return the timestamp of the resource
    return {'balance': account_balances}


@app.route('/commit/', method=['POST'])
def commit():
    json_data = request.get_json()
    write_set = json_data['write_set']
    read_set = json_data['read_set']
    read_time = json_data['read_time']
    commit_status = agent_instance.commit_transaction(read_set, write_set, read_time)
    return {'commit': commit_status}


# APIs from External source for clock synchronize and leader selection


def main():
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
