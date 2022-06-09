from fileinput import filename
from flask import Flask, request, send_from_directory, abort
import json
from agent import Agent
#from client.transation import Transaction

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
    if agent_instance.leader:
        print("Received READ: ", transaction)
        status, return_data, timestamp = agent_instance.read_transaction(transaction)
        # need to return the timestamp of the resource
        return {'read_status': status, 'data': return_data, 'timestamp': timestamp}
    else:
        return {'read_status': "503", 'data': [], 'timestamp': 0}


@app.route('/commit/', methods=['POST'])
def commit():
    json_data = request.get_json()
    # write_set = json_data['write_set']
    # read_set = json_data['read_set']
    # read_time = json_data['read_time']
    transaction = json_data['transaction']
    # commit_status = agent_instance.commit_transaction(read_set, write_set, read_time)
    if agent_instance.leader:
        commit_status = agent_instance.commit_transaction(transaction)
        print("commit_status", commit_status)
        return {'commit': commit_status}
    else:
        return {'commit': "503"}


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

@app.route('/leader_changed/', methods=['POST'])
def leader_changed():
    agent_instance.leader_changed()
    return {'response': 'OK'}

@app.route('/down_leader/', methods=['POST'])
def down_leader():
    agent_instance.down_leader()
    return {'response': 'OK'}

@app.route('/become_leader/', methods=['POST'])
def become_leader():
    agent_instance.become_leader()
    return {'response': 'OK'}

# send the current replication log to service
@app.route('/replication_log/', methods=['GET'])
def replication_log():
    try:
        # stop processing the requests from clients till recovery is complete
        data = ""
        with open('recovery_log.txt', 'r') as file:
            data = file.read()
        return {'data': data}
    except FileNotFoundError:
        abort(404)

# recover the node by using leader's replication log
@app.route('/sync_log/', methods=['POST'])
def sync_log():
    data = request.get_json()['data']
    f_name = "new_leader_recovery_log.txt"
    with open(f_name, 'w') as f:
        f.write(data)
    # recover using logs
    from recovery import recovery
    recovery(agent_instance)
    return {'response': 'OK'}

def main():
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
