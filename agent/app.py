from fileinput import filename
from flask import Flask, request, send_from_directory, abort
import json
from agent import Agent

# from client.transation import Transaction

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
    if agent_instance.up and not agent_instance.stop_receiving:
        print("Received READ: ", transaction)
        status, return_data, timestamp = agent_instance.read_transaction(transaction)
        # need to return the timestamp of the resource
        return {'read_status': status, 'data': return_data, 'timestamp': timestamp}
    else:
        return {'read_status': "503", 'data': [], 'timestamp': 0}


@app.route('/commit/', methods=['POST'])
def commit():
    json_data = request.get_json()
    transaction = json_data['transaction']
    # commit_status = agent_instance.commit_transaction(read_set, write_set, read_time)
    if agent_instance.leader and not agent_instance.stop_receiving:
        commit_status = agent_instance.commit_transaction(transaction)
        print("commit_status", commit_status)
        return {'commit': commit_status}
    else:
        return {'commit': "503"}


@app.route('/commit_message/', methods=['POST'])
def commit_message():
    if agent_instance.up:
        json_data = request.get_json()
        tx = json_data['transaction']
        status = agent_instance.log_commit_transaction(tx)
        return {'commit_status': status}
    else:
        return {'commit_status': 'YES'}


@app.route('/prepare_message/', methods=['POST'])
def prepare_message():
    if agent_instance.up:
        json_data = request.get_json()
        tx = json_data['transaction']
        status = agent_instance.prepare_for_commit(tx)
        return {'prepare_status': status}
    else:
        return {'prepare_status': "YES"}


# APIs from External source for clock synchronize and leader selection

@app.route('/poll/', methods=['POST'])
def poll():
    if agent_instance.up:
        return {'status': 200}
    else:
        return {'status': 503}


@app.route('/stop_receiving/', methods=['GET'])
def stop_receiving():
    agent_instance.stop_receiving_set()
    return {'status': 200}


@app.route('/resume_receiving/', methods=['GET'])
def resume_receiving():
    agent_instance.stop_receiving_reset()
    return {'status': 200}


@app.route('/leader_changed/', methods=['POST'])
def leader_changed():
    agent_instance.leader_changed()
    return {'response': 'OK'}


@app.route('/down_leader/', methods=['GET'])
def down_leader():
    status = agent_instance.down_leader()
    return {'status': status}


@app.route('/up_node/', methods=['GET'])
def up_node():
    status = agent_instance.up_node()
    return {'status': status}


@app.route('/become_leader/', methods=['POST'])
def become_leader():
    status = agent_instance.become_leader()
    return {'status': status}


# send the current replication log to service
@app.route('/replication_log/', methods=['GET'])
def replication_log():
    try:
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
