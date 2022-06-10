from flask import Flask, render_template, request, flash
import json
from typing import Set, Dict
from client import run_transaction

app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True
app.config["SECRET_KEY"] = server_configs["secret_key"]

current_leader = 8000


def parse_reads_and_writes(content: str) -> (Set, Dict):
    if ' ' in content:
        raise Exception("Contains spaces")

    content_length = len(content)

    read_set: Set[int] = set()
    write_dict: Dict[int, int] = {}

    i = 0
    while i < content_length:
        if content[i] == 'r':
            begin = i + 2
            end = i + 2
            while content[end] != ')':
                end += 1
            read_set.add(int(content[begin:end]))
            i = end + 1
        elif content[i] == 'w':
            begin = i + 2
            end = i + 2
            while content[end] != '=':
                end += 1

            begin1 = end + 1
            end1 = end + 1
            while content[end1] != ')':
                end1 += 1

            write_dict[int(content[begin:end])] = int(content[begin1:end1])
            i = end1 + 1
            print(i)
        else:
            print(i)
            raise Exception("Invalid transactionx")

    return read_set, write_dict


@app.route('/create/', methods=('GET', 'POST'))
def create():
    if request.method == 'POST':
        content = request.form['content']
        if not content:
            flash('Transaction is required')

        try:
            read_set, write_dict = parse_reads_and_writes(content)
            run_transaction(read_set, write_dict, current_leader)
        except Exception as e:
            flash(str(e))

    return render_template('create.html')


@app.route('/', methods=['GET'])
def home():
    return "<h1> Home page of Transaction Project"


@app.route('/leader_changed/', methods=['POST'])
def leader_changed():
    leader_port = request.get_json()['leader']
    global current_leader
    current_leader = leader_port
    return {'response': 'OK'}


def main():
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
