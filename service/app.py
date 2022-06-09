from flask import Flask, request
import json
from service import Service

app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True

url = "http://" + IP
service_instance = Service(url)

@app.route('/restart/<node_id>/', methods=['GET'])
# API from crashed leader
def restart(node_id):
    print(node_id)
    service_instance.node_recover(node_id)
    return {}

def main():
    app.run(host=IP, port=PORT)

if __name__ == "__main__":
    main()
