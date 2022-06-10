from flask import Flask
import json
from service import Service
import threading

app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True

url = "http://" + IP
service_instance = Service(url)


# API from crashed leader
@app.route('/restart/<node_id>/', methods=['GET'])
def restart(node_id):
    print(node_id)
    service_instance.node_recover(node_id)
    return {}


def main():
    t = threading.Thread(target=service_instance.polling, args=())
    t.start()
    app.run(host=IP, port=PORT)


if __name__ == "__main__":
    main()
