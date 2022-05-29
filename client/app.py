from flask import Flask, render_template
import json

config_file = "config.json"
with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]

app = flask.Flask(__name__)
app.config["DEBUG"] = True


# ...

@app.route('/create/', methods=('GET', 'POST'))
def create():
    return render_template('create.html')

def main():
    
    app.run(host=IP, port=PORT)

if __name__ == "__main__":
    main()
