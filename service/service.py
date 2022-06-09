import requests
import time
import json

class Service:
    def __init__(self, url):
        self.URL = url
        self.number_of_nodes = 3
        self.node_ports = [8000, 8001, 8002]
        self.client_ports = [5000, 5001, 5002]
        self.current_leader = 8000

    def polling(self):
        while True:
            api_call = self.URL + ':' + self.current_leader + '/poll/'
            r = requests.get(api_call)
            if r.status_code == 200:
                print(r)
                time.sleep(5)
            else:
                old_leader = self.current_leader
                self.current_leader = (self.current_leader+1) % self.number_of_nodes

                for node in self.node_ports:
                    if node != self.current_leader and node != old_leader:
                        # sending notification to other participants about the leader change
                        api_call = self.URL + ':' + str(node) + '/leader_changed/'
                        r = requests.post(api_call, data={'leader': self.current_leader})

                api_call = self.URL + ':' + str(self.current_leader) + '/become_leader/'
                # post request to set next leader
                r = requests.post(api_call, data={'leader': self.current_leader})
                # send a notification to clients to send transactions to new leader
                if r.status_code == 200:
                    for node in self.client_ports:
                        client_api_call = self.URL + ':' + str(node) + '/leader_changed/'
                        c = requests.post(client_api_call, data={'leader': self.current_leader})

    def node_recover(self, port):
        while True:
            print(port)
            api_call = self.URL + ':' + str(self.current_leader) + '/replication_log/'
            # get request to get latest replication log
            r = requests.get(api_call, timeout=5)
            data = r.json()['data']
            if r.status_code == 200:
                # receive the file from leader and send to recovering node
                api_call = self.URL + ':' + str(port) + '/sync_log/'
                r = requests.post(api_call, json={'data': data})
                break