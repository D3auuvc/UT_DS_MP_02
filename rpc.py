import socket
import threading
from collections import Counter
from threading import Thread

import numpy as np
import rpyc
from rpyc.utils.server import ThreadedServer

from constants import _ACTION_KEY, _STATE_VALUE
from node import Node


class RPCService(rpyc.Service):
    def __init__(self, id: int, ip: str, port: int, primary: int):
        self.n = Node(id, ip, port, primary)
        self.n.start()

    # Primary send order to secondary
    def exposed_send_order_from_primary(self, action_key):
        self.n.ACTION = action_key

        if _STATE_VALUE[self.n.STATE] == "NF":
            for node in self.n.rpc_nodes:
                node_ip, node_port = node
                if self.n.port != node_port:
                    conn = rpyc.connect(node_ip, node_port)
                    conn.root.set_action(action_key)
                    conn.close()
        elif _STATE_VALUE[self.n.STATE] == "F":
            for node in self.n.rpc_nodes:
                fake_action_key = np.random.randint(1, 3)
                node_ip, node_port = node
                if self.n.port != node_port:
                    conn = rpyc.connect(node_ip, node_port)
                    conn.root.set_action(fake_action_key)
                    conn.close()

    # Secondary set action
    def exposed_set_action(self, action_key):
        self.n.ACTION = action_key

    # Secondary exchange action info to another secondaries
    def exposed_share_action_info(self):
        if _STATE_VALUE[self.n.STATE] == "NF":
            for node in self.n.rpc_nodes:
                node_ip, node_port = node
                if (node_port != self.n.primary) and (node_port != self.n.port):
                    conn = rpyc.connect(node_ip, node_port)
                    conn.root.collect_action(self.n.ACTION)
                    conn.close()
                elif node_port == self.n.port:
                    self.n.info_exchange_lst.append(self.n.ACTION)
        elif _STATE_VALUE[self.n.STATE] == "F":
            for node in self.n.rpc_nodes:
                node_ip, node_port = node
                if (node_port != self.n.primary) and (node_port != self.n.port):
                    fake_action_key = np.random.randint(1, 3)
                    conn = rpyc.connect(node_ip, node_port)
                    conn.root.collect_action(fake_action_key)
                    conn.close()
                elif node_port == self.n.port:
                    self.n.info_exchange_lst.append(self.n.ACTION)

    # Secondary collects action info from another secondaries
    def exposed_collect_action(self, action_key):
        self.n.info_exchange_lst.append(action_key)

    # Clean votes collection
    def exposed_clean_votes_collection(self):
        self.n.info_exchange_lst = []

    # Secondary votes the final action
    def exposed_vote_final_action(self):
        action_lst = self.n.info_exchange_lst
        action_lst.append(self.n.ACTION)
        vote_count = Counter(action_lst)
        if vote_count[_ACTION_KEY["attack"]] > int((len(self.n.rpc_nodes)-1)/2):
            self.n.ACTION = 1
        elif vote_count[_ACTION_KEY["retreat"]] > int((len(self.n.rpc_nodes)-1)/2):
            self.n.ACTION = 2
        else:
            self.n.ACTION = 0

    # Set primary port info
    def exposed_set_primary(self, primary_port):
        self.n.primary = primary_port

    # Get primary port info
    def exposed_get_primary(self):
        return self.n.primary

    # Send the detail of nodes
    def exposed_get_detail(self):
        return [self.n.id, self.n.ACTION, self.n.STATE, self.n.port, self.n.primary]

    def exposed_set_state(self, state):
        self.n.STATE = state

    # Kill target node
    def exposed_kill(self):
        pool = threading.enumerate()
        for t in pool:
            if(type(t._target) != type(None)):
                obj = t._target.__self__
                if(isinstance(obj, ThreadedServer)):
                    if(obj.port == self.n.port):
                        obj.close()
                        break

    # Add new node
    def exposed_add_node(self, k):
        max_id = 0
        max_port = 0
        primary_port = 0
        for node in self.n.rpc_nodes:
            node_ip, node_port = node
            conn = rpyc.connect(node_ip, node_port)
            result = conn.root.get_detail()

            max_id = result[0] if(result[0] > max_id) else max_id
            max_port = result[3] if(result[3] > max_port) else max_port
            primary_port = result[4]

        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)

        for i in range(1, k+1):
            id = max_id+i
            port = max_port+i
            service = RPCService(id=id, ip=ip,
                                 port=port, primary=primary_port)
            server = ThreadedServer(service, port=port, auto_register=True)
            t = Thread(target=server.start, name=str(id))
            t.daemon = True
            t.start()
