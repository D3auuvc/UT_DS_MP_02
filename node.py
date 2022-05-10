from threading import Thread
from time import sleep

import rpyc
from rpyc.utils.factory import DiscoveryError


class Node(Thread):
    def __init__(self, id: int, ip: str, port: int, primary: int):
        Thread.__init__(self)
        self.id: int = id
        self.ip: int = ip
        self.port: int = port
        self.primary: int = primary

        self.STATE: int = 0
        self.ACTION: int = 0

        self.rpc_nodes: list = []
        self.info_exchange_lst: list = []

    def run(self):
        while True:
            try:
                self.rpc_nodes = rpyc.discover("RPC")
                sleep(2)
            except DiscoveryError:
                pass
