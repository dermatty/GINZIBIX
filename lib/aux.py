import zmq


class PWDBSender():
    def __init__(self, cfg):
        self.cfg = cfg
        self.set_host_port_from_cfg()
        self.context = None
        self.socket = None

    def set_host_port_from_cfg(self):
        try:
            self.port = self.cfg["OPTIONS"]["db_port"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception:
            self.port = "37703"
        self.host = "127.0.0.1"

    def exc(self, funcstr, args0, kwargs0):
        if not self.context:
            try:
                self.context = zmq.Context()
                self.socket = self.context.socket(zmq.REQ)
                self.socket.setsockopt(zmq.LINGER, 0)
                socketurl = "tcp://" + self.host + ":" + self.port
                self.socket.connect(socketurl)
            except Exception as e:
                self.socket = None
                self.context = None
                return None
        try:
            self.socket.send_pyobj((funcstr, args0, kwargs0))
            ret0 = self.socket.recv_pyobj()
            if ret0 == "NOOK":
                return False
            return ret0
        except Exception as e:
            self.context = None
            return False
